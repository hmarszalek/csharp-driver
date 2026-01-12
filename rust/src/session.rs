use std::convert::Infallible;

use scylla::client::session::Session;
use scylla::client::session_builder::SessionBuilder;
use scylla::errors::{NewSessionError, PagerExecutionError, PrepareError};
use scylla_cql::serialize::row::SerializedValues;
use tokio::sync::RwLock;

use crate::CSharpStr;
use crate::error_conversion::MaybeShutdownError;
use crate::ffi::{
    ArcFFI, BoxFFI, BridgedBorrowedSharedPtr, BridgedOwnedExclusivePtr, BridgedOwnedSharedPtr, FFI,
    FFIStr, FromArc,
};
use crate::pre_serialized_values::pre_serialized_values::PreSerializedValues;
use crate::prepared_statement::BridgedPreparedStatement;
use crate::row_set::RowSet;
use crate::task::{BridgedFuture, Tcb};

/// Internal representation of a session bridged to C#.
/// It contains optional connected session state to allow for shutdown.
/// If None, the session has been shut down and cannot be used for queries.
#[derive(Debug)]
pub(crate) struct BridgedSessionInner {
    session: Option<Session>,
}

/// BridgedSession is a thread-safe, asynchronously accessible session wrapper.
/// It uses RwLock to allow multiple concurrent read accesses (queries)
/// while ensuring exclusive access for write operations (shutdown).
pub type BridgedSession = tokio::sync::RwLock<BridgedSessionInner>;
impl FFI for BridgedSession {
    type Origin = FromArc;
}

/// BridgedFuture currently needs to return some result that implements ArcFFI.
/// For operations that don't need to return any data we use EmptyBridgedResult.
/// The user must call empty_bridged_result_free after using such functions.
#[derive(Debug)]
pub struct EmptyBridgedResult;
impl FFI for EmptyBridgedResult {
    type Origin = FromArc;
}

#[unsafe(no_mangle)]
pub extern "C" fn empty_bridged_result_free(ptr: BridgedOwnedSharedPtr<EmptyBridgedResult>) {
    ArcFFI::free(ptr);
    tracing::trace!("[FFI] EmptyBridgedResult freed");
}

#[unsafe(no_mangle)]
pub extern "C" fn session_create(tcb: Tcb, uri: CSharpStr<'_>) {
    // Convert the raw C string to a Rust string
    let uri = uri.as_cstr().unwrap().to_str().unwrap();
    let uri = uri.to_owned();

    BridgedFuture::spawn::<_, _, NewSessionError>(tcb, async move {
        tracing::debug!("[FFI] Create Session... {}", uri);
        let session = SessionBuilder::new().known_node(&uri).build().await?;
        tracing::info!("[FFI] Session created! URI: {}", uri);
        tracing::trace!(
            "[FFI] Contacted node's address: {}",
            session.get_cluster_state().get_nodes_info()[0].address
        );
        Ok(RwLock::new(BridgedSessionInner {
            session: Some(session),
        }))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn session_free(session_ptr: BridgedOwnedSharedPtr<BridgedSession>) {
    ArcFFI::free(session_ptr);
    tracing::debug!("[FFI] Session freed");
}

/// Shuts down the session by acquiring a write lock and clearing the connected state.
/// This blocks all future queries. Once shutdown, the session cannot be used for queries anymore.
#[unsafe(no_mangle)]
pub extern "C" fn session_shutdown(
    tcb: Tcb,
    session_ptr: BridgedBorrowedSharedPtr<'_, BridgedSession>,
) {
    // Session pointer being null or invalid implies a serious error on the C# side.
    // We unwrap here to catch such issues early and panic.
    let session_arc = ArcFFI::cloned_from_ptr(session_ptr).unwrap();

    tracing::trace!("[FFI] Scheduling session shutdown");

    BridgedFuture::spawn::<_, _, Infallible>(tcb, async move {
        tracing::debug!("[FFI] Shutting down session");

        // Acquire write lock - this will pause the asynchronous execution until all read locks (queries)
        // are released and then clear the connected state - no more queries can proceed after this.
        let mut session_guard = session_arc.write().await;

        if session_guard.session.is_none() {
            panic!("Session is already shut down");
        }

        session_guard.session = None;
        tracing::info!("[FFI] Session shutdown complete");

        // Return an EmptyBridgedResult to satisfy the return type
        Ok(EmptyBridgedResult)
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn session_prepare(
    tcb: Tcb,
    session_ptr: BridgedBorrowedSharedPtr<'_, BridgedSession>,
    statement: CSharpStr<'_>,
) {
    // Convert the raw C string to a Rust string.
    let statement = statement.as_cstr().unwrap().to_str().unwrap().to_owned();
    let session_arc = ArcFFI::cloned_from_ptr(session_ptr).unwrap();

    tracing::trace!(
        "[FFI] Scheduling statement for preparation: \"{}\"",
        statement
    );

    // Try to acquire an owned read lock.
    // If the operation fails, treat it as session shutting down.
    let session_guard_res = session_arc.try_read_owned();

    BridgedFuture::spawn::<_, _, MaybeShutdownError<PrepareError>>(tcb, async move {
        tracing::debug!("[FFI] Preparing statement \"{}\"", statement);

        let Ok(session_guard) = session_guard_res else {
            // Session is currently shutting down - exit with appropriate error.
            return Err(MaybeShutdownError::AlreadyShutdown);
        };

        // Check if session is connected or if it has been shut down.
        // If it has been shut down, return appropriate error.
        let Some(session) = session_guard.session.as_ref() else {
            return Err(MaybeShutdownError::AlreadyShutdown);
        };

        // Lock is held for the entire duration of the prepare operation,
        // preventing shutdown until this future completes
        // Map underlying `PrepareError` into `MaybeShutdownError::Inner` so
        // the BridgedFuture's error type matches.
        let ps = session
            .prepare(statement)
            .await
            .map_err(MaybeShutdownError::Inner)?;

        tracing::trace!("[FFI] Statement prepared");

        Ok(BridgedPreparedStatement { inner: ps })
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn session_query(
    tcb: Tcb,
    session_ptr: BridgedBorrowedSharedPtr<'_, BridgedSession>,
    statement: CSharpStr<'_>,
) {
    // Convert the raw C string to a Rust string.
    let statement = statement.as_cstr().unwrap().to_str().unwrap().to_owned();
    let session_arc = ArcFFI::cloned_from_ptr(session_ptr).unwrap();
    //TODO: use safe error propagation mechanism

    tracing::trace!(
        "[FFI] Scheduling statement for execution: \"{}\"",
        statement
    );

    // Try to acquire an owned read lock.
    // If the operation fails, treat it as session shutting down.
    let session_guard_res = session_arc.try_read_owned();

    BridgedFuture::spawn::<_, _, MaybeShutdownError<PagerExecutionError>>(tcb, async move {
        tracing::debug!("[FFI] Executing statement \"{}\"", statement);

        let Ok(session_guard) = session_guard_res else {
            // Session is currently shutting down - exit with appropriate error.
            return Err(MaybeShutdownError::AlreadyShutdown);
        };

        // Check if session is connected or if it has been shut down.
        // If it has been shut down, return appropriate error.
        let Some(session) = session_guard.session.as_ref() else {
            return Err(MaybeShutdownError::AlreadyShutdown);
        };

        // Lock is held for the entire duration of the query operation,
        // preventing shutdown until this future completes
        // Map underlying `PagerExecutionError` into `MaybeShutdownError::Inner` so
        // the BridgedFuture's error type matches.
        let query_pager = session
            .query_iter(statement, ())
            .await
            .map_err(MaybeShutdownError::Inner)?;

        tracing::trace!("[FFI] Statement executed");

        Ok(RowSet {
            pager: std::sync::Mutex::new(Some(query_pager)),
        })
    });
}

#[unsafe(no_mangle)]
pub extern "C" fn session_query_with_values(
    tcb: Tcb,
    session_ptr: BridgedBorrowedSharedPtr<'_, BridgedSession>,
    statement: CSharpStr<'_>,
    values_ptr: BridgedOwnedExclusivePtr<PreSerializedValues>,
) {
    // Take ownership of the pre-serialized values box so we can move it into the async task.
    // Important: the order of operations here matters. We need to ensure we take ownership of the box first. In case any further operations panic,
    // we don't want to leak the pointer.
    // Note: this transfers ownership, so the C# side must not free it!
    let values_box = BoxFFI::from_ptr(values_ptr).expect("non-null PreSerializedValues pointer");

    // Convert the raw C string to a Rust string.
    let statement = statement.as_cstr().unwrap().to_str().unwrap().to_owned();
    let session_arc = ArcFFI::cloned_from_ptr(session_ptr).unwrap();
    //TODO: use safe error propagation mechanism

    // Try to acquire an owned read lock.
    // If the operation fails, treat it as session shutting down.
    let session_guard_res = session_arc.try_read_owned();

    BridgedFuture::spawn::<_, _, MaybeShutdownError<PagerExecutionError>>(tcb, async move {
        tracing::debug!(
            "[FFI] Preparing and executing statement with pre-serialized values \"{}\"",
            statement
        );

        let Ok(session_guard) = session_guard_res else {
            // Session is currently shutting down - exit with appropriate error.
            return Err(MaybeShutdownError::AlreadyShutdown);
        };

        // Check if session is connected or if it has been shut down.
        // If it has been shut down, return appropriate error.
        let Some(session) = session_guard.session.as_ref() else {
            return Err(MaybeShutdownError::AlreadyShutdown);
        };

        // First, prepare the statement. Map PrepareError into PagerExecutionError::PrepareError
        // and then into MaybeShutdownError::Inner so the error type matches.
        let prepared = session
            .prepare(statement)
            .await
            .map_err(|e| MaybeShutdownError::Inner(PagerExecutionError::PrepareError(e)))?;

        // Convert our FFI wrapper into SerializedValues by consuming it.
        let serialized_values: SerializedValues = values_box.into_serialized_values();

        // Now execute using the internal execute_iter_preserialized helper.
        // Map to appropriate error type.
        let query_pager = session
            .execute_iter_preserialized(prepared, serialized_values)
            .await
            .map_err(MaybeShutdownError::Inner)?;

        tracing::trace!("[FFI] Prepared statement executed with pre-serialized values");

        Ok(RowSet {
            pager: std::sync::Mutex::new(Some(query_pager)),
        })
    });
}

#[unsafe(no_mangle)]
pub extern "C" fn session_query_bound(
    tcb: Tcb,
    session_ptr: BridgedBorrowedSharedPtr<'_, BridgedSession>,
    prepared_statement_ptr: BridgedBorrowedSharedPtr<'_, BridgedPreparedStatement>,
) {
    let bridged_prepared = ArcFFI::cloned_from_ptr(prepared_statement_ptr).unwrap();
    let session_arc = ArcFFI::cloned_from_ptr(session_ptr).unwrap();

    tracing::trace!("[FFI] Scheduling prepared statement execution");

    // Try to acquire an owned read lock.
    // If the operation fails, treat it as session shutting down.
    let session_guard_res = session_arc.try_read_owned();

    BridgedFuture::spawn::<_, _, MaybeShutdownError<PagerExecutionError>>(tcb, async move {
        tracing::debug!("[FFI] Executing prepared statement");

        let Ok(session_guard) = session_guard_res else {
            // Session is currently shutting down - exit with appropriate error.
            return Err(MaybeShutdownError::AlreadyShutdown);
        };

        // Check if session is connected or if it has been shut down.
        // If it has been shut down, return appropriate error.
        let Some(session) = session_guard.session.as_ref() else {
            return Err(MaybeShutdownError::AlreadyShutdown);
        };

        // Lock is held for the entire duration of the query operation,
        // preventing shutdown until this future completes
        // Map underlying `PagerExecutionError` into `MaybeShutdownError::Inner` so
        // the BridgedFuture's error type matches.
        let query_pager = session
            .execute_iter(bridged_prepared.inner.clone(), ())
            .await
            .map_err(MaybeShutdownError::Inner)?;

        tracing::trace!("[FFI] Prepared statement executed");

        Ok(RowSet {
            pager: std::sync::Mutex::new(Some(query_pager)),
        })
    })
}

/// Callback type for session_get_keyspace.
/// The callback receives an FFIStr with the keyspace name.
type KeyspaceCallback = extern "C" fn(FFIStr<'_>);

/// Returns the current keyspace by calling the provided callback with an FFIStr.
/// The callback is invoked while the session lock is held, ensuring the string data
/// remains valid for the duration of the callback.
#[unsafe(no_mangle)]
pub extern "C" fn session_get_keyspace(
    session_ptr: BridgedBorrowedSharedPtr<'_, BridgedSession>,
    callback: KeyspaceCallback,
) {
    let session_arc = ArcFFI::cloned_from_ptr(session_ptr).unwrap();

    // Try to acquire a read lock synchronously.
    let Ok(session_guard) = session_arc.try_read() else {
        // Session is currently shutting down.
        callback(FFIStr::new(""));
        return;
    };

    // Check if session is connected or if it has been shut down.
    let Some(session) = session_guard.session.as_ref() else {
        callback(FFIStr::new(""));
        return;
    };

    // Get the current keyspace from the session.
    let Some(keyspace_arc) = session.get_keyspace() else {
        callback(FFIStr::new(""));
        return;
    };

    // Call the callback with the borrowed string while the lock is held.
    callback(FFIStr::new(keyspace_arc.as_ref()));
}
