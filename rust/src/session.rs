use scylla::client::session::Session;
use scylla::client::session_builder::SessionBuilder;
use scylla::errors::{NewSessionError, PagerExecutionError, PrepareError};

use crate::CSharpStr;
use crate::ffi::{
    ArcFFI, BoxFFI, BridgedBorrowedSharedPtr, BridgedOwnedExclusivePtr, BridgedOwnedSharedPtr, FFI,
    FromArc,
};
use crate::pre_serialized_values::PreSerializedValues;
use crate::prepared_statement::BridgedPreparedStatement;
use crate::row_set::RowSet;
use crate::task::{BridgedFuture, Tcb};

impl FFI for BridgedSession {
    type Origin = FromArc;
}

#[derive(Debug)]
pub struct BridgedSession {
    inner: Session,
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
        Ok(BridgedSession { inner: session })
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn session_free(session_ptr: BridgedOwnedSharedPtr<BridgedSession>) {
    ArcFFI::free(session_ptr);
    tracing::debug!("[FFI] Session freed");
}

#[unsafe(no_mangle)]
pub extern "C" fn session_prepare(
    tcb: Tcb,
    session_ptr: BridgedBorrowedSharedPtr<'_, BridgedSession>,
    statement: CSharpStr<'_>,
) {
    // Convert the raw C string to a Rust string.
    let statement = statement.as_cstr().unwrap().to_str().unwrap().to_owned();
    let bridged_session = ArcFFI::cloned_from_ptr(session_ptr).unwrap();

    tracing::trace!(
        "[FFI] Scheduling statement for preparation: \"{}\"",
        statement
    );

    BridgedFuture::spawn::<_, _, PrepareError>(tcb, async move {
        tracing::debug!("[FFI] Preparing statement \"{}\"", statement);
        let ps = bridged_session.inner.prepare(statement).await?;
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
    let bridged_session = ArcFFI::cloned_from_ptr(session_ptr).unwrap();

    tracing::trace!(
        "[FFI] Scheduling statement for execution: \"{}\"",
        statement
    );
    BridgedFuture::spawn::<_, _, PagerExecutionError>(tcb, async move {
        tracing::debug!("[FFI] Executing statement \"{}\"", statement);
        let query_pager = bridged_session.inner.query_iter(statement, ()).await?;
        tracing::trace!("[FFI] Statement executed");

        Ok(RowSet {
            pager: std::sync::Mutex::new(query_pager),
        })
    });
}

// I duplicated the code since it's meant to be refactored anyway
#[unsafe(no_mangle)]
pub extern "C" fn session_query_with_values(
    tcb: Tcb,
    session_ptr: BridgedBorrowedSharedPtr<'_, BridgedSession>,
    statement: CSharpStr<'_>,
    values_ptr: BridgedOwnedExclusivePtr<PreSerializedValues>,
) {
    // Convert the raw C string to a Rust string.
    let statement = statement.as_cstr().unwrap().to_str().unwrap().to_owned();
    let bridged_session = ArcFFI::cloned_from_ptr(session_ptr).unwrap();

    // Take ownership of the pre-serialized values box so we can move it into the async task.
    let values_box = BoxFFI::from_ptr(values_ptr).expect("non-null PreSerializedValues pointer");

    BridgedFuture::spawn::<_, _, PagerExecutionError>(tcb, async move {
        println!("Executing statement with values \"{}\"", statement);
        // Pass a reference to the PreSerializedValues implementing SerializeRow.

        //TODO: query_iter is discouraged for the use with parameters, investigate this
        let query_pager = bridged_session
            .inner
            .query_iter(statement, &*values_box)
            .await?;
        println!("Statement executed");

        Ok(RowSet {
            pager: std::sync::Mutex::new(query_pager),
        })
    });
}
