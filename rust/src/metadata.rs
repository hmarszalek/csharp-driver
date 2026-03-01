use crate::error_conversion::FFIException;
use crate::ffi::{
    ArcFFI, BridgedBorrowedSharedPtr, CSharpStr, FFI, FFIArray, FFIByteSlice, FFIPtr, FFIStr,
    FromArc,
};
use crate::task::ExceptionConstructors;
use scylla::cluster::ClusterState;
use std::collections::HashMap;

impl FFI for ClusterState {
    type Origin = FromArc;
}

/// Opaque type representing the C# RefreshContext.
#[derive(Clone, Copy)]
enum RefreshContext {}

/// Transparent wrapper around a pointer to the C# RefreshContext.
#[derive(Clone, Copy)]
#[repr(transparent)]
pub struct RefreshContextPtr(FFIPtr<'static, RefreshContext>);

/// Callback type for constructing C# Host objects.
/// The callback receives raw pointers to node metadata and is responsible for:
/// 1. Constructing a C# Host object from the provided data
/// 2. Adding the Host to the C# RefreshContext referenced by refresh_context_ptr
///
/// # Safety
/// - All pointer parameters must be immediately copied/consumed during the callback invocation
/// - String pointers (datacenter_ptr, rack_ptr) are only valid for the duration of the callback
/// - The callback must not store these pointers or access them after returning
/// - The callback must not throw exceptions across the FFI boundary
type ConstructCSharpHost = unsafe extern "C" fn(
    refresh_context_ptr: RefreshContextPtr,
    id_bytes: FFIByteSlice<'_>,
    ip_bytes: FFIByteSlice<'_>,
    port: u16,
    datacenter: FFIStr<'_>,
    rack: FFIStr<'_>,
);

/// Populates a C# RefreshContext with node information from the cluster state.
/// For each node in the cluster state, this function:
/// 1. Converts the node's metadata (IP, port, datacenter, rack, host ID) to FFI types
/// 2. Invokes the callback with pointers to this temporary data
/// 3. The callback must synchronously copy all data and add the Host to the RefreshContext
///
/// # Safety
/// - `refresh_context_ptr` must point to a valid C# RefreshContext that remains allocated during this call
/// - All string pointers passed to the callback are temporary and only valid during that invocation
/// - The callback must copy string data (e.g., via Marshal.PtrToStringUTF8) and byte arrays (IP, host ID) immediately.
/// - The callback must not throw exceptions; use Environment.FailFast on errors
#[unsafe(no_mangle)]
pub extern "C" fn cluster_state_fill_nodes(
    cluster_state_ptr: BridgedBorrowedSharedPtr<'_, ClusterState>,
    refresh_context_ptr: RefreshContextPtr,
    callback: ConstructCSharpHost,
) -> FFIException {
    let cluster_state =
        ArcFFI::as_ref(cluster_state_ptr).expect("valid and non-null ClusterState pointer");

    for node in cluster_state.get_nodes_info() {
        // UUID as bytes
        let uuid_bytes = FFIByteSlice::new(node.host_id.as_bytes());

        // The octets() returns an owned stack array. We store it in outer-scope
        // variables so we can take a slice that outlives the match expression.
        let ip_bytes_storage_v4: [u8; 4];
        let ip_bytes_storage_v6: [u8; 16];

        // Serialize IP address to bytes
        let port = node.address.port();
        let ip_bytes_slice: &[u8] = match node.address.ip() {
            std::net::IpAddr::V4(ipv4) => {
                ip_bytes_storage_v4 = ipv4.octets();
                let bytes = &ip_bytes_storage_v4[..];
                tracing::trace!("[FFI] Node IPv4: {:?}, port: {}", bytes, port);
                bytes
            }
            std::net::IpAddr::V6(ipv6) => {
                ip_bytes_storage_v6 = ipv6.octets();
                let bytes = &ip_bytes_storage_v6[..];
                tracing::trace!("[FFI] Node IPv6: {:?}, port: {}", bytes, port);
                bytes
            }
        };

        let ip_bytes = FFIByteSlice::new(ip_bytes_slice);

        // Get datacenter (Option<String>) - pass null when missing
        let dc_str = match node.datacenter.as_deref() {
            Some(s) => FFIStr::new(s),
            None => FFIStr::null(),
        };

        // Get rack (Option<String>) - pass null when missing
        let rack_str = match node.rack.as_deref() {
            Some(s) => FFIStr::new(s),
            None => FFIStr::null(),
        };

        // Invoke the callback to construct and add the Host to the C# list object.
        // All pointers passed to the callback are only valid during this invocation.
        // The callback must copy all data immediately.
        unsafe {
            callback(
                refresh_context_ptr,
                uuid_bytes,
                ip_bytes,
                port,
                dc_str,
                rack_str,
            );
        }
    }

    FFIException::ok()
}

/// Opaque type representing the C# KeyspaceNamesList.
#[derive(Clone, Copy)]
enum KeyspaceNamesList {}

/// Transparent wrapper around a pointer to the C# KeyspaceNamesList.
#[derive(Clone, Copy)]
#[repr(transparent)]
pub struct KeyspaceNamesListPtr(FFIPtr<'static, KeyspaceNamesList>);

/// Callback type for adding keyspace names to a C# KeyspaceNamesList.
/// The callback receives raw pointers to keyspace name and is responsible for
/// adding the name to the C# KeyspaceNamesList referenced by keyspace_names_list_ptr
///
/// # Safety
/// - Pointer parameter must be immediately copied/consumed during the callback invocation
/// - String pointer (keyspace_name) is only valid for the duration of the callback
/// - The callback must not store this pointer or access it after returning
/// - The callback must not throw exceptions across the FFI boundary
type AddKeyspaceName = unsafe extern "C" fn(
    keyspace_names_list_ptr: KeyspaceNamesListPtr,
    keyspace_names: FFIArray<'_, FFIStr<'_>>,
);

/// Populates a C# List with keyspace names from the cluster state.
/// For each keyspace in the cluster state, this function:
/// 1. Invokes the callback with a pointer to the keyspace name
/// 2. The callback must synchronously copy all data and add the KeyspaceName to the KeyspaceNamesList
///
/// # Safety
/// - `keyspace_names_list_ptr` must point to a valid C# List that remains allocated during this call
/// - All string pointers passed to the callback are temporary and only valid during that invocation
/// - The callback must copy string data of keyspace names (e.g., via Marshal.PtrToStringUTF8) immediately.
/// - The callback must not throw exceptions; use Environment.FailFast on errors
#[unsafe(no_mangle)]
pub extern "C" fn cluster_state_get_keyspace_names(
    cluster_state_ptr: BridgedBorrowedSharedPtr<'_, ClusterState>,
    keyspace_names_list_ptr: KeyspaceNamesListPtr,
    callback: AddKeyspaceName,
) -> FFIException {
    tracing::trace!("[FFI] cluster_state_get_keyspace_names called");

    let cluster_state =
        ArcFFI::as_ref(cluster_state_ptr).expect("valid and non-null ClusterState pointer");

    let keyspace_names: Vec<FFIStr<'_>> = cluster_state
        .keyspaces_iter()
        .map(|(ks_name, _)| FFIStr::new(ks_name))
        .collect();

    unsafe {
        callback(keyspace_names_list_ptr, FFIArray::from_vec(&keyspace_names));
    }

    FFIException::ok()
}

/// Opaque type representing the C# KeyspaceContext.
#[derive(Clone, Copy)]
enum KeyspaceContext {}

/// Transparent wrapper around a pointer to the C# KeyspaceContext.
#[derive(Clone, Copy)]
#[repr(transparent)]
pub struct KeyspaceContextPtr(FFIPtr<'static, KeyspaceContext>);

/// Callback type for constructing C# KeyspaceMetadata objects.
/// The callback receives raw pointers to keyspace metadata and is responsible for:
/// 1. Constructing a C# KeyspaceMetadata object from the provided data
/// 2. Setting the KeyspaceMetadata on the C# KeyspaceContext referenced by keyspace_context_ptr
///
/// # Safety
/// - All pointer parameters must be immediately copied/consumed during the callback invocation
/// - String pointers (strategy_class, key-value pairs) are only valid for the duration of the callback
/// - The callback must not store these pointers or access them after returning
/// - The callback must not throw exceptions across the FFI boundary
type ConstructCSharpKeyspaceMetadata = unsafe extern "C" fn(
    // TODO: once durable_writes is implemented, add a bool parameter for it.
    keyspace_context_ptr: KeyspaceContextPtr,
    strategy_class: FFIStr<'_>,
    replication_keys: FFIArray<'_, FFIStr<'_>>,
    replication_values: FFIArray<'_, FFIStr<'_>>,
);

/// Populates a C# KeyspaceContext with keyspace metadata from the cluster state.
/// For the specified keyspace in the cluster state, this function:
/// 1. Converts the keyspace's metadata (strategy class, replication options) to FFI types
/// 2. Invokes the callback with pointers to this temporary data
/// 3. The callback must synchronously copy all data and set the KeyspaceMetadata on the C# KeyspaceContext
///
/// # Safety
/// - `keyspace_context_ptr` must point to a valid C# KeyspaceContext that remains allocated during this call
/// - All string pointers passed to the callback are temporary and only valid during that invocation
/// - The callback must copy string data (e.g., via Marshal.PtrToStringUTF8) immediately.
/// - The callback must not throw exceptions; use Environment.FailFast on errors
#[unsafe(no_mangle)]
pub extern "C" fn cluster_state_get_keyspace_metadata(
    cluster_state_ptr: BridgedBorrowedSharedPtr<'_, ClusterState>,
    keyspace_name: CSharpStr<'_>,
    keyspace_context_ptr: KeyspaceContextPtr,
    callback: ConstructCSharpKeyspaceMetadata,
    constructors: &'static ExceptionConstructors,
) -> FFIException {
    tracing::trace!("[FFI] cluster_state_get_keyspace_metadata");

    let cluster_state =
        ArcFFI::as_ref(cluster_state_ptr).expect("valid and non-null ClusterState pointer");

    let keyspace_name = keyspace_name.as_cstr().unwrap().to_str().unwrap();
    let Some(keyspace) = cluster_state.get_keyspace(keyspace_name) else {
        // TODO: map to a more specific exception type.
        let ex = constructors
            .rust_exception_constructor
            .construct_from_rust(format!(
                "Keyspace '{}' not found in cluster metadata",
                keyspace_name
            ));
        return FFIException::from_exception(ex);
    };

    tracing::debug!("[FFI] Looking for keyspace: '{}'", keyspace_name);

    // TODO: retrive durable_writes value from keyspace metadata after it's implemented in Rust driver.

    let (strategy_class, replication) = match &keyspace.strategy {
        scylla::cluster::metadata::Strategy::SimpleStrategy { replication_factor } => (
            "SimpleStrategy",
            HashMap::from([(
                "replication_factor".to_string(),
                replication_factor.to_string(),
            )]),
        ),
        scylla::cluster::metadata::Strategy::NetworkTopologyStrategy {
            datacenter_repfactors,
        } => (
            "NetworkTopologyStrategy",
            datacenter_repfactors
                .iter()
                .map(|(dc, rf)| (dc.clone(), rf.to_string()))
                .collect(),
        ),
        scylla::cluster::metadata::Strategy::LocalStrategy => ("LocalStrategy", HashMap::new()),
        scylla::cluster::metadata::Strategy::Other { name, data } => (name.as_str(), data.clone()),
        _ => ("UnknownStrategy", HashMap::new()), // Placeholder for strategies not yet supported by the Rust driver
    };

    tracing::debug!(
        "[FFI] Keyspace '{}' has strategy '{}', replication options: {:?}",
        keyspace_name,
        strategy_class,
        replication
    );

    let replication_keys: Vec<FFIStr<'_>> = replication.keys().map(FFIStr::new).collect();
    let replication_values: Vec<FFIStr<'_>> = replication.values().map(FFIStr::new).collect();

    // Invoke the callback to construct and fill the C# KeyspaceMetadata object.
    // All pointers passed to the callback are only valid during this invocation.
    // The callback must copy all data immediately.
    // TODO: once durable_writes is implemented, pass it as a parameter to the callback.
    unsafe {
        callback(
            keyspace_context_ptr,
            FFIStr::new(strategy_class),
            FFIArray::from_vec(&replication_keys),
            FFIArray::from_vec(&replication_values),
        );
    }

    FFIException::ok()
}
