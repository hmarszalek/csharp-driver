use crate::ffi::{ArcFFI, BridgedOwnedSharedPtr, FFI, FromArc};
use scylla::cluster::ClusterState;

impl FFI for ClusterState {
    type Origin = FromArc;
}

/// Frees a ClusterState pointer obtained from `session_get_cluster_state`.
///
/// # Safety
/// - Must only be called once per pointer
/// - The pointer must have been obtained from `session_get_cluster_state`
/// - After calling this function, the pointer is invalid and must not be used
#[unsafe(no_mangle)]
pub extern "C" fn cluster_state_free(cluster_state_ptr: BridgedOwnedSharedPtr<ClusterState>) {
    ArcFFI::free(cluster_state_ptr);
    tracing::trace!("[FFI] ClusterState pointer freed");
}

/// This function returns the raw memory address of the underlying ClusterState object
/// for comparison purposes. The returned address can be stored and compared
/// with addresses from other ClusterState pointers to detect topology changes.
///
/// The returned address is ONLY valid for comparison while the Arc<ClusterState> is alive.
///
/// # Safety
/// - The pointer must be valid and not freed
/// - The returned address must only be used for comparison, never dereferenced
#[unsafe(no_mangle)]
pub extern "C" fn cluster_state_get_raw_ptr(
    cluster_state_ptr: BridgedBorrowedSharedPtr<'_, ClusterState>,
) -> *const c_void {
    cluster_state_ptr
        .to_raw()
        .map(|p| p as *const c_void)
        .unwrap_or(std::ptr::null())
}

/// Callback type for constructing C# Host objects.
/// The callback receives raw pointers to node metadata and is responsible for:
/// 1. Constructing a C# Host object from the provided data
/// 2. Adding the Host to the C# List<Host> referenced by list_ptr
///
/// # Safety
/// - All pointer parameters must be immediately copied/consumed during the callback invocation
/// - String pointers (datacenter_ptr, rack_ptr) are only valid for the duration of the callback
/// - The callback must not store these pointers or access them after returning
/// - The callback must not throw exceptions across the FFI boundary
type ConstructCSharpHost = unsafe extern "C" fn(
    list_ptr: *mut c_void,
    ip_bytes: FFIByteSlice<'_>,
    port: u16,
    datacenter: FFIStr<'_>,
    rack: FFIStr<'_>,
    host_id_bytes: FFIByteSlice<'_>,
);

/// Populates a C# List<Host> with node information from the cluster state.
/// For each node in the cluster state, this function:
/// 1. Serializes the node's metadata (IP, port, datacenter, rack, host ID) to raw bytes
/// 2. Invokes the callback with pointers to this temporary data
/// 3. The callback must synchronously copy all data and add the Host to the list
///
/// # Safety
/// - `list_ptr` must point to a valid C# List<Host> that remains allocated during this call
/// - All string pointers passed to the callback are temporary and only valid during that invocation
/// - The callback must copy string data (e.g., via Marshal.PtrToStringUTF8) and byte arrays (IP, host ID) immediately.
/// - The callback must not throw exceptions; use Environment.FailFast on errors
#[unsafe(no_mangle)]
pub extern "C" fn cluster_state_fill_nodes(
    cluster_state_ptr: BridgedBorrowedSharedPtr<'_, ClusterState>,
    list_ptr: *mut c_void,
    callback: ConstructCSharpHost,
) {
    let cluster_state =
        ArcFFI::as_ref(cluster_state_ptr).expect("valid and non-null ClusterState pointer");

    for node in cluster_state.get_nodes_info() {
        let port = node.address.port();

        // The octets() returns an owned stack array. We store it in outer-scope
        // variables so we can take a slice that outlives the match expression.
        let ip_bytes_storage_v4: [u8; 4];
        let ip_bytes_storage_v6: [u8; 16];

        // Serialize IP address to bytes
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

        // Get datacenter (Option<String>)
        let dc = FFIStr::new(node.datacenter.as_deref().unwrap_or(""));

        // Get rack (Option<String>)
        let rack = FFIStr::new(node.rack.as_deref().unwrap_or(""));

        // UUID as bytes
        let host_id_bytes = FFIByteSlice::new(node.host_id.as_bytes());

        // Invoke the callback to construct and add the Host to the C# list object.
        // All pointers passed to the callback are only valid during this invocation.
        // The callback must copy all data immediately.
        unsafe {
            callback(list_ptr, ip_bytes, port, dc, rack, host_id_bytes);
        }
    }
}
