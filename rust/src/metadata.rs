use crate::ffi::{ArcFFI, BridgedBorrowedSharedPtr, BridgedOwnedSharedPtr, FFI, FromArc};
use scylla::cluster::ClusterState;
use std::ffi::{c_char, c_void};
use std::sync::Arc;

pub struct BridgedClusterState {
    pub(crate) inner: Arc<ClusterState>,
}

impl FFI for BridgedClusterState {
    type Origin = FromArc;
}

#[unsafe(no_mangle)]
pub extern "C" fn cluster_state_free(
    cluster_state_ptr: BridgedOwnedSharedPtr<BridgedClusterState>,
) {
    ArcFFI::free(cluster_state_ptr);
    tracing::trace!("[FFI] ClusterState pointer freed");
}

#[unsafe(no_mangle)]
pub extern "C" fn cluster_state_compare_ptr(
    ptr1: BridgedBorrowedSharedPtr<'_, BridgedClusterState>,
    ptr2: BridgedBorrowedSharedPtr<'_, BridgedClusterState>,
) -> bool {
    let cs1 = ArcFFI::as_ref(ptr1).unwrap();
    let cs2 = ArcFFI::as_ref(ptr2).unwrap();
    Arc::ptr_eq(&cs1.inner, &cs2.inner)
}

type ConstructCSharpHost = unsafe extern "C" fn(
    list_ptr: *mut c_void,
    ip_bytes_ptr: *const u8,
    ip_bytes_len: usize,
    port: u16,
    datacenter_ptr: *const c_char,
    datacenter_len: usize,
    rack_ptr: *const c_char,
    rack_len: usize,
    host_id_bytes_ptr: *const u8,
);

#[unsafe(no_mangle)]
pub extern "C" fn cluster_state_fill_nodes(
    cluster_state_ptr: BridgedBorrowedSharedPtr<'_, BridgedClusterState>,
    list_ptr: *mut c_void,
    callback: ConstructCSharpHost,
) {
    let bridged_cluster_state = ArcFFI::as_ref(cluster_state_ptr).unwrap();

    for node in bridged_cluster_state.inner.get_nodes_info() {
        let port = node.address.port();

        // The octets() returns an owned stack array. We store it in outer-scope
        // variables so we can take a slice that outlives the match expression.
        let ip_bytes_storage_v4: [u8; 4];
        let ip_bytes_storage_v6: [u8; 16];

        // Serialize IP address to bytes
        let ip_bytes: &[u8] = match node.address.ip() {
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

        // Get datacenter (Option<String>)
        // Safety: The pointer from as_ptr() is only valid while the String exists.
        // This is safe because:
        // 1. The callback is invoked synchronously
        // 2. C# immediately copies the data with Marshal.PtrToStringUTF8
        // 3. The String lives in `node` which outlives this callback invocation
        let (dc_ptr, dc_len) = node
            .datacenter
            .as_deref()
            .map_or((std::ptr::null(), 0usize), |dc| {
                (dc.as_ptr() as *const c_char, dc.len())
            });

        // Get rack (Option<String>) - same safety considerations as datacenter
        let (rack_ptr, rack_len) = node
            .rack
            .as_deref()
            .map_or((std::ptr::null(), 0usize), |r| {
                (r.as_ptr() as *const c_char, r.len())
            });

        // UUID as bytes
        let uuid_bytes = node.host_id.as_bytes();

        unsafe {
            callback(
                list_ptr,
                ip_bytes.as_ptr(),
                ip_bytes.len(),
                port,
                dc_ptr,
                dc_len,
                rack_ptr,
                rack_len,
                uuid_bytes.as_ptr(),
            );
        }
    }
}
