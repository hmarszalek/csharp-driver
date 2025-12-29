use crate::CSharpStr;
use crate::FfiError;
use crate::ffi::{ArcFFI, BridgedBorrowedSharedPtr, BridgedOwnedSharedPtr, FFI, FromArc};
use crate::pre_serialized_values::csharp_memory::{CsharpSerializedValue, CsharpValuePtr};
use crate::pre_serialized_values::pre_serialized_values::PreSerializedValues;
use scylla::cluster::{ClusterState, Node};
use scylla::errors::ClusterStateTokenError;
use scylla::routing::{Shard, Token};
use std::ffi::{CString, c_char, c_void};
use std::sync::Arc;

// Helper macro: evaluate an expression that returns Result<T, E>. On Ok(v) yield v.
// On Err(e) format the provided message template with the error and return an FfiError
// with the provided numeric code.
// Usage: ffi_try!(expr, 1, "failed to compute token: {}")
macro_rules! ffi_try {
    ($expr:expr, $code:expr, $fmt:expr) => {
        match $expr {
            Ok(v) => v,
            Err(e) => {
                let msg = format!($fmt, e);
                return FfiError::new(
                    $code,
                    CString::new(msg).unwrap_or_else(|_| CString::new("error").unwrap()),
                );
            }
        }
    };
}

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

type OnReplicaPair =
    unsafe extern "C" fn(callback_state: *mut c_void, host_id_bytes_ptr: *const u8, shard: i32);

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

impl BridgedClusterState {
    fn _compute_token(
        &self,
        keyspace: &str,
        table: &str,
        pre_serialized_partition_key: PreSerializedValues,
    ) -> Result<Token, ClusterStateTokenError> {
        // Use non-consuming accessor to avoid moving out of borrowed PreSerializedValues
        self.inner.compute_token_preserialized(
            keyspace,
            table,
            &(pre_serialized_partition_key.into_serialized_values()),
        )
    }

    fn _get_token_endpoints(
        &self,
        keyspace: &str,
        table: &str,
        pre_serialized_partition_key: PreSerializedValues,
    ) -> Result<Vec<(Arc<Node>, Shard)>, ClusterStateTokenError> {
        let token = self._compute_token(keyspace, table, pre_serialized_partition_key)?;
        Ok(self.inner.get_token_endpoints(keyspace, table, token))
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn cluster_state_get_replicas(
    cluster_state_ptr: BridgedBorrowedSharedPtr<'_, BridgedClusterState>,
    keyspace: CSharpStr<'_>,
    table: CSharpStr<'_>,
    partition_key_ptr: CsharpValuePtr,
    partition_key_len: usize,
    callback_state: *mut c_void,
    callback: OnReplicaPair,
) -> FfiError {
    // Validate cluster_state pointer (use macro)
    let bridged_cluster_state = ffi_try!(
        ArcFFI::as_ref(cluster_state_ptr).ok_or_else(|| "invalid ClusterState pointer".to_string()),
        1,
        "invalid ClusterState pointer: {}"
    );

    // Parse keyspace (C# ensures pointer validity) and use macro for UTF-8 error
    let keyspace_cstr = keyspace.as_cstr().unwrap();
    let keyspace_name = ffi_try!(keyspace_cstr.to_str(), 1, "invalid keyspace string: {}");

    // Parse table (C# ensures pointer validity) and use macro for UTF-8 error
    let table_cstr = table.as_cstr().unwrap();
    let table_name = ffi_try!(table_cstr.to_str(), 1, "invalid table string: {}");

    // Build PreSerializedValues locally and add the C# buffer as a single blob; use macro
    let psv = ffi_try!(
        {
            let mut _psv = PreSerializedValues::new();
            let csharp_val = CsharpSerializedValue::new(partition_key_ptr, partition_key_len);
            // psv.add_value returns Result<(), SerializationError>
            match _psv.add_value(csharp_val) {
                Ok(()) => Ok(_psv),
                Err(e) => Err(e),
            }
        },
        1,
        "failed to add pre-serialized partition key: {}"
    );

    // Get replicas using owned PreSerializedValues (moved into function)
    let replicas = ffi_try!(
        bridged_cluster_state._get_token_endpoints(keyspace_name, table_name, psv),
        1,
        "failed to compute token: {}"
    );

    // Call the callback for each replica
    for (node, shard) in replicas {
        let host_id_bytes = node.host_id.as_bytes();
        unsafe {
            callback(callback_state, host_id_bytes.as_ptr(), shard as i32);
        }
    }

    FfiError::ok()
}
