use crate::CSharpStr;
use crate::FfiError;
use crate::FfiPtr;
use crate::ffi::{ArcFFI, BridgedBorrowedSharedPtr, BridgedOwnedSharedPtr, FFI, FromArc};
use crate::pre_serialized_values::csharp_memory::{CsharpSerializedValue, CsharpValuePtr};
use crate::pre_serialized_values::pre_serialized_values::PreSerializedValues;
use scylla::cluster::{ClusterState, Node};
use scylla::errors::ClusterStateTokenError;
use scylla::routing::partitioner::PartitionerName;
use scylla::routing::{Shard, Token};
use std::ffi::{CString, c_char, c_void};
use std::sync::Arc;

// Helper macro: evaluate an expression that returns Result<T, E>. On Ok(v) yield v.
// On Err(e) format the provided message template with the error and return an FfiError
// with the provided numeric code.
// Usage: ffi_try!(expr, "failed to compute token: {}")
macro_rules! ffi_try {
    ($expr:expr, $fmt:expr) => {
        match $expr {
            Ok(v) => v,
            Err(e) => {
                let msg = format!($fmt, e);
                return FfiError::new(
                    1,
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

// Frees a BridgedClusterState pointer obtained from `session_get_cluster_state`.
//
// # Safety
// - Must only be called once per pointer
// - The pointer must have been obtained from `session_get_cluster_state`
// - After calling this function, the pointer is invalid and must not be used
#[unsafe(no_mangle)]
pub extern "C" fn cluster_state_free(
    cluster_state_ptr: BridgedOwnedSharedPtr<BridgedClusterState>,
) {
    ArcFFI::free(cluster_state_ptr);
    tracing::trace!("[FFI] ClusterState pointer freed");
}

// This function returns the raw memory address of the underlying ClusterState object
// for comparison purposes. The returned address can be stored and compared
// with addresses from other ClusterState pointers to detect topology changes.
//
// The returned address is ONLY valid for comparison while the Arc<ClusterState> is alive.
//
// # Safety
// - The pointer must be valid and not freed
// - The returned address must only be used for comparison, never dereferenced
#[unsafe(no_mangle)]
pub extern "C" fn cluster_state_get_raw_ptr(
    cluster_state_ptr: BridgedBorrowedSharedPtr<'_, ClusterState>,
) -> *const c_void {
    cluster_state_ptr
        .to_raw()
        .map(|p| p as *const c_void)
        .unwrap_or(std::ptr::null())
}

// Callback type for constructing C# Host objects.
// The callback receives raw pointers to node metadata and is responsible for:
// 1. Constructing a C# Host object from the provided data
// 2. Adding the Host to the C# List<Host> referenced by list_ptr
//
// # Safety
// - All pointer parameters must be immediately copied/consumed during the callback invocation
// - String pointers (datacenter_ptr, rack_ptr) are only valid for the duration of the callback
// - The callback must not store these pointers or access them after returning
// - The callback must not throw exceptions across the FFI boundary
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

#[derive(Clone, Copy)]
enum ReplicaList {}

#[repr(transparent)]
#[derive(Clone, Copy)]
pub struct CallbackStatePtr(FfiPtr<'static, ReplicaList>);
type OnReplicaPair = unsafe extern "C" fn(
    callback_state: CallbackStatePtr,
    host_id_bytes_ptr: *const u8,
    shard: i32,
);

// Populates a C# ICollection<Host> with node information from the cluster state.
// For each node in the cluster state, this function:
// 1. Serializes the node's metadata (IP, port, datacenter, rack, host ID) to raw bytes
// 2. Invokes the callback with pointers to this temporary data
// 3. The callback must synchronously copy all data and add the Host to the list
//
// # Safety
// - `list_ptr` must point to a valid C# List<Host> that remains allocated during this call
// - All string pointers passed to the callback are temporary and only valid during that invocation
// - The callback must copy string data (e.g., via Marshal.PtrToStringUTF8) and byte arrays (IP, host ID) immediately.
// - The callback must not throw exceptions; use Environment.FailFast on errors
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
        let (dc_ptr, dc_len) = node
            .datacenter
            .as_deref()
            .map_or((std::ptr::null(), 0_usize), |dc| {
                (dc.as_ptr() as *const c_char, dc.len())
            });

        // Get rack (Option<String>)
        let (rack_ptr, rack_len) = node
            .rack
            .as_deref()
            .map_or((std::ptr::null(), 0_usize), |r| {
                (r.as_ptr() as *const c_char, r.len())
            });

        // UUID as bytes
        let uuid_bytes = node.host_id.as_bytes();

        // Invoke the callback to construct and add the Host to the C# list object.
        // All pointers passed to the callback are only valid during this invocation.
        // The callback must copy all data immediately.
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
    fn compute_token(
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

    fn get_token_endpoints(
        &self,
        keyspace: &str,
        table: &str,
        token: Token,
    ) -> Vec<(Arc<Node>, Shard)> {
        self.inner.get_token_endpoints(keyspace, table, token)
    }
}

struct RustReplicaBridge {
    cluster_state: BridgedClusterState,
    keyspace_name: String,
    table_name: Option<String>,
    token: Token,
}

impl RustReplicaBridge {
    fn pre_serialized_values_from(
        partition_key_ptr: CsharpValuePtr,
        partition_key_len: usize,
    ) -> Result<PreSerializedValues, String> {
        let mut psv = PreSerializedValues::new();
        let csharp_val = CsharpSerializedValue::new(partition_key_ptr, partition_key_len);
        psv.add_value(csharp_val).map_err(|e| format!("{}", e))?;
        Ok(psv)
    }

    // Helper: call the provided FFI callback for each replica in `replicas`.
    fn callback_foreach_replica(
        replicas: Vec<(Arc<Node>, Shard)>,
        callback_state: CallbackStatePtr,
        callback: OnReplicaPair,
    ) {
        replicas.into_iter().for_each(|(node, shard)| {
            let host_id_bytes = node.host_id.as_bytes();
            unsafe {
                callback(callback_state, host_id_bytes.as_ptr(), shard as i32);
            }
        });
    }

    fn new_with_table(
        cluster_state_ptr: BridgedBorrowedSharedPtr<'_, BridgedClusterState>,
        keyspace: CSharpStr<'_>,
        table: CSharpStr<'_>,
        partition_key_ptr: CsharpValuePtr,
        partition_key_len: usize,
    ) -> Result<Self, String> {
        let cluster_state = ArcFFI::as_ref(cluster_state_ptr)
            .ok_or_else(|| "invalid ClusterState pointer".to_string())?;

        let keyspace_name = keyspace
            .as_cstr()
            .ok_or_else(|| "keyspace string is null".to_string())?
            .to_str()
            .map_err(|e| format!("invalid keyspace string: {}", e))?
            .to_owned();

        let table_name = table
            .as_cstr()
            .ok_or_else(|| "table string is null".to_string())?
            .to_str()
            .map_err(|e| format!("invalid table string: {}", e))?
            .to_owned();

        let psv = Self::pre_serialized_values_from(partition_key_ptr, partition_key_len)?;

        let token = cluster_state
            .compute_token(&keyspace_name, &table_name, psv)
            .map_err(|e| format!("failed to compute token: {}", e))?;

        Ok(Self {
            cluster_state: BridgedClusterState {
                inner: cluster_state.inner.clone(),
            },
            keyspace_name,
            table_name: Some(table_name),
            token,
        })
    }

    fn new_with_partitioner(
        cluster_state_ptr: BridgedBorrowedSharedPtr<'_, BridgedClusterState>,
        keyspace: CSharpStr<'_>,
        partition_key_ptr: CsharpValuePtr,
        partition_key_len: usize,
        partitioner: PartitionerName,
    ) -> Result<Self, String> {
        let cluster_state = ArcFFI::as_ref(cluster_state_ptr)
            .ok_or_else(|| "invalid ClusterState pointer".to_string())?;

        let keyspace_name = keyspace
            .as_cstr()
            .ok_or_else(|| "keyspace string is null".to_string())?
            .to_str()
            .map_err(|e| format!("invalid keyspace string: {}", e))?
            .to_owned();

        let psv = Self::pre_serialized_values_from(partition_key_ptr, partition_key_len)?;
        let serialized_values = psv.into_serialized_values();

        let token = cluster_state
            .inner
            .compute_token_preserialized_with_partitioner(&partitioner, &serialized_values)
            .map_err(|e| format!("failed to compute token: {}", e))?;

        Ok(Self {
            cluster_state: BridgedClusterState {
                inner: cluster_state.inner.clone(),
            },
            keyspace_name,
            table_name: None,
            token,
        })
    }

    fn handle_get_replicas(
        &self,
        callback_state: CallbackStatePtr,
        callback: OnReplicaPair,
    ) -> Result<(), String> {
        let table_name = self.table_name.as_deref().unwrap_or("");

        let replicas =
            self.cluster_state
                .get_token_endpoints(&self.keyspace_name, table_name, self.token);
        Self::callback_foreach_replica(replicas, callback_state, callback);
        Ok(())
    }
}

/// For now unused - get replicas for a specific table and partition key.
/// It should allow us to seamlessly support tablet-aware replica retrieval in the future.
/// TODO: extend the C# Metadata.GetReplicas to accept the table name as a parameter.
#[unsafe(no_mangle)]
pub extern "C" fn cluster_state_get_replicas(
    cluster_state_ptr: BridgedBorrowedSharedPtr<'_, BridgedClusterState>,
    keyspace: CSharpStr<'_>,
    table: CSharpStr<'_>,
    partition_key_ptr: CsharpValuePtr,
    partition_key_len: usize,
    callback_state: CallbackStatePtr,
    callback: OnReplicaPair,
) -> FfiError {
    let bridge = ffi_try!(
        RustReplicaBridge::new_with_table(
            cluster_state_ptr,
            keyspace,
            table,
            partition_key_ptr,
            partition_key_len
        ),
        "{}"
    );

    ffi_try!(
        bridge.handle_get_replicas(callback_state, callback),
        "failed to get replicas: {}"
    );

    FfiError::ok()
}

#[unsafe(no_mangle)]
pub extern "C" fn cluster_state_get_replicas_murmur3(
    cluster_state_ptr: BridgedBorrowedSharedPtr<'_, BridgedClusterState>,
    keyspace: CSharpStr<'_>,
    partition_key_ptr: CsharpValuePtr,
    partition_key_len: usize,
    callback_state: CallbackStatePtr,
    callback: OnReplicaPair,
) -> FfiError {
    let bridge = ffi_try!(
        RustReplicaBridge::new_with_partitioner(
            cluster_state_ptr,
            keyspace,
            partition_key_ptr,
            partition_key_len,
            PartitionerName::Murmur3
        ),
        "{}"
    );

    ffi_try!(
        bridge.handle_get_replicas(callback_state, callback),
        "failed to get replicas: {}"
    );

    FfiError::ok()
}
