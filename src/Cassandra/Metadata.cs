//
//      Copyright (C) DataStax Inc.
//
//   Licensed under the Apache License, Version 2.0 (the "License");
//   you may not use this file except in compliance with the License.
//   You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//   Unless required by applicable law or agreed to in writing, software
//   distributed under the License is distributed on an "AS IS" BASIS,
//   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//   See the License for the specific language governing permissions and
//   limitations under the License.
//

using System;
using System.Collections.Generic;
using System.Net;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;
using Cassandra.Collections;

namespace Cassandra
{
    /// <summary>
    ///  Keeps metadata on the connected cluster, including known nodes and schema
    ///  definitions.
    /// </summary>
    public class Metadata : IDisposable
    {
#pragma warning disable CS0067
        public event HostsEventHandler HostsEvent;

        public event SchemaChangedEventHandler SchemaChangedEvent;
#pragma warning restore CS0067

        [DllImport(NativeLibrary.CSharpWrapper, CallingConvention = CallingConvention.Cdecl)]
        private static extern bool cluster_state_compare_ptr(
            IntPtr ptr1,
            IntPtr ptr2);
        
        [DllImport(NativeLibrary.CSharpWrapper, CallingConvention = CallingConvention.Cdecl)]
        private static extern IntPtr cluster_state_get_raw_ptr(IntPtr clusterStatePtr);

        [DllImport(NativeLibrary.CSharpWrapper, CallingConvention = CallingConvention.Cdecl)]
        private static extern void cluster_state_fill_nodes(
            IntPtr clusterStatePtr,
            IntPtr listPtr,
            IntPtr callback);

        [DllImport(NativeLibrary.CSharpWrapper, CallingConvention = CallingConvention.Cdecl)]
        private static extern void cluster_state_free(IntPtr clusterStatePtr);

        private static readonly unsafe delegate* unmanaged[Cdecl]<IntPtr, IntPtr, nuint, ushort, IntPtr, nuint, IntPtr, nuint, IntPtr, void> AddHostPtr = &AddHostToList;

        private static readonly unsafe delegate* unmanaged[Cdecl]<IntPtr, IntPtr, int, void> OnReplicaPairPtr = &OnReplicaPairCallback;

        // NOTE: Token map replica resolution without table context currently forces Murmur3
        // on the Rust side via cluster_state_get_replicas_murmur3.
        [DllImport(NativeLibrary.CSharpWrapper, CallingConvention = CallingConvention.Cdecl)]
        private static extern FfiError cluster_state_get_replicas_murmur3(
            IntPtr clusterStatePtr,
            [MarshalAs(UnmanagedType.LPUTF8Str)] string keyspace,
            [In] byte[] partitionKey,
            nuint partitionKeyLen,
            IntPtr callbackState,
            IntPtr callback);

        private class RefreshContext(CopyOnWriteDictionary<Guid, Host> oldHosts)
        {
            public Dictionary<Guid, Host> NewHosts { get; } = new(oldHosts.Count);
            public Dictionary<IPEndPoint, Guid> NewHostIdsByIp { get; } = new(oldHosts.Count);
            public CopyOnWriteDictionary<Guid, Host> OldHosts { get; } = oldHosts;
        }

        [UnmanagedCallersOnly(CallConvs = new Type[] { typeof(CallConvCdecl) })]
        private static unsafe void AddHostToList(
            IntPtr contextPtr,
            IntPtr ipBytesPtr,
            nuint ipBytesLen,
            ushort port,
            IntPtr datacenterPtr,
            nuint datacenterLen,
            IntPtr rackPtr,
            nuint rackLen,
            IntPtr hostIdBytesPtr)
        {
            try
            {
                // Safety:
                // contextPtr is an IntPtr created from a GCHandle (via GCHandle.ToIntPtr) in AllHosts().
                // We retrieve the managed RefreshContext using GCHandle.FromIntPtr(contextPtr).Target.
                // This avoids taking an address of a managed object and is safe against GC movement.
                var handle = GCHandle.FromIntPtr(contextPtr);
                var context = handle.Target as RefreshContext;
                if (context == null)
                {
                    Environment.FailFast("Invalid GCHandle context in AddHostCallback");
                }

                // Construct IPAddress directly from bytes (4 for IPv4, 16 for IPv6). ipBytes is a ReadOnlySpan over 
                // unmanaged memory (ipBytesPtr) that is only valid for the duration of this callback invocation. 
                // The IPAddress constructor must be called synchronously here so it can copy the data immediately.
                var ipBytes = new ReadOnlySpan<byte>((void*)ipBytesPtr, (int)ipBytesLen);
                var ipAddress = new IPAddress(ipBytes);
                var address = new IPEndPoint(ipAddress, port);

                // Rust UUID is in big-endian format, but .NET Guid has mixed-endian layout.
                var hostIdBytes = new ReadOnlySpan<byte>((void*)hostIdBytesPtr, 16);
                var hostId = new Guid(hostIdBytes);

                // Try to reuse existing host object if id matches and address is the same
                if (context.OldHosts != null && context.OldHosts.TryGetValue(hostId, out var host))
                {
                    // If the address matches, reuse the instance.
                    if (host.Address.Equals(address))
                    {
                        context.NewHosts[hostId] = host;
                        context.NewHostIdsByIp[address] = hostId;
                        return;
                    }
                }

                var datacenter = (datacenterPtr == IntPtr.Zero || datacenterLen == 0) ? null : Marshal.PtrToStringUTF8(datacenterPtr, (int)datacenterLen);
                var rack = (rackPtr == IntPtr.Zero || rackLen == 0) ? null : Marshal.PtrToStringUTF8(rackPtr, (int)rackLen);

                host = new Host(address, hostId, datacenter, rack);
                context.NewHosts[hostId] = host;
                context.NewHostIdsByIp[address] = hostId;
            }
            catch (Exception ex)
            {
                Environment.FailFast("Fatal error in AddHostCallback", ex);
            }
        }

        private class GetReplicasContext(CopyOnWriteDictionary<Guid, Host> hostsById)
        {
            public List<HostShard> Replicas { get; } = [];
            public CopyOnWriteDictionary<Guid, Host> HostsById { get; } = hostsById;
        }

        [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
        private static unsafe void OnReplicaPairCallback(IntPtr statePtr, IntPtr hostIdBytesPtr, int shard)
        {
            try
            {
                var handle = GCHandle.FromIntPtr(statePtr);
                var context = handle.Target as GetReplicasContext;
                if (context == null)
                {
                    Environment.FailFast("Invalid GCHandle context in OnReplicaPairCallback");
                }

                var hostIdBytes = new ReadOnlySpan<byte>((void*)hostIdBytesPtr, 16);
                var hostId = new Guid(hostIdBytes);

                if (context.HostsById.TryGetValue(hostId, out var host))
                {
                    context.Replicas.Add(new HostShard(host, shard));
                }
                else
                {
                    // Host not found in metadata, possibly removed or inconsistent state.
                    // We could log this, but for now we just skip it.
                }
            }
            catch (Exception ex)
            {
                Environment.FailFast("Fatal error in OnReplicaPairCallback", ex);
            }
        }

        /// <summary>
        ///  Returns the name of currently connected cluster.
        /// </summary>
        /// <returns>the Cassandra name of currently connected cluster.</returns>
        public String ClusterName { get; internal set; }

        /// <summary>
        /// Determines whether the cluster is provided as a service.
        /// </summary>
        public bool IsDbaas { get; private set; } = false;

        /// <summary>
        /// Gets the configuration associated with this instance.
        /// </summary>
        internal Configuration Configuration { get; private set; }

        // Function to get an active session from the cluster for FFI calls.
        // Provided by Cluster during construction. It never returns null.
        // It either returns a valid Session or throws InvalidOperationException.
        private readonly Func<Session> _getActiveSessionOrThrow;

        // Pointer to the last cluster state used to detect changes.
        // Volatile ensures visibility of updates across threads for the lock-free read in AllHosts().
        private volatile IntPtr _lastClusterStatePtr = IntPtr.Zero;

        private CopyOnWriteDictionary<Guid, Host> _hostsById = new CopyOnWriteDictionary<Guid, Host>();
        private CopyOnWriteDictionary<IPEndPoint, Guid> _hostIdsByIp = new CopyOnWriteDictionary<IPEndPoint, Guid>();

        private readonly object _hostLock = new object();

        private static readonly Logger Logger = new Logger(typeof(Metadata));

        internal Metadata(Configuration configuration, Func<Session> getActiveSessionOrThrow)
        {
            Configuration = configuration;
            _getActiveSessionOrThrow = getActiveSessionOrThrow ?? throw new ArgumentNullException(nameof(getActiveSessionOrThrow));
        }

        public void Dispose()
        {
            // No-op for now - metadata disposal not yet implemented
            // throw new NotImplementedException();
        }

        public Host GetHost(IPEndPoint address)
        {
            // Ensure cache is up to date
            RefreshTopologyCache();

            if (_hostIdsByIp.TryGetValue(address, out var hostId))
            {
                if (_hostsById.TryGetValue(hostId, out var host))
                {
                    return host;
                }
            }

            return null;
        }
        
        internal Guid? GetHostIdByIp(IPEndPoint address)
        {
             // Ensure cache is up to date
             RefreshTopologyCache();
             
             if (_hostIdsByIp.TryGetValue(address, out var hostId))
             {
                 return hostId;
             }
             return null;
        }

        /// <summary>
        ///  Returns all known hosts of this cluster.
        /// </summary>
        public ICollection<Host> AllHosts()
        {
            // Ensure cache is up to date
            RefreshTopologyCache();

            return _hostsById.Values;
        }

        /// <summary>
        /// Updates the cached topology if the cluster state has changed.
        /// </summary>
        private void RefreshTopologyCache()
        {
            // FIXME: When session is disposed (between getting it and fetching cluster state), 
            // GetClusterStatePtr throws ObjectDisposedException. But there could be another active 
            // session that wasnt disposed yet and could handle metadata query.
            // TODO: add read lock for fetched session until the end of the method

            var session = _getActiveSessionOrThrow();
            var clusterStatePtr = session.GetClusterStatePtr();

            try
            {
                // Extract the raw pointer address for comparison.
                // This address is only valid while clusterStatePtr is alive (within this try block).
                var rawPtr = cluster_state_get_raw_ptr(clusterStatePtr);

                // Check without lock if cache is still valid by comparing raw addresses.
                // _lastClusterStatePtr stores only the address, not an owned Arc.
                if (_lastClusterStatePtr != IntPtr.Zero && rawPtr == _lastClusterStatePtr)
                {
                    return;
                }

                lock (_hostLock)
                {
                    // Double-check: another thread may have updated the cache.
                    if (_lastClusterStatePtr != IntPtr.Zero && rawPtr == _lastClusterStatePtr)
                    {
                        return;
                    }


                    // Otherwise we are forced to refill all hosts.
                    var context = new RefreshContext(_hostsById);
                    var gch = GCHandle.Alloc(context, GCHandleType.Normal);
                    try
                    {
                        unsafe
                        {
                            cluster_state_fill_nodes(
                                clusterStatePtr,
                                GCHandle.ToIntPtr(gch),
                                (IntPtr)AddHostPtr
                            );
                        }
                    }
                    finally
                    {
                        gch.Free();
                    }

                    Interlocked.Exchange(ref _hostsById,
                        new CopyOnWriteDictionary<Guid, Host>(context.NewHosts));
                    Interlocked.Exchange(ref _hostIdsByIp,
                        new CopyOnWriteDictionary<IPEndPoint, Guid>(context.NewHostIdsByIp));

                    // Free the old cluster state pointer if it exists. Make sure to update it atomically
                    // and before calling free to avoid other threads reading a freed pointer.
                    var oldPtr = Interlocked.Exchange(ref _lastClusterStatePtr, clusterStatePtr);
                    if (oldPtr != IntPtr.Zero)
                    {
                        cluster_state_free(oldPtr);
                    }
                }
            }
            finally
            {
                cluster_state_free(clusterStatePtr);
            }
        }


        // for tests
        internal KeyValuePair<string, KeyspaceMetadata>[] KeyspacesSnapshot => throw new NotImplementedException();

        /// <summary>
        /// Get the replicas for a given partition key and keyspace
        /// </summary>
        public ICollection<HostShard> GetReplicas(string keyspaceName, byte[] partitionKey)
        {
            // Ensure cache is up to date
            AllHosts();

            var session = _getActiveSessionOrThrow();
            // FIXME: Handle session disposal race condition similar to AllHosts

            // Request a fresh cluster state pointer for the native replica calculation.
            // We get it once and free it in the finally block below.
            var ptr = session.GetClusterStatePtr();
            try
            {
                var context = new GetReplicasContext(_hostsById);
                var gch = GCHandle.Alloc(context, GCHandleType.Normal);
                
                try
                {
                    IntPtr callbackPtr;
                    unsafe
                    {
                        callbackPtr = (IntPtr)OnReplicaPairPtr;
                    }

                    // NOTE: C# Metadata.GetReplicas doesn't provide the table name.
                    // For correctness, token computation should use the cluster/table partitioner; and for Scylla
                    // tablet routing we also need table context. Until we extend the API/bridge, force Murmur3.
                    // FIXME: Use metadata-derived partitioner
                    FfiErrorHelpers.ExecuteAndThrowIfFails(() => cluster_state_get_replicas_murmur3(
                        ptr,
                        keyspaceName,
                        partitionKey,
                        (nuint)(partitionKey?.Length ?? 0),
                        GCHandle.ToIntPtr(gch),
                        callbackPtr
                    ));
                    
                    return context.Replicas;
                }
                finally
                {
                    gch.Free();
                }
            }
            finally
            {
                cluster_state_free(ptr);
            }
        }

        public ICollection<HostShard> GetReplicas(byte[] partitionKey)
        {
            // TODO: is it even correct?
            // The idea is to retrieve the primary replicas for the partition key when the keyspace is not specified,
            // since no replication strategy can be applied - that's how it worked in the original driver.
            // In this case, when no keyspace is specified, the Rust side replica locator with fall back to the default
            // Simple Strategy with RF = 1, which achieves exactly what we're aiming for.
            return GetReplicas("", partitionKey);
        }

        /// <summary>
        ///  Returns metadata of specified keyspace.
        /// </summary>
        /// <param name="keyspace"> the name of the keyspace for which metadata should be
        ///  returned. </param>
        /// <returns>the metadata of the requested keyspace or <c>null</c> if
        ///  <c>* keyspace</c> is not a known keyspace.</returns>
        public KeyspaceMetadata GetKeyspace(string keyspace)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        ///  Returns a collection of all defined keyspaces names.
        /// </summary>
        /// <returns>a collection of all defined keyspaces names.</returns>
        public ICollection<string> GetKeyspaces()
        {
            throw new NotImplementedException();
        }

        /// <summary>
        ///  Returns names of all tables which are defined within specified keyspace.
        /// </summary>
        /// <param name="keyspace">the name of the keyspace for which all tables metadata should be
        ///  returned.</param>
        /// <returns>an ICollection of the metadata for the tables defined in this
        ///  keyspace.</returns>
        public ICollection<string> GetTables(string keyspace)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        ///  Returns TableMetadata for specified table in specified keyspace.
        /// </summary>
        /// <param name="keyspace">name of the keyspace within specified table is defined.</param>
        /// <param name="tableName">name of table for which metadata should be returned.</param>
        /// <returns>a TableMetadata for the specified table in the specified keyspace.</returns>
        public TableMetadata GetTable(string keyspace, string tableName)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        ///  Returns the view metadata for the provided view name in the keyspace.
        /// </summary>
        /// <param name="keyspace">name of the keyspace within specified view is defined.</param>
        /// <param name="name">name of view.</param>
        /// <returns>a MaterializedViewMetadata for the view in the specified keyspace.</returns>
        public MaterializedViewMetadata GetMaterializedView(string keyspace, string name)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Gets the definition associated with a User Defined Type from Cassandra
        /// </summary>
        public UdtColumnInfo GetUdtDefinition(string keyspace, string typeName)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Gets the definition associated with a User Defined Type from Cassandra
        /// </summary>
        public Task<UdtColumnInfo> GetUdtDefinitionAsync(string keyspace, string typeName)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Gets the definition associated with a User Defined Function from Cassandra
        /// </summary>
        /// <returns>The function metadata or null if not found.</returns>
        public FunctionMetadata GetFunction(string keyspace, string name, string[] signature)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Gets the definition associated with a aggregate from Cassandra
        /// </summary>
        /// <returns>The aggregate metadata or null if not found.</returns>
        public AggregateMetadata GetAggregate(string keyspace, string name, string[] signature)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Updates the keyspace and token information
        /// </summary>
        public bool RefreshSchema(string keyspace = null, string table = null)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Updates the keyspace and token information
        /// </summary>
        public Task<bool> RefreshSchemaAsync(string keyspace = null, string table = null)
        {
            throw new NotImplementedException();
        }

        public void ShutDown(int timeoutMs = Timeout.Infinite)
        {
            // No-op for now - metadata shutdown not yet implemented
            // throw new NotImplementedException();
        }

        public Task Init()
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Initiates a schema agreement check.
        /// <para/>
        /// Schema changes need to be propagated to all nodes in the cluster.
        /// Once they have settled on a common version, we say that they are in agreement.
        /// <para/>
        /// This method does not perform retries so
        /// <see cref="ProtocolOptions.MaxSchemaAgreementWaitSeconds"/> does not apply.
        /// </summary>
        /// <returns>True if schema agreement was successful and false if it was not successful.</returns>
        public Task<bool> CheckSchemaAgreementAsync()
        {
            throw new NotImplementedException();
        }
    }
}
