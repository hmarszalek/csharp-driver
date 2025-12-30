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
using Cassandra.Tasks;

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

        [DllImport("csharp_wrapper", CallingConvention = CallingConvention.Cdecl)]
        private static extern IntPtr cluster_state_get_raw_ptr(IntPtr clusterStatePtr);

        [DllImport("csharp_wrapper", CallingConvention = CallingConvention.Cdecl)]
        private static extern void cluster_state_fill_nodes(
            IntPtr clusterStatePtr,
            IntPtr listPtr,
            IntPtr callback);

        [DllImport("csharp_wrapper", CallingConvention = CallingConvention.Cdecl)]
        private static extern void cluster_state_free(IntPtr clusterStatePtr);

        private static readonly unsafe delegate* unmanaged[Cdecl]<IntPtr, FFIByteSlice, ushort, FFIString, FFIString, FFIByteSlice, void> AddHostPtr = &AddHostToList;

        private class RefreshContext(IReadOnlyDictionary<Guid, Host> oldHosts)
        {
            private readonly Dictionary<Guid, Host> _newHosts = new(oldHosts?.Count ?? 0);
            private readonly Dictionary<IPEndPoint, Guid> _newHostIdsByIp = new(oldHosts?.Count ?? 0);

            public IReadOnlyDictionary<Guid, Host> OldHosts { get; } = oldHosts;

            public void AddHost(Host host)
            {   
                _newHosts[host.HostId] = host;
                _newHostIdsByIp[host.Address] = host.HostId;
            }
            public HostRegistry ToNewRegistry() => new HostRegistry(_newHosts, _newHostIdsByIp);
        }

        [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
        private static unsafe void AddHostToList(
            IntPtr contextPtr,
            FFIByteSlice ipBytes,
            ushort port,
            FFIString datacenter,
            FFIString rack,
            FFIByteSlice hostIdBytes)
        {
            try
            {
                // Safety:
                // contextPtr is an IntPtr created from a GCHandle (via GCHandle.ToIntPtr) in RefreshTopologyCacheInternal().
                // We retrieve the managed RefreshContext using GCHandle.FromIntPtr(contextPtr).Target.
                // This avoids taking an address of a managed object and is safe against GC movement.
                var handle = GCHandle.FromIntPtr(contextPtr);
                var context = handle.Target as RefreshContext;
                if (context == null)
                {
                    Environment.FailFast("Invalid GCHandle context in AddHostCallback");
                }

                // Construct IPAddress directly from bytes (4 for IPv4, 16 for IPv6).
                var ipSpan = ipBytes.ToSpan();
                var ipAddress = new IPAddress(ipSpan);
                var address = new IPEndPoint(ipAddress, port);

                // HostId: use the bytes provided by the FFI slice directly (no endianness manipulation).
                // Guid(ReadOnlySpan<byte>) takes the bytes as provided and copies them.
                var hostIdSpan = hostIdBytes.ToSpan();
                if (hostIdSpan.Length != 16)
                {
                    Environment.FailFast($"Invalid hostId byte length: {hostIdSpan.Length}. Expected 16.");
                }
                var hostId = new Guid(hostIdSpan);

                // Try to reuse existing host object if id matches and address is the same
                if (context.OldHosts != null && context.OldHosts.TryGetValue(hostId, out var host))
                {
                    // If the address matches, reuse the instance.
                    if (host.Address.Equals(address))
                    {
                        context.AddHost(host);
                        return;
                    }
                }

                var datacenterStr = (datacenter.ptr == IntPtr.Zero || datacenter.len == 0)
                    ? null
                    : datacenter.ToManagedString();
                var rackStr = (rack.ptr == IntPtr.Zero || rack.len == 0)
                    ? null
                    : rack.ToManagedString();

                var newHost = new Host(address, hostId, datacenterStr, rackStr);
                context.AddHost(newHost);
            }
            catch (Exception ex)
            {
                Environment.FailFast("Fatal error in AddHostCallback", ex);
            }
        }

        /// <summary>
        ///  Returns the name of currently connected cluster.
        /// </summary>
        /// <returns>the Cassandra name of currently connected cluster.</returns>
        public string ClusterName { get; internal set; }

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

        // Pointer to the last cluster state raw pointer used to detect changes.
        // Volatile ensures visibility of updates across threads that compare it
        // before deciding whether to trigger a refresh.
        private volatile IntPtr _lastClusterStatePtr = IntPtr.Zero;

        // HostRegistry groups both maps so they can be swapped atomically.
        private sealed class HostRegistry(
            IReadOnlyDictionary<Guid, Host> hostsById,
            IReadOnlyDictionary<IPEndPoint, Guid> hostIdsByIp)
        {
            public readonly IReadOnlyDictionary<Guid, Host> HostsById =
                hostsById ?? new Dictionary<Guid, Host>();

            public readonly IReadOnlyDictionary<IPEndPoint, Guid> HostIdsByIp =
                hostIdsByIp ?? new Dictionary<IPEndPoint, Guid>();
        }

        // Active host registry reference; swapped atomically on refresh.
        // NOTE: Do not access this field directly; use GetRegistry() instead, since the accessor covers the
        // refreshment logic, with a compromise between limited data staleness and performance.
        private volatile HostRegistry _hostRegistry =
            new HostRegistry(new Dictionary<Guid, Host>(), new Dictionary<IPEndPoint, Guid>());

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


        /// <summary>
        /// Returns the <see cref="Host"/> for the given address, using the most recent
        /// known topology if available. If a refresh is needed, at most one caller will
        /// perform it; others will use the last known registry (stale-ok).
        /// </summary>
        public Host GetHost(IPEndPoint address)
        {
            var registry = GetRegistry();

            return !registry.HostIdsByIp.TryGetValue(address, out var hostId) ? null : registry.HostsById.GetValueOrDefault(hostId);
        }

        internal Guid? GetHostIdByIp(IPEndPoint address)
        {
            if (GetRegistry().HostIdsByIp.TryGetValue(address, out var hostId))
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
            // Return a snapshot copy of the values as ICollection<Host>
            return new List<Host>(GetRegistry().HostsById.Values);
        }

        public IEnumerable<IPEndPoint> AllReplicas()
        {
            throw new NotImplementedException();
        }

        // for tests
        internal KeyValuePair<string, KeyspaceMetadata>[] KeyspacesSnapshot => throw new NotImplementedException();

        /// <summary>
        /// Get the replicas for a given partition key and keyspace
        /// </summary>
        public ICollection<HostShard> GetReplicas(string keyspaceName, byte[] partitionKey)
        {
            throw new NotImplementedException();
        }

        public ICollection<HostShard> GetReplicas(byte[] partitionKey)
        {
            throw new NotImplementedException();
        }
        
        private bool IsClusterStateUpToDate(IntPtr clusterStatePtr)
        {
            var rawPtr = cluster_state_get_raw_ptr(clusterStatePtr);
            return _lastClusterStatePtr != IntPtr.Zero && rawPtr == _lastClusterStatePtr;
        }

        /// <summary>
        /// Returns a registry instance, refreshing topology if needed.
        /// At most one caller performs the refresh (single-flight), others
        /// will use the last known registry.
        /// </summary>
        private HostRegistry GetRegistry()
        {
            var clusterStatePtr = IntPtr.Zero;

            try
            {
                // Always get the session; any error should propagate
                var session = _getActiveSessionOrThrow();
                clusterStatePtr = session.GetClusterStatePtr();

                var rawPtr = cluster_state_get_raw_ptr(clusterStatePtr);

                // Fast path: no changes, no lock
                if (IsClusterStateUpToDate(_lastClusterStatePtr))
                {
                    return _hostRegistry;
                }

                // Slow path: always block and update
                lock (_hostLock)
                {
                    // Re-check after acquiring lock
                    if (IsClusterStateUpToDate(_lastClusterStatePtr))
                    {
                        return _hostRegistry;
                    }

                    RefreshTopologyCacheInternal(clusterStatePtr, rawPtr);
                    return _hostRegistry;
                }
            }
            finally
            {
                if (clusterStatePtr != IntPtr.Zero)
                {
                    cluster_state_free(clusterStatePtr);
                }
            }
        }


        /// <summary>
        /// IMPORTANT: This method may only be called after acquiring _hostLock.
        /// 
        /// Performs the actual topology refresh using the provided cluster state pointer
        /// and its associated raw pointer. This method assumes that:
        /// - clusterStatePtr was obtained from the active session
        /// - rawPtr was obtained via cluster_state_get_raw_ptr(clusterStatePtr)
        /// and that the caller will handle freeing clusterStatePtr.
        /// </summary>
        private void RefreshTopologyCacheInternal(IntPtr clusterStatePtr, IntPtr rawPtr)
        {
            // Use current registry's HostsById as the "oldHosts" reference for reuse logic.
            var context = new RefreshContext(_hostRegistry.HostsById);
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

            // Swap both maps together via a new HostRegistry created from the context so readers
            // never see mismatched maps.
            Interlocked.Exchange(ref _hostRegistry, context.ToNewRegistry());

            if (_lastClusterStatePtr != IntPtr.Zero)
            {
                cluster_state_free(_lastClusterStatePtr);
            }

            _lastClusterStatePtr = rawPtr;
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
