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
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
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
        private static extern bool cluster_state_compare_ptr(
            IntPtr ptr1,
            IntPtr ptr2);

        [DllImport("csharp_wrapper", CallingConvention = CallingConvention.Cdecl)]
        private static extern void cluster_state_fill_nodes(
            IntPtr clusterStatePtr,
            IntPtr listPtr,
            IntPtr callback);

        [DllImport("csharp_wrapper", CallingConvention = CallingConvention.Cdecl)]
        private static extern void cluster_state_free(IntPtr clusterStatePtr);

        private static readonly unsafe delegate* unmanaged[Cdecl]<IntPtr, IntPtr, nuint, ushort, IntPtr, nuint, IntPtr, nuint, IntPtr, void> AddHostPtr = &AddHostCallback;

        [UnmanagedCallersOnly(CallConvs = new Type[] { typeof(CallConvCdecl) })]
        private static unsafe void AddHostCallback(
            IntPtr listPtr,
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
                // listPtr is a raw pointer to a List<Host> on the managed heap.
                // The GC could theoretically move it during this callback, but in practice:
                // 1. cluster_state_fill_nodes calls this callback synchronously and completes quickly
                // 2. The list is referenced in AllHosts() preventing collection (but not movement)
                // 3. This matches the pattern used in row_set_fill_columns_metadata
                var list = Unsafe.Read<List<Host>>((void*)listPtr);

                // Construct IPAddress directly from bytes (4 for IPv4, 16 for IPv6).
                // ipBytes is a ReadOnlySpan over unmanaged memory (ipBytesPtr) that is only 
                // valid for the duration of this callback invocation. The IPAddress constructor
                // must be called synchronously here so it can copy the data immediately. 
                var ipBytes = new ReadOnlySpan<byte>((void*)ipBytesPtr, (int)ipBytesLen);
                var ipAddress = new IPAddress(ipBytes);
                var address = new IPEndPoint(ipAddress, port);

                var datacenter = (datacenterPtr == IntPtr.Zero || datacenterLen == 0) ? null : Marshal.PtrToStringUTF8(datacenterPtr, (int)datacenterLen);
                var rack = (rackPtr == IntPtr.Zero || rackLen == 0) ? null : Marshal.PtrToStringUTF8(rackPtr, (int)rackLen);

                // Rust UUID is in big-endian format, but .NET Guid has mixed-endian layout.
                // Fortunately Guid has a constructor that automatically handles this conversion.
                var hostIdBytes = new ReadOnlySpan<byte>((void*)hostIdBytesPtr, 16);
                var hostId = new Guid(hostIdBytes);

                var host = new Host(address, hostId, datacenter, rack);
                list.Add(host);
            }
            catch (Exception ex)
            {
                // Do not throw across FFI boundary - causes undefined behavior.
                // Fail fast to match Rust's panic=abort behavior and make the error obvious.
                Environment.FailFast("Fatal error in AddHostCallback", ex);
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

        private CopyOnWriteDictionary<IPEndPoint, Host> _cachedHosts = new CopyOnWriteDictionary<IPEndPoint, Host>();

        private readonly object _hostLock = new object();

        private static readonly Logger Logger = new Logger(typeof(Metadata));

        internal Metadata(Configuration configuration, Func<Session> getActiveSessionOrThrow)
        {
            Configuration = configuration;
            _getActiveSessionOrThrow = getActiveSessionOrThrow ?? throw new ArgumentNullException(nameof(getActiveSessionOrThrow));
        }

        public void Dispose()
        {
            // Free the cluster state pointer if it exists
            var ptr = Interlocked.Exchange(ref _lastClusterStatePtr, IntPtr.Zero);
            if (ptr != IntPtr.Zero)
            {
                cluster_state_free(ptr);
            }
        }

        public Host GetHost(IPEndPoint address)
        {
            // Ensure cache is up to date
            AllHosts();

            // Use dictionary for O(1) lookup
            if (_cachedHosts.TryGetValue(address, out var host))
                return host;
            return null;
        }

        /// <summary>
        ///  Returns all known hosts of this cluster.
        /// </summary>
        public ICollection<Host> AllHosts()
        {
            var session = _getActiveSessionOrThrow();
            var clusterStatePtr = session.GetClusterStatePtr();

            // FIXME: When session is disposed (between getting it and fetching cluster state), 
            // GetClusterStatePtr throws ObjectDisposedException. But there could be another active 
            // session that wasnt disposed yet and could handle metadata query.

            // Check without lock if cache is still valid.
            if (_lastClusterStatePtr != IntPtr.Zero && cluster_state_compare_ptr(clusterStatePtr, _lastClusterStatePtr))
            {
                cluster_state_free(clusterStatePtr);
                return _cachedHosts.Values;
            }

            lock (_hostLock)
            {
                // Double-check: another thread may have updated the cache.
                if (_lastClusterStatePtr != IntPtr.Zero && cluster_state_compare_ptr(clusterStatePtr, _lastClusterStatePtr))
                {
                    // Free the pointer since we're not using it
                    cluster_state_free(clusterStatePtr);
                    return _cachedHosts.Values;
                }

                // Otherwise we are forced to refill all hosts.
                var hosts = new List<Host>();
                unsafe
                {
                    cluster_state_fill_nodes(
                        clusterStatePtr,
                        (IntPtr)Unsafe.AsPointer(ref hosts),
                        (IntPtr)AddHostPtr
                    );
                }

                Interlocked.Exchange(ref _cachedHosts, new CopyOnWriteDictionary<IPEndPoint, Host>(hosts.ToDictionary(h => h.Address)));

                // Free the old cluster state pointer if it exists. Make sure to update it atomically
                // and before calling free to avoid other threads reading a freed pointer.
                var oldPtr = Interlocked.Exchange(ref _lastClusterStatePtr, clusterStatePtr);
                if (oldPtr != IntPtr.Zero)
                {
                    cluster_state_free(oldPtr);
                }

                return _cachedHosts.Values;
            }
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