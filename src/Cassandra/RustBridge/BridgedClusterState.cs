using System;
using System.Runtime.InteropServices;
using System.Net;
using System.Runtime.CompilerServices;
using static Cassandra.RustBridge;
using System.Collections.Generic;

namespace Cassandra
{
    internal sealed class BridgedClusterState : RustResource
    {
        internal BridgedClusterState(ManuallyDestructible mdClusterState) : base(mdClusterState)
        {
        }

        internal bool Equals(BridgedClusterState other)
        {
            if (other is null) return false;
            return handle == other.handle;
        }

        [DllImport("csharp_wrapper", CallingConvention = CallingConvention.Cdecl)]
        private static extern RustBridge.FFIException cluster_state_fill_nodes(
            IntPtr clusterState,
            IntPtr contextPtr,
            IntPtr callback);

        private static readonly unsafe delegate* unmanaged[Cdecl]<IntPtr, FFISliceRaw, FFISliceRaw, ushort, FFIString, FFIString, void> AddHostPtr = &AddHostToList;
        [UnmanagedCallersOnly(CallConvs = new Type[] { typeof(CallConvCdecl) })]
        private static unsafe void AddHostToList(
            IntPtr contextPtr,
            FFISliceRaw idBytes,
            FFISliceRaw ipBytes,
            ushort port,
            FFIString datacenter,
            FFIString rack)
        {
            try
            {
                // Safety: 
                // contextPtr is a pointer to the stack slot holding the 'context' reference (not to the heap object itself).
                // Unsafe.AsPointer(ref T) returns the address of the managed pointer (the stack local).
                // The stack slot is stable for the duration of this callback since:
                // 1. cluster_state_fill_nodes calls this callback synchronously before returning
                // 2. The stack frame containing 'context' remains alive throughout the FFI call
                // 3. If GC moves the RefreshContext object on the heap, it updates the reference value in the stack slot
                // 4. Unsafe.Read dereferences the pointer to get the current reference value
                // This matches the pattern used in row_set_fill_columns_metadata.
                var context = Unsafe.AsRef<Metadata.RefreshContext>((void*)contextPtr);

                var hostId = new Guid(idBytes.As<byte>().ToSpan());

                // Construct IPAddress directly from bytes (4 for IPv4, 16 for IPv6). ipBytes is an FFISlice<byte> 
                // and it accesses unmanaged memory that is only valid for the duration of this callback invocation. 
                // The IPAddress constructor must be called synchronously here so it can copy the data immediately.
                var ipAddress = new IPAddress(ipBytes.As<byte>().ToSpan());
                var address = new IPEndPoint(ipAddress, port);

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

                // If either datacenter or rack is null, the method ToManagedString returns null.
                var dcString = datacenter.ToManagedString();
                var rackString = rack.ToManagedString();

                // Create Host instance and add it to the dictionaries.
                host = new Host(address, hostId, dcString, rackString);
                context.AddHost(host);
            }
            catch (Exception ex)
            {
                // Do not throw across FFI boundary - causes undefined behavior.
                // Fail fast to match Rust's panic=abort behavior and make the error obvious.
                Environment.FailFast("Fatal error in AddHostCallback", ex);
            }
        }

        internal void FillHostCache(Metadata.RefreshContext context)
        {
            unsafe
            {
                RunWithIncrement(handle =>
                    cluster_state_fill_nodes(
                        handle,
                        (IntPtr)Unsafe.AsPointer(ref context),
                        (IntPtr)AddHostPtr
                    )
                );
            }

            GC.KeepAlive(context);
        }

        [DllImport("csharp_wrapper", CallingConvention = CallingConvention.Cdecl)]
        unsafe private static extern FFIException cluster_state_get_keyspace_metadata(
            IntPtr clusterState,
            [MarshalAs(UnmanagedType.LPUTF8Str)] string keyspaceName,
            IntPtr contextPtr,
            IntPtr callback,
            IntPtr constructorsPtr);

        private static readonly unsafe delegate* unmanaged[Cdecl]<IntPtr, FFIString, FFISliceRaw, FFISliceRaw, void> FillKeyspaceMetadataPtr = &FillKeyspaceMetadata;
        [UnmanagedCallersOnly(CallConvs = new Type[] { typeof(CallConvCdecl) })]
        private static unsafe void FillKeyspaceMetadata(
            IntPtr contextPtr,
            FFIString strategyClass,
            FFISliceRaw replicationKeys,
            FFISliceRaw replicationValues)
        {
            try
            {
                var keyspaceMeta = Unsafe.AsRef<KeyspaceMetadata>((void*)contextPtr);
                var replication = new Dictionary<string, string>();

                var keys = replicationKeys.As<FFIString>().ToSpan();
                var values = replicationValues.As<FFIString>().ToSpan();

                if (keys.Length != values.Length)
                {
                    Environment.FailFast("Mismatched keyspace replication keys/values lengths from Rust.");
                }

                for (int i = 0; i < keys.Length; i++)
                {
                    replication[keys[i].ToManagedString()] = values[i].ToManagedString();
                }

                keyspaceMeta.FillKeyspaceMetadata(true, strategyClass.ToManagedString(), replication);
            }
            catch (Exception ex)
            {
                // Do not throw across FFI boundary - causes undefined behavior.
                // Fail fast to match Rust's panic=abort behavior and make the error obvious.
                Environment.FailFast("Fatal error in FillKeyspaceMetadataCallback", ex);
            }
        }

        internal void GetKeyspaceMetadata(KeyspaceMetadata ksmd)
        {
            unsafe
            {
                RunWithIncrement(handle =>
                    cluster_state_get_keyspace_metadata(
                        handle,
                        ksmd.Name,
                        (IntPtr)Unsafe.AsPointer(ref ksmd),
                        (IntPtr)FillKeyspaceMetadataPtr,
                        (IntPtr)Globals.ConstructorsPtr
                    )
                );
            }
        }

        [DllImport("csharp_wrapper", CallingConvention = CallingConvention.Cdecl)]
        unsafe private static extern FFIException cluster_state_get_keyspace_names(
            IntPtr clusterState,
            IntPtr keyspaceNameListPtr,
            IntPtr callback);

        private static readonly unsafe delegate* unmanaged[Cdecl]<IntPtr, FFISliceRaw, void> AddKeyspaceNamesPtr = &AddKeyspaceNames;
        [UnmanagedCallersOnly(CallConvs = new Type[] { typeof(CallConvCdecl) })]
        private static unsafe void AddKeyspaceNames(
            IntPtr keyspaceNameListPtr,
            FFISliceRaw keyspaceNames)
        {
            try
            {
                var keyspaceNameList = Unsafe.AsRef<List<string>>((void*)keyspaceNameListPtr);
                foreach (var keyspaceName in keyspaceNames.As<FFIString>().ToSpan())
                {
                    keyspaceNameList.Add(keyspaceName.ToManagedString());
                }
            }
            catch (Exception ex)
            {
                // Do not throw across FFI boundary - causes undefined behavior.
                // Fail fast to match Rust's panic=abort behavior and make the error obvious.
                Environment.FailFast("Fatal error in AddKeyspaceNamesCallback", ex);
            }
        }

        internal List<string> GetKeyspaceNames()
        {
            List<string> keyspaceNames = new List<string>();
            unsafe
            {
                RunWithIncrement(handle =>
                    cluster_state_get_keyspace_names(
                        handle,
                        (IntPtr)Unsafe.AsPointer(ref keyspaceNames),
                        (IntPtr)AddKeyspaceNamesPtr
                    )
                );
            }
            return keyspaceNames;
        }

        [DllImport("csharp_wrapper", CallingConvention = CallingConvention.Cdecl)]
        unsafe private static extern FFIException cluster_state_get_table_names(
            IntPtr clusterState,
            [MarshalAs(UnmanagedType.LPUTF8Str)] string keyspaceName,
            IntPtr tableNameListPtr,
            IntPtr callback,
            IntPtr constructorsPtr);

        private static readonly unsafe delegate* unmanaged[Cdecl]<IntPtr, FFISliceRaw, void> AddTableNamesPtr = &AddTableNames;
        [UnmanagedCallersOnly(CallConvs = new Type[] { typeof(CallConvCdecl) })]
        private static unsafe void AddTableNames(
            IntPtr tableNameListPtr,
            FFISliceRaw tableNames)
        {
            try
            {
                var tableNameList = Unsafe.AsRef<List<string>>((void*)tableNameListPtr);
                foreach (var tableName in tableNames.As<FFIString>().ToSpan())
                {
                    tableNameList.Add(tableName.ToManagedString());
                }
            }
            catch (Exception ex)
            {
                // Do not throw across FFI boundary - causes undefined behavior.
                // Fail fast to match Rust's panic=abort behavior and make the error obvious.
                Environment.FailFast("Fatal error in AddTableNamesCallback", ex);
            }
        }

        internal List<string> GetTableNames(string keyspaceName)
        {
            List<string> tableNames = new List<string>();
            unsafe
            {
                RunWithIncrement(handle =>
                    cluster_state_get_table_names(
                        handle,
                        keyspaceName,
                        (IntPtr)Unsafe.AsPointer(ref tableNames),
                        (IntPtr)AddTableNamesPtr,
                        (IntPtr)Globals.ConstructorsPtr
                    )
                );
            }
            return tableNames;
        }
    }
}