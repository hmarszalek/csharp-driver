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
            IntPtr replicationOptionsPtr,
            StrategyAddRepFactorCallbacks addRepFactorCallbacks,
            IntPtr callback,
            IntPtr constructorsPtr);

        private static readonly unsafe delegate* unmanaged[Cdecl]<IntPtr, nuint, FFIException> SimpleStrategyAddRepFactorPtr = &SimpleStrategyAddRepFactor;
        [UnmanagedCallersOnly(CallConvs = new Type[] { typeof(CallConvCdecl) })]
        private static unsafe FFIException SimpleStrategyAddRepFactor(
            IntPtr replicationOptionsPtr,
            nuint repFactor)
        {
            try
            {
                var replicationOptions = Unsafe.AsRef<Dictionary<string, string>>((void*)replicationOptionsPtr);
                replicationOptions["replication_factor"] = repFactor.ToString();
            }
            catch (Exception ex)
            {
                return FFIException.FromException(ex);
            }

            return FFIException.Ok();
        }

        private static readonly unsafe delegate* unmanaged[Cdecl]<IntPtr, FFIString, nuint, FFIException> NetworkTopologyStrategyAddRepFactorPtr = &NetworkTopologyStrategyAddRepFactor;
        [UnmanagedCallersOnly(CallConvs = new Type[] { typeof(CallConvCdecl) })]
        private static unsafe FFIException NetworkTopologyStrategyAddRepFactor(
            IntPtr replicationOptionsPtr,
            FFIString datacenter,
            nuint repFactor)
        {
            try
            {
                var replicationOptions = Unsafe.AsRef<Dictionary<string, string>>((void*)replicationOptionsPtr);
                replicationOptions[datacenter.ToManagedString()] = repFactor.ToString();
            }
            catch (Exception ex)
            {
                return FFIException.FromException(ex);
            }

            return FFIException.Ok();
        }

        private static readonly unsafe delegate* unmanaged[Cdecl]<IntPtr, FFIString, FFIString, FFIException> OtherStrategyAddRepFactorPtr = &OtherStrategyAddRepFactor;
        [UnmanagedCallersOnly(CallConvs = new Type[] { typeof(CallConvCdecl) })]
        private static unsafe FFIException OtherStrategyAddRepFactor(
            IntPtr replicationOptionsPtr,
            FFIString strategyClass,
            FFIString repFactor)
        {
            try
            {
                var replicationOptions = Unsafe.AsRef<Dictionary<string, string>>((void*)replicationOptionsPtr);
                replicationOptions[strategyClass.ToManagedString()] = repFactor.ToManagedString();
            }
            catch (Exception ex)
            {
                return FFIException.FromException(ex);
            }

            return FFIException.Ok();
        }

        [StructLayout(LayoutKind.Sequential)]
        private unsafe readonly struct StrategyAddRepFactorCallbacks
        {
            public readonly IntPtr SimpleStrategyCallback;
            public readonly IntPtr NetworkTopologyStrategyCallback;
            public readonly IntPtr OtherStrategyCallback;

            public StrategyAddRepFactorCallbacks()
            {
                SimpleStrategyCallback = (IntPtr)SimpleStrategyAddRepFactorPtr;
                NetworkTopologyStrategyCallback = (IntPtr)NetworkTopologyStrategyAddRepFactorPtr;
                OtherStrategyCallback = (IntPtr)OtherStrategyAddRepFactorPtr;
            }
        }

        private static readonly unsafe delegate* unmanaged[Cdecl]<IntPtr, FFIBool, FFIString, IntPtr, FFIException> FillKeyspaceMetadataPtr = &FillKeyspaceMetadata;
        [UnmanagedCallersOnly(CallConvs = new Type[] { typeof(CallConvCdecl) })]
        private static unsafe FFIException FillKeyspaceMetadata(
            IntPtr contextPtr,
            FFIBool durableWrites,
            FFIString strategyClass,
            IntPtr replicationOptionsPtr)
        {
            try
            {
                var keyspaceMeta = Unsafe.AsRef<KeyspaceMetadata>((void*)contextPtr);
                var replication = Unsafe.AsRef<Dictionary<string, string>>((void*)replicationOptionsPtr);

                keyspaceMeta.FillKeyspaceMetadata(durableWrites, strategyClass.ToManagedString(), replication);
            }
            catch (Exception ex)
            {
                return FFIException.FromException(ex);
            }

            return FFIException.Ok();
        }

        internal KeyspaceMetadata GetKeyspaceMetadata(Metadata parent, string keyspaceName)
        {
            var ksmd = new KeyspaceMetadata(parent, keyspaceName);
            var replicationOptions = new Dictionary<string, string>();
            var addRepFactorCallbacks = new StrategyAddRepFactorCallbacks();
            try
            {
                unsafe
                {
                    RunWithIncrement(handle =>
                        cluster_state_get_keyspace_metadata(
                            handle,
                            keyspaceName,
                            (IntPtr)Unsafe.AsPointer(ref ksmd),
                            (IntPtr)Unsafe.AsPointer(ref replicationOptions),
                            addRepFactorCallbacks,
                            (IntPtr)FillKeyspaceMetadataPtr,
                            (IntPtr)Globals.ConstructorsPtr
                        )
                    );
                }
            }
            catch (InvalidArgumentException)
            {
                // If the keyspace was not found return null.
                return null;
            }

            return ksmd;
        }

        [DllImport("csharp_wrapper", CallingConvention = CallingConvention.Cdecl)]
        unsafe private static extern FFIException cluster_state_get_keyspace_names(
            IntPtr clusterState,
            IntPtr keyspaceNameListPtr,
            IntPtr callback);

        private static readonly unsafe delegate* unmanaged[Cdecl]<IntPtr, FFIString, FFIException> AddKeyspaceNamePtr = &AddKeyspaceName;
        [UnmanagedCallersOnly(CallConvs = new Type[] { typeof(CallConvCdecl) })]
        private static unsafe FFIException AddKeyspaceName(
            IntPtr keyspaceNameListPtr,
            FFIString keyspaceName)
        {
            try
            {
                var keyspaceNameList = Unsafe.AsRef<List<string>>((void*)keyspaceNameListPtr);
                keyspaceNameList.Add(keyspaceName.ToManagedString());
            }
            catch (Exception ex)
            {
                return FFIException.FromException(ex);
            }

            return FFIException.Ok();
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
                        (IntPtr)AddKeyspaceNamePtr
                    )
                );
            }
            return keyspaceNames;
        }
    }
}