using System;
using System.Collections.Generic;
using System.Runtime.InteropServices;

#nullable enable

namespace Cassandra.RustBridge.Serialization
{
    /// <summary>
    /// Abstract base that implements common lifecycle and additive logic for serialized values containers.
    /// Derived classes provide concrete implementations for how values are recorded (eager copy vs deferred / pinned) and
    /// how the native container is materialized on detach.
    /// </summary>
    internal abstract class AbstractSerializedValues : ISerializedValues
    {
        protected IntPtr NativeHandle;

        [DllImport("csharp_wrapper", CallingConvention = CallingConvention.Cdecl)]
        unsafe private static extern IntPtr pre_serialized_values_new();

        [DllImport("csharp_wrapper", CallingConvention = CallingConvention.Cdecl)]
        unsafe protected static extern IntPtr pre_serialized_values_borrowed_new();

        [DllImport("csharp_wrapper", CallingConvention = CallingConvention.Cdecl)]
        unsafe private static extern FfiError pre_serialized_values_add_value(
            IntPtr builderPtr,
            IntPtr valuePtr,
            UIntPtr valueLen,
            IntPtr handlePtr,
            IntPtr unpinCallbackPtr);

        [DllImport("csharp_wrapper", CallingConvention = CallingConvention.Cdecl)]
        unsafe private static extern FfiError pre_serialized_values_add_null(IntPtr builderPtr);

        [DllImport("csharp_wrapper", CallingConvention = CallingConvention.Cdecl)]
        unsafe private static extern FfiError pre_serialized_values_add_unset(IntPtr builderPtr);

        protected static IntPtr PreSerializedValuesNew() => pre_serialized_values_new();

        protected static IntPtr PreSerializedValuesBorrowedNew() => pre_serialized_values_borrowed_new();

        private static void PreSerializedValuesAddNull(IntPtr builderPtr)
        {
            FfiErrorHelpers.ExecuteAndThrowIfFails(() => pre_serialized_values_add_null(builderPtr));
        }

        private static void PreSerializedValuesAddUnset(IntPtr builderPtr)
        {
            FfiErrorHelpers.ExecuteAndThrowIfFails(() => pre_serialized_values_add_unset(builderPtr));
        }

        private static void PreSerializedValuesAddValue(
            IntPtr builderPtr,
            IntPtr valuePtr,
            UIntPtr valueLen,
            IntPtr handlePtr,
            IntPtr unpinCallbackPtr)
        {
            FfiErrorHelpers.ExecuteAndThrowIfFails(
                () => pre_serialized_values_add_value(builderPtr, valuePtr, valueLen, handlePtr, unpinCallbackPtr));
        }

        // Static unmanaged function pointer to our callback method. This avoids the
        // per-call cost of Marshal.GetFunctionPointerForDelegate and matches the
        // RustBridge static delegate* pattern.
        private static readonly unsafe delegate* unmanaged[Cdecl]<IntPtr, void> UnpinCallbackPtr = &RustBridge.UnpinSerializedBuffer;

        private void AddValue(byte[] bytes)
        {
            AddBytesImpl(bytes);
        }

        private void AddNull()
        {
            PreSerializedValuesAddNull(NativeHandle);
        }

        private void AddUnset()
        {
            PreSerializedValuesAddUnset(NativeHandle);
        }

        protected void AddMany(IEnumerable<object?> values)
        {
            foreach (var v in values)
            {
                if (v == null) { AddNull(); continue; }
                if (ReferenceEquals(v, Unset.Value)) { AddUnset(); continue; }

                if (v is not byte[] bytes) throw new ArgumentException("Value must be null, Unset.Value or a pre-serialized byte[].", nameof(values));
                AddValue(bytes);
            }
        }

        private void AddBytesImpl(byte[] bytes)
        {
            var gch = GCHandle.Alloc(bytes, GCHandleType.Pinned);
            unsafe
            {
                PreSerializedValuesAddValue(
                    NativeHandle,
                    gch.AddrOfPinnedObject(),
                    (UIntPtr)bytes.Length,
                    GCHandle.ToIntPtr(gch),
                    (IntPtr)UnpinCallbackPtr
                );
            }
        }

        public IntPtr UseNativeHandle()
        {
            IntPtr handle = NativeHandle;
            NativeHandle = IntPtr.Zero; // Prevent double detach
            return handle;
        }
    }
}