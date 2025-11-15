using System;
using System.Collections.Generic;
using Cassandra.RustBridge.Serialization;

namespace Cassandra.RustBridge.Serialization;

internal sealed class BorrowedSerializedValues : AbstractSerializedValues
{
    private BorrowedSerializedValues()
    {
        // NOTE: The native handle must ultimately be consumed by a Rust-side query call
        // (e.g., session_query_with_values). Failing to do so will leak the native
        // PreSerializedValues container and any associated pinned buffers. This is a
        // hard contract of this type.
        NativeHandle = pre_serialized_values_borrowed_new();
        if (NativeHandle == IntPtr.Zero) throw new InvalidOperationException("pre_serialized_values_borrowed_new returned null");
    }

    internal static ISerializedValues Build(IEnumerable<object> values)
    {
        var inst = new BorrowedSerializedValues();
        inst.AddMany(values);
        return inst;
    }
}