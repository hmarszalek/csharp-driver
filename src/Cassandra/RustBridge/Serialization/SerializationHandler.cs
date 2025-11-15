using System;
using System.Collections.Generic;

namespace Cassandra.RustBridge.Serialization
{
    internal static class SerializationHandler
    {
        // NOTE: The native handle must ultimately be consumed by a Rust-side query call
        // (e.g., session_query_with_values). Failing to do so will leak the native
        // PreSerializedValues container and any associated pinned buffers. This is a
        // hard contract of this type.
        internal static ISerializedValues InitializeSerializedValues(IEnumerable<object> values)
        {
            if (values == null) throw new ArgumentNullException(nameof(values));
            
            return new SerializedValuesBuilder().AddMany(values).Build();
        }
    }
}