using System;

namespace Cassandra.RustBridge.Serialization
{
    internal interface ISerializedValues
    {
        IntPtr UseNativeHandle();
    }
}
