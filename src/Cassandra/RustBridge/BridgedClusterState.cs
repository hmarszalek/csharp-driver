using System;
using System.Runtime.InteropServices;
using System.Net;
using System.Runtime.CompilerServices;

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
    }
}