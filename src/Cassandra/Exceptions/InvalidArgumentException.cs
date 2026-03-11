using System;
using System.Runtime.InteropServices;
using System.Runtime.CompilerServices;

namespace Cassandra
{
    public class InvalidArgumentException : DriverException
    {
        public InvalidArgumentException(string message) : base(message, null)
        { }

        [UnmanagedCallersOnly(CallConvs = new Type[] { typeof(CallConvCdecl) })]
        internal static IntPtr InvalidArgumentExceptionFromRust(RustBridge.FFIString message)
        {
            string msg = message.ToManagedString();

            var exception = new InvalidArgumentException(msg);

            GCHandle handle = GCHandle.Alloc(exception);
            IntPtr handlePtr = GCHandle.ToIntPtr(handle);
            return handlePtr;
        }
    }
}
