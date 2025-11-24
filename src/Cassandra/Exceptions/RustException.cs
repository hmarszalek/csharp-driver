using System;

namespace Cassandra
{
    public class RustException : DriverException
    {
        public RustException(string message) : base(message, null)
        { }
    }
}
