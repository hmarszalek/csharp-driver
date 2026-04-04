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


namespace Cassandra
{
    /// <summary>
    /// Specifies the different protocol versions and provides methods (via extension methods) to check whether a
    /// feature is supported in an specific version
    /// </summary>
    public enum ProtocolVersion : byte
    {
        /// <summary>
        /// Cassandra protocol v1, supported in Apache Cassandra 1.2-->2.2.
        /// </summary>
        V1 = 0x01,

        /// <summary>
        /// Cassandra protocol v2, supported in Apache Cassandra 2.0-->2.2.
        /// </summary>
        V2 = 0x02,

        /// <summary>
        /// Cassandra protocol v3, supported in Apache Cassandra 2.1-->3.x.
        /// </summary>
        V3 = 0x03,

        /// <summary>
        /// Cassandra protocol v4, supported in ScyllaDB (all versions) and Apache Cassandra (2.2-->3.x).
        /// </summary>
        V4 = 0x04,

        /// <summary>
        /// Cassandra protocol v5, in beta from Apache Cassandra 3.x+. Currently not supported by the driver.
        /// </summary>
        V5 = 0x05,

        /// <summary>
        /// The higher protocol version that is supported by this driver.
        /// <para>When acquiring the first connection, it will use this version to start protocol negotiation.</para>
        /// </summary>
        MaxSupported = V4,
        /// <summary>
        /// The lower protocol version that is supported by this driver.
        /// </summary>
        MinSupported = V4
    }

    internal static class ProtocolVersionExtensions
    {
        /// <summary>
        /// Determines if the protocol version is supported by this driver.
        /// </summary>
        public static bool IsSupported(this ProtocolVersion version, Configuration config)
        {
            if (version.IsBeta())
            {
                return config.AllowBetaProtocolVersions;
            }

            switch (version)
            {
                case ProtocolVersion.V4:
                    return true;
                default:
                    return false;
            }
        }

        /// <summary>
        /// Determines if the protocol supports result_metadata_id on PREPARED response and EXECUTE request.
        /// </summary>
        public static bool SupportsResultMetadataId(this ProtocolVersion version)
        {
            return version >= ProtocolVersion.V5;
        }

        /// <summary>
        /// Determines if the protocol supports to send the Keyspace as part of the PREPARE, QUERY and BATCH.
        /// </summary>
        public static bool SupportsKeyspaceInRequest(this ProtocolVersion version)
        {
            return version >= ProtocolVersion.V5;
        }

        /// <summary>
        /// Determines if the protocol supports sending driver information in the STARTUP request.
        /// </summary>
        public static bool SupportsDriverInfoInStartup(this ProtocolVersion version)
        {
            return version >= ProtocolVersion.V5;
        }

        /// <summary>
        /// Determines if the protocol provides a map of reasons as part of read_failure and write_failure.
        /// </summary>
        public static bool SupportsFailureReasons(this ProtocolVersion version)
        {
            return version >= ProtocolVersion.V5;
        }

        /// <summary>
        /// Determines whether the QUERY, EXECUTE and BATCH flags are encoded using 32 bits.
        /// </summary>
        public static bool Uses4BytesQueryFlags(this ProtocolVersion version)
        {
            return version >= ProtocolVersion.V5;
        }

        public static bool IsBeta(this ProtocolVersion version)
        {
            return version == ProtocolVersion.V5;
        }
    }
}
