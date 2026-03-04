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

using Cassandra.IntegrationTests.TestClusterManagement;
using Cassandra.Tests;
using NUnit.Framework;
using Assert = NUnit.Framework.Legacy.ClassicAssert;
using CollectionAssert = NUnit.Framework.Legacy.CollectionAssert;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Threading;
using Cassandra.IntegrationTests.TestBase;
using SortOrder = Cassandra.DataCollectionMetadata.SortOrder;

namespace Cassandra.IntegrationTests.Core
{
    [TestFixture, Category(TestCategory.Short)]
    public class SchemaMetadataTests : TestGlobals
    {
        private const int DefaultNodeCount = 3;

        [Test]
        public void CheckSimpleStrategyKeyspace()
        {
            ITestCluster testCluster = TestClusterManager.CreateNew(DefaultNodeCount);
            testCluster.InitClient();
            var session = testCluster.Session;
            var metadata = testCluster.Cluster.Metadata;

            string keyspaceName = TestUtils.GetUniqueKeyspaceName().ToLower();
            bool durableWrites = Randomm.Instance.NextBoolean();
            string strategyClass = ReplicationStrategies.SimpleStrategy;
            int replicationFactor = Randomm.Instance.Next(1, DefaultNodeCount + 1);

            session.CreateKeyspace(
                keyspaceName,
                ReplicationStrategies.CreateSimpleStrategyReplicationProperty(replicationFactor),
                durableWrites
            );

            KeyspaceMetadata ksmd = metadata.GetKeyspace(keyspaceName);
            Assert.NotNull(ksmd, $"Keyspace '{keyspaceName}' should exist in cluster metadata");
            Assert.AreEqual(strategyClass, ksmd.StrategyClass);
            Assert.AreEqual(durableWrites, ksmd.DurableWrites);

            // Verify replication settings are present
            Assert.NotNull(ksmd.Replication);
            Assert.True(ksmd.Replication.ContainsKey("replication_factor"));
            Assert.AreEqual(replicationFactor, ksmd.Replication["replication_factor"]);

            var keyspaces = metadata.GetKeyspaces();
            Assert.NotNull(keyspaces, "GetKeyspaces() should not return null");
            Assert.IsTrue(keyspaces.Contains(keyspaceName), $"GetKeyspaces() should contain the newly created keyspace '{keyspaceName}'");
        }

        [Test]
        public void CheckNetworkTopologyStrategyKeyspace()
        {
            ITestCluster testCluster = TestClusterManager.CreateNew(DefaultNodeCount);
            testCluster.InitClient();
            var session = testCluster.Session;
            var metadata = testCluster.Cluster.Metadata;

            string keyspaceName = TestUtils.GetUniqueKeyspaceName().ToLower();
            bool durableWrites = Randomm.Instance.NextBoolean();
            string strategyClass = ReplicationStrategies.NetworkTopologyStrategy;
            int replicationFactor = Randomm.Instance.Next(1, DefaultNodeCount + 1);

            // Use 'replication_factor' as the key for NetworkTopologyStrategy (as per Rust example)
            Dictionary<string, int> replicationSettings = new Dictionary<string, int>(1)
            {
                { "replication_factor", replicationFactor }
            };

            session.CreateKeyspace(
                keyspaceName,
                ReplicationStrategies.CreateNetworkTopologyStrategyReplicationProperty(replicationSettings),
                durableWrites
            );

            KeyspaceMetadata ksmd = metadata.GetKeyspace(keyspaceName);
            Assert.NotNull(ksmd, $"Keyspace '{keyspaceName}' should exist in cluster metadata");
            Assert.AreEqual(strategyClass, ksmd.StrategyClass);
            Assert.AreEqual(durableWrites, ksmd.DurableWrites);

            // Verify replication settings are present
            Assert.NotNull(ksmd.Replication);
            Assert.True(ksmd.Replication.ContainsKey("datacenter1"));
            Assert.AreEqual(replicationFactor, ksmd.Replication["datacenter1"]);

            var keyspaces = metadata.GetKeyspaces();
            Assert.NotNull(keyspaces, "GetKeyspaces() should not return null");
            Assert.IsTrue(keyspaces.Contains(keyspaceName), $"GetKeyspaces() should contain the newly created keyspace '{keyspaceName}'");
        }

        [Test]
        public void SchemaMetadata_GetKeyspaceThatDoesNotExist()
        {
            ITestCluster testCluster = TestClusterManager.CreateNew(DefaultNodeCount);
            testCluster.InitClient();
            var metadata = testCluster.Cluster.Metadata;

            string keyspaceName = TestUtils.GetUniqueKeyspaceName().ToLower();

            KeyspaceMetadata ksmd = metadata.GetKeyspace(keyspaceName);
            Assert.IsNull(ksmd, $"Keyspace '{keyspaceName}' should not exist in cluster metadata");
        }

        [Test]
        public void SchemaMetadata_GetTables()
        {
            ITestCluster testCluster = TestClusterManager.CreateNew(DefaultNodeCount);
            testCluster.InitClient();
            var session = testCluster.Session;

            string keyspaceName = TestUtils.GetUniqueKeyspaceName().ToLower();
            string tableName1 = TestUtils.GetUniqueTableName().ToLower();
            string tableName2 = TestUtils.GetUniqueTableName().ToLower();
            string tableName3 = TestUtils.GetUniqueTableName().ToLower();
            session.CreateKeyspace(
                keyspaceName,
                ReplicationStrategies.CreateSimpleStrategyReplicationProperty(1),
                true
            );

            Assert.NotNull(session.Cluster.Metadata.GetKeyspace(keyspaceName),
                $"Keyspace '{keyspaceName}' should be available in metadata after creation");

            session.Execute($"CREATE TABLE IF NOT EXISTS {keyspaceName}.{tableName1} (a int, b int, c text, primary key (a, b))");
            session.Execute($"CREATE TABLE IF NOT EXISTS {keyspaceName}.{tableName2} (a int, b int, c text, primary key (a, b))");
            session.Execute($"CREATE TABLE IF NOT EXISTS {keyspaceName}.{tableName3} (a int, b int, c text, primary key (a, b))");

            // Fetch tables from metadata
            var tables = session.Cluster.Metadata.GetTables(keyspaceName);
            Assert.IsNotNull(tables);
            Assert.AreEqual(3, tables.Count, $"GetTables() should return 3 tables for keyspace '{keyspaceName}'");
            Assert.IsTrue(tables.Contains(tableName1), $"GetTables() should contain table '{tableName1}'");
            Assert.IsTrue(tables.Contains(tableName2), $"GetTables() should contain table '{tableName2}'");
            Assert.IsTrue(tables.Contains(tableName3), $"GetTables() should contain table '{tableName3}'");

            // Fetch tables from keyspace metadata
            var keyspaceMetadata = session.Cluster.Metadata.GetKeyspace(keyspaceName);
            Assert.IsNotNull(keyspaceMetadata, $"Keyspace metadata for '{keyspaceName}' should not be null");
            var tablesFromKeyspace = keyspaceMetadata.GetTablesNames();
            Assert.IsNotNull(tablesFromKeyspace);
            Assert.AreEqual(3, tablesFromKeyspace.Count, $"GetTablesNames() should return 3 tables for keyspace '{keyspaceName}'");
            Assert.IsTrue(tablesFromKeyspace.Contains(tableName1), $"GetTablesNames() should contain table '{tableName1}'");
            Assert.IsTrue(tablesFromKeyspace.Contains(tableName2), $"GetTablesNames() should contain table '{tableName2}'");
            Assert.IsTrue(tablesFromKeyspace.Contains(tableName3), $"GetTablesNames() should contain table '{tableName3}'");
        }
    }
}
