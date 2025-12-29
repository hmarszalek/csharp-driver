using System.Linq;
using System.Text;
using System.Collections.Generic;
using Cassandra.IntegrationTests.TestBase;
using Cassandra.Tests;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using Assert = NUnit.Framework.Legacy.ClassicAssert;

namespace Cassandra.IntegrationTests.Core
{
    [Category(TestCategory.Short), Category(TestCategory.RealCluster)]
    public class MetadataGetReplicasTests : SharedClusterTest
    {
        // Request a 3-node cluster
        public MetadataGetReplicasTests() : base(3)
        {
        }

        // Track created tables so we can clean them in TearDown
        private readonly List<string> _createdTables = new List<string>();

        [TearDown]
        public override void TearDown()
        {
            // Drop any tables created by tests in this fixture
            foreach (var t in _createdTables)
            {
                try
                {
                    Session.Execute($"DROP TABLE IF EXISTS {KeyspaceName}.{t}");
                }
                catch
                {
                    // best-effort cleanup, ignore failures
                }
            }
            _createdTables.Clear();

            // Run shared teardown behavior
            base.TearDown();
        }

        [Test]
        public void GetReplicas_ReturnsHosts_ForGIvenPartitionKey()
        {
            // Setup: ensure a table exists in the test keyspace and prepare a partition key
            var tableName = "table_test_replicas";
            Session.Execute($"CREATE TABLE {tableName} (id text PRIMARY KEY, value text)");
            _createdTables.Add(tableName);
            // FIXME: Do we need schema agreement here?
            // TestUtils.WaitForSchemaAgreement(Cluster);

            var partitionKey = Encoding.UTF8.GetBytes("key1");

            // Act: request replicas for the partition key
            var replicas = Cluster.Metadata.GetReplicas(KeyspaceName, partitionKey);

            // Assert: expect at least one replica and that each is a known host
            Assert.IsNotNull(replicas, "GetReplicas should not return null");
            Assert.Greater(replicas.Count, 0, "Should return at least one replica");

            var allHosts = Cluster.AllHosts();
            foreach (var replica in replicas)
            {
                Assert.IsNotNull(replica.Host, "Replica host should not be null");
                Assert.IsTrue(allHosts.Any(h => h.Address.Equals(replica.Host.Address)),
                    $"Replica {replica.Host.Address} should be one of the known cluster hosts");
            }
        }

        [Test]
        public void GetReplicas_ReturnsSameReplicas_ForSameKey()
        {
            var givenPartitionKey = Encoding.UTF8.GetBytes("consistent_key");
            var whenReplicasFirst = Cluster.Metadata.GetReplicas(KeyspaceName, givenPartitionKey);
            
            var whenReplicasSecond = Cluster.Metadata.GetReplicas(KeyspaceName, givenPartitionKey);

            Assert.AreEqual(whenReplicasFirst.Count, whenReplicasSecond.Count);
            var addresses1 = whenReplicasFirst.Select(r => r.Host.Address).OrderBy(a => a.ToString()).ToList();
            var addresses2 = whenReplicasSecond.Select(r => r.Host.Address).OrderBy(a => a.ToString()).ToList();
            CollectionAssert.AreEqual(addresses1, addresses2, "Subsequent calls for the same key should return the same replicas");
        }
    }
    
    [TestFixtureSource(nameof(FixtureArgs))]
    public class MetadataGetReplicasNodeCountParamTests : SharedClusterTest
    {
        private readonly int _replicationFactor;
        private readonly int _expectedReplicas;
        // Track created keyspaces for cleanup
        private readonly List<string> _createdKeyspaces = new List<string>();

        // The constructor parameter here drives the number of nodes for the test fixture
        public MetadataGetReplicasNodeCountParamTests(int nodeCount, int replicationFactor, int expectedReplicas) : base(nodeCount)
        {
            _replicationFactor = replicationFactor;
            _expectedReplicas = expectedReplicas;
        }

        public static IEnumerable<object[]> FixtureArgs()
        {
            // Generate combinations for node counts 1 to 5
            for (var nodeCount = 1; nodeCount <= 5; nodeCount++)
            {
                // Test RF from 1 up to nodeCount + 2 to cover cases where RF > NodeCount
                for (var rf = 1; rf <= nodeCount + 2; rf++)
                {
                    var expected = System.Math.Min(rf, nodeCount);
                    yield return new object[] { nodeCount, rf, expected };
                }
            }
        }

        [Test]
        public void GetReplicas_ReturnsCorrectReplicaCount()
        {
            // Setup: create a keyspace with the configured replication factor
            var keyspaceName = $"ks_rf{_replicationFactor}_nodes{AmountOfNodes}";
            Session.Execute($"CREATE KEYSPACE {keyspaceName} WITH replication = {{ 'class' : 'SimpleStrategy', 'replication_factor' : {_replicationFactor} }}");
            _createdKeyspaces.Add(keyspaceName);
            // FIXME: Re-add once WaitForSchemaAgreement is back
            // TestUtils.WaitForSchemaAgreement(Cluster);
            
            // Ensure the driver's metadata reflects the new keyspace
            // FIXME: Re-add once RefreshSchema is back
            Cluster.RefreshSchema();

            // Act: request replicas for a sample partition key
            var partitionKey = Encoding.UTF8.GetBytes("key1");
            var replicas = Cluster.Metadata.GetReplicas(keyspaceName, partitionKey);

            // Assert: the driver reports the expected number of replicas
            Assert.IsNotNull(replicas, "Replicas should not be null");
            Assert.AreEqual(_expectedReplicas, replicas.Count, $"Should return {_expectedReplicas} replicas (RF={_replicationFactor}, nodeCount={AmountOfNodes})");

            // cleanup is handled in TearDown
        }

        [TearDown]
        public void TearDownNodeFixture()
        {
            foreach (var ks in _createdKeyspaces)
            {
                try
                {
                    TestUtils.TryToDeleteKeyspace(Session, ks);
                }
                catch
                {
                    // ignore best-effort cleanup failures
                }
            }
            _createdKeyspaces.Clear();
        }
    }
}
