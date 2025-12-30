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

using System.Linq;
using System.Net;
using System.Threading;
using Cassandra.Tests;
using NUnit.Framework;
using Assert = NUnit.Framework.Legacy.ClassicAssert;

namespace Cassandra.IntegrationTests.Core
{
    [Category(TestCategory.Short), Category(TestCategory.RealCluster)]
    public class MetadataTests : SharedClusterTest
    {
        public MetadataTests() : base(3, true)
        {
        }

        [Test]
        public void Cluster_AllHosts_Should_Return_All_Cluster_Hosts()
        {
            var hosts = Cluster.AllHosts();
            Assert.NotNull(hosts, "AllHosts() should not return null");
            Assert.AreEqual(hosts.Count, 3, "AllHosts() should return the same number of hosts as the cluster size");
            foreach (var host in hosts)
            {
                Assert.NotNull(host, "Host should not be null");
                Assert.NotNull(host.Address, "Host.Address should not be null");
            }

            // Verify Uniqueness
            var uniqueHostIds = hosts.Select(h => h.HostId).Distinct().Count();
            Assert.AreEqual(hosts.Count, uniqueHostIds, "Each host should have a unique HostId");
            
            var uniqueAddresses = hosts.Select(h => h.Address).Distinct().Count();
            Assert.AreEqual(hosts.Count, uniqueAddresses, "Each host should have a unique Address");
        }

        [Test]
        public void Cluster_GetHost_Should_Return_Host_By_Address()
        {
            var allHosts = Cluster.AllHosts();
            Assert.Greater(allHosts.Count, 0, "Need at least one host for this test");
            
            var expectedHost = allHosts.First();
            var address = expectedHost.Address;
            
            var retrievedHost = Cluster.GetHost(address);
            
            Assert.NotNull(retrievedHost, "GetHost() should not return null for a valid address");
            
            // Verify identity and content
            Assert.AreEqual(expectedHost.Address, retrievedHost.Address, "Addresses should match");
            Assert.AreEqual(expectedHost.HostId, retrievedHost.HostId, "HostIds should match");
            Assert.AreEqual(expectedHost.Datacenter, retrievedHost.Datacenter, "Datacenters should match");
            Assert.AreEqual(expectedHost.Rack, retrievedHost.Rack, "Racks should match");
        }

        [Test]
        public void Cluster_GetHost_Should_Return_Null_For_Invalid_Address()
        {
            var invalidAddress = new IPEndPoint(IPAddress.Parse("192.0.2.1"), 9042);
            var host = Cluster.GetHost(invalidAddress);
            Assert.Null(host, "GetHost() should return null for non-existent address");
        }

        [Test]
        public void Metadata_AllHosts_Should_Return_All_Cluster_Hosts()
        {
            var metadata = Cluster.Metadata;
            var hosts = metadata.AllHosts();
            Assert.NotNull(hosts, "Metadata.AllHosts() should not return null");
            Assert.AreEqual(3, hosts.Count, "Metadata.AllHosts() should return correct number of hosts");
            
            var clusterHosts = Cluster.AllHosts();
            Assert.AreEqual(clusterHosts.Count, hosts.Count,
                "Metadata.AllHosts() should return same count as Cluster.AllHosts()");
        }

        [Test]
        public void Metadata_GetHost_Should_Return_Host_By_Address()
        {
            var metadata = Cluster.Metadata;
            var allHosts = metadata.AllHosts();
            Assert.Greater(allHosts.Count, 0, "Need at least one host for this test");
            var firstHost = allHosts.First();
            var address = firstHost.Address;
            var retrievedHost = metadata.GetHost(address);
            Assert.NotNull(retrievedHost, "Metadata.GetHost() should not return null for a valid address");
            Assert.AreEqual(address, retrievedHost.Address, "Retrieved host should have the same address");
        }

        [Test]
        public void Metadata_GetHost_Should_Return_Null_For_Invalid_Address()
        {
            var metadata = Cluster.Metadata;
            var invalidAddress = new IPEndPoint(IPAddress.Parse("192.0.2.1"), 9042);
            var host = metadata.GetHost(invalidAddress);
            Assert.Null(host, "Metadata.GetHost() should return null for non-existent address");
        }

        [Test]
        public void Cluster_And_Metadata_Should_Return_Same_Hosts()
        {
            var clusterHosts = Cluster.AllHosts().OrderBy(h => h.Address.ToString()).ToList();
            var metadataHosts = Cluster.Metadata.AllHosts().OrderBy(h => h.Address.ToString()).ToList();
            Assert.AreEqual(clusterHosts.Count, metadataHosts.Count,
                "Cluster and Metadata should return same number of hosts");

            for (int i = 0; i < clusterHosts.Count; i++)
            {
                Assert.AreEqual(clusterHosts[i].Address, metadataHosts[i].Address,
                    "Cluster and Metadata should return hosts with same addresses");
            }
        }
    }
}
