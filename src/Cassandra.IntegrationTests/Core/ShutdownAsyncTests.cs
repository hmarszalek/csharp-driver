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

using System;
using System.Threading;
using System.Threading.Tasks;
using Cassandra.Tests;
using NUnit.Framework;
using Assert = NUnit.Framework.Legacy.ClassicAssert;

namespace Cassandra.IntegrationTests.Core
{
    [Category(TestCategory.Short), Category(TestCategory.RealCluster)]
    public class ShutdownAsyncTests : SharedClusterTest
    {
        public ShutdownAsyncTests()
            : base(3, createSession: true) { }

        [Test]
        public void Session_NewQueriesFailAfterShutdownBegins()
        {
            var localSession = GetNewTemporarySession(KeyspaceName);
            string tableName = "tbl" + Guid.NewGuid().ToString("N").ToLower();
            CreateTable(localSession, tableName);

            // Prepare cql and statements
            var cqlSelect = $"SELECT * FROM {tableName};";
            var cqlInsert = $"INSERT INTO {tableName} (id, text_sample) VALUES (?, ?);";
            var cqlUse = $"USE {KeyspaceName};";
            var preparedStatementWithoutValues = localSession.Prepare(cqlSelect);
            var preparedStatementWithValues = localSession.Prepare(cqlInsert);

            // Verify session works before shutdown
            Assert.DoesNotThrow(() => localSession.Execute(cqlSelect));
            
            // Begin shutdown
            localSession.ShutdownAsync().Wait();

            // All query attempts should fail with ObjectDisposedException

            // Simple statement without values
            Assert.Throws<ObjectDisposedException>(() =>
                localSession.Execute(cqlSelect)
            );

            // Simple statement with values
            Assert.Throws<ObjectDisposedException>(() =>
                localSession.Execute(new SimpleStatement(cqlInsert, 1, "text"))
            );

            // Use keyspace
            // FIXME: This will be removed once use keyspace is executed with session_query.
            Assert.Throws<ObjectDisposedException>(() =>
                localSession.Execute(cqlUse)
            );

            // Prepare statements
            Assert.Throws<ObjectDisposedException>(() =>
                localSession.Prepare(cqlSelect)
            );
            Assert.Throws<ObjectDisposedException>(() =>
                localSession.Prepare(cqlInsert)
            );

            // Prepared statement without bound values
            Assert.Throws<ObjectDisposedException>(() =>
                localSession.Execute(preparedStatementWithoutValues.Bind())
            );

            // Prepared statement with bound values - NOT SUPPORTED YET
            // Assert.Throws<ObjectDisposedException>(() =>
            //     localSession.Execute(preparedStatementWithValues.Bind(1, "text"))
            // );

            // // GetMetrics
            // Assert.Throws<ObjectDisposedException>(() =>
            //     localSession.GetMetrics()
            // );

            // // WaitForSchemaAgreement
            // Assert.Throws<ObjectDisposedException>(() =>
            //     localSession.WaitForSchemaAgreement((RowSet)null)
            // );

            // // WaitForSchemaAgreement with IPEndPoint
            // Assert.Throws<ObjectDisposedException>(() =>
            //     localSession.WaitForSchemaAgreement(new System.Net.IPEndPoint(System.Net.IPAddress.Loopback, 9042))
            // );
        }

        [Test]
        public void Session_ConcurrentShutdownsAreIdempotent()
        {
            var localCluster = GetNewTemporaryCluster();
            var localSession = localCluster.Connect();

            // Start multiple concurrent shutdowns
            var tasks = new Task[10];
            for (int i = 0; i < tasks.Length; i++)
            {
                tasks[i] = Task.Run(async () => await localSession.ShutdownAsync());
            }

            // All should complete without throwing
            Assert.DoesNotThrow(() => Task.WaitAll(tasks));

            // Session should be disposed after shutdown
            Assert.Throws<ObjectDisposedException>(() =>
                localSession.Execute("SELECT * FROM system.local")
            );
        }

        [Test]
        public void Session_SequentialShutdownsAreIdempotent()
        {
            var localCluster = GetNewTemporaryCluster();
            var localSession = localCluster.Connect();

            // First shutdown
            Assert.DoesNotThrow(() => localSession.ShutdownAsync().Wait());

            // Subsequent shutdowns should also succeed without errors
            for (int i = 0; i < 5; i++)
            {
                Assert.DoesNotThrow(() => localSession.ShutdownAsync().Wait());
            }

            // Verify session is still disposed
            Assert.Throws<ObjectDisposedException>(() =>
                localSession.Execute("SELECT * FROM system.local")
            );
        }

        [Test]
        public void Session_PreparedExecuteDuringShutdown()
        {
            var localSession = GetNewTemporarySession(KeyspaceName);
            string tableName = "tbl" + Guid.NewGuid().ToString("N").ToLower();
            CreateTable(localSession, tableName);

            // Prepare cql and statements
            var cqlSelect = $"SELECT * FROM {tableName};";
            var cqlInsert = $"INSERT INTO {tableName} (id, text_sample) VALUES (?, ?);";
            var cqlUse = $"USE {KeyspaceName};";
            var preparedStatementWithoutValues = localSession.Prepare(cqlSelect);
            var preparedStatementWithValues = localSession.Prepare(cqlInsert);

            var allTasks = new System.Collections.Generic.List<Task>();
            
            using (var shutdownStarted = new ManualResetEventSlim(false))
            {
                for (int i = 0; i < 10; i++)
                {
                    allTasks.Add(
                        Task.Run(() =>
                        {
                            shutdownStarted.Wait();
                            try
                            {
                                // Simple statement without values
                                localSession.Execute(cqlSelect);
                            }
                            catch (ObjectDisposedException) { }
                            catch (AlreadyShutdownException) { }
                        })
                    );
                    allTasks.Add(
                        Task.Run(() =>
                        {
                            shutdownStarted.Wait();
                            try
                            {
                                // Simple statement with values
                                localSession.Execute(new SimpleStatement(cqlInsert, i, "text"));
                            }
                            catch (ObjectDisposedException) { }
                            catch (AlreadyShutdownException) { }
                        })
                    );
                    allTasks.Add(
                        Task.Run(() =>
                        {
                            shutdownStarted.Wait();
                            try
                            {
                                // Use keyspace
                                // FIXME: This will be removed once use keyspace is executed with session_query.
                                localSession.Execute(cqlUse);
                            }
                            catch (ObjectDisposedException) { }
                            catch (AlreadyShutdownException) { }
                        })
                    );
                    allTasks.Add(
                        Task.Run(() =>
                        {
                            shutdownStarted.Wait();
                            try
                            {
                                // Prepare statement
                                localSession.Prepare(cqlSelect);
                            }
                            catch (ObjectDisposedException) { }
                            catch (AlreadyShutdownException) { }
                        })
                    );
                    allTasks.Add(
                        Task.Run(() =>
                        {
                            shutdownStarted.Wait();
                            try
                            {
                                // Prepare statement
                                localSession.Prepare(cqlInsert);
                            }
                            catch (ObjectDisposedException) { }
                            catch (AlreadyShutdownException) { }
                        })
                    );
                    allTasks.Add(
                        Task.Run(() =>
                        {
                            shutdownStarted.Wait();
                            try
                            {
                                // Prepared statement without bound values
                                localSession.Execute(preparedStatementWithoutValues.Bind());
                            }
                            catch (ObjectDisposedException) { }
                            catch (AlreadyShutdownException) { }
                        })
                    );
                    // allTasks.Add(
                    //     Task.Run(() =>
                    //     {
                    //         shutdownStarted.Wait();
                    //         try
                    //         {
                    //             // Prepared statement with bound values - NOT SUPPORTED YET
                    //             localSession.Execute(preparedStatementWithValues.Bind(Guid.NewGuid(), "text"));
                    //         }
                    //         catch (ObjectDisposedException) { }
                    //         catch (AlreadyShutdownException) { }
                    //     })
                    // );
                    // allTasks.Add(
                    //     Task.Run(() =>
                    //     {
                    //         shutdownStarted.Wait();
                    //         try
                    //         {
                    //             // GetMetrics
                    //             localSession.GetMetrics();
                    //         }
                    //         catch (ObjectDisposedException) { }
                    //         catch (AlreadyShutdownException) { }
                    //     })
                    // );
                    // allTasks.Add(
                    //     Task.Run(() =>
                    //     {
                    //         shutdownStarted.Wait();
                    //         try
                    //         {
                    //             // WaitForSchemaAgreement
                    //             localSession.WaitForSchemaAgreement((RowSet)null);
                    //         }
                    //         catch (ObjectDisposedException) { }
                    //         catch (AlreadyShutdownException) { }
                    //     })
                    // );
                    // allTasks.Add(
                    //     Task.Run(() =>
                    //     {
                    //         shutdownStarted.Wait();
                    //         try
                    //         {
                    //             // WaitForSchemaAgreement with IPEndPoint
                    //             localSession.WaitForSchemaAgreement(new System.Net.IPEndPoint(System.Net.IPAddress.Loopback, 9042));
                    //         }
                    //         catch (ObjectDisposedException) { }
                    //         catch (AlreadyShutdownException) { }
                    //     })
                    // );
                }

                shutdownStarted.Set();
                var shutdownTask = Task.Run(() => localSession.ShutdownAsync().Wait());

                // Should complete without deadlock or crash
                Assert.DoesNotThrow(() =>
                {
                    Task.WaitAll(allTasks.ToArray());
                    shutdownTask.Wait();
                });
            }
        }

        //////////////////////////////
        // Test Helpers
        //////////////////////////////

        private void CreateTable(ISession session, string tableName)
        {
            var cql = $@"CREATE TABLE {tableName} (
                    id int PRIMARY KEY,
                    text_sample text
                );";

            session.Execute(cql, session.Cluster.Configuration.QueryOptions.GetConsistencyLevel());
        }
    }
}