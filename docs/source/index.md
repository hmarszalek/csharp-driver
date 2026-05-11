# ScyllaDB C# RS Driver

This book contains documentation for csharp-rs-driver - an API-compatible rewrite of [csharp-driver](https://github.com/scylladb/csharp-driver) as a wrapper over [ScyllaDB Rust Driver](https://github.com/scylladb/scylla-rust-driver). Although optimized for ScyllaDB, the driver is also compatible with [Apache Cassandra®](https://cassandra.apache.org/).

The driver targets .NET Standard 2.0. For more detailed information about platform compatibility, check [this section](#compatibility).

## Features

- Sync and [Async API](topics/getting-started.md#asynchronous-api)
- Simple, [Prepared](topics/using/statements/prepared), and [Batch](topics/using/statements/batch) statements
- Asynchronous IO, parallel execution, request pipelining
- Connection pooling
- Auto node discovery
- Automatic reconnection
- Configurable [load balancing][policies] and [retry policies][policies]
- Works with any cluster size
- [Linq2Cql][linq] and Ado.Net support

## ScyllaDB features

- Shard awarness
- Tablet awareness
- LWT prepared statements metadata mark

## Documentation

- [Documentation index][docindex]
- [API docs][apidocs]
- [FAQ][faq]
- [Developing applications with ScyllaDB drivers][dev-guide]

## Getting Help

You can create a ticket on the [Github Issues][github-issues]. Additionally, you can ask questions on [ScyllaDB Community][community].

## Upgrading from previous versions

If you are upgrading from previous versions of the driver, [visit the Upgrade Guide][upgrade-guide].


## Compatibility

- ScyllaDB 2025.1 and above.
- ScyllaDB 5.x and above.
- ScyllaDB Enterprise 2021.x and above.
- The driver targets .NET Standard 2.0

Here is a list of platforms and .NET targets that Datastax uses when testing this driver:

|  Platform             | net6 | net7 | net8  |
|-----------------------|------|------|-------|
| Windows Server 2019³  | ✓²  |  ✓¹ |  ✓   |
| Ubuntu 18.04          | ✓   |  ✓  |  ✓   |

¹ No tests are run for the `net7` target on the Windows platform but `net7` is still considered fully supported.

² Only unit tests are ran for the `net6` target on the windows platform but `net6` is still considered fully supported.

³ Appveyor's `Visual Studio 2022` image is used for these tests.

Note: Big-endian systems are not supported.

## License

© DataStax, Inc.

Licensed under the Apache License, Version 2.0 (the “License”); you may not use this file except in compliance with the License. You may obtain a copy of the License at

<http://www.apache.org/licenses/LICENSE-2.0>

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an “AS IS” BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.

[apidocs]: api-reference
[docindex]: https://csharp-driver.docs.scylladb.com/stable/
[faq]: https://csharp-driver.docs.scylladb.com/stable/faq/index.html
[github-issues]: https://github.com/scylladb/csharp-driver/issues
[poco]: http://en.wikipedia.org/wiki/Plain_Old_CLR_Object
[linq]: https://csharp-driver.docs.scylladb.com/stable/features/components/linq/index.html
[components]: https://csharp-driver.docs.scylladb.com/stable/features/components/index.html
[policies]: https://csharp-driver.docs.scylladb.com/stable/features/tuning-policies/index.html
[upgrade-guide]: https://csharp-driver.docs.scylladb.com/stable/upgrade-guide/index.html
[community]: https://forum.scylladb.com/
[implicit]: https://docs.microsoft.com/en-us/dotnet/csharp/language-reference/keywords/implicit
[dynamic]: https://msdn.microsoft.com/en-us/library/dd264736.aspx
[dev-guide]: https://docs.scylladb.com/stable/get-started/develop-with-scylladb/index.html
