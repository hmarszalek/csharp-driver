# Getting Started

## Basic Usage

```csharp
// Configure the builder with your cluster's contact points
var cluster = Cluster.Builder()
                     .AddContactPoints("host1")
                     .Build();

// Connect to the nodes using a keyspace
var session = cluster.Connect("sample_keyspace");

// Execute a query on a connection synchronously
var rs = session.Execute("SELECT * FROM sample_table");

// Iterate through the RowSet
foreach (var row in rs)
{
    var value = row.GetValue<int>("sample_int_column");

    // Do something with the value
}
```

### Prepared statements

Prepare your query **once** and bind different parameters to obtain best performance.

```csharp
// Prepare a statement once
var ps = session.Prepare("UPDATE user_profiles SET birth=? WHERE key=?");

// ...bind different parameters every time you need to execute
var statement = ps.Bind(new DateTime(1942, 11, 27), "hendrix");
// Execute the bound statement with the provided parameters
session.Execute(statement);
```

### Batching statements

You can execute multiple statements (prepared or unprepared) in a batch to update/insert several rows atomically even in different column families.

```csharp
// Prepare the statements involved in a profile update once
var profileStmt = session.Prepare("UPDATE user_profiles SET email=? WHERE key=?");
var userTrackStmt = session.Prepare("INSERT INTO user_track (key, text, date) VALUES (?, ?, ?)");
// ...you should reuse the prepared statement
// Bind the parameters and add the statement to the batch batch
var batch = new BatchStatement()
  .Add(profileStmt.Bind(emailAddress, "hendrix"))
  .Add(userTrackStmt.Bind("hendrix", "You changed your email", DateTime.Now));
// Execute the batch
session.Execute(batch);
```

### Asynchronous API

Session allows asynchronous execution of statements (for any type of statement: simple, bound or batch) by exposing the `ExecuteAsync` method.

```csharp
// Execute a statement asynchronously using await
var rs = await session.ExecuteAsync(statement);
```

### Avoid boilerplate mapping code

The driver features a built-in [Mapper][mapper] and [Linq][linq] components that can use to avoid boilerplate mapping code between cql rows and your application entities.

```csharp
User user = mapper.Single<User>("SELECT name, email FROM users WHERE id = ?", userId);
```

See the [driver components documentation][components] for more information.

### Automatic pagination of results

You can iterate indefinitely over the `RowSet`, having the rows fetched block by block until the rows available on the client side are exhausted.

```csharp
var statement = new SimpleStatement("SELECT * from large_table");
// Set the page size, in this case the RowSet will not contain more than 1000 at any time
statement.SetPageSize(1000);
var rs = session.Execute(statement);
foreach (var row in rs)
{
  // The enumerator will yield all the rows from Cassandra
  // Retrieving them in the back in blocks of 1000.
}
```

### User defined types mapping

You can map your [Cassandra User Defined Types][udt] to your application entities.

For a given udt

```cql
CREATE TYPE address (
  street text,
  city text,
  zip_code int,
  phones set<text>
);
```

For a given class

```csharp
public class Address
{
  public string Street { get; set; }
  public string City { get; set; }
  public int ZipCode { get; set; }
  public IEnumerable<string> Phones { get; set;}
}
```

You can either map the properties by name

```csharp
// Map the properties by name automatically
session.UserDefinedTypes.Define(
  UdtMap.For<Address>()
);
```

Or you can define the properties manually

```csharp
session.UserDefinedTypes.Define(
  UdtMap.For<Address>()
    .Map(a => a.Street, "street")
    .Map(a => a.City, "city")
    .Map(a => a.ZipCode, "zip_code")
    .Map(a => a.Phones, "phones")
);
```

You should **map your [UDT][udt] to your entity once** and you will be able to use that mapping during all your application lifetime.

```csharp
var rs = session.Execute("SELECT id, name, address FROM users where id = x");
var row = rs.First();
// You can retrieve the field as a value of type Address
var userAddress = row.GetValue<Address>("address");
Console.WriteLine("user lives on {0} Street", userAddress.Street);
```

### Setting cluster and statement execution options

You can set the options on how the driver connects to the nodes and the execution options.

```csharp
// Example at cluster level
var cluster = Cluster
  .Builder()
  .AddContactPoints(hosts)
  .WithCompression(CompressionType.LZ4)
  .WithLoadBalancingPolicy(new DCAwareRoundRobinPolicy("west"));

// Example at statement (simple, bound, batch) level
var statement = new SimpleStatement(query)
  .SetConsistencyLevel(ConsistencyLevel.Quorum)
  .SetRetryPolicy(DowngradingConsistencyRetryPolicy.Instance)
  .SetPageSize(1000);
```


## Examples

Here's some ways to find examples on how to use the ScyllaDB C# Driver for Scylla.

Also you might be interested in our developer guide: [Developing applications with ScyllaDB drivers][dev-guide].

### Driver's Github repository

You can also find several examples on the [driver's Github repository][driver-github]. You can read the `README.md` file for a brief summary of each example.

[dev-guide]:  https://docs.scylladb.com/stable/get-started/develop-with-scylladb/index.html
[driver-github]: https://github.com/scylladb/csharp-driver/tree/master/examples
[linq]: https://csharp-driver.docs.scylladb.com/stable/topics/using/components/linq/index.html
[mapper]: https://csharp-driver.docs.scylladb.com/stable/topics/using/components/mapper/index.html
[udt]: https://docs.datastax.com/en/cql-oss/3.x/cql/cql_using/useCreateUDT.html
