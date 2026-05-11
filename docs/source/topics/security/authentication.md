# Authentication

If you are using the `PasswordAuthenticator` which is included in the default distribution of Apache Cassandra, you can use the `Builder.WithCredentials` method or you can explicitly create a `PlainTextAuthProvider` instance.

To configure a provider, pass it when initializing the cluster:

```csharp
using Cassandra;
```

```csharp
ICluster cluster = Cluster.Builder()
    .AddContactPoint("127.0.0.1")
    .WithAuthProvider(new PlainTextAuthProvider())
    .Build();
```