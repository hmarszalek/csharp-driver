# CQL data types to C# types

When retrieving the value of a column from a `Row` object, you use a getter based on the type of the column.

CQL data type|C# type
---|---
ascii|string
bigint|long
blob|byte[]
boolean|bool
counter|long
custom|byte[]
date|[LocalDate](date-and-time)
decimal|decimal
double|double
duration|Duration
float|float
inet|IPAddress
int|int
list|IEnumerable&lt;T&gt;
map|IDictionary&lt;K, V&gt;
set|IEnumerable&lt;T&gt;
smallint|short
text|string
time|[LocalTime](date-and-time)
timestamp|[DateTimeOffset](date-and-time)
timeuuid|TimeUuid
tinyint|sbyte
uuid|Guid
varchar|string
varint|BigInteger
vector|[CqlVector](vectors)


* [Date and Time](date-and-time.md)
* [User Defined Types](user-defined-types.md)
* [Nulls and Unset Values](nulls-unset.md)
* [Vectors](vectors.md)

```{eval-rst}
.. toctree::
  :hidden:
  :glob:

  date-and-time
  user-defined-types
  nulls-unset
  vectors
```