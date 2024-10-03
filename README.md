# otf: A little Delta Lake/Iceberg inspired database implementation in Go

Only supports CREATE TABLE, INSERTs and SELECTs at the moment. Take a
look at the tests for examples of usage and concurrency control.

See the [blog post](https://notes.eatonphil.com/2024-09-29-build-a-serverless-acid-database-with-this-one-neat-trick.html) walking through this project.

```
$ go test
```

See also:

* [The Delta Lake Paper](https://www.vldb.org/pvldb/vol13/p3411-armbrust.pdf)
