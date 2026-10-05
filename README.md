# 08-iceberg

A lightweight Apache Iceberg + Spark demo project built with Scala and Maven. It shows how to create, read, and maintain Iceberg tables on top of a Hive/Hadoop environment using Spark SQL.

## Overview

This project demonstrates the core features of Apache Iceberg in a Spark local development environment:

- Create Iceberg tables with Spark DataFrame APIs
- Read data from Iceberg tables
- Query historical snapshots using time travel
- Read incremental data between snapshots
- Maintain tables by expiring snapshots, deleting orphan files, and rewriting small files

The example code is organized under `src/main/scala/com/itbys/app` and uses the following table catalogs:

- `iceberg_hive` -> Hive catalog
- `iceberg_hadoop` -> Hadoop catalog

## Project structure

```text
08-iceberg/
├── input/
│   ├── config.properties
│   └── hive-site.xml
├── src/
│   ├── main/
│   │   ├── resources/
│   │   │   ├── hive-site.xml
│   │   │   └── log4j.properties
│   │   └── scala/
│   │       └── com/itbys/app/
│   │           ├── _01_Write.scala
│   │           ├── _01_Read.scala
│   │           └── _03_Maintain.scala
│   └── test/
│       └── java/
│           ├── TestUnit.java
│           └── TestSca.scala
├── pom.xml
├── .gitignore
└── README.md
```

## Tech stack

- Scala 2.12
- Spark 3.3.1
- Apache Iceberg 1.1.0
- Hadoop / Hive metastore integration
- Maven build tool

## Prerequisites

Before running the examples, prepare the following environment:

- JDK 8+
- Maven 3.x
- Apache Hadoop + HDFS
- Hive metastore (or compatible catalog service)
- Spark local runtime or cluster runtime

This project contains hard-coded connection settings such as:

- Hadoop warehouse: `hdfs://hadoop1:8020/warehouse/spark-iceberg`
- Hive metastore thrift URI: `thrift://hadoop1:9083`

These values are examples and should be adjusted to match your actual cluster configuration.

## Build

```bash
mvn clean package
```

The Maven build includes:

- `scala-maven-plugin` for Scala compilation
- `maven-assembly-plugin` to generate a fat JAR with dependencies

## Run examples

After packaging, run the classes with `spark-submit` or a local Spark shell.

### 1) Write table example

```bash
spark-submit \
  --class com.itbys.app._01_Write \
  target/08-iceberg-1.0-SNAPSHOT-jar-with-dependencies.jar
```

This example does the following:

- creates a Spark session
- configures the Hive and Hadoop catalogs
- creates an Iceberg table using `writeTo(...).create()`
- configures table properties
- writes and appends data
- demonstrates partition overwrite and partitioned writes

### 2) Read table example

```bash
spark-submit \
  --class com.itbys.app._01_Read \
  target/08-iceberg-1.0-SNAPSHOT-jar-with-dependencies.jar
```

This example shows:

- reading an Iceberg table via `format("iceberg")`
- snapshot query with `as-of-timestamp`
- snapshot query with `snapshot-id`
- incremental query using `start-snapshot-id` and `end-snapshot-id`

### 3) Maintenance example

```bash
spark-submit \
  --class com.itbys.app._03_Maintain \
  target/08-iceberg-1.0-SNAPSHOT-jar-with-dependencies.jar
```

This example demonstrates table maintenance tasks:

- expire old snapshots
- delete orphan files
- rewrite small data files using `SparkActions`

## Key code behavior

### Write path (`_01_Write.scala`)

The script creates a sample dataset:

```scala
case class Sample(id: Int, data: String, category: String)
```

Then it writes to an Iceberg table via:

```scala
df.writeTo("iceberg_hadoop.default.table1").create()
```

It also demonstrates:

```scala
df.writeTo("iceberg_hadoop.default.table1")
  .partitionedBy($"category")
  .createOrReplace()
```

This is a typical pattern for Iceberg table lifecycle management in Spark.

### Read path (`_01_Read.scala`)

The read example loads from a table location and tests features such as:

```scala
.option("as-of-timestamp", "499162860000")
```

and

```scala
.option("snapshot-id", 7601163594701794741L)
```

These options are useful for time-travel queries and snapshot-based auditing.

### Maintain path (`_03_Maintain.scala`)

This script uses `HiveCatalog` and `SparkActions` to submit maintenance work to the Iceberg table:

```scala
val catalog = new HiveCatalog()
```

and then performs:

- `expireSnapshots()`
- `deleteOrphanFiles()`
- `rewriteDataFiles()`

## Notes

- The examples use local development defaults and are meant for learning and environment testing.
- For production, replace hard-coded URIs and warehouse paths with environment variables or a proper config management layer.
- You should ensure the Hive metastore and HDFS are reachable from the runtime where Spark is executed.

## Example environment configuration

The repository includes a sample `hive-site.xml` under `src/main/resources` and under `input/`. These files are helpful as references when configuring Hive metastore connectivity.

## License

This project is intended for learning and educational purposes. Please check your organization’s compliance requirements before using it in production systems.
