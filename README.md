# Debezium Server with FileStore Sink

This project provides a standalone, deployable distribution of Debezium Server that includes a custom **FileStore sink**. It is designed to be built and maintained independently from the main `debezium/debezium-server` repository.  This is a Work In Progress.  Currently it is only possible to store data to compressed GZIP files.  More options will potentially be added in future.

It is also possible with this sink to pass-through to a different debezium-server sink using a filter.  For example, you can elect to store all records with op 'r' to disk and all others be pushed to the pubsub connector.  This is

## Background

When using debezium-server

## How to Build

To create the deployment archives, run the following Maven command from the root of the `debezium-server-filestore` directory:

```bash
mvn clean install -Passembly -DskipITs -DskipTests
```

The final `.zip` and `.tar.gz` files will be located in the `debezium-server-filestore-dist/target/` directory.

## Configuration

The following configuration properties are available for the FileStore sink:

### Basic Configuration
```properties
# Required: Set the sink type to filestore
debezium.sink.type=filestore

# Directory where output files will be written (default: "data")
debezium.sink.filestore.directory=data

# How to handle null values - "ignore" or "write" (default: "ignore")
debezium.sink.filestore.null.handling=ignore
```

### Performance Configuration
```properties
# Number of records to buffer before writing to disk (default: 1000)
debezium.sink.filestore.batch.size=1000

# Interval in milliseconds for periodic flushes (default: 1000)
debezium.sink.filestore.flush.interval.ms=1000

# Buffer size in bytes for the writer (default: 262144 = 256KB)
debezium.sink.filestore.writer.buffer.bytes=262144
```

### Filtering and Pass-through Configuration
```properties
# Header name to use for filtering records (default: empty)
debezium.sink.filestore.header.filter.name=

# Header value to match for filtering records (default: empty)
debezium.sink.filestore.header.filter.value=

# Name of another sink to pass filtered records to (default: empty)
debezium.sink.filestore.pass.through.sink=
```
#### Filtering and Pass-through Example
Currently, filtering is achieved by adding headers.  Here is an example implementation

```properties
# First we implement a transform to flatten our message and add the op as a header
debezium.transforms=unwrap
debezium.transforms.unwrap.type=io.debezium.transforms.ExtractNewRecordState
debezium.transforms.unwrap.delete.tombstone.handling.mode=rewrite
debezium.transforms.unwrap.add.headers=op

#Next we configure the sink connector
debezium.sink.type=filestore
debezium.sink.filestore.directory=data
debezium.sink.filestore.header.filter.name=__op
debezium.sink.filestore.header.filter.value=r
debezium.sink.filestore.pass.through.sink=pubsub
```

All relevant debezium.sink.pubsub configurations can then be added.  When only storing to disk, records are marked as committed by the filesink connector.  When using pass-through we rely on the target sink to mark records and batches as committed.

**Note:** 
- If filtering is configured but no pass-through sink is available, the filter will be disabled and all records will be written to files
- If no filtering is configured, all records are written to files regardless of pass-through sink configuration

## Complete Application Properties Example

For a complete example `application.properties` file showing FileStore sink with pass-through to PubSub, see [`debezium-server-filestore-dist/src/main/resources/distro/conf/application.properties.example`](debezium-server-filestore-dist/src/main/resources/distro/conf/application.properties.example).

## Output Format

Events are written as JSON lines (one JSON object per line) in GZIP compressed files. Files are named based on the destination topic with a `.json.gz` extension.

For example, events from a topic named `dbserver1.inventory.customers` will be written to a file named `dbserver1.inventory.customers.json.gz`.
