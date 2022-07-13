# OxideDB

[![CI workflow](https://github.com/fcoury/oxide/actions/workflows/ci.yml/badge.svg)](https://github.com/fcoury/oxide/actions/workflows/ci.yml)

OxideDB is a personal pet project, inspired by [FerretDB](https://ferretdb.io) and its aim is to close the gap between applications that need MongoDB on top of an existing PostgreSQL deployment and don't want to spend time and effort managing yet another database environment.

This README is a work in progress.

## Quickstart

Download the [latest binary](https://github.com/fcoury/oxide/releases/latest) and run it. You will need to point it to a running PostgreSQL for Oxide to use as its backend.

```shell
> $ ./oxide
[2022-07-13T02:56:15Z ERROR oxide] No PostgreSQL URL specified.
    Use --postgres-url <url> or env var DATABASE_URL to set the connection URL and try again.
    For more information use --help.

> $ ./target/debug/oxide --postgres-url postgres://postgres:postgres@localhost:5432/test
[2022-07-13T02:57:46Z INFO  oxide::server] Connecting to PostgreSQL database...
[2022-07-13T02:57:46Z INFO  oxide::server] OxideDB listening on localhost:27017...
```

And now you can use any [MongoDB](https://www.mongodb.com) client to connect to OxideDB, like [mongosh](https://www.mongodb.com/docs/mongodb-shell/):

```shell
> $ mongosh
Current Mongosh Log ID:	62ce3531d10f489bc82520c4
Connecting to:		mongodb://127.0.0.1:27017/?directConnection=true&serverSelectionTimeoutMS=2000&appName=mongosh+1.5.0
Using MongoDB:		3.6.23
Using Mongosh:		1.5.0

For mongosh info see: https://docs.mongodb.com/mongodb-shell/

------
   The server generated these startup warnings when booting
   2022-07-12T18:56:41.654-0300:
   2022-07-12T18:56:41.654-0300: ** WARNING: Access control is not enabled for the database.
   2022-07-12T18:56:41.654-0300: **          Read and write access to data and configuration is unrestricted.
   2022-07-12T18:56:41.654-0300:
------

test> db.col.insertMany([{ name: "Felipe" }, { name: "Fernanda" }]);
{
  acknowledged: true,
  insertedIds: {
    '0': ObjectId("62ce3536d10f489bc82520c5"),
    '1': ObjectId("62ce3536d10f489bc82520c6")
  }
}
test> db.col.find({ "name": "Fernanda" })
[ { _id: ObjectId("62ce3536d10f489bc82520c6"), name: 'Fernanda' } ]
```

By default oxide will bind to localhost's port 27017. You can change those settings using the following parameters:

```shell
> $ ./oxide --help
oxide 0.1.0
A database compatible with MongoDB Wire Protocol that uses PostgreSQL for backend storage.

USAGE:
    oxide [OPTIONS]

OPTIONS:
    -h, --help                           Print help information
    -l, --listen-addr <LISTEN_ADDR>
    -p, --port <PORT>
    -u, --postgres-url <POSTGRES_URL>
    -V, --version                        Print version information
```

### Running from source

```shell
git clone https://github.com/fcoury/oxide.git
cd oxide
cargo run
```
