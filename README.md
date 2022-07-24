# OxideDB

[![CI workflow](https://github.com/fcoury/oxide/actions/workflows/ci.yml/badge.svg)](https://github.com/fcoury/oxide/actions/workflows/ci.yml)

OxideDB is a translation layer that works as a MongoDB database server while using PostgreSQL's JSON capabilities as the underlying data store.

This project might be something that you could be interested on if:

- You spend too much time managing; or too much money paying for a MongoDB instance, while only using it as a simple
document store, without any sharding features
- You already have a running PostgreSQL deployment, or prefer to manage it over MongoDB

On the other hand, if your use-case leverages MongoDB as a distributed database, then unfortunately this project might
not be for you. At least right now supporting multi-sharding and scale-out deployments is not part of the roadmap.

## Current status

The project was heavily inspired by [FerretDB](https://ferretdb.io) and is on its early days. The main difference is that
there is no intention to support any database other than PostgreSQL (FerretDB is also supporting Tigris) and it's written
in Rust, as opposed to Go.

In order to translate the MongoDB Query language - which is based on JSON - to SQL I have ported [the mongodb-language-model library](https://github.com/mongodb-js/mongodb-language-model) that was originally written in Node.js and PEG.js to Rust and [pest.rs](https://pest.rs/). It was an excellent opportunity to learn how parsers work in a bit more depth.

You can check it out here: [mongodb-language-model-rust](https://github.com/fcoury/mongodb-language-model-rust).

At this moment, it's being developed as a personal project, but contributors are highly welcomed. If that something you'd
be interested on, be more than welcome to contact me.

## Quickstart

Download the [latest binary](https://github.com/fcoury/oxide/releases/latest) and run it. You will need to point it to a running PostgreSQL for Oxide to use as its backend.

```
> $ ./oxide
[2022-07-13T02:56:15Z ERROR oxide] No PostgreSQL URL specified.
    Use --postgres-url <url> or env var DATABASE_URL to set the connection URL and try again.
    For more information use --help.

> $ ./oxide --postgres-url "postgres://postgres:postgres@localhost:5432/test"
[2022-07-13T02:57:46Z INFO  oxide::server] Connecting to PostgreSQL database...
[2022-07-13T02:57:46Z INFO  oxide::server] OxideDB listening on 127.0.0.1:27017...
```

You can also set the `DATABASE_URL` environment variable or even use a `.env` file.

And with the database configuration set, you can use any [MongoDB](https://www.mongodb.com) client to connect to OxideDB, like [mongosh](https://www.mongodb.com/docs/mongodb-shell/):

```
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

By default oxide will bind to 127.0.0.1 and port 27017. You can change those settings using the following parameters:

```
> $ ./oxide --help
oxide 0.1.3
A database compatible with MongoDB Wire Protocol that uses PostgreSQL for backend storage.

USAGE:
    oxide [OPTIONS]

OPTIONS:
    -d, --debug                          Show debugging information
    -h, --help                           Print help information
    -l, --listen-addr <LISTEN_ADDR>      Listening address defaults to 127.0.0.1
    -p, --port <PORT>                    Listening port, defaults to 27017
    -u, --postgres-url <POSTGRES_URL>    PostgreSQL connection URL
    -V, --version                        Print version information
```

### Running from source

```shell
git clone https://github.com/fcoury/oxide.git
cd oxide
export DATABASE_URL="postgres://postgres:postgres@localhost:5432/test"
make start
```
