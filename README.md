![banner-small](https://user-images.githubusercontent.com/1371/184679510-4f455245-8580-4b00-809b-19f865dd0931.png)

<p align="center">
   <a href="https://github.com/fcoury/oxide/actions/workflows/ci.yml">
      <img src="https://github.com/fcoury/oxide/actions/workflows/ci.yml/badge.svg">
   </a>
   <!-- [![CI workflow](https://github.com/fcoury/oxide/actions/workflows/ci.yml/badge.svg)](https://github.com/fcoury/oxide/actions/workflows/ci.yml) -->
</p>
   
<p align="center">
   &ldquo;<i>If it looks like MongoDB, swims like MongoDB, and quacks like MongoDB, then it probably is PostgreSQL.</i>&rdquo; :upside_down_face:
</p>

<p align="center">
   <a href="https://discord.gg/f7RwsbWEVh">Discord</a> | <a href="https://demo.oxidedb.com" target="_blank">Online Demo</a> | <a href="https://youtu.be/hC-CZOEjzaM">Intro Video</a> | <a href="https://github.com/fcoury/oxide#quickstart">Quickstart</a>
</p>

# OxideDB

OxideDB is a translation layer that works as a MongoDB database server while using PostgreSQL's JSON capabilities as the underlying data store.

## Audience

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

## Online demo

There's an online [demo of the Web Interface live hosted at Railway.app](https://demo.oxidedb.com/).

## Quickstart

Download the [latest binary](https://github.com/fcoury/oxide/releases/latest) and run it. You will need to point it to a running PostgreSQL for Oxide to use as its backend.

```
> $ ./oxide --postgres-url "postgres://postgres:postgres@localhost:5432/test"
[2022-07-13T02:57:46Z INFO  oxide::server] Connecting to postgres://*****:*****@localhost:5432/test...
[2022-07-13T02:57:46Z INFO  oxide::server] OxideDB listening on 127.0.0.1:27017...
```

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

## Quick intro

[![Intro to OxideDB](docs/assets/video.png)](https://youtu.be/8TkcGV0TkgM)

## Web interface

![Web UI](docs/assets/screenshot.png)

If you want to perform quick queries on your database, you can use the built in web interface, just start oxide with the `--web` flag:

```
> $ ./oxide --postgres-url postgres://postgres:postgres@localhost:5432/oxide
[2022-07-29T00:20:11Z INFO  oxide::server] Connecting to postgres://*****:*****@localhost:5432/oxide...
[2022-07-29T00:20:11Z INFO  oxide::server] OxideDB listening on 0.0.0.0:27017...
```

Now just navigate to http://localhost:5432 and you'll be able to perform quick queries.

## Running options

You can also set the `DATABASE_URL` environment variable or even use a `.env` file.

By default oxide will bind to 127.0.0.1 and port 27017. You can change those settings using the following parameters:

```
> $ ./oxide --help
oxide 0.1.5
A database compatible with MongoDB Wire Protocol that uses PostgreSQL for backend storage.

USAGE:
    oxide [OPTIONS]
    oxide <SUBCOMMAND>

OPTIONS:
    -d, --debug                          Show debugging information
    -h, --help                           Print help information
    -l, --listen-addr <LISTEN_ADDR>      Listening address, defaults to 127.0.0.1
    -p, --port <PORT>                    Listening port, defaults to 27017
    -u, --postgres-url <POSTGRES_URL>    PostgreSQL connection URL
    -V, --version                        Print version information
    -w, --web                            Starts web interface
        --web-addr <WEB_ADDR>            Web binding address

SUBCOMMANDS:
    help    Print this message or the help of the given subcommand(s)
    web     Start OxideDB web interface
```

### Running with Docker

Assuming you're running a local PostgreSQL instance, you can run OxideDB with Docker with the command below.

```
docker build -t oxide .
docker run -d -p 27017:27017 -p 8087:8087 -e DATABASE_URL=postgres://postgres:postgres@host.docker.internal:5432/test --name oxide oxide
```

### Running from source

```shell
git clone https://github.com/fcoury/oxide.git
cd oxide
export DATABASE_URL="postgres://postgres:postgres@localhost:5432/test"
make start
```
