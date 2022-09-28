class Collection {
  static get(db, name) {
    return new Collection(db, name);
  }

  constructor(db, name) {
    this.db = db;
    this.name = name;
  }

  find(filter = {}) {
    return Deno.core.opSync("op_find", this, filter);
  }

  insertOne(doc) {
    return Deno.core.opSync("op_insert_one", this, doc);
  }

  insertMany(docs) {
    return Deno.core.opSync("op_insert_many", this, docs);
  }

  updateOne(doc, update) {
    return Deno.core.opSync("op_update_one", this, doc, update);
  }

  updateMany(docs, update) {
    return Deno.core.opSync("op_update_many", this, docs, update);
  }

  deleteOne(doc) {
    return Deno.core.opSync("op_delete_one", this, doc);
  }

  deleteMany(docs) {
    return Deno.core.opSync("op_delete_many", this, docs);
  }

  aggregate(pipeline) {
    return Deno.core.opSync("op_aggregate", this, pipeline);
  }

  drop() {
    return Deno.core.opSync("op_drop", this);
  }

  save(doc) {
    return Deno.core.opSync("op_save", this, doc);
  }
}

class Db {
  static get(global) {
    const { db, dbAddr, dbPort } = global._state;
    const target = new Db(db, dbAddr, dbPort);
    const handler = {
      get(target, prop, _receiver) {
        if (
          !target.hasOwnProperty(prop) &&
          typeof target[prop] !== "function"
        ) {
          return Collection.get(target, prop);
        }
        return Reflect.get(...arguments);
      },
    };

    return new Proxy(target, handler);
  }

  constructor(name = "test", addr, port) {
    this.name = name;
    this.addr = addr;
    this.port = port;
  }

  listCollections() {
    return Deno.core.opSync("op_list_collections", this);
  }
  
  getCollection(name) {
    const { db } = Object.assign({}, this.db);
    return new Collection(db, name);
  }
}

function ObjectId(value) {
  return { $oid: value };
}

((globalThis) => {
  const core = Deno.core;

  function argsToMessage(...args) {
    return args.map((arg) => JSON.stringify(arg)).join(" ");
  }

  const console = {
    log: (...args) => {
      core.print(`${argsToMessage(...args)}\n`, false);
    },
    error: (...args) => {
      core.print(`${argsToMessage(...args)}\n`, true);
    },
  };

  globalThis.console = console;

  globalThis.__defineGetter__("db", () => {
    return Db.get(globalThis);
  });
  
  globalThis.assert = {
    eq: (a, b, err) => {
      if (a != b) {
        console.log(err);
        return false;
      }
      return true;
    },
    
    throws: (fn) => {
      try {
        fn();
        console.log("Expected", fn, "to throw but nothing was thrown");
      } catch (e) {
        return true;
      }
    }
  };
})(globalThis);
