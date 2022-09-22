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
}

class Db {
  static get(global) {
    const { db, dbAddr, dbPort } = global._state;
    const target = new Db(db, dbAddr, dbPort);
    const handler = {
      get(target, prop, _receiver) {
        if (!target.hasOwnProperty(prop)) {
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

  globalThis.use = (name) => {
    globalThis._state = globalThis._state || {};
    globalThis._state.db = name;
    return name;
  };
})(globalThis);
