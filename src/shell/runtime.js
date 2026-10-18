// Our shell API. The engine and MongoDB transport are supplied by Rust.
(() => {
  const command = globalThis.__oxideCommand;
  const scalar = globalThis.__oxideScalar;
  const size = globalThis.__oxideSize;
  delete globalThis.__oxideSize;
  const write = globalThis.__oxidePrint;
  delete globalThis.__oxideCommand;
  delete globalThis.__oxideScalar;
  delete globalThis.__oxidePrint;

  for (const kind of ['ObjectId', 'NumberInt', 'NumberLong', 'NumberDouble', 'NumberDecimal']) {
    globalThis[kind] = function (value = kind === 'ObjectId' ? '' : '0') {
      if (typeof value === 'number' && kind !== 'NumberDouble' && !Number.isSafeInteger(value)) {
        throw new TypeError('Use a string for exact BSON numeric values');
      }
      return scalar(kind, Object.is(value, -0) ? '-0' : String(value), 0);
    };
  }
  globalThis.BinData = function (subtype, base64) {
    if (!Number.isInteger(subtype) || subtype < 0 || subtype > 255) throw new TypeError('Invalid binary subtype');
    return scalar('BinData', String(base64), subtype);
  };
  globalThis.ISODate = function (value) {
    const date = value === undefined ? new Date() : new Date(value);
    if (!Number.isFinite(date.getTime())) throw new TypeError('Invalid date');
    return date;
  };

  function run(database, cmd) {
    const result = command(database, cmd);
    const failure = result.writeErrors?.[0] ?? result.writeConcernError;
    if (failure || result.ok == 0) {
      const error = new Error((failure ?? result).errmsg ?? 'Database command failed');
      error.code = (failure ?? result).code;
      throw error;
    }
    return result;
  }

  // Preserve the original shell's eager array results, with a finite result limit.
  function collect(database, response) {
    const rows = [];
    let bytes = 0;
    let cursor = response.cursor;
    while (cursor) {
      const batch = cursor.firstBatch ?? cursor.nextBatch;
      bytes += size(batch);
      if (bytes > 8 * 1024 * 1024) throw new Error('Shell result exceeds 8 MiB; use a limit or projection');
      for (const row of batch) {
        if (rows.length >= 10000) throw new Error('Shell result exceeds 10000 documents; use a limit');
        rows.push(row);
      }
      if (cursor.id == 0) break;
      const collection = cursor.ns.slice(database.length + 1);
      cursor = run(database, { getMore: cursor.id, collection, batchSize: 100 }).cursor;
    }
    return rows;
  }

  function document(value) {
    if (value === null || typeof value !== 'object' ||
        (Object.getPrototypeOf(value) !== Object.prototype && Object.getPrototypeOf(value) !== null)) {
      throw new TypeError('Expected a document');
    }
    return value;
  }

  class Collection {
    constructor(database, name) { this.database = database; this.name = name; }
    find(filter = {}, options = {}) {
      const cmd = { find: this.name, ...options, filter };
      cmd.find = this.name;
      return collect(this.database, run(this.database, cmd));
    }
    findOne(filter = {}) { return this.find(filter, { limit: 1 })[0] ?? null; }
    aggregate(pipeline = []) {
      return collect(this.database, run(this.database, { aggregate: this.name, pipeline, cursor: { batchSize: 100 } }));
    }
    insertOne(doc) {
      const value = { ...document(doc) };
      if (!Object.hasOwn(value, '_id')) value._id = ObjectId();
      run(this.database, { insert: this.name, documents: [value], ordered: true });
      return { acknowledged: true, insertedId: value._id };
    }
    insertMany(docs) {
      const documents = docs.map(doc => ({ ...document(doc), _id: Object.hasOwn(doc, '_id') ? doc._id : ObjectId() }));
      const result = run(this.database, { insert: this.name, documents, ordered: true });
      return { acknowledged: true, insertedCount: result.n, insertedIds: documents.map(doc => doc._id) };
    }
    updateOne(filter, update) { return this.update(filter, update, false); }
    updateMany(filter, update) { return this.update(filter, update, true); }
    replaceOne(filter, replacement) { return this.update(filter, replacement, false); }
    update(filter, update, multi) {
      const result = run(this.database, { update: this.name, updates: [{ q: filter, u: update, multi }], ordered: true });
      return { acknowledged: true, matchedCount: result.n, modifiedCount: result.nModified };
    }
    deleteOne(filter) { return this.remove(filter, 1); }
    deleteMany(filter) { return this.remove(filter, 0); }
    remove(filter, limit) {
      const result = run(this.database, { delete: this.name, deletes: [{ q: document(filter), limit }], ordered: true });
      return { acknowledged: true, deletedCount: result.n };
    }
  }

  class Database {
    constructor(name) { this.name = name; }
    getName() { return this.name; }
    getSiblingDB(name) { return database(name); }
    getCollection(name) { return new Collection(this.name, name); }
    runCommand(cmd) { return run(this.name, cmd); }
    listCollections() { return collect(this.name, run(this.name, { listCollections: 1, cursor: {} })); }
  }
  function database(name) {
    if (typeof name !== 'string' || !name || /[\/\\. "$\0]/.test(name)) throw new TypeError('Invalid database name');
    return new Proxy(new Database(name), {
      get(target, key, receiver) {
        return typeof key === 'string' && !(key in target)
          ? target.getCollection(key) : Reflect.get(target, key, receiver);
      }
    });
  }
  globalThis.use = function (name) {
    globalThis.db = database(name);
    return `switched to db ${name}`;
  };
  const formatJSON = function (value) {
    if (value === undefined) return undefined;
    if (typeof value === 'function') return String(value);
    return JSON.stringify(value, (_, v) => typeof v === 'bigint' ? { $numberLong: String(v) } : v, 2);
  };
  globalThis.__oxideFormat = globalThis.__oxideInteractive ? globalThis.__oxideInspect : formatJSON;
  delete globalThis.__oxideInteractive;
  globalThis.print = (...args) => write(args.map(v => typeof v === 'string' ? v : __oxideFormat(v)).join(' '));
  globalThis.console = { log: print, error: print };
  use('test');
})();
