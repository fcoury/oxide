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
    return globalThis?._state?.db;
  });

  globalThis.use = (name) => {
    globalThis._state = globalThis._state || {};
    globalThis._state.db = name;
    return name;
  };
})(globalThis);
