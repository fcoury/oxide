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
    return globalThis?.state?.db;
  });

  globalThis.use = (name) => {
    globalThis.state = globalThis.state || {};
    globalThis.state.db = name;
    return name;
  };
})(globalThis);
