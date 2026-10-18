// Interactive presentation only. Never interpret Extended JSON-shaped objects.
(() => {
  const scalar = globalThis.__oxideInspectScalar;
  delete globalThis.__oxideInspectScalar;
  const descriptors = Object.getOwnPropertyDescriptors;
  const dateTime = Date.prototype.getTime;
  const dateISO = Date.prototype.toISOString;
  const regexSource = Object.getOwnPropertyDescriptor(RegExp.prototype, 'source').get;
  const regexFlags = [
    ['hasIndices', 'd'], ['global', 'g'], ['ignoreCase', 'i'], ['multiline', 'm'],
    ['dotAll', 's'], ['unicode', 'u'], ['unicodeSets', 'v'], ['sticky', 'y']
  ].map(([key, flag]) => [Object.getOwnPropertyDescriptor(RegExp.prototype, key)?.get, flag]);
  globalThis.__oxideInspect = function (value) {
    if (value === undefined) return undefined;
    const ancestors = new Set();
    let remaining = 1000;
    let characters = 64 * 1024;
    function text(value) {
      const result = value.length > characters ? value.slice(0, characters) + ' [Truncated]' : value;
      characters = Math.max(0, characters - result.length);
      return result;
    }
    function quote(value) {
      // Bound before quoting: escaping may expand a string by up to six times.
      const limit = Math.max(0, Math.floor((characters - 2) / 6));
      const shortened = value.length > limit;
      return text(JSON.stringify(value.slice(0, limit)) + (shortened ? ' [Truncated]' : ''));
    }
    function inspect(value, depth) {
      if (--remaining < 0 || characters <= 0) return '[Truncated]';
      if (value === null) return 'null';
      if (typeof value === 'string') return quote(value);
      if (typeof value === 'bigint') return text(String(value) + 'n');
      if (typeof value === 'number') return Object.is(value, -0) ? '-0' : String(value);
      if (typeof value === 'function') return '[Function]';
      if (typeof value !== 'object') return String(value);
      const bson = scalar(value, characters);
      if (bson !== undefined) return text(bson);
      if (value instanceof Date) {
        return Number.isNaN(dateTime.call(value)) ? 'Invalid Date' : 'ISODate(' + quote(dateISO.call(value)) + ')';
      }
      if (value instanceof RegExp) {
        const pattern = regexSource.call(value).slice(0, characters).replace(/[\x00-\x1f\x7f-\x9f]/g,
          ch => '\\u' + ch.charCodeAt(0).toString(16).padStart(4, '0'));
        const flags = regexFlags.filter(([get]) => get && get.call(value)).map(([, flag]) => flag).join('');
        return text('/' + pattern + '/' + flags);
      }
      if (ancestors.has(value)) return '[Circular]';
      if (depth >= 10) return '[Max depth]';
      ancestors.add(value);
      const array = Array.isArray(value);
      const fields = descriptors(value);
      const entries = [];
      let nextIndex = 0;
      for (const key of Object.keys(fields)) {
        const field = fields[key];
        if (!field.enumerable) continue;
        if (remaining <= 0 || characters <= 0) { entries.push('[Truncated]'); break; }
        const label = /^[A-Za-z_$][\w$]*$/.test(key) ? text(key) : quote(key);
        const item = Object.hasOwn(field, 'value') ? inspect(field.value, depth + 1) : '[Getter/Setter]';
        if (!Object.hasOwn(field, 'value')) remaining--;
        const index = array && /^(0|[1-9]\d*)$/.test(key) && Number(key) < fields.length.value;
        if (index) {
          if (Number(key) > nextIndex) entries.push('<' + (Number(key) - nextIndex) + ' empty items>');
          nextIndex = Number(key) + 1;
        }
        entries.push(index ? item : label + ': ' + item);
      }
      if (array && remaining > 0 && characters > 0 && nextIndex < fields.length.value) {
        entries.push('<' + (fields.length.value - nextIndex) + ' empty items>');
      }
      ancestors.delete(value);
      const [open, close] = array ? ['[', ']'] : ['{', '}'];
      if (!entries.length) return open + close;
      const compact = open + ' ' + entries.join(', ') + ' ' + close;
      if (!compact.includes('\n') && compact.length + depth * 2 <= 90) return compact;
      const indent = '  '.repeat(depth + 1);
      return open + '\n' + indent + entries.join(',\n' + indent) + '\n' + '  '.repeat(depth) + close;
    }
    return inspect(value, 0);
  };
})();
