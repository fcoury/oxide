import { h, render } from "https://cdn.skypack.dev/preact";
import { useEffect, useState } from "https://cdn.skypack.dev/preact/hooks";
// import { Router } from "https://cdn.skypack.dev/preact-router";
import htm from "https://cdn.skypack.dev/htm";

const html = htm.bind(h);

async function post(url, data) {
  const res = await fetch(url, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify(data),
  });
  return await res.json();
}

async function get(url) {
  const res = await fetch(url);
  return await res.json();
}

function Select(props) {
  const value = props.value;
  const classes = [
    "form-select",
    "form-select-sm",
    "box-border",
    "appearance-none",
    "block",
    "px-2",
    "py-1",
    "text-sm",
    "font-normal",
    "text-gray-700",
    "bg-white bg-clip-padding bg-no-repeat",
    "border border-solid border-gray-300",
    "rounded",
    "transition",
    "ease-in-out",
    "m-0",
    "focus:text-gray-700",
    "focus:bg-white",
    "focus:border-blue-600",
    "focus:outline-none",
  ];

  props.className && props.className.split(" ").forEach((c) => classes.push(c));
  return html`
    <select
      class=${classes.join(" ")}
      value=${value}
      onInput=${(e) => setValue(e.target.value)}
      onChange=${(e) => props.onChange && props.onChange(e.target.value)}
    >
      ${props.options.map(
        (option) => html`
          <option value=${option.value}>${option.label}</option>
        `
      )}
    </select>
  `;
}

function Nav({ children }) {
  return html`
    <nav
      class="
      relative
      w-full
      flex flex-wrap
      items-center
      justify-between
      py-4
      bg-gray-100
      text-gray-500
      hover:text-gray-700
      focus:text-gray-700
      shadow-lg
      navbar navbar-expand-lg navbar-light
      "
    >
      <div
        class="container-fluid w-full flex flex-wrap items-center justify-between px-6"
      >
        <ul class="navbar-nav flex flex-col pl-0 list-style-none mr-auto">
          <li class="nav-item p-2"><b>OxideDB</b> | Query Builder</li>
        </ul>
        ${children}
      </div>
    </nav>
  `;
}

function App(props) {
  // const [json, setJson] = useState(`{"$or": [{"name": "Felipe"}, {"age": {"$gt": 30}}]}`);
  const [error, setError] = useState(null);
  const [valid, setValid] = useState(true);
  const [json, setJson] = useState(`{}`);
  const [where, setWhere] = useState("");
  const [databases, setDatabases] = useState([]);
  const [collections, setCollections] = useState([]);
  const [database, setDatabase] = useState("");
  const [collection, setCollection] = useState("");
  const [data, setData] = useState(null);
  const [sql, setSql] = useState("");

  const convert = async (e) => {
    console.log("convert");
    // setJson(JSON.stringify(JSON.parse(json), null, 2));
    const data = await post("/convert", JSON.parse(json));
    if (data.error) {
      setError(data.error);
      return;
    }
    setWhere(data.sql);
  };

  const makeSql = () => {
    const w = where ? `\nWHERE ${where}` : "";
    setSql(`SELECT _jsonb\nFROM "${database}"."${collection}"${w}`);
  };

  const run = async (e) => {
    const { rows } = await post("/run", { query: sql });
    setData(rows);
  };

  const loadDatabases = async (e) => {
    let { databases } = await get("/databases");
    if (!databases) return;
    const filteredDatabases = databases.filter((db) => db !== "public");
    setDatabases(filteredDatabases);
    setDatabase(filteredDatabases[0]);
  };

  const loadCollections = async (e) => {
    if (!database) return;
    let { collections } = await get(`/databases/${database}/collections`);
    setCollections(collections);
  };

  const debounce = (fn, ms = 0) => {
    let timeoutId;
    return function (...args) {
      clearTimeout(timeoutId);
      timeoutId = setTimeout(() => {
        fn.apply(this, args);
      }, ms);
    };
  };

  const onJsonChanged = debounce((str) => {
    try {
      setJson(str);
      JSON.parse(str);
      setValid(true);
    } catch (e) {
      setValid(false);
    }
  }, 250);

  useEffect(() => loadCollections(), [database]);
  useEffect(() => setCollection(collections[0]), [collections]);
  useEffect(() => makeSql(), [collection, where]);
  useEffect(() => convert(), [json]);
  useEffect(() => loadDatabases(), []);

  const dboptions = databases
    ? databases.map((db) => ({
        value: db,
        label: db,
      }))
    : [];

  const coloptions = collections
    ? collections.map((col) => ({
        value: col,
        label: col,
      }))
    : [];

  let attrs = {};
  if (!valid) {
    attrs["disabled"] = true;
  }

  const handleBlur = (e) => {
    setJson(JSON.stringify(JSON.parse(json), null, 2));
  };

  const handleClear = (e) => {
    setJson("{}");
    setData(null);
  };

  return html`
    <div>
      <${Nav} />
      <div class="container mx-auto my-10">
        <div class="box-border py-2 flex space-x-2">
          <${Select}
            className="w-48"
            options=${dboptions}
            value=${database}
            onChange=${setDatabase}
          />
          <${Select}
            className="w-48"
            options=${coloptions}
            value=${collection}
            onChange=${setCollection}
          />
        </div>
        <div class="columns-2 h-96">
          <div class="w-full h-full">
            <${TextArea}
              className="w-full h-full box-border form-control
                block
                px-3
                py-1.5
                text-sm
                font-mono
                text-gray-700
                bg-white bg-clip-padding
                border border-solid border-gray-300
                rounded
                transition
                ease-in-out
                m-0
                focus:text-gray-700 focus:bg-white focus:border-blue-600 focus:outline-none"
              value=${json}
              onBlur=${handleBlur}
              onInput=${(e) => onJsonChanged(e.target.value)}
            />
          </div>
          <div class="w-full h-96">
            <${TextArea}
              className="w-full h-full box-border form-control
                block
                px-3
                py-1.5
                text-sm
                font-mono
                text-gray-700
                bg-white bg-clip-padding
                border border-solid border-gray-300
                rounded
                transition
                ease-in-out
                m-0
                focus:text-gray-700 focus:bg-white focus:border-blue-600 focus:outline-none"
              value=${sql}
              readonly
            />
          </div>
        </div>
        <div class="flex space-x-2 justify-center mt-10">
          <button
            class="inline-block px-6 py-2.5 bg-blue-600 text-white font-medium text-xs leading-tight uppercase rounded shadow-md hover:bg-blue-700 hover:shadow-lg focus:bg-blue-700 focus:shadow-lg focus:outline-none focus:ring-0 active:bg-blue-800 active:shadow-lg transition duration-150 ease-in-out"
            onClick=${run}
            ...${attrs}
          >
            Run
          </button>
          <button
            class="inline-block px-6 py-2.5 bg-gray-600 text-white font-medium text-xs leading-tight uppercase rounded shadow-md hover:bg-blue-700 hover:shadow-lg focus:bg-blue-700 focus:shadow-lg focus:outline-none focus:ring-0 active:bg-blue-800 active:shadow-lg transition duration-150 ease-in-out"
            onClick=${handleClear}
          >
            Clear
          </button>
        </div>
        <div>${data && html`${data.length} record(s)`}</div>
        <pre>${data && JSON.stringify(data, null, 2)}</pre>
      </div>
    </div>
  `;
}

function TextArea(props) {
  return html`
    <textarea
      ...${props?.readonly ? { disabled: true } : null}
      class=${props?.className}
      onBlur=${props.onBlur}
      onInput=${props.onInput}
      value=${props.value}
    >
      ${props.value}
    </textarea
    >
  `;
}

render(html`<${App} name="World" />`, document.body);
