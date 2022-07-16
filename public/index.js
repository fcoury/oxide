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
  const [value, setValue] = useState(props.value);
  return html`
    <select
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

function App(props) {
  // const [json, setJson] = useState(`{"$or": [{"name": "Felipe"}, {"age": {"$gt": 30}}]}`);
  const [json, setJson] = useState(`{}`);
  const [where, setWhere] = useState("");
  const [databases, setDatabases] = useState([]);
  const [collections, setCollections] = useState([]);
  const [database, setDatabase] = useState("");
  const [collection, setCollection] = useState("");
  const [data, setData] = useState([]);

  const convert = async (e) => {
    setJson(JSON.stringify(JSON.parse(json), null, 2));
    const data = await post("/convert", JSON.parse(json));
    setWhere(data.sql);
  };

  const makeSql = () => {
    const w = where ? `\nWHERE ${where}` : "";
    return `SELECT _jsonb\nFROM "${database}"."${collection}"${w}`;
  };

  const run = async (e) => {
    const { rows } = await post("/run", { query: makeSql() });
    console.log("rows", rows);
    setData(rows);
  };

  const loadDatabases = async (e) => {
    let { databases } = await get("/databases");
    if (!databases) return;
    const filteredDatabases = databases.filter((db) => db !== "public");
    setDatabases(filteredDatabases);
    setDatabase(filteredDatabases[0], () => loadCollections());
  };

  const loadCollections = async (e) => {
    if (!database) return;
    let { collections } = await get(`/databases/${database}/collections`);
    setCollections(collections);
    setCollection(collections[0]);
  };

  useEffect(() => convert(), [json]);
  useEffect(() => loadCollections(), [database]);
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

  const sql = makeSql();

  return html`
    <div>
      <div>
        <${Select}
          options=${dboptions}
          value=${props.value}
          onChange=${setDatabase}
        />
        <${Select}
          options=${coloptions}
          value=${props.value}
          onChange=${setCollection}
        />
      </div>
      <${TextArea} value=${json} onChange=${(e) => setJson(e.target.value)} />
      <div><button onClick=${convert}>Convert</button></div>
      <${TextArea} value=${sql} readonly />
      <div><button onClick=${run}>Run</button></div>
      <pre>${JSON.stringify(data, null, 2)}</pre>
    </div>
  `;
}

function TextArea(props) {
  return html`
    <textarea
      ...${props?.readonly ? { disabled: true } : null}
      onChange=${props.onChange}
      value=${props.value}
      cols="80"
      rows="10"
    >
      ${props.value}
    </textarea
    >
  `;
}

render(html`<${App} name="World" />`, document.body);
