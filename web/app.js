import * as duckdb from "https://cdn.jsdelivr.net/npm/@duckdb/duckdb-wasm@1.29.0/+esm";

const TABLES = ["dim_patient", "dim_date", "dim_condition", "fact_encounter", "fact_medication", "fact_condition"];
const $ = (id) => document.getElementById(id);
const esc = (v) => String(v ?? "").replace(/[&<>]/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;" }[c]));

function table(el, rows) {
  if (!rows.length) { el.innerHTML = "<tr><td>No rows</td></tr>"; return; }
  const cols = Object.keys(rows[0]);
  el.innerHTML = `<thead><tr>${cols.map((c) => `<th>${esc(c)}</th>`).join("")}</tr></thead><tbody>` +
    rows.map((r) => `<tr>${cols.map((c) => `<td>${esc(r[c])}</td>`).join("")}</tr>`).join("") + "</tbody>";
}

function bar(id, labels, data, label) {
  new Chart($(id), { type: "bar", data: { labels, datasets: [{ label, data }] },
    options: { indexAxis: "y", plugins: { legend: { display: false } } } });
}

async function loadReport() {
  const [report, insights] = await Promise.all(["run_report.json", "insights.json"].map((f) => fetch(`data/${f}`).then((r) => r.json())));
  const t = report.timings_sec || {};
  const total = Object.values(t).reduce((a, b) => a + b, 0);
  $("run-meta").textContent = `Finished ${report.finished_at} · ${total.toFixed(1)}s end to end`;
  $("layer-stats").innerHTML = Object.entries(report.layer_stats || {}).map(([name, s]) =>
    `<div class="stat"><b>${s.silver.toLocaleString()}</b><span>${name}: ${s.bronze} raw, ${s.quarantined} quarantined, ${s.duplicates} duplicates</span></div>`).join("");

  const dq = [];
  for (const [layer, tables] of Object.entries(report.quality || {}))
    for (const [tbl, checks] of Object.entries(tables))
      for (const c of checks) dq.push(`<tr><td>${layer}</td><td>${tbl}</td><td>${esc(c.check)}</td><td>${c.failed_rows}</td>` +
        `<td class="${c.passed ? "pass" : "fail"}">${c.passed ? "pass" : "fail"}</td></tr>`);
  $("dq-table").querySelector("tbody").innerHTML = dq.join("");

  const cost = insights.cost_per_encounter;
  bar("cost-chart", cost.map((r) => r.encounter_class), cost.map((r) => r.avg_claim_cost), "Avg claim cost");
  const re = insights.readmission_30d;
  bar("readmit-chart", re.map((r) => r.reason.slice(0, 40)), re.map((r) => r.readmission_rate_pct), "Readmission %");
  const meds = insights.med_trends, years = [...new Set(meds.map((r) => r.year))];
  const names = [...new Set(meds.map((r) => r.description))];
  new Chart($("med-chart"), { type: "line", data: { labels: years, datasets: names.map((n) => ({
    label: n.slice(0, 30), data: years.map((y) => meds.find((r) => r.year === y && r.description === n)?.prescriptions ?? 0) })) } });
}

async function startDuckDB() {
  const bundle = await duckdb.selectBundle(duckdb.getJsDelivrBundles());
  const workerUrl = URL.createObjectURL(new Blob([`importScripts("${bundle.mainWorker}");`], { type: "text/javascript" }));
  const db = new duckdb.AsyncDuckDB(new duckdb.VoidLogger(), new Worker(workerUrl));
  await db.instantiate(bundle.mainModule, bundle.pthreadWorker);
  const conn = await db.connect();
  for (const t of TABLES) {
    await db.registerFileURL(`${t}.parquet`, new URL(`data/${t}.parquet`, location.href).href, duckdb.DuckDBDataProtocol.HTTP, false);
    await conn.query(`CREATE VIEW ${t} AS SELECT * FROM '${t}.parquet'`);
  }
  return conn;
}

async function runSql(conn) {
  $("sql-status").textContent = "Running…";
  const start = performance.now();
  try {
    const res = await conn.query($("sql-input").value);
    const rows = res.toArray().slice(0, 200).map((r) => Object.fromEntries(
      Object.entries(r.toJSON()).map(([k, v]) => [k, typeof v === "bigint" ? Number(v) : v])));
    table($("sql-result"), rows);
    $("sql-status").textContent = `${res.numRows} rows in ${(performance.now() - start).toFixed(0)} ms` + (res.numRows > 200 ? " (showing 200)" : "");
  } catch (e) {
    $("sql-status").textContent = e.message;
  }
}

loadReport().catch((e) => { $("run-meta").textContent = `Could not load run report: ${e.message}`; });
startDuckDB().then((conn) => {
  $("sql-status").textContent = "Ready";
  $("sql-run").onclick = () => runSql(conn);
  document.querySelectorAll("button[data-sql]").forEach((b) => b.onclick = () => { $("sql-input").value = b.dataset.sql; runSql(conn); });
  runSql(conn);
}).catch((e) => { $("sql-status").textContent = `DuckDB failed to load: ${e.message}`; });
