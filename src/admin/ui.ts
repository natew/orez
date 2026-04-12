export function getAdminHtml(): string {
  return `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>oreZ admin</title>
<style>
:root {
  --bg: #000;
  --surface: #0a0a0a;
  --border: #222;
  --text: #fff;
  --text-dim: #666;
  --accent: #fff;
  --green: #888;
  --yellow: #999;
  --red: #f55;
  --purple: #aaa;
}
* { margin: 0; padding: 0; box-sizing: border-box; }
::-webkit-scrollbar { width: 8px; height: 8px; }
::-webkit-scrollbar-track { background: var(--bg); }
::-webkit-scrollbar-thumb { background: #444; border-radius: 4px; }
::-webkit-scrollbar-thumb:hover { background: #555; }
body {
  font-family: -apple-system, BlinkMacSystemFont, "SF Pro", system-ui, sans-serif;
  background: var(--bg);
  color: var(--text);
  height: 100vh;
  display: flex;
  flex-direction: column;
  overflow: hidden;
}
.header {
  display: flex;
  align-items: center;
  padding: 4px 12px;
  background: var(--surface);
  border-bottom: 0.5px solid var(--border);
  gap: 8px;
  flex-shrink: 0;
}
.header .logo {
  font-size: 12px;
  font-weight: 700;
  color: var(--accent);
  letter-spacing: -0.5px;
}
.badge {
  display: inline-flex;
  align-items: center;
  padding: 1px 6px;
  border-radius: 12px;
  font-size: 10px;
  border: 0.5px solid var(--border);
  color: var(--text-dim);
  gap: 3px;
}
.badge .dot {
  width: 6px;
  height: 6px;
  border-radius: 50%;
  background: var(--green);
}
.spacer { flex: 1; }
.tabs {
  display: flex;
  padding: 0 12px;
  background: var(--surface);
  border-bottom: 0.5px solid var(--border);
  gap: 0;
  flex-shrink: 0;
}
.tab {
  padding: 4px 10px;
  font-size: 11px;
  color: var(--text-dim);
  cursor: pointer;
  border-bottom: 2px solid transparent;
  transition: all 0.15s;
  background: none;
  border-top: none;
  border-left: none;
  border-right: none;
  font-family: inherit;
}
.tab:hover { color: var(--text); }
.tab.active {
  color: var(--accent);
  border-bottom-color: var(--accent);
}
.toolbar {
  display: flex;
  align-items: center;
  padding: 3px 12px;
  gap: 8px;
  border-bottom: 0.5px solid var(--border);
  flex-shrink: 0;
}
.toolbar label {
  font-size: 10px;
  color: var(--text-dim);
  text-transform: uppercase;
  letter-spacing: 0.5px;
}
.toolbar select {
  background: var(--surface);
  color: var(--text);
  border: 0.5px solid var(--border);
  border-radius: 4px;
  padding: 2px 6px;
  font-size: 11px;
  font-family: inherit;
  cursor: pointer;
}
.toolbar select:focus { outline: none; border-color: var(--accent); }
.toolbar input[type="text"] {
  background: var(--surface);
  color: var(--text);
  border: 0.5px solid var(--border);
  border-radius: 4px;
  padding: 2px 6px;
  font-size: 11px;
  font-family: inherit;
  width: 180px;
}
.toolbar input[type="text"]:focus { outline: none; border-color: var(--accent); }
.toolbar input[type="text"]::placeholder { color: var(--text-dim); }
.sep { width: 1px; height: 20px; background: var(--border); }
.action-btn {
  padding: 2px 8px;
  border-radius: 4px;
  border: 1px solid;
  background: transparent;
  cursor: pointer;
  font-family: inherit;
  font-size: 10px;
  transition: all 0.15s ease;
  white-space: nowrap;
}
.action-btn:disabled { opacity: 0.4; cursor: not-allowed; }
.action-btn.blue { color: var(--accent); border-color: #ffffff22; }
.action-btn.blue:hover:not(:disabled) { background: #ffffff11; border-color: var(--accent); }
.action-btn.orange { color: var(--yellow); border-color: #ffffff22; }
.action-btn.orange:hover:not(:disabled) { background: #ffffff11; border-color: var(--yellow); }
.action-btn.red { color: var(--red); border-color: #ff555522; }
.action-btn.red:hover:not(:disabled) { background: #ff555511; border-color: var(--red); }
.action-btn.gray { color: var(--text-dim); border-color: #ffffff22; }
.action-btn.gray:hover:not(:disabled) { background: #ffffff11; border-color: var(--text-dim); }
.content-area {
  flex: 1;
  overflow: hidden;
  position: relative;
  display: flex;
  flex-direction: column;
}
.log-wrap {
  flex: 1;
  overflow: hidden;
  position: relative;
}
.log-view {
  height: 100%;
  overflow-y: auto;
  padding: 4px 12px;
  font-size: 11px;
  line-height: 1.4;
}
.log-line { white-space: pre-wrap; word-break: break-all; }
.log-line .ts { color: var(--text-dim); }
.log-line .src { display: inline-block; width: 7ch; }
.log-line .src.zero { color: var(--purple); }
.log-line .src.pglite { color: var(--green); }
.log-line .src.proxy { color: var(--yellow); }
.log-line .src.orez { color: var(--accent); }
.log-line .src.s3 { color: #888; }
.log-line.level-error .msg { color: var(--red); }
.log-line.level-warn .msg { color: var(--yellow); }
.log-line.level-info .msg { color: var(--text); }
.log-line.level-debug .msg { color: var(--text-dim); }
.jump-btn {
  position: absolute;
  bottom: 16px;
  left: 50%;
  transform: translateX(-50%);
  padding: 6px 16px;
  border-radius: 20px;
  background: #333;
  color: var(--text);
  border: 1px solid var(--border);
  font-size: 12px;
  font-family: inherit;
  cursor: pointer;
  opacity: 0;
  transition: opacity 0.2s;
  pointer-events: none;
  z-index: 10;
}
.jump-btn.visible { opacity: 1; pointer-events: auto; }
.env-view {
  height: 100%;
  overflow-y: auto;
  padding: 16px;
  display: none;
}
.env-table { width: 100%; border-collapse: collapse; font-size: 12px; }
.env-table th {
  text-align: left;
  padding: 6px 12px;
  color: var(--text-dim);
  border-bottom: 0.5px solid var(--border);
  font-weight: 500;
  text-transform: uppercase;
  font-size: 10px;
  letter-spacing: 0.5px;
}
.env-table td {
  padding: 6px 12px;
  border-bottom: 0.5px solid var(--border);
}
.env-table td:first-child { color: var(--accent); white-space: nowrap; }
.env-table td:last-child { color: var(--text); word-break: break-all; }
.env-table tr:hover td { background: #111; }
.http-view {
  height: 100%;
  overflow-y: auto;
  padding: 0;
  display: none;
}
.http-table { width: 100%; border-collapse: collapse; font-size: 12px; }
.http-table th {
  text-align: left;
  padding: 6px 12px;
  color: var(--text-dim);
  border-bottom: 0.5px solid var(--border);
  font-weight: 500;
  text-transform: uppercase;
  font-size: 10px;
  letter-spacing: 0.5px;
  position: sticky;
  top: 0;
  background: var(--bg);
  z-index: 1;
}
.http-table td {
  padding: 5px 12px;
  border-bottom: 0.5px solid var(--border);
  white-space: nowrap;
}
.http-table tr.http-row { cursor: pointer; }
.http-table tr.http-row:hover td { background: #111; }
.http-table .method { font-weight: 600; }
.http-table .method.get { color: var(--green); }
.http-table .method.post { color: var(--yellow); }
.http-table .method.put { color: var(--accent); }
.http-table .method.delete { color: var(--red); }
.http-table .method.patch { color: #888; }
.http-table .method.ws { color: var(--purple); }
.http-table .status.s2 { color: var(--green); }
.http-table .status.s3 { color: var(--yellow); }
.http-table .status.s4 { color: var(--red); }
.http-table .status.s5 { color: var(--red); font-weight: 600; }
.http-table .path { color: var(--text); max-width: 500px; overflow: hidden; text-overflow: ellipsis; }
.http-table .dur { color: var(--text-dim); }
.http-table .sz { color: var(--text-dim); }
.http-detail {
  display: none;
}
.http-detail.open { display: table-row; }
.http-detail td {
  padding: 8px 12px 12px 24px;
  background: #080808;
  border-bottom: 0.5px solid var(--border);
}
.http-detail .hdr-section { margin-bottom: 8px; }
.http-detail .hdr-title {
  font-size: 10px;
  text-transform: uppercase;
  color: var(--text-dim);
  letter-spacing: 0.5px;
  margin-bottom: 4px;
}
.http-detail .hdr-line {
  font-size: 11px;
  line-height: 1.6;
  white-space: pre-wrap;
  word-break: break-all;
}
.http-detail .hdr-key { color: var(--accent); }
.http-detail .hdr-val { color: var(--text-dim); }
.toolbar-actions {
  display: flex;
  align-items: center;
  gap: 6px;
  margin-left: auto;
}
/* data explorer */
.data-view {
  height: 100%;
  display: none;
  flex-direction: column;
  overflow: hidden;
}
.data-view.visible {
  display: flex;
}
.data-toolbar {
  display: flex;
  align-items: center;
  padding: 3px 12px;
  gap: 8px;
  border-bottom: 0.5px solid var(--border);
  flex-shrink: 0;
}
.data-sub-tabs {
  display: flex;
  gap: 0;
}
.data-sub-tab {
  padding: 3px 10px;
  font-size: 10px;
  color: var(--text-dim);
  cursor: pointer;
  background: none;
  border: none;
  border-bottom: 2px solid transparent;
  font-family: inherit;
  text-transform: uppercase;
  letter-spacing: 0.5px;
  transition: all 0.15s;
}
.data-sub-tab:hover { color: var(--text); }
.data-sub-tab.active { color: var(--accent); border-bottom-color: var(--accent); }
.data-content {
  flex: 1;
  display: flex;
  overflow: hidden;
}
.data-sidebar {
  width: 220px;
  border-right: 0.5px solid var(--border);
  overflow-y: auto;
  flex-shrink: 0;
}
.data-sidebar-header {
  padding: 6px 10px;
  font-size: 10px;
  color: var(--text-dim);
  text-transform: uppercase;
  letter-spacing: 0.5px;
  border-bottom: 0.5px solid var(--border);
  position: sticky;
  top: 0;
  background: var(--bg);
}
.data-table-item {
  padding: 4px 10px;
  font-size: 11px;
  cursor: pointer;
  display: flex;
  justify-content: space-between;
  align-items: center;
  border-bottom: 0.5px solid transparent;
  transition: background 0.1s;
}
.data-table-item:hover { background: #111; }
.data-table-item.active { background: #181818; color: var(--accent); }
.data-table-item .tbl-name { overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.data-table-item .tbl-size { color: var(--text-dim); font-size: 10px; flex-shrink: 0; margin-left: 8px; }
.data-main {
  flex: 1;
  display: flex;
  flex-direction: column;
  overflow: hidden;
}
.sql-editor-wrap {
  border-bottom: 0.5px solid var(--border);
  display: flex;
  flex-direction: column;
  flex-shrink: 0;
}
.sql-editor {
  width: 100%;
  background: var(--surface);
  color: var(--text);
  border: none;
  padding: 8px 12px;
  font-family: "SF Mono", "Fira Code", "Cascadia Code", monospace;
  font-size: 12px;
  line-height: 1.5;
  resize: vertical;
  min-height: 60px;
  max-height: 300px;
  outline: none;
}
.sql-editor::placeholder { color: #444; }
.sql-bar {
  display: flex;
  align-items: center;
  padding: 3px 8px;
  gap: 8px;
  background: var(--surface);
  border-top: 0.5px solid var(--border);
}
.sql-bar .sql-status {
  font-size: 10px;
  color: var(--text-dim);
  flex: 1;
}
.sql-bar .sql-status.error { color: var(--red); }
.data-results {
  flex: 1;
  overflow: auto;
}
.data-results table {
  width: 100%;
  border-collapse: collapse;
  font-size: 11px;
  font-family: "SF Mono", "Fira Code", "Cascadia Code", monospace;
}
.data-results th {
  text-align: left;
  padding: 4px 10px;
  color: var(--text-dim);
  border-bottom: 0.5px solid var(--border);
  font-weight: 500;
  font-size: 10px;
  letter-spacing: 0.3px;
  position: sticky;
  top: 0;
  background: var(--bg);
  z-index: 1;
  cursor: pointer;
  user-select: none;
  white-space: nowrap;
}
.data-results th:hover { color: var(--text); }
.data-results td {
  padding: 3px 10px;
  border-bottom: 0.5px solid #1a1a1a;
  white-space: nowrap;
  max-width: 300px;
  overflow: hidden;
  text-overflow: ellipsis;
}
.data-results tr:hover td { background: #0a0a0a; }
.data-results td.null-val { color: #444; font-style: italic; }
.data-results tr.clickable { cursor: pointer; }
.data-results tr.clickable:hover td { background: #111; }
.data-empty {
  display: flex;
  align-items: center;
  justify-content: center;
  height: 100%;
  color: var(--text-dim);
  font-size: 12px;
}
.data-paging {
  display: flex;
  align-items: center;
  justify-content: space-between;
  padding: 4px 10px;
  border-top: 0.5px solid var(--border);
  font-size: 10px;
  color: var(--text-dim);
  flex-shrink: 0;
}
.data-paging button {
  background: none;
  border: 0.5px solid var(--border);
  color: var(--text-dim);
  padding: 2px 8px;
  border-radius: 3px;
  font-size: 10px;
  cursor: pointer;
  font-family: inherit;
}
.data-paging button:hover { color: var(--text); border-color: var(--text-dim); }
.data-paging button:disabled { opacity: 0.3; cursor: not-allowed; }
.data-search {
  padding: 4px 10px;
  border-bottom: 0.5px solid var(--border);
}
.data-search input {
  width: 100%;
  background: var(--bg);
  color: var(--text);
  border: 0.5px solid var(--border);
  border-radius: 3px;
  padding: 3px 8px;
  font-size: 11px;
  font-family: inherit;
  outline: none;
}
.data-search input:focus { border-color: var(--accent); }
.data-search input::placeholder { color: #444; }
/* row detail overlay */
.row-detail-overlay {
  position: fixed;
  top: 0; left: 0; right: 0; bottom: 0;
  background: rgba(0,0,0,0.6);
  z-index: 50;
  display: none;
  align-items: center;
  justify-content: center;
}
.row-detail-overlay.open { display: flex; }
.row-detail-panel {
  background: var(--bg);
  border: 0.5px solid var(--border);
  border-radius: 8px;
  max-width: 700px;
  width: 90%;
  max-height: 80vh;
  overflow-y: auto;
  padding: 0;
}
.row-detail-header {
  display: flex;
  align-items: center;
  padding: 8px 14px;
  border-bottom: 0.5px solid var(--border);
  font-size: 11px;
  color: var(--text-dim);
  position: sticky;
  top: 0;
  background: var(--bg);
}
.row-detail-header .spacer { flex: 1; }
.row-detail-close {
  background: none;
  border: none;
  color: var(--text-dim);
  font-size: 16px;
  cursor: pointer;
  padding: 0 4px;
}
.row-detail-close:hover { color: var(--text); }
.row-detail-body {
  padding: 0;
}
.row-detail-field {
  display: flex;
  border-bottom: 0.5px solid #1a1a1a;
  font-size: 12px;
}
.row-detail-field:last-child { border-bottom: none; }
.row-detail-key {
  width: 180px;
  flex-shrink: 0;
  padding: 6px 14px;
  color: var(--text-dim);
  font-weight: 500;
  border-right: 0.5px solid #1a1a1a;
  word-break: break-all;
}
.row-detail-val {
  flex: 1;
  padding: 6px 14px;
  color: var(--text);
  white-space: pre-wrap;
  word-break: break-all;
  font-family: "SF Mono", "Fira Code", "Cascadia Code", monospace;
  font-size: 11px;
}
.row-detail-val.null-val { color: #444; font-style: italic; }
/* toast */
.toast {
  position: fixed;
  bottom: 20px;
  right: 20px;
  padding: 10px 16px;
  border-radius: 8px;
  background: var(--surface);
  border: 0.5px solid var(--border);
  color: var(--text);
  font-size: 12px;
  font-family: inherit;
  opacity: 0;
  transform: translateY(10px);
  transition: all 0.3s ease;
  pointer-events: none;
  z-index: 100;
}
.toast.show { opacity: 1; transform: translateY(0); }
.toast.error { border-color: var(--red); color: var(--red); }
.toast.success { border-color: var(--green); color: var(--green); }
</style>
</head>
<body>
  <div class="header" id="admin-header">
    <span class="logo">&#9670; oreZ admin</span>
    <div class="spacer"></div>
    <span class="badge"><span class="dot"></span> pg <span id="pg-port">-</span></span>
    <span class="badge"><span class="dot"></span> zero <span id="zero-port">-</span></span>
    <span class="badge" id="sqlite-badge">sqlite: --</span>
    <span class="badge" id="uptime-badge">&#9201; --</span>
  </div>

  <div class="tabs" id="tab-bar">
    <button class="tab active" data-source="data">Data</button>
    <button class="tab" data-source="">Logs</button>
    <button class="tab" data-source="zero">Zero</button>
    <button class="tab" data-source="pglite">PGlite</button>
    <button class="tab" data-source="proxy">Proxy</button>
    <button class="tab" data-source="orez">Orez</button>
    <button class="tab" data-source="s3">S3</button>
    <button class="tab" data-source="http">HTTP</button>
    <button class="tab" data-source="env">Env</button>
  </div>

  <div class="toolbar" id="toolbar" style="display:none">
    <label>Level</label>
    <select id="level-filter">
      <option value="">all levels</option>
      <option value="error">error only</option>
      <option value="warn">warn+</option>
      <option value="info">info+</option>
      <option value="debug">debug+</option>
    </select>
    <div class="toolbar-actions" id="toolbar-log-actions">
      <button class="action-btn gray" onclick="doAction('clear-logs', this)">&#x2715; Clear</button>
    </div>
  </div>

  <div class="toolbar" id="zero-toolbar" style="display:none">
    <label>Level</label>
    <select id="zero-level-filter">
      <option value="">all levels</option>
      <option value="error">error only</option>
      <option value="warn">warn+</option>
      <option value="info" selected>info+</option>
      <option value="debug">debug+</option>
    </select>
    <div class="toolbar-actions">
      <button class="action-btn blue" data-zero-action onclick="doAction('restart-zero', this)">&#x21bb; Restart</button>
      <button class="action-btn orange" data-zero-action onclick="doAction('reset-zero', this)">&#x21ba; Reset</button>
      <button class="action-btn red" data-zero-action onclick="doAction('reset-zero-full', this)">&#x26a0; Full</button>
      <button class="action-btn gray" onclick="doAction('clear-logs', this)">&#x2715; Clear</button>
    </div>
  </div>

  <div class="toolbar" id="http-toolbar" style="display:none">
    <label>Filter</label>
    <input type="text" id="http-path-filter" placeholder="filter by path...">
    <div class="toolbar-actions">
      <button class="action-btn gray" onclick="doAction('clear-http', this)">&#x2715; Clear</button>
    </div>
  </div>

  <div class="content-area">
    <div class="data-view" id="data-view">
      <div class="data-toolbar">
        <div class="data-sub-tabs" id="data-sub-tabs">
          <button class="data-sub-tab active" data-db="postgres">Main</button>
          <button class="data-sub-tab" data-db="cvr">CVR</button>
          <button class="data-sub-tab" data-db="cdb">CDB</button>
          <button class="data-sub-tab" data-db="sqlite">SQLite</button>
        </div>
      </div>
      <div class="data-content">
        <div class="data-sidebar" id="data-sidebar">
          <div class="data-sidebar-header">Tables</div>
          <div class="data-search"><input type="text" id="data-table-search" placeholder="filter tables..."></div>
          <div id="data-table-list"></div>
        </div>
        <div class="data-main">
          <div class="sql-editor-wrap">
            <textarea class="sql-editor" id="sql-editor" rows="3" placeholder="SELECT * FROM ... (Cmd+Enter to run)" spellcheck="false"></textarea>
            <div class="sql-bar">
              <span class="sql-status" id="sql-status"></span>
              <button class="action-btn blue" id="sql-run-btn" onclick="runSql()">&#9654; Run</button>
            </div>
          </div>
          <div class="data-results" id="data-results">
            <div class="data-empty">select a table or run a query</div>
          </div>
          <div class="data-paging" id="data-paging" style="display:none">
            <span id="data-paging-info"></span>
            <div>
              <button id="data-prev-btn" onclick="browseTable(-1)" disabled>&#x25C0; Prev</button>
              <button id="data-next-btn" onclick="browseTable(1)">Next &#x25B6;</button>
            </div>
          </div>
        </div>
      </div>
    </div>

    <div class="row-detail-overlay" id="row-detail-overlay">
      <div class="row-detail-panel">
        <div class="row-detail-header">
          <span>Row detail</span>
          <div class="spacer"></div>
          <button class="row-detail-close" onclick="closeRowDetail()">&#x2715;</button>
        </div>
        <div class="row-detail-body" id="row-detail-body"></div>
      </div>
    </div>

    <div class="log-wrap">
      <div class="log-view" id="log-view"></div>
      <div class="env-view" id="env-view">
        <table class="env-table">
          <thead><tr><th>Variable</th><th>Value</th></tr></thead>
          <tbody id="env-body"></tbody>
        </table>
      </div>
      <div class="http-view" id="http-view">
        <table class="http-table">
          <thead><tr>
            <th>Time</th>
            <th>Method</th>
            <th>Path</th>
            <th>Status</th>
            <th>Duration</th>
            <th>Size</th>
          </tr></thead>
          <tbody id="http-body"></tbody>
        </table>
      </div>
      <button class="jump-btn" id="jump-btn" onclick="jumpToBottom()">&#x2193; Jump to bottom</button>
    </div>
  </div>

  <div class="toast" id="toast"></div>

<script>
// resolve initial tab from url path
var pathMap = {"/":"data","/data":"data","/all":"","/zero":"zero","/pglite":"pglite","/proxy":"proxy","/orez":"orez","/s3":"s3","/http":"http","/env":"env"};
var initPath = window.location.pathname.replace(/\\/$/, "") || "/";
var initSource = pathMap[initPath] !== undefined ? pathMap[initPath] : "data";
var standalone = initPath !== "/" && initPath !== "/data" && initPath !== "/all";
var activeSource = "";
var activeLevel = "";
var lastCursor = 0;
var autoScroll = true;
var envLoaded = false;
var isEnvTab = false;
var isHttpTab = false;
var isDataTab = initSource === "data";
var httpCursor = 0;
var httpAutoScroll = true;

// data explorer state
var dataDb = "postgres";
var dataTables = [];
var dataActiveTable = null;

var logView = document.getElementById("log-view");
var envView = document.getElementById("env-view");
var httpView = document.getElementById("http-view");
var dataView = document.getElementById("data-view");
var jumpBtn = document.getElementById("jump-btn");
var toastEl = document.getElementById("toast");
var toolbar = document.getElementById("toolbar");
var zeroToolbar = document.getElementById("zero-toolbar");
var httpToolbar = document.getElementById("http-toolbar");
var sqlEditor = document.getElementById("sql-editor");
var sqlStatus = document.getElementById("sql-status");
var dataResults = document.getElementById("data-results");

function sourceToPath(s) {
  if (s === "data") return "/data";
  return s ? "/" + s : "/all";
}

function switchTab(source, pushState) {
  isEnvTab = source === "env";
  isHttpTab = source === "http";
  isDataTab = source === "data";
  var isZero = source === "zero";
  if (pushState) history.pushState(null, "", sourceToPath(source));
  logView.style.display = "none";
  envView.style.display = "none";
  httpView.style.display = "none";
  dataView.style.display = "none";
  dataView.classList.remove("visible");
  toolbar.style.display = "none";
  zeroToolbar.style.display = "none";
  httpToolbar.style.display = "none";
  logView.parentElement.style.display = "none";
  if (isDataTab) {
    dataView.style.display = "flex";
    dataView.classList.add("visible");
    loadTables();
  } else if (isEnvTab) {
    logView.parentElement.style.display = "block";
    envView.style.display = "block";
    if (!envLoaded) loadEnv();
  } else if (isHttpTab) {
    logView.parentElement.style.display = "block";
    httpView.style.display = "block";
    httpToolbar.style.display = "flex";
    httpCursor = 0;
    document.getElementById("http-body").innerHTML = "";
    fetchHttp();
  } else {
    logView.parentElement.style.display = "block";
    logView.style.display = "block";
    activeSource = source;
    if (isZero) {
      zeroToolbar.style.display = "flex";
      activeLevel = "info";
    } else {
      toolbar.style.display = "flex";
      if (activeLevel === "info") { activeLevel = ""; document.getElementById("level-filter").value = ""; }
    }
    lastCursor = 0;
    logView.innerHTML = "";
    fetchLogs();
  }
}

// standalone mode: hide header + tabs
if (standalone) {
  document.getElementById("admin-header").style.display = "none";
  document.getElementById("tab-bar").style.display = "none";
}
// activate initial tab
switchTab(initSource, false);

document.getElementById("tab-bar").addEventListener("click", function(e) {
  var tab = e.target.closest(".tab");
  if (!tab) return;
  document.querySelectorAll("#tab-bar .tab").forEach(function(t) { t.classList.remove("active"); });
  tab.classList.add("active");
  switchTab(tab.dataset.source, true);
});

document.getElementById("level-filter").addEventListener("change", function(e) {
  activeLevel = e.target.value;
  lastCursor = 0;
  logView.innerHTML = "";
  fetchLogs();
});

document.getElementById("zero-level-filter").addEventListener("change", function(e) {
  activeLevel = e.target.value;
  lastCursor = 0;
  logView.innerHTML = "";
  fetchLogs();
});

var httpFilterTimeout = null;
document.getElementById("http-path-filter").addEventListener("input", function() {
  clearTimeout(httpFilterTimeout);
  httpFilterTimeout = setTimeout(function() {
    httpCursor = 0;
    document.getElementById("http-body").innerHTML = "";
    fetchHttp();
  }, 300);
});

logView.addEventListener("scroll", function() {
  var atBottom = logView.scrollHeight - logView.scrollTop - logView.clientHeight < 40;
  autoScroll = atBottom;
  jumpBtn.classList.toggle("visible", !atBottom);
});

httpView.addEventListener("scroll", function() {
  var atBottom = httpView.scrollHeight - httpView.scrollTop - httpView.clientHeight < 40;
  httpAutoScroll = atBottom;
});

function jumpToBottom() {
  var el = isHttpTab ? httpView : logView;
  el.scrollTop = el.scrollHeight;
  autoScroll = true;
  httpAutoScroll = true;
  jumpBtn.classList.remove("visible");
}

function fmtTime(ts) {
  var d = new Date(ts);
  return d.toLocaleTimeString("en-US", { hour12: false, hour: "2-digit", minute: "2-digit", second: "2-digit" })
    + "." + String(d.getMilliseconds()).padStart(3, "0");
}

function escHtml(s) {
  return s.replace(/&/g,"&amp;").replace(/</g,"&lt;").replace(/>/g,"&gt;");
}

function fmtSize(bytes) {
  if (bytes === 0 || bytes == null) return "-";
  if (bytes < 1024) return bytes + "B";
  if (bytes < 1024 * 1024) return (bytes / 1024).toFixed(1) + "kb";
  return (bytes / (1024 * 1024)).toFixed(1) + "MB";
}

function renderEntries(entries) {
  var frag = document.createDocumentFragment();
  for (var i = 0; i < entries.length; i++) {
    var e = entries[i];
    var div = document.createElement("div");
    div.className = "log-line level-" + e.level;
    div.innerHTML = '<span class="ts">' + fmtTime(e.ts) + "</span> "
      + '<span class="src ' + e.source + '">' + e.source.padEnd(6) + "</span> "
      + '<span class="msg">' + escHtml(e.msg) + "</span>";
    frag.appendChild(div);
  }
  logView.appendChild(frag);
  if (autoScroll) logView.scrollTop = logView.scrollHeight;
}

function renderHttpEntries(entries) {
  var tbody = document.getElementById("http-body");
  var frag = document.createDocumentFragment();
  for (var i = 0; i < entries.length; i++) {
    var e = entries[i];
    var tr = document.createElement("tr");
    tr.className = "http-row";
    tr.dataset.id = e.id;
    var mc = e.method.toLowerCase();
    var sc = "s" + String(e.status).charAt(0);
    tr.innerHTML = "<td>" + fmtTime(e.ts) + "</td>"
      + '<td><span class="method ' + mc + '">' + e.method + "</span></td>"
      + '<td class="path">' + escHtml(e.path) + "</td>"
      + '<td><span class="status ' + sc + '">' + e.status + "</span></td>"
      + '<td class="dur">' + e.duration + "ms</td>"
      + '<td class="sz">' + fmtSize(e.resSize) + "</td>";
    tr.addEventListener("click", (function(entry) {
      return function() { toggleHttpDetail(this, entry); };
    })(e));
    frag.appendChild(tr);
  }
  tbody.appendChild(frag);
  if (httpAutoScroll) httpView.scrollTop = httpView.scrollHeight;
}

function toggleHttpDetail(row, entry) {
  var next = row.nextElementSibling;
  if (next && next.classList.contains("http-detail")) {
    next.classList.toggle("open");
    return;
  }
  var detail = document.createElement("tr");
  detail.className = "http-detail open";
  var html = '<td colspan="6">';
  html += '<div class="hdr-section"><div class="hdr-title">request headers</div>';
  var rk = Object.keys(entry.reqHeaders || {}).sort();
  for (var i = 0; i < rk.length; i++) {
    html += '<div class="hdr-line"><span class="hdr-key">' + escHtml(rk[i]) + '</span>: <span class="hdr-val">' + escHtml(entry.reqHeaders[rk[i]]) + "</span></div>";
  }
  html += "</div>";
  html += '<div class="hdr-section"><div class="hdr-title">response headers</div>';
  var sk = Object.keys(entry.resHeaders || {}).sort();
  for (var j = 0; j < sk.length; j++) {
    html += '<div class="hdr-line"><span class="hdr-key">' + escHtml(sk[j]) + '</span>: <span class="hdr-val">' + escHtml(entry.resHeaders[sk[j]]) + "</span></div>";
  }
  html += "</div>";
  if (entry.reqSize > 0) html += '<div class="hdr-line"><span class="hdr-key">request body size</span>: <span class="hdr-val">' + fmtSize(entry.reqSize) + "</span></div>";
  html += "</td>";
  detail.innerHTML = html;
  row.parentNode.insertBefore(detail, row.nextSibling);
}

function fetchLogs() {
  var params = new URLSearchParams();
  if (activeSource) params.set("source", activeSource);
  if (activeLevel) params.set("level", activeLevel);
  if (lastCursor) params.set("since", String(lastCursor));
  fetch("/api/logs?" + params).then(function(res) { return res.json(); }).then(function(data) {
    if (data.entries && data.entries.length > 0) renderEntries(data.entries);
    if (data.cursor) lastCursor = data.cursor;
  }).catch(function() {});
}

function fetchHttp() {
  var params = new URLSearchParams();
  if (httpCursor) params.set("since", String(httpCursor));
  var pathFilter = document.getElementById("http-path-filter").value;
  if (pathFilter) params.set("path", pathFilter);
  fetch("/api/http-log?" + params).then(function(res) { return res.json(); }).then(function(data) {
    if (data.entries && data.entries.length > 0) renderHttpEntries(data.entries);
    if (data.cursor) httpCursor = data.cursor;
  }).catch(function() {});
}

function loadEnv() {
  fetch("/api/env").then(function(res) { return res.json(); }).then(function(data) {
    var tbody = document.getElementById("env-body");
    tbody.innerHTML = "";
    var keys = Object.keys(data.env).sort();
    for (var i = 0; i < keys.length; i++) {
      var tr = document.createElement("tr");
      tr.innerHTML = "<td>" + escHtml(keys[i]) + "</td><td>" + escHtml(String(data.env[keys[i]])) + "</td>";
      tbody.appendChild(tr);
    }
    envLoaded = true;
  }).catch(function() {});
}

function fetchStatus() {
  fetch("/api/status").then(function(res) { return res.json(); }).then(function(data) {
    document.getElementById("pg-port").textContent = ":" + data.pgPort;
    document.getElementById("zero-port").textContent = ":" + data.zeroPort;
    document.getElementById("sqlite-badge").textContent = "sqlite: " + (data.sqliteMode || "wasm");
    var m = Math.floor(data.uptime / 60);
    var s = data.uptime % 60;
    document.getElementById("uptime-badge").textContent = "\\u23F1 " + (m > 0 ? m + "m " : "") + s + "s";
    var zeroDisabled = data.skipZeroCache;
    document.querySelectorAll("[data-zero-action]").forEach(function(btn) {
      btn.disabled = zeroDisabled;
    });
  }).catch(function() {});
}

function doAction(action, btn) {
  if (action === "reset-zero") {
    if (!confirm("Reset zero-cache? This deletes the replica and resyncs from scratch.")) return;
  }
  if (action === "reset-zero-full") {
    if (!confirm("Full reset zero state? This deletes CVR, CDB, and replica databases. Use after schema changes.")) return;
  }
  btn.disabled = true;
  var origText = btn.textContent;
  btn.textContent = "...";
  fetch("/api/actions/" + action, { method: "POST" })
    .then(function(res) { return res.json(); })
    .then(function(data) {
      showToast(data.message || "done", data.ok ? "success" : "error");
      if (action === "clear-logs") {
        logView.innerHTML = "";
        lastCursor = 0;
      }
      if (action === "clear-http") {
        document.getElementById("http-body").innerHTML = "";
        httpCursor = 0;
      }
    })
    .catch(function(err) {
      showToast("failed: " + err.message, "error");
    })
    .finally(function() {
      btn.disabled = false;
      btn.textContent = origText;
    });
}

function showToast(msg, type) {
  toastEl.textContent = msg;
  toastEl.className = "toast " + type + " show";
  setTimeout(function() { toastEl.className = "toast"; }, 2500);
}

// --- data explorer ---

var isSqlite = false;
var browseOffset = 0;
var browseTotal = 0;
var browseSearch = "";
var browseLimit = 100;
var lastBrowseFields = [];
var lastBrowseRows = [];

document.getElementById("data-sub-tabs").addEventListener("click", function(e) {
  var btn = e.target.closest(".data-sub-tab");
  if (!btn) return;
  document.querySelectorAll(".data-sub-tab").forEach(function(t) { t.classList.remove("active"); });
  btn.classList.add("active");
  dataDb = btn.dataset.db;
  isSqlite = dataDb === "sqlite";
  dataActiveTable = null;
  browseOffset = 0;
  browseSearch = "";
  document.getElementById("data-table-search").value = "";
  document.getElementById("data-paging").style.display = "none";
  dataResults.innerHTML = '<div class="data-empty">select a table or run a query</div>';
  sqlEditor.value = "";
  sqlStatus.textContent = "";
  loadTables();
});

var tableSearchTimeout = null;
document.getElementById("data-table-search").addEventListener("input", function() {
  clearTimeout(tableSearchTimeout);
  tableSearchTimeout = setTimeout(renderTableList, 150);
});

function loadTables() {
  var url = isSqlite ? "/api/sqlite/tables" : "/api/db/tables?db=" + dataDb;
  fetch(url).then(function(r) { return r.json(); }).then(function(data) {
    if (data.error) {
      document.getElementById("data-table-list").innerHTML = '<div style="padding:8px 10px;color:var(--red);font-size:11px">' + escHtml(data.error) + '</div>';
      return;
    }
    dataTables = data.tables || [];
    renderTableList();
  }).catch(function() {
    document.getElementById("data-table-list").innerHTML = '<div style="padding:8px 10px;color:var(--text-dim);font-size:11px">failed to load tables</div>';
  });
}

function renderTableList() {
  var filter = (document.getElementById("data-table-search").value || "").toLowerCase();
  var list = document.getElementById("data-table-list");
  list.innerHTML = "";
  for (var i = 0; i < dataTables.length; i++) {
    var t = dataTables[i];
    var fullName = isSqlite ? t.name : (t.table_schema === "public" ? t.table_name : t.table_schema + "." + t.table_name);
    if (filter && fullName.toLowerCase().indexOf(filter) === -1) continue;
    var div = document.createElement("div");
    div.className = "data-table-item";
    if (dataActiveTable === fullName) div.classList.add("active");
    var sizeText = isSqlite ? (t.col_count + " cols") : fmtSize(t.size_bytes);
    div.innerHTML = '<span class="tbl-name">' + escHtml(fullName) + '</span><span class="tbl-size">' + sizeText + '</span>';
    div.dataset.table = fullName;
    div.addEventListener("click", function() {
      dataActiveTable = this.dataset.table;
      document.querySelectorAll(".data-table-item").forEach(function(el) { el.classList.remove("active"); });
      this.classList.add("active");
      browseOffset = 0;
      browseSearch = "";
      browseTableData();
    });
    list.appendChild(div);
  }
}

function browseTableData() {
  if (!dataActiveTable) return;
  var baseUrl = isSqlite ? "/api/sqlite/table-data" : "/api/db/table-data";
  var params = new URLSearchParams();
  if (!isSqlite) params.set("db", dataDb);
  params.set("table", dataActiveTable);
  params.set("offset", String(browseOffset));
  params.set("limit", String(browseLimit));
  if (browseSearch) params.set("search", browseSearch);
  sqlStatus.textContent = "loading...";
  sqlStatus.className = "sql-status";
  sqlEditor.value = "";
  fetch(baseUrl + "?" + params).then(function(r) { return r.json(); }).then(function(data) {
    if (data.error) {
      sqlStatus.textContent = data.error;
      sqlStatus.className = "sql-status error";
      return;
    }
    browseTotal = data.total;
    var cols = data.columns || [];
    var fields = cols.map(function(c) { return c.name; });
    lastBrowseFields = fields;
    lastBrowseRows = data.rows;
    sqlStatus.textContent = data.total + " total row" + (data.total !== 1 ? "s" : "");
    sqlStatus.className = "sql-status";
    renderBrowseResults(fields, data.rows);
    updatePaging();
  }).catch(function(err) {
    sqlStatus.textContent = err.message;
    sqlStatus.className = "sql-status error";
  });
}

function browseTable(dir) {
  browseOffset = Math.max(0, browseOffset + dir * browseLimit);
  browseTableData();
}

function updatePaging() {
  var paging = document.getElementById("data-paging");
  if (browseTotal <= browseLimit && browseOffset === 0) {
    paging.style.display = "none";
    return;
  }
  paging.style.display = "flex";
  var from = browseOffset + 1;
  var to = Math.min(browseOffset + browseLimit, browseTotal);
  document.getElementById("data-paging-info").textContent = from + "-" + to + " of " + browseTotal;
  document.getElementById("data-prev-btn").disabled = browseOffset === 0;
  document.getElementById("data-next-btn").disabled = browseOffset + browseLimit >= browseTotal;
}

function quoteIdent(name) {
  if (name.indexOf(".") > -1) {
    var parts = name.split(".");
    return '"' + parts[0] + '"."' + parts[1] + '"';
  }
  if (/^[a-z_][a-z0-9_]*$/.test(name)) return name;
  return '"' + name + '"';
}

function runSql() {
  var sql = sqlEditor.value.trim();
  if (!sql) return;
  var btn = document.getElementById("sql-run-btn");
  btn.disabled = true;
  sqlStatus.textContent = "running...";
  sqlStatus.className = "sql-status";
  document.getElementById("data-paging").style.display = "none";
  var endpoint = isSqlite ? "/api/sqlite/query" : "/api/db/query";
  var body = isSqlite ? { sql: sql } : { db: dataDb, sql: sql };
  fetch(endpoint, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body)
  }).then(function(r) { return r.json(); }).then(function(data) {
    btn.disabled = false;
    if (data.error) {
      sqlStatus.textContent = data.error;
      sqlStatus.className = "sql-status error";
      return;
    }
    sqlStatus.textContent = data.rowCount + " row" + (data.rowCount !== 1 ? "s" : "") + " in " + data.durationMs + "ms";
    sqlStatus.className = "sql-status";
    lastBrowseFields = data.fields;
    lastBrowseRows = data.rows;
    renderBrowseResults(data.fields, data.rows);
  }).catch(function(err) {
    btn.disabled = false;
    sqlStatus.textContent = err.message;
    sqlStatus.className = "sql-status error";
  });
}

function renderBrowseResults(fields, rows) {
  if (!fields || fields.length === 0) {
    dataResults.innerHTML = '<div class="data-empty">no columns returned</div>';
    return;
  }
  var html = '<table><thead><tr>';
  for (var i = 0; i < fields.length; i++) {
    html += '<th>' + escHtml(fields[i]) + '</th>';
  }
  html += '</tr></thead><tbody>';
  for (var r = 0; r < rows.length; r++) {
    html += '<tr class="clickable" data-row-idx="' + r + '">';
    for (var c = 0; c < fields.length; c++) {
      var val = rows[r][fields[c]];
      if (val === null || val === undefined) {
        html += '<td class="null-val">null</td>';
      } else if (typeof val === "object") {
        html += '<td>' + escHtml(JSON.stringify(val)) + '</td>';
      } else {
        var s = String(val);
        html += '<td>' + escHtml(s.length > 120 ? s.slice(0, 120) + "..." : s) + '</td>';
      }
    }
    html += '</tr>';
  }
  html += '</tbody></table>';
  dataResults.innerHTML = html;
  // click rows to open detail
  dataResults.querySelectorAll("tr.clickable").forEach(function(tr) {
    tr.addEventListener("click", function() {
      var idx = Number(this.dataset.rowIdx);
      openRowDetail(lastBrowseFields, lastBrowseRows[idx]);
    });
  });
}

function openRowDetail(fields, row) {
  if (!row) return;
  var body = document.getElementById("row-detail-body");
  var html = "";
  for (var i = 0; i < fields.length; i++) {
    var val = row[fields[i]];
    var valStr;
    var cls = "row-detail-val";
    if (val === null || val === undefined) {
      valStr = "null";
      cls += " null-val";
    } else if (typeof val === "object") {
      valStr = JSON.stringify(val, null, 2);
    } else {
      valStr = String(val);
    }
    html += '<div class="row-detail-field">';
    html += '<div class="row-detail-key">' + escHtml(fields[i]) + '</div>';
    html += '<div class="' + cls + '">' + escHtml(valStr) + '</div>';
    html += '</div>';
  }
  body.innerHTML = html;
  document.getElementById("row-detail-overlay").classList.add("open");
}

function closeRowDetail() {
  document.getElementById("row-detail-overlay").classList.remove("open");
}

document.getElementById("row-detail-overlay").addEventListener("click", function(e) {
  if (e.target === this) closeRowDetail();
});

document.addEventListener("keydown", function(e) {
  if (e.key === "Escape") closeRowDetail();
});

// cmd+enter / ctrl+enter to run sql
sqlEditor.addEventListener("keydown", function(e) {
  if ((e.metaKey || e.ctrlKey) && e.key === "Enter") {
    e.preventDefault();
    runSql();
  }
});

// --- polling ---

fetchStatus();
setInterval(function() {
  if (document.hidden) return;
  if (isDataTab) return;
  if (isHttpTab) fetchHttp();
  else if (!isEnvTab) fetchLogs();
}, 1000);
setInterval(function() { if (!document.hidden) fetchStatus(); }, 5000);
document.addEventListener("visibilitychange", function() {
  if (document.hidden) return;
  if (isDataTab) return;
  if (isHttpTab) fetchHttp();
  else if (!isEnvTab) fetchLogs();
  fetchStatus();
});
window.addEventListener("popstate", function() {
  var p = window.location.pathname.replace(/\\/$/, "") || "/";
  var s = pathMap[p] !== undefined ? pathMap[p] : "data";
  var tab = document.querySelector('#tab-bar .tab[data-source="' + s + '"]');
  if (tab) tab.click();
});
</script>
</body>
</html>`
}
