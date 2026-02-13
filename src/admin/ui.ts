export function getAdminHtml(): string {
  return (
    '<!DOCTYPE html>\n' +
    '<html lang="en">\n' +
    '<head>\n' +
    '<meta charset="utf-8">\n' +
    '<meta name="viewport" content="width=device-width, initial-scale=1">\n' +
    '<title>oreZ admin</title>\n' +
    '<style>\n' +
    ':root {\n' +
    '  --bg: #000;\n' +
    '  --surface: #0a0a0a;\n' +
    '  --border: #222;\n' +
    '  --text: #fff;\n' +
    '  --text-dim: #666;\n' +
    '  --accent: #fff;\n' +
    '  --green: #888;\n' +
    '  --yellow: #999;\n' +
    '  --red: #f55;\n' +
    '  --purple: #aaa;\n' +
    '}\n' +
    '* { margin: 0; padding: 0; box-sizing: border-box; }\n' +
    'body {\n' +
    '  font-family: -apple-system, BlinkMacSystemFont, "SF Pro", system-ui, sans-serif;\n' +
    '  background: var(--bg);\n' +
    '  color: var(--text);\n' +
    '  height: 100vh;\n' +
    '  display: flex;\n' +
    '  flex-direction: column;\n' +
    '  overflow: hidden;\n' +
    '}\n' +
    '.header {\n' +
    '  display: flex;\n' +
    '  align-items: center;\n' +
    '  padding: 12px 16px;\n' +
    '  background: var(--surface);\n' +
    '  border-bottom: 0.5px solid var(--border);\n' +
    '  gap: 12px;\n' +
    '  flex-shrink: 0;\n' +
    '}\n' +
    '.header .logo {\n' +
    '  font-size: 15px;\n' +
    '  font-weight: 700;\n' +
    '  color: var(--accent);\n' +
    '  letter-spacing: -0.5px;\n' +
    '}\n' +
    '.badge {\n' +
    '  display: inline-flex;\n' +
    '  align-items: center;\n' +
    '  padding: 2px 8px;\n' +
    '  border-radius: 12px;\n' +
    '  font-size: 11px;\n' +
    '  border: 0.5px solid var(--border);\n' +
    '  color: var(--text-dim);\n' +
    '  gap: 4px;\n' +
    '}\n' +
    '.badge .dot {\n' +
    '  width: 6px;\n' +
    '  height: 6px;\n' +
    '  border-radius: 50%;\n' +
    '  background: var(--green);\n' +
    '}\n' +
    '.spacer { flex: 1; }\n' +
    '.tabs {\n' +
    '  display: flex;\n' +
    '  padding: 0 16px;\n' +
    '  background: var(--surface);\n' +
    '  border-bottom: 0.5px solid var(--border);\n' +
    '  gap: 2px;\n' +
    '  flex-shrink: 0;\n' +
    '}\n' +
    '.tab {\n' +
    '  padding: 8px 14px;\n' +
    '  font-size: 12px;\n' +
    '  color: var(--text-dim);\n' +
    '  cursor: pointer;\n' +
    '  border-bottom: 2px solid transparent;\n' +
    '  transition: all 0.15s;\n' +
    '  background: none;\n' +
    '  border-top: none;\n' +
    '  border-left: none;\n' +
    '  border-right: none;\n' +
    '  font-family: inherit;\n' +
    '}\n' +
    '.tab:hover { color: var(--text); }\n' +
    '.tab.active {\n' +
    '  color: var(--accent);\n' +
    '  border-bottom-color: var(--accent);\n' +
    '}\n' +
    '.toolbar {\n' +
    '  display: flex;\n' +
    '  align-items: center;\n' +
    '  padding: 8px 16px;\n' +
    '  gap: 10px;\n' +
    '  border-bottom: 0.5px solid var(--border);\n' +
    '  flex-shrink: 0;\n' +
    '}\n' +
    '.toolbar label {\n' +
    '  font-size: 11px;\n' +
    '  color: var(--text-dim);\n' +
    '  text-transform: uppercase;\n' +
    '  letter-spacing: 0.5px;\n' +
    '}\n' +
    '.toolbar select {\n' +
    '  background: var(--surface);\n' +
    '  color: var(--text);\n' +
    '  border: 0.5px solid var(--border);\n' +
    '  border-radius: 6px;\n' +
    '  padding: 4px 8px;\n' +
    '  font-size: 12px;\n' +
    '  font-family: inherit;\n' +
    '  cursor: pointer;\n' +
    '}\n' +
    '.toolbar select:focus { outline: none; border-color: var(--accent); }\n' +
    '.toolbar input[type="text"] {\n' +
    '  background: var(--surface);\n' +
    '  color: var(--text);\n' +
    '  border: 0.5px solid var(--border);\n' +
    '  border-radius: 6px;\n' +
    '  padding: 4px 8px;\n' +
    '  font-size: 12px;\n' +
    '  font-family: inherit;\n' +
    '  width: 200px;\n' +
    '}\n' +
    '.toolbar input[type="text"]:focus { outline: none; border-color: var(--accent); }\n' +
    '.toolbar input[type="text"]::placeholder { color: var(--text-dim); }\n' +
    '.sep { width: 1px; height: 20px; background: var(--border); }\n' +
    '.action-btn {\n' +
    '  padding: 5px 12px;\n' +
    '  border-radius: 6px;\n' +
    '  border: 1px solid;\n' +
    '  background: transparent;\n' +
    '  cursor: pointer;\n' +
    '  font-family: inherit;\n' +
    '  font-size: 11px;\n' +
    '  transition: all 0.15s ease;\n' +
    '  white-space: nowrap;\n' +
    '}\n' +
    '.action-btn:disabled { opacity: 0.4; cursor: not-allowed; }\n' +
    '.action-btn.blue { color: var(--accent); border-color: #ffffff22; }\n' +
    '.action-btn.blue:hover:not(:disabled) { background: #ffffff11; border-color: var(--accent); }\n' +
    '.action-btn.orange { color: var(--yellow); border-color: #ffffff22; }\n' +
    '.action-btn.orange:hover:not(:disabled) { background: #ffffff11; border-color: var(--yellow); }\n' +
    '.action-btn.red { color: var(--red); border-color: #ff555522; }\n' +
    '.action-btn.red:hover:not(:disabled) { background: #ff555511; border-color: var(--red); }\n' +
    '.action-btn.gray { color: var(--text-dim); border-color: #ffffff22; }\n' +
    '.action-btn.gray:hover:not(:disabled) { background: #ffffff11; border-color: var(--text-dim); }\n' +
    '.content-area {\n' +
    '  flex: 1;\n' +
    '  overflow: hidden;\n' +
    '  position: relative;\n' +
    '  display: flex;\n' +
    '  flex-direction: column;\n' +
    '}\n' +
    '.log-wrap {\n' +
    '  flex: 1;\n' +
    '  overflow: hidden;\n' +
    '  position: relative;\n' +
    '}\n' +
    '.log-view {\n' +
    '  height: 100%;\n' +
    '  overflow-y: auto;\n' +
    '  padding: 8px 16px;\n' +
    '  font-size: 12px;\n' +
    '  line-height: 1.5;\n' +
    '}\n' +
    '.log-line { white-space: pre-wrap; word-break: break-all; }\n' +
    '.log-line .ts { color: var(--text-dim); }\n' +
    '.log-line .src { display: inline-block; width: 7ch; }\n' +
    '.log-line .src.zero { color: var(--purple); }\n' +
    '.log-line .src.pglite { color: var(--green); }\n' +
    '.log-line .src.proxy { color: var(--yellow); }\n' +
    '.log-line .src.orez { color: var(--accent); }\n' +
    '.log-line .src.s3 { color: #888; }\n' +
    '.log-line.level-error .msg { color: var(--red); }\n' +
    '.log-line.level-warn .msg { color: var(--yellow); }\n' +
    '.log-line.level-info .msg { color: var(--text); }\n' +
    '.log-line.level-debug .msg { color: var(--text-dim); }\n' +
    '.jump-btn {\n' +
    '  position: absolute;\n' +
    '  bottom: 16px;\n' +
    '  left: 50%;\n' +
    '  transform: translateX(-50%);\n' +
    '  padding: 6px 16px;\n' +
    '  border-radius: 20px;\n' +
    '  background: var(--accent);\n' +
    '  color: #fff;\n' +
    '  border: none;\n' +
    '  font-size: 12px;\n' +
    '  font-family: inherit;\n' +
    '  cursor: pointer;\n' +
    '  opacity: 0;\n' +
    '  transition: opacity 0.2s;\n' +
    '  pointer-events: none;\n' +
    '  z-index: 10;\n' +
    '}\n' +
    '.jump-btn.visible { opacity: 1; pointer-events: auto; }\n' +
    '.env-view {\n' +
    '  height: 100%;\n' +
    '  overflow-y: auto;\n' +
    '  padding: 16px;\n' +
    '  display: none;\n' +
    '}\n' +
    '.env-table { width: 100%; border-collapse: collapse; font-size: 12px; }\n' +
    '.env-table th {\n' +
    '  text-align: left;\n' +
    '  padding: 6px 12px;\n' +
    '  color: var(--text-dim);\n' +
    '  border-bottom: 0.5px solid var(--border);\n' +
    '  font-weight: 500;\n' +
    '  text-transform: uppercase;\n' +
    '  font-size: 10px;\n' +
    '  letter-spacing: 0.5px;\n' +
    '}\n' +
    '.env-table td {\n' +
    '  padding: 6px 12px;\n' +
    '  border-bottom: 0.5px solid var(--border);\n' +
    '}\n' +
    '.env-table td:first-child { color: var(--accent); white-space: nowrap; }\n' +
    '.env-table td:last-child { color: var(--text); word-break: break-all; }\n' +
    '.env-table tr:hover td { background: #111; }\n' +
    // http view
    '.http-view {\n' +
    '  height: 100%;\n' +
    '  overflow-y: auto;\n' +
    '  padding: 0;\n' +
    '  display: none;\n' +
    '}\n' +
    '.http-table { width: 100%; border-collapse: collapse; font-size: 12px; }\n' +
    '.http-table th {\n' +
    '  text-align: left;\n' +
    '  padding: 6px 12px;\n' +
    '  color: var(--text-dim);\n' +
    '  border-bottom: 0.5px solid var(--border);\n' +
    '  font-weight: 500;\n' +
    '  text-transform: uppercase;\n' +
    '  font-size: 10px;\n' +
    '  letter-spacing: 0.5px;\n' +
    '  position: sticky;\n' +
    '  top: 0;\n' +
    '  background: var(--bg);\n' +
    '  z-index: 1;\n' +
    '}\n' +
    '.http-table td {\n' +
    '  padding: 5px 12px;\n' +
    '  border-bottom: 0.5px solid var(--border);\n' +
    '  white-space: nowrap;\n' +
    '}\n' +
    '.http-table tr.http-row { cursor: pointer; }\n' +
    '.http-table tr.http-row:hover td { background: #111; }\n' +
    '.http-table .method { font-weight: 600; }\n' +
    '.http-table .method.get { color: var(--green); }\n' +
    '.http-table .method.post { color: var(--yellow); }\n' +
    '.http-table .method.put { color: var(--accent); }\n' +
    '.http-table .method.delete { color: var(--red); }\n' +
    '.http-table .method.patch { color: #888; }\n' +
    '.http-table .method.ws { color: var(--purple); }\n' +
    '.http-table .status.s2 { color: var(--green); }\n' +
    '.http-table .status.s3 { color: var(--yellow); }\n' +
    '.http-table .status.s4 { color: var(--red); }\n' +
    '.http-table .status.s5 { color: var(--red); font-weight: 600; }\n' +
    '.http-table .path { color: var(--text); max-width: 500px; overflow: hidden; text-overflow: ellipsis; }\n' +
    '.http-table .dur { color: var(--text-dim); }\n' +
    '.http-table .sz { color: var(--text-dim); }\n' +
    '.http-detail {\n' +
    '  display: none;\n' +
    '}\n' +
    '.http-detail.open { display: table-row; }\n' +
    '.http-detail td {\n' +
    '  padding: 8px 12px 12px 24px;\n' +
    '  background: #080808;\n' +
    '  border-bottom: 0.5px solid var(--border);\n' +
    '}\n' +
    '.http-detail .hdr-section { margin-bottom: 8px; }\n' +
    '.http-detail .hdr-title {\n' +
    '  font-size: 10px;\n' +
    '  text-transform: uppercase;\n' +
    '  color: var(--text-dim);\n' +
    '  letter-spacing: 0.5px;\n' +
    '  margin-bottom: 4px;\n' +
    '}\n' +
    '.http-detail .hdr-line {\n' +
    '  font-size: 11px;\n' +
    '  line-height: 1.6;\n' +
    '  white-space: pre-wrap;\n' +
    '  word-break: break-all;\n' +
    '}\n' +
    '.http-detail .hdr-key { color: var(--accent); }\n' +
    '.http-detail .hdr-val { color: var(--text-dim); }\n' +
    // actions panel
    '.actions-panel {\n' +
    '  flex-shrink: 0;\n' +
    '  border-top: 0.5px solid var(--border);\n' +
    '  background: var(--surface);\n' +
    '  padding: 8px 16px;\n' +
    '}\n' +
    '.action-row {\n' +
    '  display: flex;\n' +
    '  align-items: center;\n' +
    '  gap: 8px;\n' +
    '  padding: 4px 0;\n' +
    '}\n' +
    '.action-label {\n' +
    '  font-size: 11px;\n' +
    '  font-weight: 600;\n' +
    '  width: 7ch;\n' +
    '  flex-shrink: 0;\n' +
    '}\n' +
    '.action-label.zero { color: var(--purple); }\n' +
    '.action-label.logs { color: var(--text-dim); }\n' +
    // toast
    '.toast {\n' +
    '  position: fixed;\n' +
    '  bottom: 20px;\n' +
    '  right: 20px;\n' +
    '  padding: 10px 16px;\n' +
    '  border-radius: 8px;\n' +
    '  background: var(--surface);\n' +
    '  border: 0.5px solid var(--border);\n' +
    '  color: var(--text);\n' +
    '  font-size: 12px;\n' +
    '  font-family: inherit;\n' +
    '  opacity: 0;\n' +
    '  transform: translateY(10px);\n' +
    '  transition: all 0.3s ease;\n' +
    '  pointer-events: none;\n' +
    '  z-index: 100;\n' +
    '}\n' +
    '.toast.show { opacity: 1; transform: translateY(0); }\n' +
    '.toast.error { border-color: var(--red); color: var(--red); }\n' +
    '.toast.success { border-color: var(--green); color: var(--green); }\n' +
    '</style>\n' +
    '</head>\n' +
    '<body>\n' +
    '  <div class="header">\n' +
    '    <span class="logo">&#9670; oreZ admin</span>\n' +
    '    <div class="spacer"></div>\n' +
    '    <span class="badge"><span class="dot"></span> pg <span id="pg-port">-</span></span>\n' +
    '    <span class="badge"><span class="dot"></span> zero <span id="zero-port">-</span></span>\n' +
    '    <span class="badge" id="uptime-badge">&#9201; --</span>\n' +
    '  </div>\n' +
    '\n' +
    '  <div class="tabs" id="tab-bar">\n' +
    '    <button class="tab active" data-source="">All</button>\n' +
    '    <button class="tab" data-source="zero">Zero</button>\n' +
    '    <button class="tab" data-source="pglite">PGlite</button>\n' +
    '    <button class="tab" data-source="proxy">Proxy</button>\n' +
    '    <button class="tab" data-source="orez">Orez</button>\n' +
    '    <button class="tab" data-source="s3">S3</button>\n' +
    '    <button class="tab" data-source="http">HTTP</button>\n' +
    '    <button class="tab" data-source="env">Env</button>\n' +
    '  </div>\n' +
    '\n' +
    '  <div class="toolbar" id="toolbar">\n' +
    '    <label>Level</label>\n' +
    '    <select id="level-filter">\n' +
    '      <option value="">all levels</option>\n' +
    '      <option value="error">error only</option>\n' +
    '      <option value="warn">warn+</option>\n' +
    '      <option value="info">info+</option>\n' +
    '      <option value="debug">debug+</option>\n' +
    '    </select>\n' +
    '  </div>\n' +
    '\n' +
    '  <div class="toolbar" id="http-toolbar" style="display:none">\n' +
    '    <label>Filter</label>\n' +
    '    <input type="text" id="http-path-filter" placeholder="filter by path...">\n' +
    '  </div>\n' +
    '\n' +
    '  <div class="content-area">\n' +
    '    <div class="log-wrap">\n' +
    '      <div class="log-view" id="log-view"></div>\n' +
    '      <div class="env-view" id="env-view">\n' +
    '        <table class="env-table">\n' +
    '          <thead><tr><th>Variable</th><th>Value</th></tr></thead>\n' +
    '          <tbody id="env-body"></tbody>\n' +
    '        </table>\n' +
    '      </div>\n' +
    '      <div class="http-view" id="http-view">\n' +
    '        <table class="http-table">\n' +
    '          <thead><tr>\n' +
    '            <th>Time</th>\n' +
    '            <th>Method</th>\n' +
    '            <th>Path</th>\n' +
    '            <th>Status</th>\n' +
    '            <th>Duration</th>\n' +
    '            <th>Size</th>\n' +
    '          </tr></thead>\n' +
    '          <tbody id="http-body"></tbody>\n' +
    '        </table>\n' +
    '      </div>\n' +
    '      <button class="jump-btn" id="jump-btn" onclick="jumpToBottom()">&#x2193; Jump to bottom</button>\n' +
    '    </div>\n' +
    '\n' +
    '    <div class="actions-panel" id="actions-panel">\n' +
    '      <div class="action-row">\n' +
    '        <span class="action-label zero">zero</span>\n' +
    '        <button class="action-btn blue" data-zero-action onclick="doAction(\'restart-zero\', this)">&#x21bb; Restart</button>\n' +
    '        <button class="action-btn orange" data-zero-action onclick="doAction(\'reset-zero\', this)">&#x21ba; Reset</button>\n' +
    '      </div>\n' +
    '      <div class="action-row">\n' +
    '        <span class="action-label logs">logs</span>\n' +
    '        <button class="action-btn gray" onclick="doAction(\'clear-logs\', this)">&#x2715; Clear Logs</button>\n' +
    '        <button class="action-btn gray" onclick="doAction(\'clear-http\', this)">&#x2715; Clear HTTP</button>\n' +
    '      </div>\n' +
    '    </div>\n' +
    '  </div>\n' +
    '\n' +
    '  <div class="toast" id="toast"></div>\n' +
    '\n' +
    '<script>\n' +
    'var activeSource = "";\n' +
    'var activeLevel = "";\n' +
    'var levelSetByUser = false;\n' +
    'var lastCursor = 0;\n' +
    'var autoScroll = true;\n' +
    'var envLoaded = false;\n' +
    'var isEnvTab = false;\n' +
    'var isHttpTab = false;\n' +
    'var httpCursor = 0;\n' +
    'var httpAutoScroll = true;\n' +
    '\n' +
    'var logView = document.getElementById("log-view");\n' +
    'var envView = document.getElementById("env-view");\n' +
    'var httpView = document.getElementById("http-view");\n' +
    'var jumpBtn = document.getElementById("jump-btn");\n' +
    'var toastEl = document.getElementById("toast");\n' +
    'var toolbar = document.getElementById("toolbar");\n' +
    'var httpToolbar = document.getElementById("http-toolbar");\n' +
    '\n' +
    'document.getElementById("tab-bar").addEventListener("click", function(e) {\n' +
    '  var tab = e.target.closest(".tab");\n' +
    '  if (!tab) return;\n' +
    '  document.querySelectorAll(".tab").forEach(function(t) { t.classList.remove("active"); });\n' +
    '  tab.classList.add("active");\n' +
    '  var source = tab.dataset.source;\n' +
    '  isEnvTab = source === "env";\n' +
    '  isHttpTab = source === "http";\n' +
    '  logView.style.display = "none";\n' +
    '  envView.style.display = "none";\n' +
    '  httpView.style.display = "none";\n' +
    '  toolbar.style.display = "none";\n' +
    '  httpToolbar.style.display = "none";\n' +
    '  if (isEnvTab) {\n' +
    '    envView.style.display = "block";\n' +
    '    if (!envLoaded) loadEnv();\n' +
    '  } else if (isHttpTab) {\n' +
    '    httpView.style.display = "block";\n' +
    '    httpToolbar.style.display = "flex";\n' +
    '    httpCursor = 0;\n' +
    '    document.getElementById("http-body").innerHTML = "";\n' +
    '    fetchHttp();\n' +
    '  } else {\n' +
    '    logView.style.display = "block";\n' +
    '    toolbar.style.display = "flex";\n' +
    '    activeSource = source;\n' +
    '    lastCursor = 0;\n' +
    '    logView.innerHTML = "";\n' +
    '    fetchLogs();\n' +
    '  }\n' +
    '});\n' +
    '\n' +
    'document.getElementById("level-filter").addEventListener("change", function(e) {\n' +
    '  activeLevel = e.target.value;\n' +
    '  levelSetByUser = true;\n' +
    '  lastCursor = 0;\n' +
    '  logView.innerHTML = "";\n' +
    '  fetchLogs();\n' +
    '});\n' +
    '\n' +
    'var httpFilterTimeout = null;\n' +
    'document.getElementById("http-path-filter").addEventListener("input", function() {\n' +
    '  clearTimeout(httpFilterTimeout);\n' +
    '  httpFilterTimeout = setTimeout(function() {\n' +
    '    httpCursor = 0;\n' +
    '    document.getElementById("http-body").innerHTML = "";\n' +
    '    fetchHttp();\n' +
    '  }, 300);\n' +
    '});\n' +
    '\n' +
    'logView.addEventListener("scroll", function() {\n' +
    '  var atBottom = logView.scrollHeight - logView.scrollTop - logView.clientHeight < 40;\n' +
    '  autoScroll = atBottom;\n' +
    '  jumpBtn.classList.toggle("visible", !atBottom);\n' +
    '});\n' +
    '\n' +
    'httpView.addEventListener("scroll", function() {\n' +
    '  var atBottom = httpView.scrollHeight - httpView.scrollTop - httpView.clientHeight < 40;\n' +
    '  httpAutoScroll = atBottom;\n' +
    '});\n' +
    '\n' +
    'function jumpToBottom() {\n' +
    '  var el = isHttpTab ? httpView : logView;\n' +
    '  el.scrollTop = el.scrollHeight;\n' +
    '  autoScroll = true;\n' +
    '  httpAutoScroll = true;\n' +
    '  jumpBtn.classList.remove("visible");\n' +
    '}\n' +
    '\n' +
    'function fmtTime(ts) {\n' +
    '  var d = new Date(ts);\n' +
    '  return d.toLocaleTimeString("en-US", { hour12: false, hour: "2-digit", minute: "2-digit", second: "2-digit" })\n' +
    '    + "." + String(d.getMilliseconds()).padStart(3, "0");\n' +
    '}\n' +
    '\n' +
    'function escHtml(s) {\n' +
    '  return s.replace(/&/g,"&amp;").replace(/</g,"&lt;").replace(/>/g,"&gt;");\n' +
    '}\n' +
    '\n' +
    'function fmtSize(bytes) {\n' +
    '  if (bytes === 0) return "-";\n' +
    '  if (bytes < 1024) return bytes + "B";\n' +
    '  if (bytes < 1024 * 1024) return (bytes / 1024).toFixed(1) + "kb";\n' +
    '  return (bytes / (1024 * 1024)).toFixed(1) + "MB";\n' +
    '}\n' +
    '\n' +
    'function renderEntries(entries) {\n' +
    '  var frag = document.createDocumentFragment();\n' +
    '  for (var i = 0; i < entries.length; i++) {\n' +
    '    var e = entries[i];\n' +
    '    var div = document.createElement("div");\n' +
    '    div.className = "log-line level-" + e.level;\n' +
    '    div.innerHTML = \'<span class="ts">\' + fmtTime(e.ts) + "</span> "\n' +
    '      + \'<span class="src \' + e.source + \'">\' + e.source.padEnd(6) + "</span> "\n' +
    '      + \'<span class="msg">\' + escHtml(e.msg) + "</span>";\n' +
    '    frag.appendChild(div);\n' +
    '  }\n' +
    '  logView.appendChild(frag);\n' +
    '  if (autoScroll) logView.scrollTop = logView.scrollHeight;\n' +
    '}\n' +
    '\n' +
    'function renderHttpEntries(entries) {\n' +
    '  var tbody = document.getElementById("http-body");\n' +
    '  var frag = document.createDocumentFragment();\n' +
    '  for (var i = 0; i < entries.length; i++) {\n' +
    '    var e = entries[i];\n' +
    '    var tr = document.createElement("tr");\n' +
    '    tr.className = "http-row";\n' +
    '    tr.dataset.id = e.id;\n' +
    '    var mc = e.method.toLowerCase();\n' +
    '    var sc = "s" + String(e.status).charAt(0);\n' +
    '    tr.innerHTML = "<td>" + fmtTime(e.ts) + "</td>"\n' +
    '      + \'<td><span class="method \' + mc + \'">\' + e.method + "</span></td>"\n' +
    '      + \'<td class="path">\' + escHtml(e.path) + "</td>"\n' +
    '      + \'<td><span class="status \' + sc + \'">\' + e.status + "</span></td>"\n' +
    '      + \'<td class="dur">\' + e.duration + "ms</td>"\n' +
    '      + \'<td class="sz">\' + fmtSize(e.resSize) + "</td>";\n' +
    '    tr.addEventListener("click", (function(entry) {\n' +
    '      return function() { toggleHttpDetail(this, entry); };\n' +
    '    })(e));\n' +
    '    frag.appendChild(tr);\n' +
    '  }\n' +
    '  tbody.appendChild(frag);\n' +
    '  if (httpAutoScroll) httpView.scrollTop = httpView.scrollHeight;\n' +
    '}\n' +
    '\n' +
    'function toggleHttpDetail(row, entry) {\n' +
    '  var next = row.nextElementSibling;\n' +
    '  if (next && next.classList.contains("http-detail")) {\n' +
    '    next.classList.toggle("open");\n' +
    '    return;\n' +
    '  }\n' +
    '  var detail = document.createElement("tr");\n' +
    '  detail.className = "http-detail open";\n' +
    '  var html = \'<td colspan="6">\';\n' +
    '  html += \'<div class="hdr-section"><div class="hdr-title">request headers</div>\';\n' +
    '  var rk = Object.keys(entry.reqHeaders || {}).sort();\n' +
    '  for (var i = 0; i < rk.length; i++) {\n' +
    '    html += \'<div class="hdr-line"><span class="hdr-key">\' + escHtml(rk[i]) + \'</span>: <span class="hdr-val">\' + escHtml(entry.reqHeaders[rk[i]]) + "</span></div>";\n' +
    '  }\n' +
    '  html += "</div>";\n' +
    '  html += \'<div class="hdr-section"><div class="hdr-title">response headers</div>\';\n' +
    '  var sk = Object.keys(entry.resHeaders || {}).sort();\n' +
    '  for (var j = 0; j < sk.length; j++) {\n' +
    '    html += \'<div class="hdr-line"><span class="hdr-key">\' + escHtml(sk[j]) + \'</span>: <span class="hdr-val">\' + escHtml(entry.resHeaders[sk[j]]) + "</span></div>";\n' +
    '  }\n' +
    '  html += "</div>";\n' +
    '  if (entry.reqSize > 0) html += \'<div class="hdr-line"><span class="hdr-key">request body size</span>: <span class="hdr-val">\' + fmtSize(entry.reqSize) + "</span></div>";\n' +
    '  html += "</td>";\n' +
    '  detail.innerHTML = html;\n' +
    '  row.parentNode.insertBefore(detail, row.nextSibling);\n' +
    '}\n' +
    '\n' +
    'function fetchLogs() {\n' +
    '  var params = new URLSearchParams();\n' +
    '  if (activeSource) params.set("source", activeSource);\n' +
    '  if (activeLevel) params.set("level", activeLevel);\n' +
    '  if (lastCursor) params.set("since", String(lastCursor));\n' +
    '  fetch("/api/logs?" + params).then(function(res) { return res.json(); }).then(function(data) {\n' +
    '    if (data.entries && data.entries.length > 0) renderEntries(data.entries);\n' +
    '    if (data.cursor) lastCursor = data.cursor;\n' +
    '  }).catch(function() {});\n' +
    '}\n' +
    '\n' +
    'function fetchHttp() {\n' +
    '  var params = new URLSearchParams();\n' +
    '  if (httpCursor) params.set("since", String(httpCursor));\n' +
    '  var pathFilter = document.getElementById("http-path-filter").value;\n' +
    '  if (pathFilter) params.set("path", pathFilter);\n' +
    '  fetch("/api/http-log?" + params).then(function(res) { return res.json(); }).then(function(data) {\n' +
    '    if (data.entries && data.entries.length > 0) renderHttpEntries(data.entries);\n' +
    '    if (data.cursor) httpCursor = data.cursor;\n' +
    '  }).catch(function() {});\n' +
    '}\n' +
    '\n' +
    'function loadEnv() {\n' +
    '  fetch("/api/env").then(function(res) { return res.json(); }).then(function(data) {\n' +
    '    var tbody = document.getElementById("env-body");\n' +
    '    tbody.innerHTML = "";\n' +
    '    var keys = Object.keys(data.env).sort();\n' +
    '    for (var i = 0; i < keys.length; i++) {\n' +
    '      var tr = document.createElement("tr");\n' +
    '      tr.innerHTML = "<td>" + escHtml(keys[i]) + "</td><td>" + escHtml(String(data.env[keys[i]])) + "</td>";\n' +
    '      tbody.appendChild(tr);\n' +
    '    }\n' +
    '    envLoaded = true;\n' +
    '  }).catch(function() {});\n' +
    '}\n' +
    '\n' +
    'function fetchStatus() {\n' +
    '  fetch("/api/status").then(function(res) { return res.json(); }).then(function(data) {\n' +
    '    document.getElementById("pg-port").textContent = ":" + data.pgPort;\n' +
    '    document.getElementById("zero-port").textContent = ":" + data.zeroPort;\n' +
    '    var m = Math.floor(data.uptime / 60);\n' +
    '    var s = data.uptime % 60;\n' +
    '    document.getElementById("uptime-badge").textContent = "\\u23F1 " + (m > 0 ? m + "m " : "") + s + "s";\n' +
    '    var zeroDisabled = data.skipZeroCache;\n' +
    '    document.querySelectorAll("[data-zero-action]").forEach(function(btn) {\n' +
    '      btn.disabled = zeroDisabled;\n' +
    '    });\n' +
    '  }).catch(function() {});\n' +
    '}\n' +
    '\n' +
    'function doAction(action, btn) {\n' +
    '  if (action === "reset-zero") {\n' +
    '    if (!confirm("Reset zero-cache? This deletes the replica and resyncs from scratch.")) return;\n' +
    '  }\n' +
    '  btn.disabled = true;\n' +
    '  var origText = btn.textContent;\n' +
    '  btn.textContent = "...";\n' +
    '  fetch("/api/actions/" + action, { method: "POST" })\n' +
    '    .then(function(res) { return res.json(); })\n' +
    '    .then(function(data) {\n' +
    '      showToast(data.message || "done", data.ok ? "success" : "error");\n' +
    '      if (action === "clear-logs") {\n' +
    '        logView.innerHTML = "";\n' +
    '        lastCursor = 0;\n' +
    '      }\n' +
    '      if (action === "clear-http") {\n' +
    '        document.getElementById("http-body").innerHTML = "";\n' +
    '        httpCursor = 0;\n' +
    '      }\n' +
    '    })\n' +
    '    .catch(function(err) {\n' +
    '      showToast("failed: " + err.message, "error");\n' +
    '    })\n' +
    '    .finally(function() {\n' +
    '      btn.disabled = false;\n' +
    '      btn.textContent = origText;\n' +
    '    });\n' +
    '}\n' +
    '\n' +
    'function showToast(msg, type) {\n' +
    '  toastEl.textContent = msg;\n' +
    '  toastEl.className = "toast " + type + " show";\n' +
    '  setTimeout(function() { toastEl.className = "toast"; }, 2500);\n' +
    '}\n' +
    '\n' +
    'fetchLogs();\n' +
    'fetchStatus();\n' +
    'setInterval(function() {\n' +
    '  if (document.hidden) return;\n' +
    '  if (isHttpTab) fetchHttp();\n' +
    '  else if (!isEnvTab) fetchLogs();\n' +
    '}, 1000);\n' +
    'setInterval(function() { if (!document.hidden) fetchStatus(); }, 5000);\n' +
    'document.addEventListener("visibilitychange", function() {\n' +
    '  if (document.hidden) return;\n' +
    '  if (isHttpTab) fetchHttp();\n' +
    '  else if (!isEnvTab) fetchLogs();\n' +
    '  fetchStatus();\n' +
    '});\n' +
    '</script>\n' +
    '</body>\n' +
    '</html>'
  )
}
