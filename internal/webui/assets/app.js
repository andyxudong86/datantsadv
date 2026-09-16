(function () {
  "use strict";

  var $ = function (sel) { return document.querySelector(sel); };

  function fmtBytes(n) {
    if (n < 1024) return n + " B";
    var units = ["KB", "MB", "GB", "TB", "PB"];
    var v = n, i = -1;
    do { v /= 1024; i++; } while (v >= 1024 && i < units.length - 1);
    return v.toFixed(1) + " " + units[i];
  }

  function fmtRate(n) {
    if (!n) return "—";
    return fmtBytes(n) + "/s";
  }

  function fmtTime(iso) {
    if (!iso) return "—";
    var d = new Date(iso);
    return isNaN(d) ? "—" : d.toLocaleString();
  }

  function fmtDur(ms) {
    if (ms == null) return "—";
    var s = Math.floor(ms / 1000);
    if (s < 60) return s + " 秒";
    var m = Math.floor(s / 60); s = s % 60;
    if (m < 60) return m + " 分 " + s + " 秒";
    var h = Math.floor(m / 60); m = m % 60;
    return h + " 小时 " + m + " 分";
  }

  function esc(s) {
    return String(s == null ? "" : s)
      .replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;").replace(/'/g, "&#39;");
  }

  // 视图切换
  var views = { dashboard: 1, history: 1, tasks: 1, workers: 1 };
  document.querySelectorAll("nav button").forEach(function (b) {
    b.addEventListener("click", function () {
      document.querySelectorAll("nav button").forEach(function (x) { x.classList.remove("active"); });
      b.classList.add("active");
      Object.keys(views).forEach(function (v) {
        $("#" + v).classList.toggle("active", v === b.dataset.view);
      });
      if (b.dataset.view === "history") loadRuns();
      if (b.dataset.view === "tasks") loadTasks();
      if (b.dataset.view === "workers") loadWorkers();
    });
  });

  function badge(kind, text) {
    return '<span class="badge badge-' + esc(kind) + '">' + esc(text) + "</span>";
  }

  // ---- 实时状态（SSE）----
  function applyStatus(d) {
    // 运行徽标
    var badgeK = d.state === "running" ? "running" : d.state === "paused" ? "paused" : "idle";
    $("#run-badge").outerHTML = badge(badgeK, d.state === "running" ? "运行中" : d.state === "paused" ? "已暂停" : "空闲");
    $("#run-badge").classList.add("badge", "badge-" + badgeK);

    // 进度
    var pct = d.progress_pct != null ? d.progress_pct : 0;
    $("#progress-bar").style.width = pct.toFixed(1) + "%";

    // 任务统计
    var t = d.tasks || {};
    $("#st-tsk").textContent = t.total || 0;
    $("#st-done").textContent = t.done || 0;
    $("#st-fail").textContent = t.failed || 0;
    $("#st-rate").textContent = fmtRate(d.rate_bytes_per_sec);

    // 当前 run 明细
    var run = d.run;
    if (run) {
      $("#run-detail").innerHTML =
        "运行 <b>" + esc(run.id) + "</b>，扫描 " +
        (run.scan_done ? "完成" : "进行中（已发现 " + (run.scanned_tasks || 0) + " 个任务）") +
        "，已迁移 <b>" + run.files_done + "</b> 个文件 / " + fmtBytes(run.bytes_done) +
        "，失败 <b>" + run.files_failed + "</b>，耗时 " + fmtDur(run.elapsed_ms);
      $("#btn-pause").disabled = d.state !== "running";
      $("#btn-resume").disabled = d.state !== "paused";
      $("#btn-stop").disabled = d.state === "idle";
      $("#btn-rescan").disabled = d.state === "idle" || (run && !run.scan_done);
      // 已有活动迁移时不允许再点“开始”，避免撞“任务已存在”
      $("#btn-start").disabled = true;
    } else {
      $("#run-detail").textContent = "当前没有活动迁移。";
      $("#btn-pause").disabled = $("#btn-resume").disabled = true;
      $("#btn-stop").disabled = $("#btn-rescan").disabled = true;
      $("#btn-start").disabled = false;
      $("#progress-bar").style.width = "0%";
    }
  }

  function setConn(ok) {
    $("#conn").className = "badge " + (ok ? "badge-live" : "badge-error");
    $("#conn").textContent = ok ? "已连接" : "连接中断，重连中…";
  }

  function connectStream() {
    setConn(false);
    var es = new EventSource("/api/events");
    es.addEventListener("open", function () { setConn(true); });
    es.addEventListener("status", function (e) {
      try { applyStatus(JSON.parse(e.data)); } catch (err) { /* ignore malformed frame */ }
    });
    es.addEventListener("error", function () { setConn(false); });
  }

  // ---- API helpers ----
  function api(url, opts) {
    return fetch(url, opts).then(function (r) {
      if (!r.ok) {
        return r.json().catch(function () { return {}; }).then(function (j) {
          throw new Error((j && j.error) || ("HTTP " + r.status));
        });
      }
      return r.json().catch(function () { return {}; });
    });
  }

  function apiCmd(url) {
    return api(url, { method: "POST" });
  }

  // ---- 控制按钮 ----
  function bindCmd(id, url) {
    $("#" + id).addEventListener("click", function () {
      $("#" + id).disabled = true;
      apiCmd(url).then(function () { $("#" + id).disabled = false; })
        .catch(function (err) {
          alert(err.message);
          $("#" + id).disabled = false;
        });
    });
  }
  bindCmd("btn-pause", "/api/pause");
  bindCmd("btn-resume", "/api/resume");
  bindCmd("btn-stop", "/api/stop");
  bindCmd("btn-rescan", "/api/rescan");

  // ---- 启动迁移 ----
  $("#startform").addEventListener("submit", function (e) {
    e.preventDefault();
    var ignore = $("#f-ignore").value.split(",").map(function (s) { return s.trim(); }).filter(Boolean);
    var body = {
      src: $("#f-src").value.trim(),
      dst: $("#f-dst").value.trim(),
      include_dirs: $("#f-dirs").checked,
      sync_meta: $("#f-meta").checked,
      delete_stale: $("#f-del").checked,
      fresh: $("#f-fresh").checked,
      ignore: ignore
    };
    if (!body.src || !body.dst) { $("#start-err").textContent = "请填写源目录和目标目录。"; return; }
    $("#btn-start").disabled = true;
    $("#start-err").textContent = "";
    api("/api/start", { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(body) })
      .then(function () { $("#btn-start").disabled = false; })
      .catch(function (err) { $("#btn-start").disabled = false; $("#start-err").textContent = err.message; });
  });

  // ---- 迁移历史 ----
  function loadRuns() {
    api("/api/runs").then(function (data) {
      var tb = $("#history-table tbody");
      var runs = data.runs || [];
      if (!runs.length) { tb.innerHTML = '<tr><td colspan="8" class="muted">还没有迁移记录。</td></tr>'; return; }
      tb.innerHTML = runs.map(function (r) {
        var st = badge(r.status === "done" ? "running" : r.status === "failed" ? "error" : r.status === "stopped" ? "stopped" : "idle",
          { done: "完成", failed: "失败", stopped: "已停止", running: "运行中" }[r.status] || r.status);
        return "<tr>" +
          "<td>" + esc(r.id) + "</td>" +
          "<td>" + fmtTime(r.started_at) + "</td>" +
          "<td>" + st + "</td>" +
          "<td>" + (r.result ? r.result.files_done : 0) + " / " + (r.result ? r.result.files_failed : 0) + "</td>" +
          "<td>" + fmtBytes(r.result ? r.result.bytes_done : 0) + "</td>" +
          "<td>" + fmtRate(r.result ? r.result.avg_bytes_per_sec : 0) + "</td>" +
          "<td>" + fmtDur(r.duration_ms) + "</td>" +
          "<td class='muted'>" + esc(r.src) + " → " + esc(r.dst) + "</td>" +
          "</tr>";
      }).join("");
    }).catch(function (err) {
      $("#history-table tbody").innerHTML = '<tr><td colspan="8" class="error">' + esc(err.message) + "</td></tr>";
    });
  }
  $("#history-refresh").addEventListener("click", loadRuns);
  $("#history-refresh").click();

  // ---- 任务明细 ----
  function loadTasks() {
    var st = $("#task-status").value;
    api("/api/tasks?status=" + encodeURIComponent(st) + "&limit=500").then(function (data) {
      var tb = $("#task-table tbody");
      var tasks = data.tasks || [];
      if (!tasks.length) { tb.innerHTML = '<tr><td colspan="6" class="muted">没有匹配的任务。</td></tr>'; return; }
      tb.innerHTML = tasks.map(function (t) {
        return "<tr>" +
          "<td>" + esc(t.id) + "</td>" +
          "<td>" + esc(t.kind) + "</td>" +
          "<td>" + esc(t.status) + "</td>" +
          "<td>" + esc(t.owner || "—") + "</td>" +
          "<td>" + fmtBytes(t.size) + "</td>" +
          "<td class='muted'>" + esc(t.src_path) + " → " + esc(t.dst_path) + "</td>" +
          "</tr>";
      }).join("");
      $("#task-table").insertAdjacentHTML("beforeend", '<p class="muted">共 ' + data.total + " 条（显示前 " + tasks.length + " 条）</p>");
    }).catch(function (err) {
      $("#task-table tbody").innerHTML = '<tr><td colspan="6" class="error">' + esc(err.message) + "</td></tr>";
    });
  }
  $("#task-refresh").addEventListener("click", loadTasks);
  $("#task-status").addEventListener("change", loadTasks);
  $("#task-refresh").click();

  // ---- Workers ----
  var lastWorkers = { workers: [], pool: [] };

  function renderSpawned(sp) {
    var box = $("#spawn-list");
    if (!sp || !sp.length) { box.innerHTML = '<div class="muted">没有由控制机拉起的 Worker。</div>'; return; }
    box.innerHTML = sp.map(function (s) {
      var where = s.remote ? "节点 " + esc(s.host) : s.alive ? "存活 pid " + esc(s.pid) : "已退出";
      return '<div class="worker-card">' +
        '<div class="w-id">' + esc(s.id) + " " + badge(s.alive ? "running" : "idle", s.alive ? (s.remote ? "远程就绪" : "存活 pid " + esc(s.pid)) : "已退出") + "</div>" +
        '<div class="w-meta">' + where + " · 启动 " + fmtTime(s.started_at) + "</div>" +
        '<div class="w-meta">' + (s.args || []).join(" ") + "</div>" +
        '<button class="ghost spawn-stop" data-id="' + esc(s.id) + '"' + (s.alive ? "" : " disabled") + ">停止</button>" +
        "</div>";
    }).join("");
  }

  function badges() {
    var states = { running: "在线", idle: "离线" };
    return {
      ready: function () { return badge("running", "准备接收任务"); },
      online: function (label) { return badge("running", label || "在线"); },
      offline: function () { return badge("idle", "离线"); }
    };
  }

  function renderPool(pool, running) {
    var tb = $("#pool-table tbody");
    if (!pool || !pool.length) { tb.innerHTML = '<tr><td colspan="5" class="muted">未配置 — 迁移会派发给全部在线 Worker。</td></tr>'; return; }
    tb.innerHTML = pool.map(function (p) {
      var ip = p.ip || "";
      var n = (running && running[ip]) || 0;
      var stopBtn = n > 0
        ? '<button class="ghost pool-stop" data-ip="' + esc(ip) + '">停止 ' + n + '</button>'
        : '';
      return "<tr>" +
        "<td>" + esc(p.id || "—") + "</td>" +
        "<td>" + esc(ip) + "</td>" +
        "<td>" + (p.capacity || "—") + "</td>" +
        "<td>" + (p.batch || "—") + "</td>" +
        "<td><button class=\"primary pool-start\" data-ip=\"" + esc(ip) + '" data-cap="' + (p.capacity || 4) + "\" data-batch=\"" + (p.batch || 16) + '">启动</button> ' + stopBtn + "</td>" +
        "</tr>";
    }).join("");
  }

  function loadWorkers() {
    api("/api/workers").then(function (data) {
      lastWorkers = data;
      var ws = data.workers || [];
      var pool = data.pool || [];
      var spawned = data.spawned || [];
      var running = {};
      spawned.forEach(function (s) {
        if (s.remote && s.alive) running[s.host] = (running[s.host] || 0) + 1;
      });
      renderPool(pool, running);
      var box = $("#worker-list");
      if (!ws.length) { box.innerHTML = '<div class="muted">还没有 Worker 注册。</div>'; renderSpawned(spawned); return; }
      box.innerHTML = ws.map(function (w) {
        var b;
        if (!w.connected) b = badges().offline();
        else if (w.in_pool) b = badges().ready();
        else b = badges().online("在线 · 未入池");
        return '<div class="worker-card">' +
          '<div class="w-id">' + esc(w.id) + " " + b + "</div>" +
          '<div class="w-meta">IP ' + esc(w.ip || "—") + " · 并发 " + (w.capacity || "—") + "</div>" +
          '<div class="w-meta">地址 ' + esc(w.address || "—") + "</div>" +
          '<div class="w-meta">已完成 ' + w.files_done + " 文件 / " + fmtBytes(w.bytes_done) + " · 失败 " + w.files_failed + "</div>" +
          '<div class="w-meta">注册 ' + fmtTime(w.joined_at) + "</div>" +
          "</div>";
      }).join("");
      renderSpawned(spawned);
    }).catch(function (err) {
      $("#worker-list").innerHTML = '<div class="error">' + esc(err.message) + "</div>";
    });
  }
  $("#worker-refresh").addEventListener("click", loadWorkers);
  $("#worker-refresh").click();

  // Worker 池：保存 / 从在线节点填充
  function parsePoolText() {
    return $("#pool-ips").value.split("\n").map(function (line) {
      var toks = line.trim().split(/\s+/).filter(Boolean);
      if (!toks.length || toks[0].charAt(0) === "#") return null;
      return {
        ip: toks[0],
        capacity: parseInt(toks[1], 10) || 0,
        batch: parseInt(toks[2], 10) || 0,
        local: toks[0] === "127.0.0.1" || toks[0] === "::1"
      };
    }).filter(Boolean);
  }

  $("#poolform").addEventListener("submit", function (e) {
    e.preventDefault();
    var workers = parsePoolText();
    if (!workers.length) { $("#pool-msg").textContent = "请至少填写一行 IP。"; return; }
    $("#pool-save").disabled = true;
    $("#pool-msg").textContent = "";
    var entry = { workers: workers };
    api("/api/workers/pool", {
      method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(entry)
    }).then(function () {
      $("#pool-save").disabled = false;
      $("#pool-msg").textContent = "已保存 Worker 池（" + workers.length + " 个节点）。";
      loadWorkers();
    }).catch(function (err) {
      $("#pool-save").disabled = false;
      $("#pool-msg").textContent = err.message;
    });
  });

  $("#pool-fill").addEventListener("click", function () {
    var lines = (lastWorkers.workers || []).filter(function (w) { return w.connected; }).map(function (w) {
      return w.ip + (w.capacity ? " " + w.capacity : "");
    });
    var merged = mergeLines($("#pool-ips").value, lines);
    $("#pool-ips").value = merged.length ? merged.join("\n") : $("#pool-ips").value;
    $("#pool-msg").textContent = "";
  });

  function mergeLines(current, add) {
    var seen = {};
    var out = [];
    current.split("\n").forEach(function (l) {
      var t = l.trim();
      var ip = t.split(/\s+/)[0] || "";
      if (t && !seen[ip]) { seen[ip] = true; if (ip) out.push(t); }
    });
    add.forEach(function (l) {
      var ip = l.split(/\s+/)[0] || "";
      if (ip && !seen[ip]) { seen[ip] = true; out.push(l); }
    });
    return out;
  }

  // 本机启动 / 停止 Worker
  $("#spawnform").addEventListener("submit", function (e) {
    e.preventDefault();
    var body = {
      count: parseInt($("#sp-count").value, 10) || 1,
      capacity: parseInt($("#sp-cap").value, 10) || 4,
      batch: parseInt($("#sp-batch").value, 10) || 16,
      id_prefix: $("#sp-prefix").value.trim() || "local"
    };
    $("#sp-start").disabled = true;
    $("#spawn-err").textContent = "";
    api("/api/workers/spawn", {
      method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(body)
    }).then(function () {
      // 本机 Worker 启动即视为"准备接收任务"：确保 127.0.0.1 在池中
      var pool = (lastWorkers.pool || []).filter(function (p) { return p.ip !== "127.0.0.1" && p.ip !== "::1"; });
      pool.push({ ip: "127.0.0.1", local: true, count: body.count, capacity: body.capacity, batch: body.batch });
      return api("/api/workers/pool", {
        method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ workers: pool })
      });
    }).then(function () {
      $("#sp-start").disabled = false;
      loadWorkers();
    }).catch(function (err) {
      $("#sp-start").disabled = false;
      $("#spawn-err").textContent = err.message;
    });
  });

  document.addEventListener("click", function (e) {
    var b = e.target.closest(".spawn-stop");
    if (b) {
      b.disabled = true;
      var raw = { id: b.dataset.id };
      api("/api/workers/stop", {
        method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(raw)
      }).then(loadWorkers).catch(function (err) { alert(err.message); loadWorkers(); });
      return;
    }
    var ps = e.target.closest(".pool-start");
    if (ps) {
      ps.disabled = true;
      var body = {
        ip: ps.dataset.ip,
        capacity: parseInt(ps.dataset.cap, 10) || 4,
        batch: parseInt(ps.dataset.batch, 10) || 16
      };
      api("/api/workers/pool/spawn", {
        method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(body)
      }).then(function () {
        loadWorkers();
      }).catch(function (err) {
        ps.disabled = false;
        alert("启动 " + body.ip + " 失败：" + err.message);
        loadWorkers();
      });
      return;
    }
    var st = e.target.closest(".pool-stop");
    if (st) {
      st.disabled = true;
      api("/api/workers/pool/stop", {
        method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ ip: st.dataset.ip })
      }).then(loadWorkers).catch(function (err) { alert(err.message); loadWorkers(); });
    }
  });

  // Worker 页激活时每 5 秒刷新一次本机拉起状态
  setInterval(function () {
    if ($("#workers").classList.contains("active")) loadWorkers();
  }, 5000);

  // 初始：先拉一发状态，再连 SSE
  api("/api/status").then(applyStatus).catch(function () { });
  connectStream();
})();
