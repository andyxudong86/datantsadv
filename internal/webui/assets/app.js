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
    if (!n) return "\u2014";
    return fmtBytes(n) + "/s";
  }

  function fmtTime(iso) {
    if (!iso) return "\u2014";
    var d = new Date(iso);
    return isNaN(d) ? "\u2014" : d.toLocaleString();
  }

  function fmtDur(ms) {
    if (ms == null) return "\u2014";
    var s = Math.floor(ms / 1000);
    if (s < 60) return s + " \u79d2";
    var m = Math.floor(s / 60); s = s % 60;
    if (m < 60) return m + " \u5206 " + s + " \u79d2";
    var h = Math.floor(m / 60); m = m % 60;
    return h + " \u5c0f\u65f6 " + m + " \u5206";
  }

  function esc(s) {
    return String(s == null ? "" : s)
      .replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;").replace(/'/g, "&#39;");
  }

  // ---- \u89c6\u56fe\u5207\u6362 ----
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

  // ---- \u5b9e\u65f6\u72b6\u6001\uff08SSE\uff09----
  function applyStatus(d) {
    var state = d.state || "idle";
    var run   = d.run;
    var t     = d.tasks || {};
    var hasActiveRun  = run != null;
    var hasWork       = t.pending > 0 || t.in_progress > 0;
    var working       = state !== "idle" || hasActiveRun || hasWork;

    // \u8fd0\u884c\u5fbd\u6807
    var badgeK = state === "running" ? "running" : state === "paused" ? "paused" : "idle";
    $("#run-badge").outerHTML = badge(badgeK, state === "running" ? "\u8fd0\u884c\u4e2d" : state === "paused" ? "\u5df2\u6682\u505c" : "\u7a7a\u95f2");
    $("#run-badge").classList.add("badge", "badge-" + badgeK);

    // \u8fdb\u5ea6
    var pct = d.progress_pct != null ? d.progress_pct : 0;
    if (state === "idle" && !hasActiveRun && !hasWork) pct = 0;
    $("#progress-bar").style.width = pct.toFixed(1) + "%";

    // \u4efb\u52a1\u7edf\u8ba1
    $("#st-tsk").textContent = t.total || 0;
    $("#st-done").textContent = t.done || 0;
    $("#st-fail").textContent = t.failed || 0;
    $("#st-rate").textContent = fmtRate(d.rate_bytes_per_sec);

    // \u5f53\u524d run \u660e\u7ec6
    if (hasActiveRun) {
      var targets = run.targets || [];
      var targetDesc;
      if (targets.length > 1) {
        targetDesc = targets.length + " \u7ec4\u76ee\u5f55\u8fc1\u79fb\u4e2d";
      } else {
        targetDesc = esc(run.src || "") + " \u2192 " + esc(run.dst || "");
      }
      $("#run-detail").innerHTML =
        "\u8fd0\u884c <b>" + esc(run.id) + "</b>\uff0c" + targetDesc +
        "\uff0c\u626b\u63cf " +
        (run.scan_done ? "\u5b8c\u6210" : "\u8fdb\u884c\u4e2d\uff08\u5df2\u53d1\u73b0 " + (run.scanned_tasks || 0) + " \u4e2a\u4efb\u52a1\uff09") +
        "\uff0c\u5df2\u8fc1\u79fb <b>" + run.files_done + "</b> \u4e2a\u6587\u4ef6 / " + fmtBytes(run.bytes_done) +
        "\uff0c\u5931\u8d25 <b>" + run.files_failed + "</b>\uff0c\u8017\u65f6 " + fmtDur(run.elapsed_ms);
    } else if (working) {
      $("#run-detail").textContent = hasWork ? "\u4efb\u52a1\u4ecd\u5728\u5904\u7406\u4e2d\uff08" + (t.pending || 0) + " \u7b49\u5f85\uff0c" + (t.in_progress || 0) + " \u6267\u884c\u4e2d\uff09\u3002" : "\u5f53\u524d\u6ca1\u6709\u6d3b\u52a8\u8fc1\u79fb\u3002";
    } else {
      $("#run-detail").textContent = "\u5f53\u524d\u6ca1\u6709\u6d3b\u52a8\u8fc1\u79fb\u3002";
    }

    // \u6309\u94ae\uff1a\u4efb\u4f55\u8fd0\u884c\u6001\u6216\u4ecd\u6709\u4efb\u52a1\u90fd\u7981\u6b62\u518d\u6b21\u542f\u52a8
    $("#btn-start").disabled = state !== "idle" || hasActiveRun || hasWork;
    $("#btn-pause").disabled = state !== "running";
    $("#btn-resume").disabled = state !== "paused";
    $("#btn-stop").disabled = !hasActiveRun;
    $("#btn-rescan").disabled = state === "idle" || (hasActiveRun && !run.scan_done);
  }

  function setConn(ok) {
    $("#conn").className = "badge " + (ok ? "badge-live" : "badge-error");
    $("#conn").textContent = ok ? "\u5df2\u8fde\u63a5" : "\u8fde\u63a5\u4e2d\u65ad\uff0c\u91cd\u8fde\u4e2d\u2026";
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

  // ---- \u63a7\u5236\u6309\u94ae ----
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

  // ---- \u76ee\u6807\u76ee\u5f55\u884c\u7ba1\u7406 ----
  var targetIdx = 1;
  $("#add-target").addEventListener("click", function () {
    var div = document.createElement("div");
    div.className = "target-row";
    div.dataset.idx = targetIdx++;
    div.innerHTML =
      '<b>\u76ee\u6807 ' + div.dataset.idx + '</b> ' +
      '<button type="button" class="ghost remove-target">\u79fb\u9664</button>' +
      '<div class="grid">' +
      '<label>\u6e90\u76ee\u5f55<input class="target-src" required placeholder="/mnt/source"></label>' +
      '<label>\u76ee\u6807\u76ee\u5f55<input class="target-dst" required placeholder="/mnt/destination"></label>' +
      '</div>';
    $("#target-list").appendChild(div);
    div.querySelector(".remove-target").addEventListener("click", function () {
      div.remove();
    });
  });

  // ---- \u542f\u52a8\u8fc1\u79fb ----
  $("#startform").addEventListener("submit", function (e) {
    e.preventDefault();
    var ignore = $("#f-ignore").value.split(",").map(function (s) { return s.trim(); }).filter(Boolean);
    var rows = document.querySelectorAll("#target-list .target-row");
    var targets = [];
    for (var i = 0; i < rows.length; i++) {
      var src = rows[i].querySelector(".target-src").value.trim();
      var dst = rows[i].querySelector(".target-dst").value.trim();
      if (src && dst) targets.push({ src: src, dst: dst });
    }
    if (!targets.length) { $("#start-err").textContent = "\u8bf7\u586b\u5199\u81f3\u5c11\u4e00\u7ec4\u6e90\u76ee\u5f55\u548c\u76ee\u6807\u76ee\u5f55\u3002"; return; }
    var body = {
      src: targets[0].src,
      dst: targets[0].dst,
      targets: targets,
      include_dirs: $("#f-dirs").checked,
      sync_meta: $("#f-meta").checked,
      delete_stale: $("#f-del").checked,
      fresh: $("#f-fresh").checked,
      ignore: ignore
    };
    $("#btn-start").disabled = true;
    $("#start-err").textContent = "";
    api("/api/start", { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(body) })
      .then(function () { $("#btn-start").disabled = false; })
      .catch(function (err) { $("#btn-start").disabled = false; $("#start-err").textContent = err.message; });
  });

  // ---- \u8fc1\u79fb\u5386\u53f2 ----
  function loadRuns() {
    api("/api/runs").then(function (data) {
      var tb = $("#history-table tbody");
      var runs = data.runs || [];
      if (!runs.length) { tb.innerHTML = '<tr><td colspan="8" class="muted">\u8fd8\u6ca1\u6709\u8fc1\u79fb\u8bb0\u5f55\u3002</td></tr>'; return; }
      tb.innerHTML = runs.map(function (r) {
        var st = badge(r.status === "done" ? "running" : r.status === "failed" ? "error" : r.status === "stopped" ? "stopped" : "idle",
          { done: "\u5b8c\u6210", failed: "\u5931\u8d25", stopped: "\u5df2\u505c\u6b62", running: "\u8fd0\u884c\u4e2d" }[r.status] || r.status);
        var pathDesc;
        if (r.targets && r.targets.length > 1) {
          pathDesc = r.targets.length + " \u7ec4\u76ee\u5f55";
        } else {
          pathDesc = esc(r.src || "") + " \u2192 " + esc(r.dst || "");
        }
        return "<tr>" +
          "<td>" + esc(r.id) + "</td>" +
          "<td>" + fmtTime(r.started_at) + "</td>" +
          "<td>" + st + "</td>" +
          "<td>" + (r.result ? r.result.files_done : 0) + " / " + (r.result ? r.result.files_failed : 0) + "</td>" +
          "<td>" + fmtBytes(r.result ? r.result.bytes_done : 0) + "</td>" +
          "<td>" + fmtRate(r.result ? r.result.avg_bytes_per_sec : 0) + "</td>" +
          "<td>" + fmtDur(r.duration_ms) + "</td>" +
          "<td class='muted'>" + pathDesc + "</td>" +
          "</tr>";
      }).join("");
    }).catch(function (err) {
      $("#history-table tbody").innerHTML = '<tr><td colspan="8" class="error">' + esc(err.message) + "</td></tr>";
    });
  }
  $("#history-refresh").addEventListener("click", loadRuns);
  $("#history-refresh").click();

  // ---- \u4efb\u52a1\u660e\u7ec6 ----
  function loadTasks() {
    var st = $("#task-status").value;
    api("/api/tasks?status=" + encodeURIComponent(st) + "&limit=500").then(function (data) {
      var tb = $("#task-table tbody");
      var tasks = data.tasks || [];
      if (!tasks.length) { tb.innerHTML = '<tr><td colspan="6" class="muted">\u6ca1\u6709\u5339\u914d\u7684\u4efb\u52a1\u3002</td></tr>'; return; }
      tb.innerHTML = tasks.map(function (t) {
        return "<tr>" +
          "<td>" + esc(t.id) + "</td>" +
          "<td>" + esc(t.kind) + "</td>" +
          "<td>" + esc(t.status) + "</td>" +
          "<td>" + esc(t.owner || "\u2014") + "</td>" +
          "<td>" + fmtBytes(t.size) + "</td>" +
          "<td class='muted'>" + esc(t.src_path) + " \u2192 " + esc(t.dst_path) + "</td>" +
          "</tr>";
      }).join("");
      $("#task-table").insertAdjacentHTML("beforeend", '<p class="muted">\u5171 ' + data.total + " \u6761\uff08\u663e\u793a\u524d " + tasks.length + " \u6761\uff09</p>");
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
    if (!sp || !sp.length) { box.innerHTML = '<div class="muted">\u6ca1\u6709\u7531\u63a7\u5236\u673a\u62c9\u8d77\u7684 Worker\u3002</div>'; return; }
    box.innerHTML = sp.map(function (s) {
      var where = s.remote ? "\u8282\u70b9 " + esc(s.host) : s.alive ? "\u5b58\u6d3b pid " + esc(s.pid) : "\u5df2\u9000\u51fa";
      var reason = (!s.alive && s.exit_error) ? '<div class="w-meta error">' + esc(s.exit_error) + "</div>" : "";
      return '<div class="worker-card">' +
        '<div class="w-id">' + esc(s.id) + " " + badge(s.alive ? "running" : "idle", s.alive ? (s.remote ? "\u8fdc\u7a0b\u5c31\u7eea" : "\u5b58\u6d3b pid " + esc(s.pid)) : "\u5df2\u9000\u51fa") + "</div>" +
        '<div class="w-meta">' + where + " \u00b7 \u542f\u52a8 " + fmtTime(s.started_at) + "</div>" +
        '<div class="w-meta">' + (s.args || []).join(" ") + "</div>" +
        reason +
        '<button class="ghost spawn-stop" data-id="' + esc(s.id) + '"' + (s.alive ? "" : " disabled") + ">\u505c\u6b62</button>" +
        "</div>";
    }).join("");
  }

  function badges() {
    var states = { running: "\u5728\u7ebf", idle: "\u79bb\u7ebf" };
    return {
      ready: function () { return badge("running", "\u51c6\u5907\u63a5\u6536\u4efb\u52a1"); },
      online: function (label) { return badge("running", label || "\u5728\u7ebf"); },
      offline: function () { return badge("idle", "\u79bb\u7ebf"); }
    };
  }

  function renderPool(pool, running) {
    var tb = $("#pool-table tbody");
    if (!pool || !pool.length) { tb.innerHTML = '<tr><td colspan="5" class="muted">\u672a\u914d\u7f6e \u2014 \u8fc1\u79fb\u4f1a\u6d3e\u53d1\u7ed9\u5168\u90e8\u5728\u7ebf Worker\u3002</td></tr>'; return; }
    tb.innerHTML = pool.map(function (p) {
      var ip = p.ip || "";
      var n = (running && running[ip]) || 0;
      var stopBtn = n > 0
        ? '<button class="ghost pool-stop" data-ip="' + esc(ip) + '">\u505c\u6b62 ' + n + '</button>'
        : '';
      return "<tr>" +
        "<td>" + esc(p.id || "\u2014") + "</td>" +
        "<td>" + esc(ip) + "</td>" +
        "<td>" + (p.capacity || "\u2014") + "</td>" +
        "<td>" + (p.batch || "\u2014") + "</td>" +
        '<td><button class="primary pool-start" data-ip="' + esc(ip) + '" data-cap="' + (p.capacity || 4) + '" data-batch="' + (p.batch || 16) + '">\u542f\u52a8</button> ' + stopBtn + "</td>" +
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
      if (!ws.length) { box.innerHTML = '<div class="muted">\u8fd8\u6ca1\u6709 Worker \u6ce8\u518c\u3002</div>'; renderSpawned(spawned); return; }
      box.innerHTML = ws.map(function (w) {
        var b;
        if (!w.connected) b = badges().offline();
        else if (w.in_pool) b = badges().ready();
        else b = badges().online("\u5728\u7ebf \u00b7 \u672a\u5165\u6c60");
        return '<div class="worker-card">' +
          '<div class="w-id">' + esc(w.id) + " " + b + "</div>" +
          '<div class="w-meta">IP ' + esc(w.ip || "\u2014") + " \u00b7 \u5e76\u53d1 " + (w.capacity || "\u2014") + "</div>" +
          '<div class="w-meta">\u5730\u5740 ' + esc(w.address || "\u2014") + "</div>" +
          '<div class="w-meta">\u5df2\u5b8c\u6210 ' + w.files_done + " \u6587\u4ef6 / " + fmtBytes(w.bytes_done) + " \u00b7 \u5931\u8d25 " + w.files_failed + "</div>" +
          '<div class="w-meta">\u6ce8\u518c ' + fmtTime(w.joined_at) + "</div>" +
          "</div>";
      }).join("");
      renderSpawned(spawned);
    }).catch(function (err) {
      $("#worker-list").innerHTML = '<div class="error">' + esc(err.message) + "</div>";
    });
  }
  $("#worker-refresh").addEventListener("click", loadWorkers);
  $("#worker-refresh").click();

  // Worker \u6c60\uff1a\u4fdd\u5b58 / \u4ece\u5728\u7ebf\u8282\u70b9\u586b\u5145
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
    if (!workers.length) { $("#pool-msg").textContent = "\u8bf7\u81f3\u5c11\u586b\u5199\u4e00\u884c IP\u3002"; return; }
    $("#pool-save").disabled = true;
    $("#pool-msg").textContent = "";
    var entry = { workers: workers };
    api("/api/workers/pool", {
      method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(entry)
    }).then(function () {
      $("#pool-save").disabled = false;
      $("#pool-msg").textContent = "\u5df2\u4fdd\u5b58 Worker \u6c60\uff08" + workers.length + " \u4e2a\u8282\u70b9\uff09\u3002";
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

  // ---- \u672c\u673a\u542f\u52a8 / \u505c\u6b62 Worker ----
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
      // \u672c\u673a Worker \u542f\u52a8\u5373\u89c6\u4e3a\u201c\u51c6\u5907\u63a5\u6536\u4efb\u52a1\u201d\uff1a\u786e\u91cd 127.0.0.1 \u5728\u6c60\u4e2d
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
        alert("\u542f\u52a8 " + body.ip + " \u5931\u8d25\uff1a" + err.message);
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

  // Worker \u9875\u6fc0\u6d3b\u65f6\u6bcf 5 \u79d2\u5237\u65b0\u4e00\u6b21\u672c\u673a\u62c9\u8d77\u72b6\u6001
  setInterval(function () {
    if ($("#workers").classList.contains("active")) loadWorkers();
  }, 5000);

  // \u521d\u59cb\uff1a\u5148\u62c9\u4e00\u53d1\u72b6\u6001\uff0c\u518d\u8fde SSE
  api("/api/status").then(applyStatus).catch(function () { });
  connectStream();
})();
