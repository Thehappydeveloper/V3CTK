const THEME_KEY = "v3ctk-ui-theme";
const availableThemes = ["nocturne","slate","dawn","forest","ember","arctic","pastel"];
const themeSelect = document.getElementById("theme");
const applyTheme = (name) => {
  const next = availableThemes.includes(name) ? name : "nocturne";
  document.body.setAttribute("data-theme", next);
  if (themeSelect) themeSelect.value = next;
  try { localStorage.setItem(THEME_KEY, next); } catch (e) {}
};
const savedTheme = (() => {
  try { return localStorage.getItem(THEME_KEY); } catch (e) { return null; }
})();
applyTheme(savedTheme || document.body.getAttribute("data-theme") || "nocturne");
themeSelect?.addEventListener("change", (e) => applyTheme(e.target.value));

const fields = ["folder","project","segment","nx","ny","nz","frameRate","encodingParallelism","encodingThreadsPerInstance","vox","frameCount","startFrame","qpPairs","tilesOutput","encoderOutput","logsDir","v3cOutput"];
const defaults = {
  project: "default_project",
  segment: "16",
  frameRate: "30",
  nx: "2",
  ny: "3",
  nz: "1",
  encodingParallelism: "1",
  encodingThreadsPerInstance: "",
  qpPairs: "24:32:43",
  tilesOutput: "output/tiles",
  encoderOutput: "output/encoded",
  logsDir: "output/logs",
  v3cOutput: "output/v3c",
  segSplitComponents: true
};
const stageIds = ["stageTile","stageEncode","stageSegment","stageMPD"];
const requiredFields = ["folder","project","segment","nx","ny","nz","frameRate","encodingParallelism","qpPairs","tilesOutput","encoderOutput","logsDir","v3cOutput"];
let running = false;
let currentRun = null;
let lastMeta = null;
let currentStep = "step-folder";
const LAST_PROJECT_KEY = "v3ctk-last-project";
let autoScroll = true;
let statusInterval = null;

const segSplitInput = document.getElementById("segSplitComponents");
const segSplitPill = document.getElementById("seg-split-pill");

const syncSegSplit = () => {
  if (!segSplitPill) return;
  const on = segSplitInput.checked;
  segSplitPill.textContent = on ? "On" : "Off";
  segSplitPill.classList.toggle("muted", !on);
};
segSplitInput.addEventListener("change", syncSegSplit);
syncSegSplit();

const asPositiveInt = (v) => {
  const n = parseInt(v, 10);
  return Number.isFinite(n) && n > 0 ? n : null;
};
const asPositiveNumber = (v) => {
  const n = parseFloat(v);
  return Number.isFinite(n) && n > 0 ? n : null;
};
const normalizeQpPairs = (raw) => raw.replace(/\s+/g, ",").replace(/,+/g, ",").replace(/^,|,$/g, "");
const validateQpPairs = (raw) => {
  if (!raw) return false;
  const groups = raw.split(",").map(s => s.trim()).filter(Boolean);
  if (!groups.length) return false;
  return groups.every(group => {
    const parts = group.split(":");
    if (parts.length !== 3) return false;
    const nums = parts.map(p => Number.parseInt(p, 10));
    return nums.every(n => Number.isInteger(n) && n >= 0);
  });
};

document.querySelectorAll("#stage-row .toggle-chip").forEach(chip => {
  const input = chip.querySelector("input");
  const sync = () => chip.classList.toggle("active", input.checked);
  input.addEventListener("change", sync);
  chip.addEventListener("click", (e) => {
    if (e.target !== input) {
      e.preventDefault();
      input.checked = !input.checked;
      input.dispatchEvent(new Event("change", { bubbles: true }));
    }
  });
  sync();
});

function setStageLock(locked) {
  const hint = document.getElementById("stage-running-hint");
  hint.classList.toggle("hidden-inline", !locked);
  stageIds.forEach(id => {
    const input = document.getElementById(id);
    input.disabled = locked;
    const chip = input.closest(".toggle-chip");
    chip.style.opacity = locked ? "0.6" : "1";
  });
}

function setStatus(state) {
  const badge = document.getElementById("run-status-badge");
  const text = document.getElementById("run-status-text");
  badge.classList.remove("running", "stopped", "failed");
  if (state === "running") {
    badge.classList.add("running");
    text.textContent = "Running";
  } else if (state === "failed") {
    badge.classList.add("failed");
    text.textContent = "Failed";
  } else {
    badge.classList.add("stopped");
    text.textContent = "Stopped";
  }
}

function refreshCacheButton() {
  const project = document.getElementById("project").value.trim() || defaults.project;
  const hasCache = !!localStorage.getItem(`v3ctk-${project}`);
  const btn = document.getElementById("load-cache");
  btn.classList.toggle("hidden", !hasCache);
}

function loadCache(project) {
  const raw = localStorage.getItem(`v3ctk-${project}`);
  if (!raw) return;
  try {
    const vals = JSON.parse(raw);
    fields.forEach(id => {
      if (vals[id] !== undefined) document.getElementById(id).value = vals[id];
    });
    stageIds.forEach(id => {
      if (vals[id] !== undefined) document.getElementById(id).checked = !!vals[id];
      const chip = document.getElementById(id)?.closest(".toggle-chip");
      if (chip) chip.classList.toggle("active", document.getElementById(id).checked);
    });
    if (vals.segSplitComponents !== undefined) {
      document.getElementById("segSplitComponents").checked = !!vals.segSplitComponents;
    }
  } catch (e) {}
  updateHint();
}

document.getElementById("project").addEventListener("input", refreshCacheButton);
document.getElementById("load-cache").addEventListener("click", () => {
  const project = document.getElementById("project").value.trim() || defaults.project;
  loadCache(project);
});
requiredFields.forEach(id => {
  const el = document.getElementById(id);
  el.addEventListener("input", updateHint);
});

function updateHint() {
  const hint = document.getElementById("fill-hint");
  const anyEmpty = requiredFields.some(id => !document.getElementById(id).value.trim());
  hint.classList.toggle("hidden-inline", !anyEmpty);
}

(() => {
  refreshCacheButton();
  updateHint();
  updateHint();
})();

async function detectMetadata(folder) {
  if (location.protocol === "file:") {
    return { warning: "Auto-detection disabled when opened via file://. Serve over http://localhost to enable." };
  }
  try {
    const res = await fetch(`/api/inspect?folder=${encodeURIComponent(folder)}`);
    if (!res.ok) throw new Error("No metadata endpoint");
    const data = await res.json();
    return {
      vox: data.vox,
      frameCount: data.frameCount,
      startFrame: data.startFrameNumber,
      uncompressedPattern: data.uncompressedDataPath
    };
  } catch (e) {
    return { warning: "Metadata endpoint unavailable; please fill fields manually." };
  }
}

function showMeta(meta) {
  const box = document.getElementById("meta");
  const status = document.getElementById("detect-status");
  const parts = [];
  if (meta.uncompressedPattern) parts.push(`Pattern: ${meta.uncompressedPattern}`);
  if (meta.vox) parts.push(`Detected vox: ${meta.vox}`);
  if (meta.frameCount) parts.push(`Frame count: ${meta.frameCount}`);
  if (meta.startFrame !== undefined) parts.push(`Start frame: ${meta.startFrame}`);
  if (meta.warning) {
    status.style.display = "flex";
    status.textContent = meta.warning;
  } else {
    status.style.display = "none";
  }
  if (!parts.length) {
    box.style.display = "none";
    return;
  }
  box.style.display = "block";
  box.textContent = parts.join("  |  ");
}

const showStep = (id) => {
  ["step-folder","step-form","step-logs"].forEach(step => {
    document.getElementById(step).classList.toggle("hidden", step !== id);
  });
  if (id === "step-form") {
    document.getElementById("subtitle").textContent = "Content Preparation Parameters";
  } else if (id === "step-logs") {
    document.getElementById("subtitle").textContent = "Run Logs";
  } else {
    document.getElementById("subtitle").textContent = "Select uncompressed content";
  }
  document.getElementById("run-status").style.display = id === "step-logs" ? "flex" : document.getElementById("run-status").style.display;
  const backToLogs = document.getElementById("back-to-logs");
  if (backToLogs) backToLogs.classList.toggle("hidden", !(running && currentRun) || id === "step-logs");
  currentStep = id;
};

document.getElementById("next").addEventListener("click", async () => {
  const folder = document.getElementById("folder").value.trim();
  if (!folder) {
    alert("Input folder is required.");
    return;
  }
  const stepFolder = document.getElementById("step-folder");
  const stepForm = document.getElementById("step-form");
  stepFolder.classList.add("fade-out");
  setTimeout(() => {
    stepFolder.classList.add("hidden");
    stepForm.classList.remove("hidden");
    stepForm.classList.add("showing");
  }, 420);
  showStep("step-form");
  const projectField = document.getElementById("project");
  if (!projectField.value) {
    const base = folder.split("/").filter(Boolean).pop();
    projectField.value = base || defaults.project;
  }
  refreshCacheButton();
  const meta = await detectMetadata(folder);
  lastMeta = meta;
  if (meta.vox && !document.getElementById("vox").value) {
    document.getElementById("vox").value = meta.vox;
  }
  if (meta.frameCount && !document.getElementById("frameCount").value) {
    document.getElementById("frameCount").value = meta.frameCount;
  }
  if (meta.startFrame !== undefined && !document.getElementById("startFrame").value) {
    document.getElementById("startFrame").value = meta.startFrame;
  }
  showMeta(meta);
  updateHint();
  const autoBlocks = document.querySelectorAll(".autodetect.collapsible");
  if (meta.vox || meta.frameCount || meta.startFrame !== undefined) {
    autoBlocks.forEach(el => el.classList.remove("show"));
    const toggle = document.getElementById("edit-detected");
    toggle.style.display = "inline-block";
    toggle.textContent = "Show detected fields";
  } else {
    autoBlocks.forEach(el => el.classList.add("show"));
  }
});

document.getElementById("edit-detected").addEventListener("click", () => {
  const blocks = document.querySelectorAll(".autodetect.collapsible");
  const toggle = document.getElementById("edit-detected");
  const showing = blocks[0].classList.contains("show");
  blocks.forEach(el => el.classList.toggle("show"));
  const anyShowing = blocks[0].classList.contains("show");
  toggle.textContent = anyShowing ? "Hide detected fields" : "Show detected fields";
});

document.getElementById("build").addEventListener("click", () => {
  if (running) {
    alert("A pipeline run is already in progress. Stop it before starting another.");
    return;
  }
  const values = {};
  fields.forEach(id => {
    const el = document.getElementById(id);
    values[id] = el.value.trim() || defaults[id] || "";
  });
  values.segSplitComponents = document.getElementById("segSplitComponents").checked;

  if (!values.folder) {
    alert("Input folder is required.");
    return;
  }
  const segmentVal = asPositiveInt(values.segment || defaults.segment);
  if (!segmentVal) {
    alert("Segment size must be a positive integer.");
    return;
  }
  const nxVal = asPositiveInt(values.nx || defaults.nx);
  const nyVal = asPositiveInt(values.ny || defaults.ny);
  const nzVal = asPositiveInt(values.nz || defaults.nz);
  if (!nxVal || !nyVal || !nzVal) {
    alert("Tiling dimensions (n-x, n-y, n-z) must be positive integers.");
    return;
  }
  const frameRateVal = asPositiveNumber(values.frameRate || defaults.frameRate);
  if (!frameRateVal) {
    alert("Frame rate must be a positive number.");
    return;
  }
  const parallelismVal = asPositiveInt(values.encodingParallelism || defaults.encodingParallelism);
  if (!parallelismVal) {
    alert("Thread cap must be a positive integer.");
    return;
  }
  let requestedPerInstance = null;
  if (values.encodingThreadsPerInstance) {
    requestedPerInstance = asPositiveInt(values.encodingThreadsPerInstance);
    if (!requestedPerInstance) {
      alert("Threads per encode must be a positive integer.");
      return;
    }
  }
  const qpArg = normalizeQpPairs(values.qpPairs);
  if (!validateQpPairs(qpArg)) {
    alert("QP triplets must include at least one non-negative occ:geo:attr group.");
    return;
  }
  values.segment = String(segmentVal);
  values.nx = String(nxVal);
  values.ny = String(nyVal);
  values.nz = String(nzVal);
  values.frameRate = String(frameRateVal);
  values.encodingParallelism = String(parallelismVal);
  values.encodingThreadsPerInstance = requestedPerInstance ? String(requestedPerInstance) : "";
  values.qpPairs = qpArg;
  document.getElementById("encodingParallelism").value = values.encodingParallelism;
  document.getElementById("encodingThreadsPerInstance").value = values.encodingThreadsPerInstance;
  try {
    stageIds.forEach(id => values[id] = document.getElementById(id).checked);
    values.segSplitComponents = document.getElementById("segSplitComponents").checked;
    localStorage.setItem(`v3ctk-${values.project}`, JSON.stringify(values));
    localStorage.setItem(LAST_PROJECT_KEY, values.project);
  } catch (e) {}
  refreshCacheButton();

  const segmentSize = values.segment;
  const encoderGof = segmentSize;

  const args = [
    ["--project-name", values.project],
    ["--folder", values.folder],
    ["--segment-size", segmentSize],
    ["--frame-rate", values.frameRate],
    ["--n-x", values.nx],
    ["--n-y", values.ny],
    ["--n-z", values.nz],
    ["--encoding-parallelism", values.encodingParallelism],
    ["--qp-pairs", qpArg],
    ["--tiles-output", values.tilesOutput],
    ["--encoder-output", values.encoderOutput],
    ["--logs-dir", values.logsDir],
    ["--v3c-output", values.v3cOutput],
    ["--encoder-gof", encoderGof]
  ];
  if (values.encodingThreadsPerInstance) {
    args.push(["--encoding-threads-per-instance", values.encodingThreadsPerInstance]);
  }
  if (!document.getElementById("stageTile").checked) args.push(["--skip-tiling"]);
  if (!document.getElementById("stageSegment").checked) args.push(["--skip-segmentation"]);
  if (!document.getElementById("stageEncode").checked) args.push(["--skip-encoding"]);
  if (!document.getElementById("stageMPD").checked) args.push(["--skip-mpd"]);
  if (!values.segSplitComponents) args.push(["--no-seg-split-components"]);

  if (values.vox) {
    args.push(["--vox", values.vox]);
  }
  if (values.frameCount) {
    args.push(["--frame-count", values.frameCount]);
  }
  if (values.startFrame) {
    args.push(["--start-frame-number", values.startFrame]);
  }

  const cmd = ["python -m src.main"].concat(args.map(pair => pair.join(" "))).join(" ");
  const box = document.getElementById("command");
  box.textContent = cmd;
  box.classList.remove("hidden");
  updateHint();
});

document.getElementById("run").addEventListener("click", async () => {
  if (running) {
    alert("A pipeline run is already in progress. Stop it before starting another.");
    return;
  }
  const values = {};
  fields.forEach(id => {
    const el = document.getElementById(id);
    values[id] = el.value.trim() || defaults[id] || "";
  });
  if (!values.folder) {
    alert("Input folder is required.");
    return;
  }
  const segmentVal = asPositiveInt(values.segment || defaults.segment);
  if (!segmentVal) {
    alert("Segment size must be a positive integer.");
    return;
  }
  const nxVal = asPositiveInt(values.nx || defaults.nx);
  const nyVal = asPositiveInt(values.ny || defaults.ny);
  const nzVal = asPositiveInt(values.nz || defaults.nz);
  if (!nxVal || !nyVal || !nzVal) {
    alert("Tiling dimensions (n-x, n-y, n-z) must be positive integers.");
    return;
  }
  const frameRateVal = asPositiveNumber(values.frameRate || defaults.frameRate);
  if (!frameRateVal) {
    alert("Frame rate must be a positive number.");
    return;
  }
  const parallelismVal = asPositiveInt(values.encodingParallelism || defaults.encodingParallelism);
  if (!parallelismVal) {
    alert("Thread cap must be a positive integer.");
    return;
  }
  let requestedPerInstance = null;
  if (values.encodingThreadsPerInstance) {
    requestedPerInstance = asPositiveInt(values.encodingThreadsPerInstance);
    if (!requestedPerInstance) {
      alert("Threads per encode must be a positive integer.");
      return;
    }
  }
  const qpArg = normalizeQpPairs(values.qpPairs);
  if (!validateQpPairs(qpArg)) {
    alert("QP triplets must include at least one non-negative occ:geo:attr group.");
    return;
  }
  values.segment = String(segmentVal);
  values.nx = String(nxVal);
  values.ny = String(nyVal);
  values.nz = String(nzVal);
  values.frameRate = String(frameRateVal);
  values.encodingParallelism = String(parallelismVal);
  values.encodingThreadsPerInstance = requestedPerInstance ? String(requestedPerInstance) : "";
  values.qpPairs = qpArg;
  document.getElementById("encodingParallelism").value = values.encodingParallelism;
  document.getElementById("encodingThreadsPerInstance").value = values.encodingThreadsPerInstance;
  try {
    stageIds.forEach(id => values[id] = document.getElementById(id).checked);
    values.segSplitComponents = document.getElementById("segSplitComponents").checked;
    localStorage.setItem(`v3ctk-${values.project}`, JSON.stringify(values));
    localStorage.setItem(LAST_PROJECT_KEY, values.project);
  } catch (e) {}
  const stages = {};
  stageIds.forEach(id => {
    stages[id] = document.getElementById(id).checked;
  });
  const payload = { args: values, stages, qpPairs: values.qpPairs };
  const res = await fetch("/api/run", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload)
  });
  if (!res.ok) {
    const msg = await res.text();
    alert("Run failed: " + msg);
    return;
  }
  const data = await res.json();
  running = true;
  currentRun = data;
  setStatus("running");
  setStageLock(true);
  showStep("step-logs");
  const box = document.getElementById("command");
  box.textContent = data.command;
  box.classList.remove("hidden");
  const logBox = document.getElementById("log-output");
  const liveBox = document.getElementById("live-log");
  logBox.textContent = "Starting...\n";
  liveBox.textContent = "Starting...\n";
  logBox.classList.remove("hidden");
  liveBox.classList.remove("hidden");
  liveBox.scrollIntoView({ behavior: "smooth", block: "start" });
  if (window._logInterval) clearInterval(window._logInterval);
  if (statusInterval) clearInterval(statusInterval);
  autoScroll = true;
  liveBox.addEventListener("scroll", () => {
    const nearBottom = liveBox.scrollTop + liveBox.clientHeight >= liveBox.scrollHeight - 20;
    autoScroll = nearBottom;
  });
  window._logInterval = setInterval(async () => {
    try {
      const resp = await fetch(`/api/logs?path=${encodeURIComponent(data.logFile)}`);
      if (!resp.ok) return;
      const txt = await resp.text();
      logBox.textContent = txt || "(waiting for logs...)";
      liveBox.textContent = txt || "(waiting for logs...)";
      if (autoScroll) {
        logBox.scrollTop = logBox.scrollHeight;
        liveBox.scrollTop = liveBox.scrollHeight;
      }
    } catch (e) {
      console.error(e);
    }
  }, 1500);
  statusInterval = setInterval(async () => {
    try {
      const resp = await fetch("/api/status");
      if (!resp.ok) return;
      const info = await resp.json();
      if (!info.running) {
        running = false;
        currentRun = null;
        if (window._logInterval) clearInterval(window._logInterval);
        if (statusInterval) clearInterval(statusInterval);
        setStatus(info.exitCode && info.exitCode !== 0 ? "failed" : "stopped");
        setStageLock(false);
      }
    } catch (e) {
      console.error(e);
    }
  }, 2000);
  updateHint();
});

document.getElementById("stop-run").addEventListener("click", async () => {
  if (!running || !currentRun) return;
  const stopBtn = document.getElementById("stop-run");
  const prevText = stopBtn.textContent;
  stopBtn.textContent = "Stopping...";
  stopBtn.disabled = true;
  await fetch("/api/stop", { method: "POST" });
  running = false;
  currentRun = null;
  if (window._logInterval) clearInterval(window._logInterval);
  if (statusInterval) clearInterval(statusInterval);
  setStatus("stopped", false);
  setStageLock(false);
  stopBtn.textContent = prevText;
  stopBtn.disabled = false;
});

document.getElementById("back-to-form").addEventListener("click", () => {
  showStep("step-form");
});

document.getElementById("back-to-logs")?.addEventListener("click", () => {
  if (running && currentRun) showStep("step-logs");
});

if (location.protocol === "file:") {
  const warn = document.getElementById("file-warning");
  warn.style.display = "flex";
}
