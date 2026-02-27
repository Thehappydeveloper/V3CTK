import os
import signal
import subprocess
import threading
import time
from datetime import datetime
from pathlib import Path
from typing import Optional
import re
from flask import Flask, jsonify, request, send_from_directory, Response

from src.utils import extract_metadata_from_filename

app = Flask(__name__, static_folder="web", static_url_path="")

_FRAME_NUM_RE = re.compile(r"(\d+)(?=\.ply$)")
LOG_FILE = Path("logs/server.log")
PID_FILE = Path("logs/.active_pipeline_pid")
_current_proc = None
_run_pid_group = None
_last_run_meta = {}
_last_exit_code = None


def _log_line(message: str):
    LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{timestamp}] {message}"
    print(line)
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(line + "\n")


def _derive_uncompressed_pattern(frame_name: str) -> str:
    match = _FRAME_NUM_RE.search(frame_name)
    if not match:
        return frame_name
    digits = len(match.group(1))
    return f"{frame_name[:match.start(1)]}%0{digits}d{frame_name[match.end(1):]}"


def _write_active_pid(pid: int):
    try:
        PID_FILE.parent.mkdir(parents=True, exist_ok=True)
        PID_FILE.write_text(str(pid), encoding="utf-8")
    except Exception as e:
        _log_line(f"[WARNING] Failed to write active pid file: {e}")


def _read_active_pid() -> Optional[int]:
    try:
        return int(PID_FILE.read_text(encoding="utf-8").strip())
    except Exception:
        return None


def _clear_active_pid():
    try:
        PID_FILE.unlink(missing_ok=True)
    except Exception:
        pass


def _is_pid_running(pid: int) -> bool:
    try:
        os.kill(pid, 0)
        return True
    except OSError:
        return False


def _looks_like_pipeline(pid: int) -> bool:
    try:
        cmdline = Path(f"/proc/{pid}/cmdline").read_text(encoding="utf-8")
        return "src.main" in cmdline
    except Exception:
        return False


def _reset_proc_state(proc=None):
    """Clear tracked process info if it still matches the given proc (or always if None)."""
    global _current_proc, _run_pid_group
    if proc is not None and proc is not _current_proc:
        return
    _current_proc = None
    _run_pid_group = None
    _clear_active_pid()


@app.route("/")
def index():
    return send_from_directory(app.static_folder, "index.html")


@app.route("/api/inspect")
def inspect():
    folder = request.args.get("folder")
    if not folder:
        return jsonify({"error": "folder parameter is required"}), 400

    p = Path(folder)
    if not p.exists() or not p.is_dir():
        return jsonify({"error": f"folder not found: {folder}"}), 404

    frame_paths = sorted(p.glob("*.ply"))
    if not frame_paths:
        return jsonify({"error": "no PLY frames found"}), 404

    first_frame = frame_paths[0]
    vox = extract_metadata_from_filename(first_frame).get("vox")

    start_frame = None
    for fp in frame_paths:
        m = _FRAME_NUM_RE.search(fp.name)
        if m:
            num = int(m.group(1))
            start_frame = num if start_frame is None else min(start_frame, num)

    pattern = _derive_uncompressed_pattern(first_frame.name)

    return jsonify({
        "vox": vox,
        "frameCount": len(frame_paths),
        "startFrameNumber": start_frame,
        "uncompressedDataPath": pattern
    })


@app.route("/<path:path>")
def static_proxy(path):
    return send_from_directory(app.static_folder, path)


@app.route("/api/run", methods=["POST"])
def run_pipeline():
    global _last_run_meta, _last_exit_code
    payload = request.get_json(force=True, silent=True) or {}
    args = payload.get("args", {})
    stages = payload.get("stages", {})

    project = args.get("project", "default_project")
    base_dir = Path(__file__).parent.resolve()
    log_root = (base_dir / args.get("logsDir", "output/logs") / project)
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    log_root.mkdir(parents=True, exist_ok=True)
    log_file_host = log_root / f"ui_run_{timestamp}.log"

    _log_line(f"[INFO] New run requested for project={project}")
    _log_line(f"[INFO] Host log file: {log_file_host}")
    _last_run_meta = {}
    _last_exit_code = None

    cli = ["python", "-m", "src.main"]

    def add(flag, val=None):
        if val is None or val == "":
            return
        cli.append(flag)
        if val is not True:
            cli.append(str(val))

    segment_size = args.get("segment") or args.get("segFramesPerSegment") or "16"
    encoder_gof = args.get("encoderGof") or args.get("segFramesPerAd") or segment_size

    add("--project-name", project)
    add("--folder", args.get("folder"))
    add("--segment-size", segment_size)
    add("--frame-rate", args.get("frameRate", "30"))
    add("--n-x", args.get("nx", "2"))
    add("--n-y", args.get("ny", "3"))
    add("--n-z", args.get("nz", "1"))
    add("--encoder-gof", encoder_gof)
    add("--encoding-parallelism", args.get("encodingParallelism", "1"))
    enc_threads = args.get("encodingThreadsPerInstance")
    if enc_threads not in (None, ""):
        add("--encoding-threads-per-instance", enc_threads)
    if args.get("segSplitComponents") is False:
        cli.append("--no-seg-split-components")
    qp_raw = args.get("qpPairs", "24:32:43").replace(" ", ",")
    add("--qp-pairs", qp_raw)
    add("--tiles-output", args.get("tilesOutput"))
    add("--encoder-output", args.get("encoderOutput"))
    add("--logs-dir", args.get("logsDir"))
    add("--v3c-output", args.get("v3cOutput"))
    add("--log-file", str(log_file_host))

    if args.get("vox"):
        add("--vox", args.get("vox"))
    if args.get("frameCount"):
        add("--frame-count", args.get("frameCount"))
    if args.get("startFrame"):
        add("--start-frame-number", args.get("startFrame"))

    if not stages.get("stageTile", True):
        cli.append("--skip-tiling")
    if not stages.get("stageSegment", True):
        cli.append("--skip-segmentation")
    if not stages.get("stageEncode", True):
        cli.append("--skip-encoding")
    if not stages.get("stageMPD", True):
        cli.append("--skip-mpd")

    global _current_proc, _run_pid_group
    if _current_proc and _current_proc.poll() is None:
        return jsonify({"error": "A pipeline run is already in progress"}), 409

    try:
        _log_line(f"[INFO] Launching: {' '.join(cli)}")
        proc = subprocess.Popen(cli, cwd=base_dir, start_new_session=True)
        _current_proc = proc
        _run_pid_group = proc.pid
        _write_active_pid(proc.pid)
        _last_run_meta = {
            "pid": proc.pid,
            "command": " ".join(cli),
            "logFile": str(log_file_host),
        }

        def _monitor_proc(p):
            global _last_exit_code
            try:
                exit_code = p.wait()
                _last_exit_code = exit_code
                _log_line(f"[INFO] Pipeline process exited with code {exit_code}")
            except Exception as e:
                _log_line(f"[WARNING] Error while waiting for pipeline process: {e}")
            finally:
                _reset_proc_state(p)

        threading.Thread(target=_monitor_proc, args=(proc,), daemon=True).start()
    except Exception as e:
        _log_line(f"[ERROR] Failed to start pipeline: {e}")
        return jsonify({"error": str(e)}), 500

    return jsonify({
        "pid": proc.pid,
        "command": " ".join(cli),
        "logFile": str(log_file_host)
    })


@app.route("/api/logs")
def read_logs():
    path = request.args.get("path")
    if not path:
        return jsonify({"error": "path required"}), 400
    p = Path(path).resolve()
    root = Path(".").resolve()
    if root not in p.parents and p != root:
        return jsonify({"error": "invalid path"}), 400
    if not p.exists():
        return Response("", mimetype="text/plain")
    try:
        content = p.read_text(errors="ignore")
        lines = content.splitlines()
        if len(lines) > 400:
            lines = lines[-400:]
        return Response("\n".join(lines), mimetype="text/plain")
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/status")
def status():
    """Lightweight status endpoint so UI can detect completion."""
    pid = _current_proc.pid if _current_proc and _current_proc.poll() is None else _run_pid_group or _read_active_pid()
    running = bool(pid and _is_pid_running(pid) and _looks_like_pipeline(pid))
    resp = {"running": running}
    if _last_run_meta:
        resp.update(_last_run_meta)
    if not running and _last_exit_code is not None:
        resp["exitCode"] = _last_exit_code
    return jsonify(resp)


@app.route("/api/stop", methods=["POST"])
def stop_pipeline():
    global _current_proc, _run_pid_group, _last_exit_code
    target_proc = _current_proc if _current_proc and _current_proc.poll() is None else None
    pid = target_proc.pid if target_proc else _run_pid_group or _read_active_pid()

    if not pid or not _is_pid_running(pid) or not _looks_like_pipeline(pid):
        _reset_proc_state()
        return jsonify({"message": "No active process"}), 400

    try:
        pgid = os.getpgid(pid)
    except Exception:
        pgid = None

    _log_line(f"[INFO] Stop requested; signaling pipeline pid={pid}, pgid={pgid}.")
    try:
        if pgid:
            os.killpg(pgid, signal.SIGTERM)
    except Exception as e:
        _log_line(f"[WARNING] Failed to SIGTERM process group {pgid}: {e}")

    try:
        if target_proc:
            target_proc.terminate()
        else:
            os.kill(pid, signal.SIGTERM)
    except Exception as e:
        _log_line(f"[WARNING] Failed to SIGTERM pid {pid}: {e}")

    try:
        subprocess.Popen(["pkill", "-TERM", "-P", str(pid)])
    except Exception as e:
        _log_line(f"[WARNING] Failed to signal child processes for pid {pid}: {e}")

    deadline = time.time() + 15
    while time.time() < deadline and _is_pid_running(pid):
        time.sleep(0.5)

    if _is_pid_running(pid):
        _log_line(f"[WARNING] Pipeline pid {pid} still alive after SIGTERM; sending SIGKILL.")
        try:
            if pgid:
                os.killpg(pgid, signal.SIGKILL)
            os.kill(pid, signal.SIGKILL)
        except Exception as e:
            _log_line(f"[ERROR] Failed to SIGKILL pid {pid}: {e}")

    _log_line("[INFO] Pipeline process terminated by user")
    if not _last_exit_code:
        _last_exit_code = -1
    _reset_proc_state(target_proc)
    return jsonify({"message": "Stopped"})


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080, debug=True)
