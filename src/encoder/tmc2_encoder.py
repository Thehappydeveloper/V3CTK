#!/usr/bin/env python3
import logging
import os
from pathlib import Path
import re
import subprocess
import sys
import time
from datetime import datetime
from typing import Optional, Dict, Any, List, Tuple
import concurrent.futures
import threading


class TMC2EncoderRunner:
    DEFAULT_PARAMS: Dict[str, Any] = {
        "configurationFolder": "/workspace/TMC2/cfg/",
        "config": [
            "/workspace/TMC2/cfg/common/ctc-common.cfg",
            "/workspace/TMC2/cfg/condition/ctc-all-intra.cfg",
            "/workspace/TMC2/cfg/rate/ctc-r1.cfg"
        ],
        "uncompressedDataFolder": "/data/input/",
        "uncompressedDataPath": "BlueBackpack_UVG_vox9_25_0_250_%04d.ply",  # relative to folder
        "frameCount": 250,
        "resolution": 2047,
        "keepIntermediateFiles": 0,
        "mapCountMinus1": 0,
        "videoEncoderInternalBitdepth": 8,
        "compressedStreamPath": "/data/output/bluebackpack.bin",
        "nbThread": 8,
        "computeMetrics": 0,
        "computeChecksum": 0,
        "reconstructedDataPath": '""',
        "geometry3dCoordinatesBitdepth": 9,
        "geometryNominal2dBitdepth": 8,
        "occupancyMapQP": 24,
        "geometryQP": 32,
        "attributeQP": 43,
        "startFrameNumber": 0,
        "groupOfFramesSize": 16,
        "voxelDimensionRefineSegmentation": 2,
        "minNormSumOfInvDist4MPSelection": 0.36,
        "partialAdditionalProjectionPlane": 0.15,
        "minPointCountPerCCPatchSegmentation": 16,
        "maxNNCountRefineSegmentation": 32,
        "nnNormalEstimation": 6,
        "iterationCountRefineSegmentation": 3,
        "lambdaRefineSegmentation": 3.5,
        "minimumImageWidth": 1024,
        "minimumImageHeight": 1024,
    }

    ACTIVE_PROCS: List[subprocess.Popen] = []
    ACTIVE_CONTAINERS: List[str] = []
    CANCEL_REQUESTED: bool = False
    CANCEL_EVENT: threading.Event = threading.Event()

    def __init__(
        self,
        tmc2_src_dir: str = "./src/encoder",
        docker_image: str = "tmc2-builder-image",
        repo_url: str = "https://github.com/Thehappydeveloper/mpeg-pcc-tmc2.git",
        tmc2_commit: str = "790fb4da7a6a2ba9d61a1a595531be9d75624365",
        log_file: Optional[str] = None,
        secondary_log_file: Optional[str] = None,
        log_debug_to_file: bool = False,
        stop_event: Optional["threading.Event"] = None,
    ):
        self.tmc2_src_dir = Path(tmc2_src_dir)
        self.tmc2_repo_dir = self.tmc2_src_dir / "TMC2"
        self.docker_image = docker_image
        self.repo_url = repo_url
        self.tmc2_commit = tmc2_commit
        self.log_debug_to_file = log_debug_to_file
        self.stop_event = stop_event

        # Reset cancellation flag for a fresh runner instance
        TMC2EncoderRunner.CANCEL_REQUESTED = False
        TMC2EncoderRunner.CANCEL_EVENT.clear()

        logging.basicConfig(level=logging.INFO)
        self.log_file = Path(log_file) if log_file else None
        self.secondary_log_file = Path(secondary_log_file) if secondary_log_file else None
        if self.log_file:
            self._write_header()

        self._log(f"[INFO] Init TMC2 encoder: src_dir={self.tmc2_src_dir}, image={self.docker_image}")
        self._ensure_repo()
        self._ensure_repo_commit()
        self._ensure_docker_image()
        self._ensure_build()
        self._log("[INFO] TMC2 encoder ready.")

    # ---------------- Logging ----------------
    def _write_header(self):
        PURPLE = "\033[38;5;141m"
        RESET = "\033[0m"
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        with open(self.log_file, "a", encoding="utf-8") as f:
            f.write(f"\n{PURPLE}TMC2 Encoding Log — {timestamp}{RESET}\n\n")

    def _log(self, msg: str):
        logging.info(msg)
        if self.log_file:
            with open(self.log_file, "a", encoding="utf-8") as f:
                f.write(msg + "\n")
        if self.secondary_log_file:
            with open(self.secondary_log_file, "a", encoding="utf-8") as f:
                f.write(msg + "\n")

    def _debug(self, msg: str):
        logging.debug(msg)
        if self.log_debug_to_file and self.log_file:
            with open(self.log_file, "a", encoding="utf-8") as f:
                f.write(msg + "\n")
        if self.log_debug_to_file and self.secondary_log_file:
            with open(self.secondary_log_file, "a", encoding="utf-8") as f:
                f.write(msg + "\n")

    # ---------------- Cancellation helpers ----------------
    @classmethod
    def _cancel_requested(cls, stop_event: Optional["threading.Event"] = None) -> bool:
        return cls.CANCEL_EVENT.is_set() or cls.CANCEL_REQUESTED or (stop_event and stop_event.is_set())

    def _cancel_requested_local(self) -> bool:
        return self._cancel_requested(self.stop_event)

    def _run_cmd_with_cancel(self, cmd: List[str], cwd: Optional[Path] = None, desc: str = ""):
        """Run a subprocess but honor cancellation promptly."""
        proc = subprocess.Popen(
            cmd,
            cwd=str(cwd) if cwd else None,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            start_new_session=True,
        )
        output_lines: List[str] = []
        try:
            while True:
                if self._cancel_requested_local():
                    self._log(f"[WARNING] Stop requested; terminating '{desc or ' '.join(cmd)}'.")
                    proc.terminate()
                    try:
                        proc.wait(timeout=5)
                    except subprocess.TimeoutExpired:
                        proc.kill()
                    raise RuntimeError("Cancelled")

                line = proc.stdout.readline()
                if line:
                    output_lines.append(line.rstrip("\n"))
                if proc.poll() is not None:
                    # Read remaining buffered lines
                    output_lines.extend(proc.stdout.read().splitlines())
                    if proc.returncode != 0:
                        raise subprocess.CalledProcessError(proc.returncode, cmd, "\n".join(output_lines))
                    return "\n".join(output_lines)
        finally:
            if proc.poll() is None:
                proc.kill()

    @staticmethod
    def _kill_container(name: str):
        if not name:
            return
        try:
            subprocess.run(
                ["docker", "kill", name],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                timeout=10,
            )
        except Exception:
            pass
        try:
            subprocess.run(
                ["docker", "rm", "-f", name],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                timeout=10,
            )
        except Exception:
            pass

    # ---------------- Repo & Docker ----------------
    def _ensure_repo(self):
        if self._cancel_requested_local():
            self._log("[WARNING] Stop requested before cloning; aborting.")
            sys.exit(1)
        self.tmc2_src_dir.mkdir(parents=True, exist_ok=True)
        if not self.tmc2_repo_dir.exists():
            self._log(f"[INFO] Cloning TMC2 repo into {self.tmc2_repo_dir} ...")
            try:
                self._run_cmd_with_cancel(
                    ["git", "clone", self.repo_url, str(self.tmc2_repo_dir)],
                    desc="git clone TMC2",
                )
                self._log("[INFO] Clone successful.")
            except subprocess.CalledProcessError as e:
                self._log(f"[ERROR] Failed to clone repo: {e}")
                sys.exit(1)
            except RuntimeError:
                self._log("[WARNING] Clone cancelled.")
                sys.exit(1)
        else:
            self._log("[INFO] TMC2 repo already exists, skipping clone.")

    def _ensure_repo_commit(self):
        if self._cancel_requested_local():
            self._log("[WARNING] Stop requested before checkout; aborting.")
            sys.exit(1)
        target = self.tmc2_commit

        def _commit_exists(commit: str) -> bool:
            try:
                subprocess.run(
                    ["git", "cat-file", "-e", f"{commit}^{{commit}}"],
                    cwd=self.tmc2_repo_dir,
                    check=True,
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                )
                return True
            except subprocess.CalledProcessError:
                return False

        try:
            current_head = subprocess.run(
                ["git", "rev-parse", "HEAD"],
                cwd=self.tmc2_repo_dir,
                capture_output=True,
                text=True,
                check=True,
            ).stdout.strip()
        except subprocess.CalledProcessError:
            current_head = None

        if current_head == target:
            self._log(f"[INFO] TMC2 already at commit {target[:7]}.")
            return

        if not _commit_exists(target):
            self._log(f"[INFO] Commit {target[:7]} missing locally; fetching origin ...")
            try:
                self._run_cmd_with_cancel(
                    ["git", "fetch", "--all", "--tags"],
                    cwd=self.tmc2_repo_dir,
                    desc="git fetch",
                )
            except subprocess.CalledProcessError as e:
                self._log(f"[ERROR] Failed to fetch commit {target}: {e}")
                sys.exit(1)
            except RuntimeError:
                self._log("[WARNING] Fetch cancelled.")
                sys.exit(1)
            if not _commit_exists(target):
                self._log(f"[ERROR] Commit {target} not found after fetch; aborting.")
                sys.exit(1)

        try:
            self._run_cmd_with_cancel(
                ["git", "checkout", "--detach", target],
                cwd=self.tmc2_repo_dir,
                desc="git checkout",
            )
            self._log(f"[INFO] Checked out TMC2 commit {target[:7]}.")
        except subprocess.CalledProcessError as e:
            self._log(f"[ERROR] Failed to check out commit {target}: {e}")
            sys.exit(1)
        except RuntimeError:
            self._log("[WARNING] Checkout cancelled.")
            sys.exit(1)

    def _ensure_docker_image(self):
        self._log(f"[INFO] Checking Docker image {self.docker_image}")
        try:
            images_proc = subprocess.run(
                ["docker", "images", "-q", self.docker_image],
                capture_output=True,
                text=True,
                timeout=30
            )
            images = images_proc.stdout.strip()
        except subprocess.TimeoutExpired:
            self._log(f"[ERROR] docker images timed out while checking {self.docker_image}")
            sys.exit(1)
        except FileNotFoundError:
            self._log("[ERROR] docker executable not found; ensure Docker is installed and available.")
            sys.exit(1)
        except Exception as e:
            self._log(f"[ERROR] docker images failed: {e}")
            sys.exit(1)
        if not images:
            self._log(f"[INFO] Building Docker image {self.docker_image} ...")
            try:
                dockerfile = str(self.tmc2_src_dir.resolve() / "Dockerfile.build")
                context = str(self.tmc2_src_dir.resolve())
                result_stdout = self._run_cmd_with_cancel(
                    ["docker", "build", "-t", self.docker_image, "-f", dockerfile, context],
                    desc="docker build",
                )
                if result_stdout:
                    self._log(result_stdout)
                self._log(f"[INFO] Docker image {self.docker_image} build finished.")
            except subprocess.CalledProcessError as e:
                self._log(f"[ERROR] Failed to build Docker image: {e}")
                sys.exit(1)
            except RuntimeError:
                self._log("[WARNING] Docker build cancelled.")
                sys.exit(1)
        else:
            self._log(f"[INFO] Docker image {self.docker_image} already exists, skipping build.")

    def _ensure_build(self):
        if self._cancel_requested_local():
            self._log("[WARNING] Stop requested before build; aborting.")
            sys.exit(1)
        self._log("[INFO] Running build.sh inside Docker to ensure binaries exist ...")
        try:
            self._run_cmd_with_cancel(
                [
                    "docker", "run", "--rm",
                    "-v", f"{self.tmc2_repo_dir.resolve()}:/workspace/TMC2",
                    self.docker_image,
                    "/bin/bash", "-c", "chmod +x /workspace/TMC2/build.sh && /workspace/TMC2/build.sh release"
                ],
                desc="docker build.sh",
            )
            self._log("[INFO] build.sh executed successfully.")
        except subprocess.CalledProcessError as e:
            self._log(f"[ERROR] build.sh failed: {e}")
            sys.exit(1)
        except RuntimeError:
            self._log("[WARNING] build.sh cancelled.")
            sys.exit(1)

    # ---------------- Encoder Run ----------------

    def run(self, params: Optional[Dict[str, Any]] = None,
            host_input_path: Optional[str] = None,
            host_output_path: Optional[str] = None,
            compressed_stream_filename: Optional[str] = None,
            task_log_file: Optional[str] = None,
            stop_event: Optional["threading.Event"] = None):
        """
        Run TMC2 PccAppEncoder inside Docker.
        
        host_input_path  -> folder on host to mount to /data/input
        host_output_path -> folder on host to mount to /data/output
            params overwrite DEFAULT_PARAMS (uncompressedDataFolder stays /data/input)
            compressed_stream_filename sets the output name under /data/output
        """
        if self._cancel_requested(stop_event):
            self._log("[WARNING] Stop requested before starting encoder; skipping task.")
            return

        # Start from default container paths
        encoder_params = self.DEFAULT_PARAMS.copy()
        
        # Update only other params, but skip overwriting uncompressedDataFolder or compressedStreamPath
        if params:
            for k, v in params.items():
                if k not in ["uncompressedDataFolder", "compressedStreamPath"]:
                    encoder_params[k] = v
        if compressed_stream_filename:
            encoder_params["compressedStreamPath"] = f"/data/output/{compressed_stream_filename}"

        cmd = ["/workspace/TMC2/bin/PccAppEncoder"]

        # Add params to command
        for k, v in encoder_params.items():
            if isinstance(v, list):
                for item in v:
                    cmd.append(f"--{k}={item}")
            else:
                cmd.append(f"--{k}={v}")

        # Docker volume mounts
        host_input = Path(host_input_path or "/data/input").resolve()
        host_output = Path(host_output_path or "/data/output").resolve()
        container_name = f"tmc2_enc_{os.getpid()}_{int(time.time() * 1000)}_{threading.get_ident()}"

        docker_cmd = [
            "docker", "run", "--rm",
            "--name", container_name,
            "-v", f"{self.tmc2_repo_dir.resolve()}:/workspace/TMC2",
            "-v", f"{host_input}:/data/input:ro",
            "-v", f"{host_output}:/data/output",
            self.docker_image,
            "/bin/bash", "-c",
            f"echo 'Running inside container: {' '.join(cmd)}' && {' '.join(cmd)}"
        ]

        self._debug(f"[DEBUG] Running encoder inside container: {' '.join(cmd)}")

        log_file_handle = None
        chosen_log_file = task_log_file or self.log_file
        proc: Optional[subprocess.Popen] = None
        try:
            if chosen_log_file:
                log_file_handle = open(chosen_log_file, "a", encoding="utf-8")
            proc = subprocess.Popen(
                docker_cmd,
                stdout=log_file_handle or subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                start_new_session=True
            )
            TMC2EncoderRunner.ACTIVE_PROCS.append(proc)
            TMC2EncoderRunner.ACTIVE_CONTAINERS.append(container_name)
            while True:
                try:
                    proc.wait(timeout=0.5)
                    break
                except subprocess.TimeoutExpired:
                    if self._cancel_requested(stop_event):
                        self._log("[WARNING] Stop requested; terminating encoder process and container.")
                        if container_name:
                            self._log(f"[INFO] Killing container {container_name}")
                        self._kill_container(container_name)
                        proc.terminate()
                        try:
                            self._log(f"[INFO] Waiting for encoder process {proc.pid} to exit after terminate...")
                            proc.wait(timeout=5)
                            self._log(f"[INFO] Encoder process {proc.pid} exited with code {proc.returncode}.")
                        except subprocess.TimeoutExpired:
                            self._log(f"[WARNING] Encoder process {proc.pid} did not exit; killing.")
                            proc.kill()
                            try:
                                proc.wait(timeout=5)
                                self._log(f"[INFO] Encoder process {proc.pid} killed (code {proc.returncode}).")
                            except subprocess.TimeoutExpired:
                                self._log(f"[ERROR] Encoder process {proc.pid} still alive after kill attempt.")
                        break
            if proc.returncode != 0 and not self._cancel_requested(stop_event):
                self._log(f"[ERROR] PccAppEncoder failed with code {proc.returncode}")
                sys.exit(proc.returncode)
            self._debug("[DEBUG] Encoding finished successfully.")
        finally:
            if proc in TMC2EncoderRunner.ACTIVE_PROCS:
                TMC2EncoderRunner.ACTIVE_PROCS.remove(proc)
            if log_file_handle:
                log_file_handle.close()
            if container_name in TMC2EncoderRunner.ACTIVE_CONTAINERS:
                TMC2EncoderRunner.ACTIVE_CONTAINERS.remove(container_name)
            # Ensure container is gone
            self._kill_container(container_name)

    @staticmethod
    def cancel_all():
        """Terminate all active encoder processes."""
        TMC2EncoderRunner.CANCEL_REQUESTED = True
        TMC2EncoderRunner.CANCEL_EVENT.set()
        logging.info("[INFO] Cancel requested; terminating active encoder workers.")

        # Terminate running docker containers first
        containers = list(TMC2EncoderRunner.ACTIVE_CONTAINERS)
        if containers:
            logging.info(f"[INFO] Stopping {len(containers)} active container(s).")
        for name in containers:
            logging.info(f"[INFO] Killing container {name}")
            TMC2EncoderRunner._kill_container(name)
        TMC2EncoderRunner.ACTIVE_CONTAINERS.clear()

        # Terminate the docker run wrapper processes
        procs = list(TMC2EncoderRunner.ACTIVE_PROCS)
        if procs:
            logging.info(f"[INFO] Signaling {len(procs)} encoder process(es) to exit.")
        for proc in procs:
            try:
                if proc.poll() is None:
                    subprocess.Popen(["pkill", "-TERM", "-P", str(proc.pid)])
                    proc.terminate()
            except Exception as e:
                logging.warning(f"[WARNING] Failed to signal process {getattr(proc, 'pid', '?')}: {e}")

        # Wait for processes to die to avoid respawn/races
        for proc in procs:
            if proc.poll() is None:
                logging.info(f"[INFO] Waiting for encoder process {proc.pid} to exit...")
                try:
                    proc.wait(timeout=5)
                    logging.info(f"[INFO] Encoder process {proc.pid} exited with code {proc.returncode}.")
                except subprocess.TimeoutExpired:
                    logging.warning(f"[WARNING] Encoder process {proc.pid} did not exit; killing.")
                    proc.kill()
                    try:
                        proc.wait(timeout=3)
                        logging.info(f"[INFO] Encoder process {proc.pid} killed (code {proc.returncode}).")
                    except subprocess.TimeoutExpired:
                        logging.error(f"[ERROR] Encoder process {proc.pid} did not die after kill.")
                except Exception as e:
                    logging.warning(f"[WARNING] Error while waiting for process {proc.pid}: {e}")

        # Remove any finished processes from tracking
        TMC2EncoderRunner.ACTIVE_PROCS = [p for p in TMC2EncoderRunner.ACTIVE_PROCS if p.poll() is None]
        logging.info("[INFO] Encoder cancellation routine completed.")

    # ---------------- Batch Encoding ----------------
    def _derive_frame_sequence(self, tile_dir: Path) -> Tuple[str, int, int]:
        """Infer sequence pattern, start frame, and count from ply files in tile_dir."""
        frame_files = sorted(tile_dir.glob("*.ply"))
        if not frame_files:
            raise ValueError(f"{tile_dir}: no .ply frames found")

        # Use the first readable file to infer the numbering pattern
        stem = frame_files[0].stem
        match = re.search(r"(.*?)(\d+)$", stem)
        if not match:
            raise ValueError(f"{tile_dir}: unable to infer numbering pattern from {stem}")

        prefix, digits = match.group(1), match.group(2)
        width = len(digits)

        frame_indices = []
        for fp in frame_files:
            m = re.search(r"(.*?)(\d+)$", fp.stem)
            if m:
                width = max(width, len(m.group(2)))
                frame_indices.append(int(m.group(2)))

        if not frame_indices:
            raise ValueError(f"{tile_dir}: no numbered frames found in tile directory")

        pattern = f"{prefix}%0{width}d.ply"
        start_frame = min(frame_indices)
        frame_count = len(frame_indices)
        return pattern, start_frame, frame_count

    def _normalize_qp_pairs(self, qp_pairs: List[Any]) -> List[Tuple[int, int, int]]:
        """Normalize qp_pairs into a list of (occupancyMapQP, geometryQP, attributeQP) tuples."""
        normalized: List[Tuple[int, int, int]] = []
        if not qp_pairs:
            raise ValueError("qp_pairs cannot be empty")

        for idx, pair in enumerate(qp_pairs):
            if isinstance(pair, dict):
                if "occupancyMapQP" not in pair or "geometryQP" not in pair or "attributeQP" not in pair:
                    raise ValueError(
                        f"qp_pairs[{idx}] missing occupancyMapQP and/or geometryQP and/or attributeQP"
                    )
                occ = int(pair["occupancyMapQP"])
                geo = int(pair["geometryQP"])
                attr = int(pair["attributeQP"])
            elif isinstance(pair, (list, tuple)):
                if len(pair) != 3:
                    raise ValueError(f"qp_pairs[{idx}] must have exactly three entries (occ, geo, attr)")
                occ, geo, attr = pair
                occ, geo, attr = int(occ), int(geo), int(attr)
            else:
                raise ValueError(
                    f"qp_pairs[{idx}] must be dict or (occ, geo, attr) tuple/list, got {type(pair).__name__}"
                )

            normalized.append((occ, geo, attr))
        return normalized

    def encode_tiles_with_qp_pairs(
        self,
        tiles_root: str,
        qp_pairs: List[Any],
        base_params: Optional[Dict[str, Any]] = None,
        host_output_path: Optional[str] = None,
        max_parallel_encodes: int = 1,
        threads_per_instance: Optional[int] = None,
        log_file: Optional[str] = None,
        encoding_logs_dir: Optional[str] = None,
        stop_event: Optional["threading.Event"] = None,
    ):
        """
        Encode every tile folder under tiles_root once per (occupancyMapQP, geometryQP, attributeQP) tuple.

        tiles_root: path containing tile_* directories with .ply frames
        qp_pairs: iterable of {"occupancyMapQP": int, "geometryQP": int, "attributeQP": int}
                  or (occ, geo, attr) tuples
        base_params: optional encoder params applied to every run
        host_output_path: where bitstreams are written; defaults to ./bitstreams
        max_parallel_encodes: maximum concurrent TMC2 instances
        threads_per_instance: nbThread to pass to each TMC2 run
        log_file: optional log file path for per-task logging
        encoding_logs_dir: optional folder to store per-task logs
        stop_event: optional threading.Event to abort outstanding tasks
        """
        base_params = base_params.copy() if base_params else {}
        normalized_pairs = self._normalize_qp_pairs(qp_pairs)

        tiles_dir = Path(tiles_root)
        if not tiles_dir.exists():
            raise FileNotFoundError(f"tiles_root does not exist: {tiles_root}")

        output_root = Path(host_output_path or "./bitstreams")
        output_root.mkdir(parents=True, exist_ok=True)
        enc_log_root = Path(encoding_logs_dir) if encoding_logs_dir else None
        if enc_log_root:
            enc_log_root.mkdir(parents=True, exist_ok=True)

        if self._cancel_requested(stop_event):
            self._log("[WARNING] Encoding cancelled before start; exiting.")
            return

        tile_dirs = sorted([p for p in tiles_dir.iterdir() if p.is_dir()])
        if not tile_dirs:
            raise ValueError(f"No tile directories found in {tiles_root}")

        tile_infos = []
        for tile_dir in tile_dirs:
            try:
                pattern, start_frame, frame_count = self._derive_frame_sequence(tile_dir)
            except ValueError as e:
                raise ValueError(f"Tile {tile_dir} is invalid: {e}") from e

            tile_params = base_params.copy()
            if threads_per_instance:
                tile_params["nbThread"] = threads_per_instance
            tile_params.setdefault("uncompressedDataPath", pattern)
            tile_params.setdefault("startFrameNumber", start_frame)
            tile_params.setdefault("frameCount", frame_count)
            tile_infos.append({"dir": tile_dir, "params": tile_params})

        tasks: List[Tuple[int, Path, Dict[str, Any], Tuple[int, int, int]]] = []
        for qp_idx, (occ_qp, geo_qp, attr_qp) in enumerate(normalized_pairs):
            for info in tile_infos:
                tasks.append((qp_idx, info["dir"], info["params"], (occ_qp, geo_qp, attr_qp)))

        total_tasks = len(tasks)
        completed_tasks = 0
        progress_lock = threading.Lock()
        if log_file:
            with open(log_file, "a") as lf:
                lf.write(f"[INFO] Encoding {total_tasks} task(s): {len(tile_infos)} tiles x {len(normalized_pairs)} qp sets\n")

        def encode_task(task):
            nonlocal completed_tasks
            if self._cancel_requested(stop_event):
                return
            qp_idx, tile_dir, tile_params, (occ_qp, geo_qp, attr_qp) = task
            params_with_qp = tile_params.copy()
            params_with_qp["occupancyMapQP"] = occ_qp
            params_with_qp["geometryQP"] = geo_qp
            params_with_qp["attributeQP"] = attr_qp

            bitstream_name = f"{tiles_dir.name}_{tile_dir.name}_occ{occ_qp}_geo{geo_qp}_attr{attr_qp}.bin"
            self._log(
                f"[INFO] Encoding tile={tile_dir.name} rep={qp_idx} (occQP={occ_qp}, geoQP={geo_qp}, attrQP={attr_qp}) -> {bitstream_name}"
            )
            task_log_path = None
            if enc_log_root:
                task_log_path = enc_log_root / f"{tile_dir.name}_rep{qp_idx}_occ{occ_qp}_geo{geo_qp}_attr{attr_qp}.log"
            self.run(
                params_with_qp,
                host_input_path=str(tile_dir),
                host_output_path=str(output_root),
                compressed_stream_filename=bitstream_name,
                task_log_file=str(task_log_path) if task_log_path else None,
                stop_event=stop_event
            )
            if log_file and not (stop_event and stop_event.is_set()):
                with progress_lock:
                    completed_tasks += 1
                    pct = (completed_tasks / total_tasks) * 100 if total_tasks else 100.0
                    lines = [
                        f"[INFO] Completed tile={tile_dir.name} rep={qp_idx} ({occ_qp}/{geo_qp}/{attr_qp})",
                        f"[INFO] Encoding progress: {completed_tasks}/{total_tasks} task(s) ({pct:.1f}%)"
                    ]
                with open(log_file, "a") as lf:
                    for line in lines:
                        lf.write(line + "\n")

        if max_parallel_encodes <= 1:
            for t in tasks:
                if self._cancel_requested(stop_event):
                    self._log("[WARNING] Stop requested; aborting remaining encoding tasks.")
                    break
                encode_task(t)
            return

        next_task_idx = 0
        in_flight = set()

        executor = concurrent.futures.ThreadPoolExecutor(max_workers=max_parallel_encodes)
        cancelled = False

        try:
            def launch_available():
                nonlocal next_task_idx
                while (
                    not self._cancel_requested(stop_event)
                    and len(in_flight) < max_parallel_encodes
                    and next_task_idx < len(tasks)
                ):
                    fut = executor.submit(encode_task, tasks[next_task_idx])
                    in_flight.add(fut)
                    next_task_idx += 1

            launch_available()
            while in_flight:
                if self._cancel_requested(stop_event):
                    cancelled = True
                    self._log("[WARNING] Stop requested; waiting for in-flight encoding tasks to terminate.")
                    TMC2EncoderRunner.cancel_all()
                    for fut in list(in_flight):
                        fut.cancel()
                    break
                done, _ = concurrent.futures.wait(
                    in_flight,
                    return_when=concurrent.futures.FIRST_COMPLETED,
                    timeout=0.5
                )
                in_flight.difference_update(done)
                launch_available()
        finally:
            if cancelled or self._cancel_requested(stop_event):
                self._log("[INFO] Waiting for encoder worker threads to exit...")
                executor.shutdown(wait=True, cancel_futures=True)
                self._log("[INFO] Encoder worker threads stopped.")
            else:
                executor.shutdown(wait=True)



if __name__ == "__main__":
    encoder = TMC2EncoderRunner(log_file="tmc2_encoding.log")

    user_params = {
        "nbThread": 8,
        "startFrameNumber": 0,
        "frameCount": 250,
        "geometry3dCoordinatesBitdepth": 9,
        "uncompressedDataFolder": "/smb/shared/Datasets/BlueBackpack_vox9/BlueBackpack/ply_vox9/ply_xyz_rgb",
        "uncompressedDataPath": "BlueBackpack_UVG_vox9_25_0_250_%04d.ply",
        "compressedStreamPath": "./bitstreams/bluebackpack.bin"
    }

    encoder.run(
        user_params,
        host_input_path="/smb/shared/Datasets/BlueBackpack_vox9/BlueBackpack/ply_vox9/ply_xyz_rgb",
        host_output_path="./bitstreams"
    )


#BlueBackpack_UVG_vox9_25_0_250_0000
#longdress_vox10_1051

#occupancyMapQP
#geometryQP
