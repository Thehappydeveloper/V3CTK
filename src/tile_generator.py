import logging
import re
import json
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor
from itertools import repeat
import numpy as np
import pandas as pd
from tqdm import tqdm

from src.tile_io import PlyIO

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

_FRAME_NUM_RE = re.compile(r"(\d+)(?=\.[^.]+$)")


BLUE = "\033[94m"
RESET = "\033[0m"


def _append_log(log_file, text):
    """Append text to a log file safely."""
    try:
        with open(log_file, "a") as f:
            f.write(text + "\n")
    except Exception as e:
        logger.error(f"Failed writing to log file {log_file}: {e}")


def _process_frame(frame_path, out_dir, boundaries_meta, cols, cx, cy, cz, total_tiles, log_file):
    """Worker function to process a single frame in a segment."""
    try:
        df = PlyIO().read(frame_path)
    except Exception as e:
        _append_log(log_file, f"[ERROR] Failed reading frame {frame_path}: {e}")
        return frame_path, False, f"Failed reading frame: {e}"

    # Assign tiles
    ix = np.floor((df["x"] - boundaries_meta["x_min"]) / boundaries_meta["dx"]).astype(int)
    iy = np.floor((df["y"] - boundaries_meta["y_min"]) / boundaries_meta["dy"]).astype(int)
    iz = np.floor((df["z"] - boundaries_meta["z_min"]) / boundaries_meta["dz"]).astype(int)

    ix = np.clip(ix, 0, boundaries_meta["nx"] - 1)
    iy = np.clip(iy, 0, boundaries_meta["ny"] - 1)
    iz = np.clip(iz, 0, boundaries_meta["nz"] - 1)

    tile_ids = ix * (boundaries_meta["ny"] * boundaries_meta["nz"]) + iy * boundaries_meta["nz"] + iz
    frame_name = Path(frame_path).stem

    for t in range(total_tiles):
        tile_df = df[tile_ids == t].copy()
        tile_folder = Path(out_dir) / f"tile_{t}"
        tile_folder.mkdir(parents=True, exist_ok=True)
        output_path = tile_folder / f"{frame_name}.ply"

        if tile_df.empty:
            dummy_vals = []
            for c in cols:
                if c == "x":
                    dummy_vals.append(float(cx))
                elif c == "y":
                    dummy_vals.append(float(cy))
                elif c == "z":
                    dummy_vals.append(float(cz))
                else:
                    dummy_vals.append(0)
            tile_df = pd.DataFrame([dummy_vals], columns=cols)

        PlyIO().write(str(output_path), tile_df)

    return frame_path, True, None


class TileGenerator:
    """Memory-efficient multiprocessing TileProcessor for dynamic-per-segment tiling."""

    def __init__(self, n_x=1, n_y=1, n_z=1, segment_size=10, output_dir="tiles", threads=4):
        self.n_x = max(1, int(n_x))
        self.n_y = max(1, int(n_y))
        self.n_z = max(1, int(n_z))
        self.segment_size = max(1, int(segment_size))
        self.out_dir = Path(output_dir)
        self.out_dir.mkdir(parents=True, exist_ok=True)
        self.threads = max(1, int(threads))

    def _compute_segment_boundaries(self, frame_paths):
        """Compute union bounding box and centroid incrementally to save memory."""
        x_min = y_min = z_min = float("inf")
        x_max = y_max = z_max = float("-inf")
        cx_sum = cy_sum = cz_sum = 0.0
        n_total = 0
        cols = None

        for fp in frame_paths:
            try:
                df = PlyIO().read(fp)
            except Exception as e:
                logger.warning(f"Could not read frame {fp}: {e}")
                continue

            if df.empty:
                continue

            if cols is None:
                cols = df.columns

            x_min = min(x_min, df["x"].min())
            y_min = min(y_min, df["y"].min())
            z_min = min(z_min, df["z"].min())

            x_max = max(x_max, df["x"].max())
            y_max = max(y_max, df["y"].max())
            z_max = max(z_max, df["z"].max())

            n = len(df)
            n_total += n
            cx_sum += df["x"].sum()
            cy_sum += df["y"].sum()
            cz_sum += df["z"].sum()

        if n_total == 0 or cols is None:
            return None

        cx, cy, cz = cx_sum / n_total, cy_sum / n_total, cz_sum / n_total

        dx = max((x_max - x_min) / self.n_x, 1e-9)
        dy = max((y_max - y_min) / self.n_y, 1e-9)
        dz = max((z_max - z_min) / self.n_z, 1e-9)

        total_tiles = self.n_x * self.n_y * self.n_z

        boundaries_meta = {
            "x_min": x_min, "y_min": y_min, "z_min": z_min,
            "dx": dx, "dy": dy, "dz": dz,
            "nx": self.n_x, "ny": self.n_y, "nz": self.n_z
        }
        return boundaries_meta, total_tiles, cols, cx, cy, cz

    def generate_tiles_from_frames(self, frame_paths, log_file_path, progress_log_file=None, stop_event=None):
        """
        Process frames in segments using multiprocessing.
        Logs and errors are appended to log_file_path.
        progress_log_file: optional secondary log (e.g., unified run log) for coarse progress updates.
        stop_event: optional threading.Event to gracefully abort remaining work.
        """
        log_file = Path(log_file_path)
        log_file.parent.mkdir(parents=True, exist_ok=True)

        # --- Write header ---
        header = (
            "\n\n"
            f"{BLUE}==============================================\n"
            f"      TILE PROCESSOR SEGMENT (PLY → TILES)\n"
            f"=============================================={RESET}\n"
        )
        _append_log(log_file, header)
        if progress_log_file:
            _append_log(progress_log_file, "[INFO] Tiling started")

        frame_paths = list(frame_paths)
        if not frame_paths:
            _append_log(log_file, "[ERROR] No frame paths provided.")
            return

        frame_paths = sorted(frame_paths, key=lambda fp: int(_FRAME_NUM_RE.search(Path(fp).name).group(1)) if _FRAME_NUM_RE.search(Path(fp).name) else float("inf"))
        _append_log(log_file, f"[INFO] Tiling {len(frame_paths)} frames with grid {self.n_x}x{self.n_y}x{self.n_z} (segment size={self.segment_size})")
        total_frames = len(frame_paths)
        total_segments = (total_frames + self.segment_size - 1) // self.segment_size
        processed_frames = 0
        boundaries_per_segment = {}

        for i in range(0, len(frame_paths), self.segment_size):
            if stop_event and stop_event.is_set():
                _append_log(log_file, "[WARNING] Stop requested during tiling; exiting early.")
                if progress_log_file:
                    _append_log(progress_log_file, "[WARNING] Stop requested during tiling; exiting early.")
                break

            seg_frames = frame_paths[i:i + self.segment_size]
            _append_log(log_file, f"[INFO] Segment {i // self.segment_size + 1}: processing {len(seg_frames)} frames ({seg_frames[0]} … {seg_frames[-1]})")
            boundaries_data = self._compute_segment_boundaries(seg_frames)

            if boundaries_data is None:
                msg = f"[WARNING] No readable frames in segment starting at {seg_frames[0]}"
                logger.warning(msg)
                _append_log(log_file, msg)
                continue

            boundaries_meta, total_tiles, cols, cx, cy, cz = boundaries_data
            seg_idx = (i // self.segment_size) + 1

            # Save per-segment tile boundaries for MPD EventStream
            tile_bounds = []
            for xi in range(self.n_x):
                for yi in range(self.n_y):
                    for zi in range(self.n_z):
                        tid = xi * (self.n_y * self.n_z) + yi * self.n_z + zi
                        xmin = boundaries_meta["x_min"] + xi * boundaries_meta["dx"]
                        xmax = xmin + boundaries_meta["dx"]
                        ymin = boundaries_meta["y_min"] + yi * boundaries_meta["dy"]
                        ymax = ymin + boundaries_meta["dy"]
                        zmin = boundaries_meta["z_min"] + zi * boundaries_meta["dz"]
                        zmax = zmin + boundaries_meta["dz"]
                        tile_bounds.append({
                            "id": tid,
                            "xmin": xmin,
                            "xmax": xmax,
                            "ymin": ymin,
                            "ymax": ymax,
                            "zmin": zmin,
                            "zmax": zmax,
                        })
            boundaries_per_segment[str(seg_idx)] = tile_bounds

            # --- Multiprocessing ---
            with ProcessPoolExecutor(max_workers=self.threads) as exe:
                results = exe.map(
                    _process_frame,
                    seg_frames,
                    repeat(self.out_dir),
                    repeat(boundaries_meta),
                    repeat(cols),
                    repeat(cx),
                    repeat(cy),
                    repeat(cz),
                    repeat(total_tiles),
                    repeat(log_file)
                )

                for fp, success, err in tqdm(results, total=len(seg_frames),
                                             desc=f"Processing segment {i // self.segment_size + 1}"):

                    if not success:
                        msg = f"[ERROR] Failed frame {fp}: {err}"
                        logger.error(msg)
                        _append_log(log_file, msg)
                    else:
                        _append_log(log_file, f"[INFO] Finished frame {fp}")

            processed_frames += len(seg_frames)
            if progress_log_file:
                pct = (processed_frames / total_frames) * 100
                _append_log(progress_log_file, f"[INFO] Tiling progress: {processed_frames}/{total_frames} frames ({pct:.1f}%) after segment {seg_idx}/{total_segments}")

        _append_log(log_file, f"{BLUE}--- Tile generation completed successfully ---{RESET}")
        if progress_log_file and not (stop_event and stop_event.is_set()):
            _append_log(progress_log_file, "[INFO] Tiling completed")

        # Write boundaries JSON for MPD EventStream consumption
        if boundaries_per_segment:
            boundaries_path = self.out_dir / "tile_boundaries.json"
            try:
                with open(boundaries_path, "w", encoding="utf-8") as f:
                    json.dump(boundaries_per_segment, f, indent=2)
                _append_log(log_file, f"[INFO] Wrote tile boundaries for {len(boundaries_per_segment)} segment(s): {boundaries_path}")
            except Exception as e:
                _append_log(log_file, f"[WARNING] Failed to write tile boundaries JSON: {e}")
