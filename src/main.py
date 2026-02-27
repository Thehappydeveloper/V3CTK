import argparse
import os
import re
from datetime import datetime
import threading
import signal
from pathlib import Path

from src.encoder.tmc2_encoder import TMC2EncoderRunner
from src.mpd_generator import V3CMPDBuilder
from src.tile_generator import TileGenerator
from src.utils import extract_metadata_from_filename
from src.segmenter import V3CSegmenter


_FRAME_NUM_RE = re.compile(r"(\d+)(?=\.ply$)")


def parse_args():
    parser = argparse.ArgumentParser(description="V3CTK pipeline runner")
    parser.add_argument("--project-name", default="default_project")
    parser.add_argument("--folder", default="/smb/shared/Datasets/8i/longdress/longdress/Ply")
    parser.add_argument("--n-x", type=int, default=2)
    parser.add_argument("--n-y", type=int, default=3)
    parser.add_argument("--n-z", type=int, default=1)
    parser.add_argument(
        "--segment-size",
        type=int,
        default=16,
        help="Frames per segment (used for tiling and segmentation). Must be a multiple of --encoder-gof.",
    )
    parser.add_argument(
        "--encoder-gof",
        dest="encoder_gof",
        type=int,
        default=16,
        help="Encoder GoF size (frames per AD unit); output segments are aligned to this.",
    )
    parser.add_argument(
        "--seg-frames-per-ad",  # deprecated alias for backward compatibility
        dest="encoder_gof",
        type=int,
        help=argparse.SUPPRESS,
    )
    parser.add_argument(
        "--frame-rate",
        type=float,
        default=30.0,
        help="Input frame rate (fps); used to derive segment durations and MPD timing metadata",
    )
    parser.add_argument("--seg-split-components", action="store_true", default=True, help="Split segments into atlas/occp/geom/attr streams")
    parser.add_argument("--no-seg-split-components", dest="seg_split_components", action="store_false", help="Do not split segments into components")
    parser.add_argument(
        "--encoding-parallelism",
        type=int,
        default=1,
        help="Total thread cap for tiling and encoding"
    )
    parser.add_argument(
        "--encoding-threads-per-instance",
        type=int,
        help="Threads per TMC2 instance (1, capped by --encoding-parallelism)"
    )
    parser.add_argument("--vox", type=int, help="Geometry bitdepth override")
    parser.add_argument("--tiles-output", default="output/tiles")
    parser.add_argument("--encoder-output", default="output/encoded")
    parser.add_argument("--logs-dir", default="output/logs")
    parser.add_argument("--v3c-output", default="output/v3c")
    parser.add_argument("--start-frame-number", type=int, help="Override start frame number for encoding")
    parser.add_argument("--frame-count", type=int, help="Override frame count for encoding")
    parser.add_argument(
        "--qp-pairs",
        default="24:32:43",
        help="Comma-separated occ:geo:attr triplets, e.g., 24:32:43,28:36:45"
    )
    parser.add_argument("--log-file", help="Optional explicit log file path")
    parser.add_argument("--skip-tiling", action="store_true", help="Skip tiling step")
    parser.add_argument("--skip-segmentation", action="store_true", help="Skip segmentation step (placeholder)")
    parser.add_argument("--skip-encoding", action="store_true", help="Skip encoding step")
    parser.add_argument("--skip-mpd", action="store_true", help="Skip MPD generation")
    return parser.parse_args()


def log_and_raise(message, log_file=None):
    if log_file:
        with open(log_file, "a") as f:
            f.write(message + "\n")
    raise RuntimeError(message)


def log_line(log_file, message):
    if not log_file:
        return
    with open(log_file, "a") as f:
        f.write(message + "\n")


def log_to_files(primary: Path | str | None, secondary: Path | str | None, message: str):
    """Append a message to up to two log files (stage + unified)."""
    for path in (primary, secondary):
        if not path:
            continue
        with open(path, "a") as f:
            f.write(message + "\n")


def init_log_dir(base_dir: Path, project_name: Path, log_file_override: str = None):
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    run_dir = base_dir / project_name / timestamp
    run_dir.mkdir(parents=True, exist_ok=True)
    tiling_dir = run_dir / "tiling"
    encoding_dir = run_dir / "encoding"
    segmentation_dir = run_dir / "segmentation"
    mpd_dir = run_dir / "mpd"
    tiling_dir.mkdir(exist_ok=True)
    encoding_dir.mkdir(exist_ok=True)
    segmentation_dir.mkdir(exist_ok=True)
    mpd_dir.mkdir(exist_ok=True)
    if log_file_override:
        run_log = Path(log_file_override)
        run_log.parent.mkdir(parents=True, exist_ok=True)
    else:
        run_log = run_dir / "run.log"
    with open(run_log, "a") as f:
        f.write("\n\n")
        f.write(f"=============================================\n")
        f.write(f" V3CTK PIPELINE LOG — {datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}\n")
        f.write(f"=============================================\n\n")
    print(f"[INFO] Log folder created: {run_dir}")
    return run_dir, run_log, tiling_dir, encoding_dir, segmentation_dir, mpd_dir


def derive_uncompressed_pattern(frame_name):
    match = _FRAME_NUM_RE.search(frame_name)
    if not match:
        return frame_name
    digits = len(match.group(1))
    return f"{frame_name[:match.start(1)]}%0{digits}d{frame_name[match.end(1):]}"


def extract_frame_number(frame_path):
    match = _FRAME_NUM_RE.search(Path(frame_path).name)
    return int(match.group(1)) if match else None


def parse_qp_pairs(value):
    pairs = []
    for item in value.split(","):
        item = item.strip()
        if not item:
            continue
        parts = item.split(":")
        if len(parts) != 3:
            raise ValueError(f"Invalid qp group '{item}', expected occ:geo:attr")
        occ, geo, attr = parts
        pairs.append((int(occ), int(geo), int(attr)))
    if not pairs:
        raise ValueError("qp_pairs cannot be empty")
    return pairs


def validate_qp_pairs(pairs, log_file: Path | None):
    if not pairs:
        log_and_raise("qp_pairs cannot be empty.", log_file)
    for occ, geo, attr in pairs:
        if occ < 0 or geo < 0 or attr < 0:
            log_and_raise(
                f"Invalid qp triplet ({occ}:{geo}:{attr}); QP values must be non-negative.",
                log_file,
            )


def resolve_vox(cli_vox, frame_paths, log_file):
    if cli_vox is not None:
        return cli_vox
    parsed = extract_metadata_from_filename(frame_paths[0])
    vox = parsed.get("vox")
    if vox is None:
        log_and_raise("Vox bitdepth could not be inferred; provide --vox.", log_file)
    return int(vox)


def validate_segment_settings(segment_size: int, encoder_gof: int, log_file: Path | None):
    if segment_size <= 0:
        log_and_raise("segment-size must be positive.", log_file)
    if encoder_gof <= 0:
        log_and_raise("encoder-gof must be positive.", log_file)
    if segment_size % encoder_gof != 0:
        warn = (
            f"segment-size ({segment_size}) must be a multiple of encoder-gof ({encoder_gof}). "
            "Adjust one of the values to proceed."
        )
        log_line(log_file, f"[WARNING] {warn}")
        log_and_raise(f"[ERROR] {warn}", log_file)
    if segment_size == encoder_gof:
        log_line(log_file, "[INFO] Segment size matches encoder GoF (one GoF per segment).")
    else:
        gofs_per_segment = segment_size // encoder_gof
        log_line(
            log_file,
            f"[INFO] Segment size validated: {segment_size} frames -> {gofs_per_segment} GoF(s) at encoder GoF {encoder_gof}.",
        )


def validate_tiling_settings(n_x: int, n_y: int, n_z: int, log_file: Path | None):
    if n_x <= 0 or n_y <= 0 or n_z <= 0:
        log_and_raise("n-x, n-y, and n-z must all be positive integers.", log_file)


def validate_runtime_settings(
    frame_rate: float,
    frame_count: int | None,
    start_frame_number: int | None,
    vox: int | None,
    log_file: Path | None,
):
    if frame_rate <= 0:
        log_and_raise("frame-rate must be positive.", log_file)
    if frame_count is not None and frame_count <= 0:
        log_and_raise("frame-count must be positive when provided.", log_file)
    if start_frame_number is not None and start_frame_number < 0:
        log_and_raise("start-frame-number must be >= 0 when provided.", log_file)
    if vox is not None and vox <= 0:
        log_and_raise("vox must be positive when provided.", log_file)


def validate_parallel_settings(encoding_parallelism: int, log_file: Path | None):
    if encoding_parallelism <= 0:
        log_and_raise("encoding-parallelism must be positive.", log_file)


def resolve_threads_per_instance(
    encoding_parallelism: int,
    encoding_threads_per_instance: int | None,
    log_file: Path | None,
) -> int:
    if encoding_threads_per_instance is None:
        return 1
    if encoding_threads_per_instance <= 0:
        log_and_raise("encoding-threads-per-instance must be positive.", log_file)
    if encoding_threads_per_instance > encoding_parallelism:
        log_line(
            log_file,
            f"[WARNING] Capping encoding-threads-per-instance to {encoding_parallelism} "
            f"(requested {encoding_threads_per_instance})",
        )
        return encoding_parallelism
    return encoding_threads_per_instance


def log_config_summary(
    args,
    qp_pairs,
    log_file,
    threads_per_instance: int,
    max_parallel_encodes: int,
):
    summary_lines = [
        "[CONFIG] Tiling grid: "
        f"{args.n_x}x{args.n_y}x{args.n_z}, segment-size={args.segment_size} frames (shared tiling/segmentation)",
        f"[CONFIG] Encoder GoF: {args.encoder_gof} frames; split components: {'yes' if args.seg_split_components else 'no'}",
        f"[CONFIG] Frame rate: {args.frame_rate} fps",
        f"[CONFIG] Thread cap (tiling+encoding): {args.encoding_parallelism}",
        f"[CONFIG] Threads per encoder instance: {threads_per_instance}",
        f"[CONFIG] Max concurrent encodes from cap: {max_parallel_encodes}",
        f"[CONFIG] QP sets: {len(qp_pairs)} ({', '.join(f'occ{occ}/geo{geo}/attr{attr}' for occ, geo, attr in qp_pairs)})",
        f"[CONFIG] Input folder: {args.folder}",
    ]
    for line in summary_lines:
        print(line)
        log_line(log_file, line)


def main():
    args = parse_args()
    project_name = Path(args.project_name)
    tiles_output = Path(args.tiles_output) / project_name
    encoder_output = Path(args.encoder_output) / project_name
    log_output = Path(args.logs_dir)
    v3c_stream_output = Path(args.v3c_output)
    segmented_output = v3c_stream_output / project_name
    mpd_output_dir = segmented_output  # place MPD alongside its content

    for p in (tiles_output, encoder_output, log_output, v3c_stream_output, mpd_output_dir, segmented_output):
        p.mkdir(parents=True, exist_ok=True)

    run_dir, log_file, tiling_log_dir, encoding_log_dir, segmentation_log_dir, mpd_log_dir = init_log_dir(log_output, project_name, args.log_file)

    validate_segment_settings(args.segment_size, args.encoder_gof, log_file)
    validate_tiling_settings(args.n_x, args.n_y, args.n_z, log_file)
    validate_runtime_settings(
        frame_rate=args.frame_rate,
        frame_count=args.frame_count,
        start_frame_number=args.start_frame_number,
        vox=args.vox,
        log_file=log_file,
    )
    validate_parallel_settings(args.encoding_parallelism, log_file)
    threads_per_instance = resolve_threads_per_instance(
        args.encoding_parallelism,
        args.encoding_threads_per_instance,
        log_file,
    )
    max_parallel_encodes = max(1, args.encoding_parallelism // threads_per_instance)

    try:
        qp_pairs = parse_qp_pairs(args.qp_pairs)
    except ValueError as e:
        log_and_raise(str(e), log_file)
    validate_qp_pairs(qp_pairs, log_file)

    stop_event = threading.Event()

    def handle_term(signum, frame):
        stop_event.set()
        log_line(log_file, f"[WARNING] Termination signal ({signum}) received; stopping pipeline...")
        TMC2EncoderRunner.cancel_all()

    signal.signal(signal.SIGTERM, handle_term)
    signal.signal(signal.SIGINT, handle_term)

    folder = Path(args.folder)
    frame_paths = sorted(folder.glob("*.ply"))
    if not frame_paths:
        log_and_raise(f"No PLY frames found in {folder}", log_file)

    log_config_summary(args, qp_pairs, log_file, threads_per_instance, max_parallel_encodes)

    log_line(log_file, f"[INFO] Starting pipeline for project={args.project_name} with {len(frame_paths)} frames")

    vox = resolve_vox(args.vox, frame_paths, log_file)
    log_line(log_file, f"[INFO] Resolved vox: {vox}")

    if not args.skip_tiling:
        log_line(log_file, "[INFO] Starting tiling step")
        tiler = TileGenerator(
            n_x=args.n_x,
            n_y=args.n_y,
            n_z=args.n_z,
            segment_size=args.segment_size,
            output_dir=tiles_output,
            threads=args.encoding_parallelism,
        )
        tiler.generate_tiles_from_frames(
            frame_paths,
            tiling_log_dir / "tiling.log",
            progress_log_file=log_file,
            stop_event=stop_event
        )
        log_line(log_file, "[INFO] Finished tiling step")
    else:
        log_line(log_file, "[INFO] Skipping tiling step")

    start_frame_number = (
        args.start_frame_number
        if args.start_frame_number is not None
        else extract_frame_number(frame_paths[0])
    )
    if start_frame_number is None:
        log_and_raise("Cannot determine start frame number from filename.", log_file)

    if not args.skip_encoding:
        frame_count = args.frame_count if args.frame_count is not None else len(frame_paths)
        log_line(log_file, "[INFO] Starting encoding step")
        encoder = TMC2EncoderRunner(
            log_file=str(encoding_log_dir / "encoding.log"),
            secondary_log_file=str(log_file),
            stop_event=stop_event
        )

        base_params = {
            "nbThread": threads_per_instance,
            "geometry3dCoordinatesBitdepth": vox,
            "groupOfFramesSize": args.encoder_gof,
        }
        if args.start_frame_number is not None:
            base_params["startFrameNumber"] = args.start_frame_number
        if args.frame_count is not None:
            base_params["frameCount"] = args.frame_count

        effective_parallelism = max_parallel_encodes

        log_line(
            log_file,
            f"[INFO] Encoding tiles in {tiles_output} with {len(qp_pairs)} qp set(s) "
            f"using up to {effective_parallelism} parallel encoder(s)"
        )
        try:
            encoder.encode_tiles_with_qp_pairs(
                tiles_root=str(tiles_output),
                qp_pairs=qp_pairs,
                base_params=base_params,
                host_output_path=str(encoder_output),
                max_parallel_encodes=effective_parallelism,
                threads_per_instance=threads_per_instance,
                log_file=log_file,
                encoding_logs_dir=str(encoding_log_dir),
                stop_event=stop_event
            )
        finally:
            if stop_event.is_set():
                TMC2EncoderRunner.cancel_all()
        if stop_event.is_set():
            log_line(log_file, "[WARNING] Encoding aborted due to stop request")
        else:
            log_line(log_file, "[INFO] Finished encoding step")
    else:
        log_line(log_file, "[INFO] Skipping encoding step")

    if stop_event.is_set():
        log_line(log_file, "[WARNING] Segmentation skipped due to stop request")
        return

    if not args.skip_segmentation:
        log_line(log_file, "[INFO] Starting segmentation step")
        seg_stage_log = segmentation_log_dir / "segmentation.log"

        def seg_logger(msg: str):
            log_to_files(seg_stage_log, log_file, msg)

        segmenter = V3CSegmenter(
            segment_size=args.segment_size,
            frames_per_ad=args.encoder_gof,
            split_components=args.seg_split_components,
            log_func=seg_logger,
        )
        bin_files = sorted(encoder_output.glob("*.bin"))
        if not bin_files:
            log_line(log_file, "[WARNING] No .bin files found for segmentation")
        for bf in bin_files:
            out_dir = segmented_output / bf.stem
            try:
                segmenter.segment_file(bf, out_dir)
            except Exception as e:
                log_line(log_file, f"[ERROR] Segmentation failed for {bf}: {e}")
                stop_event.set()
                break
        if stop_event.is_set():
            log_line(log_file, "[WARNING] Segmentation aborted due to stop request")
            return
        log_line(log_file, "[INFO] Finished segmentation step")
    else:
        log_line(log_file, "[INFO] Skipping segmentation step")

    #TODO : Segment and decompose tiles bitstreams before MPD generation

    if stop_event.is_set():
        log_line(log_file, "[WARNING] MPD generation skipped due to stop request")
        return

    if not args.skip_mpd:
        log_line(log_file, "[INFO] Starting MPD generation")
        fps = args.frame_rate
        segment_duration_ms = max(1, round(args.segment_size * 1000 / fps))
        total_frames = args.frame_count or len(frame_paths)
        total_duration_ms = max(1, round(total_frames * 1000 / fps))
        mpd_path = mpd_output_dir / f"{project_name}.mpd"
        mpd_source = str(segmented_output if segmented_output.exists() else encoder_output)
        base_url_rel = os.path.relpath(mpd_source, mpd_path.parent)

        boundaries_json_path = None
        tile_boundaries_candidate = tiles_output / "tile_boundaries.json"
        if tile_boundaries_candidate.exists():
            boundaries_json_path = str(tile_boundaries_candidate)

        mpd_stage_log = mpd_log_dir / "mpd.log"
        log_to_files(mpd_stage_log, log_file, "[INFO] Preparing MPD builder")

        mpd_builder = V3CMPDBuilder(
            duration="PT10S",
            minBufferTime="PT1S",
            mpd_type="static",
            segment_duration_ms=segment_duration_ms,
            media_presentation_duration_ms=total_duration_ms,
            base_url=base_url_rel + "/",
        )

        mpd_builder.build_from_source(
            tiles_path=mpd_source,
            boundaries_json_path=boundaries_json_path,
            output_path=str(mpd_path),
            split_components=args.seg_split_components,
        )
        log_to_files(mpd_stage_log, log_file, f"[INFO] Finished MPD generation: {mpd_path}")
    else:
        log_line(log_file, "[INFO] Skipping MPD generation")

if __name__ == "__main__":
    main()
