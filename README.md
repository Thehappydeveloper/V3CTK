# V3CTK - V3C Toolkit (MMSys 2026)

V3CTK prepares tiled V3C point-cloud content for streaming:
1. tile PLY frames,
2. encode each tile with MPEG TMC2 in Docker,
3. segment encoded V3C streams,
4. generate a DASH MPD.

This repo provides both a CLI pipeline and a web UI.

## Platform Support

- Linux: recommended and tested path.
- Windows: use WSL2 (Ubuntu recommended). Do not run this pipeline natively in Windows Python.
  - Run all commands inside WSL.
  - Use Docker Desktop with WSL integration enabled.
  - Prefer storing working data inside the WSL filesystem (not `/mnt/c`) for performance.
  - You can still open the UI in Windows at `http://localhost:8080` after starting `server.py` in WSL.

## Requirements

- Python 3.9+ (for this branch, the shipped segmenter binary targets CPython 3.12 on Linux x86_64)
- Docker (daemon running)
- Git
- Python dependencies:

```bash
pip install -r requirements.txt
```

First encoder run will clone and build TMC2 in Docker, which can take several minutes.

## Segmenter Module (Compiled Only)

`src.segmenter` is shipped as a compiled extension:

- `src/segmenter.cpython-312-x86_64-linux-gnu.so`

Plain Python/Cython sources for segmenter are intentionally not tracked.

Important:
- The `.so` is Python/ABI specific. If your Python version does not match, `import src.segmenter` can fail.
- Rebuilding requires source that is not tracked in this branch.

## Linux / WSL Quick Start

```bash
git clone <repo-url>
cd v3ctk
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python -m src.main \
  --project-name longdress \
  --folder /path/to/plys \
  --segment-size 16 \
  --encoder-gof 16 \
  --n-x 2 --n-y 3 --n-z 1 \
  --encoding-parallelism 4 \
  --encoding-threads-per-instance 1 \
  --qp-pairs 24:32:43
```

## Input Requirements

- Input folder must contain `.ply` frames.
- Filenames should include a numeric frame index (for example `sequence_0000.ply`).
- If filename contains `vox<bitdepth>` (for example `vox10`), vox can be inferred; otherwise pass `--vox`.

## CLI Behavior and Validation

Entry point: `python -m src.main`

Core options:
- `--segment-size` frames per segment.
- `--encoder-gof` frames per encoder GoF.
- `--n-x --n-y --n-z` tiling grid.
- `--encoding-parallelism` total thread cap budget.
- `--encoding-threads-per-instance` encoder threads per process.
- `--qp-pairs` comma-separated `occ:geo:attr` triplets.

Runtime rules:
- `segment-size > 0`
- `encoder-gof > 0`
- `segment-size % encoder-gof == 0`
- `n-x, n-y, n-z > 0`
- `frame-rate > 0`
- `frame-count > 0` if provided
- `start-frame-number >= 0` if provided
- `vox > 0` if provided
- `qp-pairs` must include at least one triplet and each QP must be non-negative

Thread model:
- If `--encoding-threads-per-instance` is omitted, value is `1`.
- If `--encoding-threads-per-instance > --encoding-parallelism`, it is capped to `--encoding-parallelism` (warning logged).
- Max concurrent encodes is derived as:
  - `floor(encoding-parallelism / encoding-threads-per-instance)` (minimum 1).
- Tiling worker count uses `encoding-parallelism`.

Useful stage flags:
- `--skip-tiling`
- `--skip-encoding`
- `--skip-segmentation`
- `--skip-mpd`

## Web UI

Start server:

```bash
python server.py
```

Open:

```text
http://localhost:8080
```

Notes:
- Server binds to `0.0.0.0:8080`.
- UI validates key numeric inputs before launch.
- UI launches the same CLI pipeline under the hood.

## Output Layout

Outputs are grouped by project name:

```text
<logs-dir>/<project>/<timestamp>/
  tiling/
  encoding/
  segmentation/
  mpd/

<tiles-output>/<project>/
  tile_0/ tile_1/ ...
  tile_boundaries.json

<encoder-output>/<project>/
  <project>_tile_0_occ24_geo32_attr43.bin
  ...

<v3c-output>/<project>/
  <bitstream-stem>/
    atlas/ occp/ geom/ attr/   (or combined/)
      init.bin
      segment_0001.bin
      ...
  <project>.mpd
```

Default base directories:
- `--tiles-output`: `output/tiles`
- `--encoder-output`: `output/encoded`
- `--logs-dir`: `output/logs`
- `--v3c-output`: `output/v3c`

## Optional: Re-multiplex Split Components

```bash
python multiplexer.py \
  --input-root output/v3c/<project>/<bitstream_stem> \
  --output-root output/v3c/<project>/<bitstream_stem>_combined
```

## Troubleshooting

- `No PLY frames found`: check `--folder`.
- `Vox bitdepth could not be inferred`: pass `--vox`.
- Segment-size / GoF error: ensure `segment-size % encoder-gof == 0`.
- Docker errors: confirm daemon is running (`docker ps`).
- Import error for `src.segmenter`: your Python ABI may not match the shipped `.so`.

## Repository Structure

```text
src/
  main.py
  tile_generator.py
  encoder/
  segmenter.cpython-312-x86_64-linux-gnu.so
  mpd_generator.py
web/
  index.html
  app.js
server.py
multiplexer.py
```

## License

V3CTK uses a **dual-licensing model**:

- **GPLv3 (open-source license):** default public license for this repository.
- **Commercial license:** available for proprietary/closed-source use.

Research and academic users can use, modify, and redistribute under GPLv3.
Organizations that need to use V3CTK in proprietary workflows, products, or closed distributions must obtain a commercial license.

For commercial licensing, contact: `jeremyouellette05@gmail.com`

See [LICENSE](LICENSE) for GPLv3 terms and [COMMERCIAL-LICENSE.md](COMMERCIAL-LICENSE.md) for commercial licensing reference.
Contributor and governance rules are in [CONTRIBUTING.md](CONTRIBUTING.md), [CLA.md](CLA.md), and [docs/license-compliance-checklist.md](docs/license-compliance-checklist.md).

## ACM Reference Format

```text
Jérémy Ouellette, Jashanjot Singh Sidhu, and Abdelhak Bentaleb. 2026.
V3CTK: An End-to-End V3C Content Preparation Toolkit for Tiled Dynamic Point Cloud Streaming.
In Proceedings of the 17th ACM Multimedia Systems Conference (MMSys ’26),
Hong Kong, Hong Kong. Association for Computing Machinery,
New York, NY, USA, 6 pages.
https://doi.org/10.1145/3793853.3799816
```

### BibTeX

```bibtex
@inproceedings{ouellette2026v3ctk,
  year = {2026},
  author    = {Ouellette, J{\'e}r{\'e}my and Sidhu, Jashanjot Singh and Bentaleb, Abdelhak},
  title     = {V3CTK: An End-to-End V3C Content Preparation Toolkit for Tiled Dynamic Point Cloud Streaming},
  publisher = {Association for Computing Machinery},
  address = {New York, NY, USA},
  url = {https://doi.org/10.1145/3793853.3799816},
  doi       = {10.1145/3793853.3799816},
  booktitle = {Proceedings of the 17th ACM Multimedia Systems Conference},
  numpages = {6},
  location = {Hong Kong, Hong Kong},
  series = {MMSys '26}
}
```
