#!/usr/bin/env python3
import argparse
from pathlib import Path


def parse_v3c_units(data: bytes):
    """Yield (unit_type, full_unit_bytes) from a V3C sample stream."""
    if not data:
        return
    header = data[0]
    size_len = (header >> 5) + 1
    offset = 1
    data_len = len(data)

    while offset + size_len <= data_len:
        size = int.from_bytes(data[offset:offset+size_len], "big")
        start = offset
        offset += size_len
        end = offset + size
        if end > data_len:
            raise ValueError("Truncated unit")
        payload = data[offset:end]
        unit_type = (payload[0] >> 3) & 0x1F
        yield unit_type, data[start:end]
        offset = end


def combine_per_segment(input_root: Path, output_root: Path):
    atlas = sorted((input_root / "atlas").glob("segment_*.bin"))
    occp  = sorted((input_root / "occp").glob("segment_*.bin"))
    geom  = sorted((input_root / "geom").glob("segment_*.bin"))
    attr  = sorted((input_root / "attr").glob("segment_*.bin"))

    if not (len(atlas) == len(occp) == len(geom) == len(attr)):
        raise RuntimeError("Segment count mismatch across tracks")

    output_root.mkdir(parents=True, exist_ok=True)

    n = len(atlas)
    print(f"[INFO] Found {n} segments")

    for i in range(n):
        seg_name = f"segment_{i+1:04d}.bin"
        print(f"[INFO] Combining {seg_name}")

        # Read bytes
        atlas_data = atlas[i].read_bytes()
        occp_data  = occp[i].read_bytes()
        geom_data  = geom[i].read_bytes()
        attr_data  = attr[i].read_bytes()

        # Parse V3C units
        atlas_units = list(parse_v3c_units(atlas_data))
        occp_units  = list(parse_v3c_units(occp_data))
        geom_units  = list(parse_v3c_units(geom_data))
        attr_units  = list(parse_v3c_units(attr_data))

        # Extract the correct units
        vps = atlas_units[0][1]   # type 0
        ad  = atlas_units[1][1]   # type 1
        ovd = occp_units[1][1]    # type 2
        gvd = geom_units[1][1]    # type 3
        avd = attr_units[1][1]    # type 4

        header_byte = atlas_data[0]   # all tracks share header

        # Write combined segment
        out_path = output_root / seg_name
        with open(out_path, "wb") as f:
            f.write(bytes([header_byte]))
            f.write(vps)
            f.write(ad)
            f.write(ovd)
            f.write(gvd)
            f.write(avd)

        print(f"[INFO] Wrote {out_path}")


def main():
    parser = argparse.ArgumentParser(
        description="Combine atlas/occp/geom/attr segments into one V3C segment per GoF"
    )
    parser.add_argument("--input-root", required=True, help="Folder containing atlas/ occp/ geom/ attr/")
    parser.add_argument("--output-root", required=True, help="Destination folder for combined segments")
    args = parser.parse_args()

    combine_per_segment(Path(args.input_root), Path(args.output_root))


if __name__ == "__main__":
    main()

