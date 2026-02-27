# ============================================================
#                       PLY IO CLASS
# ============================================================
import logging
import numpy as np
import sys
from collections import defaultdict
import pandas as pd

class PlyIO:
    """Handles ONLY reading and writing PLY files."""

    ply_dtypes = {
        b'int8':'i1', b'char':'i1',
        b'uint8':'u1', b'uchar':'u1',
        b'int16':'i2', b'short':'i2',
        b'uint16':'u2', b'ushort':'u2',
        b'int32':'i4', b'int':'i4',
        b'uint32':'u4', b'uint':'u4',
        b'float32':'f4', b'float':'f4',
        b'float64':'f8', b'double':'f8'
    }
    valid_formats = {'ascii':'', 'binary_big_endian':'>', 'binary_little_endian':'<'}

    sys_byteorder = ('>', '<')[sys.byteorder == 'little']

    # ---------------- HEADER PARSE ----------------
    def _parse_header(self, filename, allow_bool):
        if allow_bool:
            self.ply_dtypes[b'bool'] = '?'

        with open(filename, 'rb') as ply:
            if b'ply' not in ply.readline():
                raise ValueError(f"{filename} is not a valid PLY file")

            fmt = ply.readline().decode().strip().split()[1]
            ext = self.valid_formats.get(fmt)
            if ext is None:
                raise ValueError(f"Unsupported PLY format: {fmt}")

            dtypes = defaultdict(list)
            vertex_count = None

            while True:
                line = ply.readline()
                if not line:
                    break

                tokens = line.decode().strip().split()
                if not tokens:
                    continue

                if tokens[0] == "element" and tokens[1] == "vertex":
                    vertex_count = int(tokens[2])

                elif tokens[0] == "property" and vertex_count is not None:
                    dtype_name = tokens[1].encode()
                    name = tokens[2]
                    dtypes["vertex"].append((name, ext + self.ply_dtypes[dtype_name]))

                elif tokens[0] == "end_header":
                    break

            if vertex_count is None:
                raise ValueError(f"Vertex count missing in {filename}")

            return fmt, ext, dtypes, vertex_count, ply.tell()

    # ---------------- READ ----------------
    def read(self, filename, allow_bool=False):
        try:
            fmt, ext, dtypes, vertex_count, header_pos = self._parse_header(filename, allow_bool)
        except Exception as e:
            logging.error(f"Failed parsing PLY header for {filename}: {e}")
            raise

        try:
            with open(filename, 'rb') as ply:
                ply.seek(header_pos)

                # ASCII mode
                if fmt == "ascii":
                    arr = []
                    for _ in range(vertex_count):
                        line = ply.readline().decode().strip()
                        if not line:
                            continue
                        arr.append([float(v) for v in line.split()])

                    df = pd.DataFrame(arr, columns=[name for name, _ in dtypes['vertex']])

                else:
                    # Binary
                    arr = np.fromfile(ply, dtype=dtypes['vertex'], count=vertex_count)
                    if ext != self.sys_byteorder:
                        arr = arr.byteswap().newbyteorder()

                    df = pd.DataFrame(arr)

                return df

        except Exception as e:
            logging.error(f"Error reading PLY data from {filename}: {e}")
            raise

    # ---------------- WRITE ----------------
    def write(self, filename, df):
        try:
            if df.empty:
                df = pd.DataFrame([[-1, -1, -1, 0, 0, 0]],
                                    columns=["x", "y", "z", "r", "g", "b"])

            rgb_cols = [c for c in df.columns if c.lower() in ["r", "g", "b", "red", "green", "blue"]]
            for c in rgb_cols:
                df[c] = df[c].astype(np.uint8)

            header = ["ply", "format ascii 1.0", f"element vertex {len(df)}"]
            for col in df.columns:
                header.append(
                    "property uchar " + col
                    if col in rgb_cols else
                    "property float " + col
                )
            header.append("end_header")

            fmt = ["%d" if col in rgb_cols else "%f" for col in df.columns]

            with open(filename, "w") as f:
                f.write("\n".join(header) + "\n")
                np.savetxt(f, df.values, fmt=fmt)

        except Exception as e:
            logging.error(f"Failed writing PLY file {filename}: {e}")
            raise