import re
from pathlib import Path
from typing import Dict, Optional

def extract_metadata_from_filename(file_path: Path) -> Dict[str, Optional[int]]:
    """
    Parse vox from a filename such as 'longdress_vox9_1051.ply'.
    Assumes 'vox' is followed immediately by the number (e.g., vox9).
    """
    stem = Path(file_path).stem
    tokens: Dict[str, Optional[int]] = {"vox": None}

    vox_match = re.search(r"vox(?P<vox>\d+)", stem)
    if vox_match:
        tokens["vox"] = int(vox_match.group("vox"))

    return tokens
