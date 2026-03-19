"""
WebODM asset detection.
Resolves known file paths within an extracted WebODM task folder or ZIP.
"""

import os
import zipfile

# Known asset paths relative to the task root
ASSET_SPEC = {
    'ortho': 'odm_orthophoto/odm_orthophoto.tif',
    'dsm':   'odm_dem/dsm.tif',
    'dtm':   'odm_dem/dtm.tif',
    'ept':   'entwine_pointcloud/ept.json',
    'laz':   'odm_georeferencing/odm_georeferenced_model.laz',
}

ASSET_LABELS = {
    'ortho': 'Orthophoto',
    'dsm':   'DSM',
    'dtm':   'DTM',
    'ept':   'Point Cloud (EPT)',
    'laz':   'Point Cloud (.laz)',
}


def detect(folder: str) -> dict:
    """フォルダから資産を検出。{key: absolute_path}"""
    found = {}
    for key, rel in ASSET_SPEC.items():
        abs_path = os.path.join(folder, rel)
        if os.path.isfile(abs_path):
            found[key] = abs_path
    return found


def detect_from_zip(zip_path: str) -> dict:
    """ZIPを展開せずに資産を検出。{key: relative_path_in_zip}"""
    found = {}
    with zipfile.ZipFile(zip_path, 'r') as zf:
        names = set(zf.namelist())
        for key, rel in ASSET_SPEC.items():
            if rel in names:
                found[key] = rel
    return found


def can_generate_chm(assets: dict) -> bool:
    return 'dsm' in assets and 'dtm' in assets
