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
    # odm_georeferencing/ 内の個別 LAS ファイルも検出（VS Export形式）
    if 'laz' not in found:
        geo_dir = os.path.join(folder, 'odm_georeferencing')
        if os.path.isdir(geo_dir):
            las_files = sorted(
                f for f in os.listdir(geo_dir)
                if f.lower().endswith(('.las', '.laz'))
            )
            if las_files:
                found['laz'] = [os.path.join(geo_dir, f) for f in las_files]
    return found


def detect_from_zip(zip_path: str) -> dict:
    """ZIPを展開せずに資産を検出。{key: relative_path_in_zip}"""
    found = {}
    with zipfile.ZipFile(zip_path, 'r') as zf:
        # ZIP spec は '/' 区切りだが Windows の zip ツールは '\' を使う場合がある。
        # 比較用に正規化したセットを作成（格納は '/' 統一パス）
        names = {n.replace('\\', '/') for n in zf.namelist()}
        for key, rel in ASSET_SPEC.items():
            if rel in names:
                found[key] = rel
        # odm_georeferencing/ 内の個別 LAS/LAZ も検出（VS Export形式）
        if 'laz' not in found:
            las_entries = sorted(
                n for n in names
                if n.startswith('odm_georeferencing/')
                and n.lower().endswith(('.las', '.laz'))
            )
            if las_entries:
                found['laz'] = las_entries
        # las_sources.json による相対パス参照（FOL VS Export形式）
        if 'laz' not in found and 'las_sources.json' in names:
            import json
            with zf.open('las_sources.json') as f:
                sources = json.load(f)
            zip_dir = os.path.dirname(zip_path)
            las_paths = []
            for entry in sources.get('las', []):
                rel = entry.get('relative', '')
                abs_path = os.path.normpath(os.path.join(zip_dir, rel))
                if os.path.isfile(abs_path):
                    las_paths.append(abs_path)
            if las_paths:
                found['laz'] = las_paths
    return found


def can_generate_chm(assets: dict) -> bool:
    return 'dsm' in assets and 'dtm' in assets
