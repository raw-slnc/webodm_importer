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

DERIVED_ASSET_SPEC = {
    'vegetation':    'vegetation.tif',
    'hillshade_dsm': 'hillshade_dsm.tif',
    'hillshade_dtm': 'hillshade_dtm.tif',
    'surface_model': 'surface_model.tif',
    'terrain_model': 'terrain_model.tif',
    'chm':           'chm.tif',
    'forest_naturalness_index': 'forest_naturalness_index.tif',
    'cs_map':        'cs_map.tif',
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
    # las_sources.json による相対パス参照（FOL VS Export形式）。
    # LAS本体はfolderにコピーされないため、.import_meta.jsonに保存された
    # 元ZIPのパスを起点に相対パスを解決する（Load Existingでの再検出用）。
    # 1つでも欠けていれば点群として扱わない（一部だけの点群を作らないため）。
    if 'laz' not in found and 'ept' not in found:
        las_paths, missing = fol_las_status(folder)
        if las_paths and not missing:
            found['laz'] = las_paths
    return found


def _las_source_paths(sources: dict, zip_dir: str) -> list:
    """las_sources.json の各エントリを、元ZIPの場所を起点に絶対パスへ解決する。"""
    las_paths = []
    for entry in sources.get('las', []):
        rel = entry.get('relative', '').replace('\\', '/')
        las_paths.append(os.path.normpath(os.path.join(zip_dir, rel)))
    return las_paths


def fol_las_status(folder: str):
    """取り込み済みフォルダの las_sources.json（FOL VS Export）が指す LAS について
    (全パス, 見つからないパス) を返す。FOL由来でない・一覧が空なら ([], [])。
    FOL は最新の Export 分の LAS しか残さないため、古いデータでは欠けることがある。"""
    sources_path = os.path.join(folder, 'las_sources.json')
    meta_path = os.path.join(folder, '.import_meta.json')
    if not os.path.isfile(sources_path) or not os.path.isfile(meta_path):
        return [], []
    import json
    try:
        with open(meta_path, encoding='utf-8') as f:
            source = json.load(f).get('source', '')
        if not source:
            return [], []
        with open(sources_path, encoding='utf-8') as f:
            sources = json.load(f)
    except (OSError, json.JSONDecodeError):
        return [], []
    las_paths = _las_source_paths(sources, os.path.dirname(source))
    return las_paths, [p for p in las_paths if not os.path.isfile(p)]


def fol_las_status_from_zip(zip_path: str):
    """FOL VS Export の ZIP について (全パス, 見つからないパス) を返す。"""
    import json
    try:
        with zipfile.ZipFile(zip_path, 'r') as zf:
            names = {n.replace('\\', '/'): n for n in zf.namelist()}
            if 'las_sources.json' not in names:
                return [], []
            with zf.open(names['las_sources.json']) as f:
                sources = json.load(f)
    except (OSError, zipfile.BadZipFile, json.JSONDecodeError):
        return [], []
    las_paths = _las_source_paths(sources, os.path.dirname(zip_path))
    return las_paths, [p for p in las_paths if not os.path.isfile(p)]


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
    # las_sources.json による相対パス参照（FOL VS Export形式）。
    # 1つでも欠けていれば点群として扱わない（一部だけの点群を作らないため）。
    if 'laz' not in found:
        las_paths, missing = fol_las_status_from_zip(zip_path)
        if las_paths and not missing:
            found['laz'] = las_paths
    return found


def detect_derived(folder: str) -> dict:
    """フォルダから派生済み資産を検出。{key: absolute_path}"""
    found = {}
    for key, rel in DERIVED_ASSET_SPEC.items():
        abs_path = os.path.join(folder, rel)
        if os.path.isfile(abs_path):
            found[key] = abs_path
    return found


def is_fol_source(source_path: str, is_zip: bool) -> bool:
    """forestry_operations_lite の Virtual Shizuoka Export 由来かを判定。
    その形式だけが同梱する las_sources.json の有無で見分ける。
    実験的な派生物(Forest Naturalness Index)は、この形式で較正・検証した
    データでしか有効な結果を出さないため、由来の絞り込みに使う。"""
    if is_zip:
        try:
            with zipfile.ZipFile(source_path, 'r') as zf:
                names = {n.replace('\\', '/') for n in zf.namelist()}
                return 'las_sources.json' in names
        except (OSError, zipfile.BadZipFile):
            return False
    return os.path.isfile(os.path.join(source_path, 'las_sources.json'))
