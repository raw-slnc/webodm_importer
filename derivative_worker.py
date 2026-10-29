"""
Derived-layer generation in a worker thread.

- LAYERS: the single layer order (top -> bottom) used by both Run and
  Load Existing.
- plan_steps(): which generation steps run for the detected assets and the
  checked options, with their dependencies.
- DerivativeWorker: extracts the ZIP and runs the steps off the UI thread.
  A failed step removes its partial output, records a readable reason and
  the remaining independent steps continue. QGIS layers are created by the
  panel (UI thread) after the worker finishes.
"""

import errno
import os
import traceback
import zipfile

from qgis.PyQt.QtCore import QThread, pyqtSignal
from qgis.core import QgsMessageLog, Qgis

from . import processor
from . import cs_map as cs_map_module
from . import custom_vegetation_index as naturalness_module


# (key, layer name, source, style, report)
#   source: 'asset' = WebODM の元データ / 'derived' = このプラグインが生成
#   style:  'vegetation' なら植生指数の配色を当てる
#   report: 完了メッセージの一覧に名前を出すか
LAYERS = [
    ('ortho',                    'Orthophoto',                           'asset',   None,         True),
    ('vegetation',               'Vegetation (VARI)',                    'derived', 'vegetation', True),
    ('surface_model',            'Surface Model',                        'derived', None,         True),
    ('terrain_model',            'Terrain Model',                        'derived', None,         True),
    ('dsm',                      'DSM',                                  'asset',   None,         False),
    ('dtm',                      'DTM',                                  'asset',   None,         False),
    ('hillshade_dtm',            'Hillshade (DTM)',                      'derived', None,         False),
    ('cs_map',                   'CS Map',                               'derived', None,         True),
    ('chm',                      'CHM',                                  'derived', None,         True),
    ('forest_naturalness_index', 'Forest Naturalness Index (Prototype)', 'derived', 'vegetation', True),
]

# 完了メッセージでの短い呼び名
REPORT_NAMES = {'forest_naturalness_index': 'Forest Naturalness Index'}


class Step:
    def __init__(self, key, label, needs=(), deps=()):
        self.key = key        # 出力キー（asset_detector.DERIVED_ASSET_SPEC と同じ）
        self.label = label    # 進捗表示の文言
        self.needs = needs    # 必要な元データ（asset キー）
        self.deps = deps      # 先に成功している必要のある Step キー


def plan_steps(asset_keys, opts):
    """実行する生成ステップを順に返す。opts は panel のチェック状態（dict）。"""
    a = set(asset_keys)
    steps = []

    def add(flag, step):
        if flag and set(step.needs) <= a:
            steps.append(step)

    add(opts.get('vegetation'), Step('vegetation', 'Generating vegetation index', ('ortho',)))
    add(opts.get('surface'), Step('hillshade_dsm', 'Generating hillshade (DSM)', ('dsm',)))
    add(opts.get('surface'), Step('surface_model', 'Rendering Surface Model', ('dsm',), ('hillshade_dsm',)))
    add(opts.get('hillshade'), Step('hillshade_dtm', 'Generating hillshade (DTM)', ('dtm',)))
    add(opts.get('hillshade'), Step('terrain_model', 'Rendering Terrain Model', ('dtm',), ('hillshade_dtm',)))
    add(opts.get('cs_map'), Step('cs_map', 'Generating CS Map', ('dtm',)))
    add(opts.get('chm'), Step('chm', 'Generating CHM', ('dsm', 'dtm')))
    add(opts.get('naturalness'), Step('forest_naturalness_index',
                                      'Generating forest naturalness index (prototype)',
                                      ('dsm', 'dtm', 'ortho')))
    return steps


def _run_step(key, assets, out_dir, outputs, progress):
    out = outputs[key]
    if key == 'vegetation':
        processor.generate_vegetation_index(assets['ortho'], out, progress)
    elif key == 'hillshade_dsm':
        processor.generate_hillshade(assets['dsm'], out, progress)
    elif key == 'surface_model':
        processor.render_elevation_composite(assets['dsm'], outputs['hillshade_dsm'], out, progress)
    elif key == 'hillshade_dtm':
        processor.generate_hillshade(assets['dtm'], out, progress)
    elif key == 'terrain_model':
        processor.render_elevation_composite(assets['dtm'], outputs['hillshade_dtm'], out, progress)
    elif key == 'cs_map':
        cs_map_module.generate_cs_map(assets['dtm'], out, progress)
    elif key == 'chm':
        processor.generate_chm(assets['dsm'], assets['dtm'], out, progress)
    elif key == 'forest_naturalness_index':
        naturalness_module.generate_forest_naturalness_index(
            assets['dsm'], assets['dtm'], assets['ortho'], out, progress)


def describe_error(exc):
    """例外を利用者向けの説明文にする。分からないものはメッセージ＋ログ案内。"""
    msg = str(exc).strip()
    low = msg.lower()
    err_no = getattr(exc, 'errno', None)
    if isinstance(exc, MemoryError) or 'out of memory' in low or 'cannot allocate' in low:
        return ('Not enough memory for this dataset. Close other applications or free up '
                'memory, then try again.')
    if (err_no == errno.ENOSPC or 'no space left' in low or 'disk space' in low
            or 'disk full' in low):
        return 'Not enough free disk space in the output folder.'
    if (isinstance(exc, PermissionError) or err_no in (errno.EACCES, errno.EPERM)
            or 'permission denied' in low or 'access is denied' in low):
        return 'Cannot write to the output folder (permission denied).'
    detail = msg or type(exc).__name__
    return f'{detail} (see the QGIS message log "webodm_importer" for details)'


def _safe_extract(zf, member, out_dir, progress, done, total):
    """ZIP メンバーを out_dir 配下へ展開する（範囲外パスは拒否）。"""
    rel = member.replace('\\', '/')
    dest = os.path.realpath(os.path.join(out_dir, rel))
    root = os.path.realpath(out_dir)
    if not dest.startswith(root + os.sep):
        raise ValueError(f'Unsafe path in ZIP: {rel}')
    info = zf.getinfo(member)
    if member.endswith(('/', '\\')):
        os.makedirs(dest, exist_ok=True)
        return done
    os.makedirs(os.path.dirname(dest), exist_ok=True)
    with zf.open(info) as src, open(dest, 'wb') as dst:
        while True:
            chunk = src.read(4 * 1024 * 1024)
            if not chunk:
                break
            dst.write(chunk)
            done += len(chunk)
            processor.report(progress, done / total if total else 1.0)
    return done


def extract_assets(zip_path, assets, out_dir, progress=None):
    """検出済みアセット（ZIP内の相対パス）を展開し、絶対パスの dict を返す。"""
    abs_assets = {}
    with zipfile.ZipFile(zip_path, 'r') as zf:
        # ZIP spec は '/' 区切りだが Windows ツールは '\' を使う場合がある。
        # 正規化名→元エントリ名のマップを作り、比較は '/' 統一で行い
        # 展開には元エントリ名を使うことで両方に対応する。
        entry_map = {n.replace('\\', '/'): n for n in zf.namelist()}
        members = []
        for key, rel in assets.items():
            if key == 'ept':
                members += [o for n, o in entry_map.items() if n.startswith('entwine_pointcloud/')]
                abs_assets[key] = os.path.join(out_dir, rel)
            elif isinstance(rel, list):
                resolved = []
                for r in rel:
                    if os.path.isabs(r):
                        resolved.append(r)
                    else:
                        members.append(entry_map.get(r, r))
                        resolved.append(os.path.join(out_dir, r))
                abs_assets[key] = resolved
            else:
                members.append(entry_map.get(rel, rel))
                abs_assets[key] = os.path.join(out_dir, rel)
        # las_sources.json はアセットではなくFOL由来判定用のマーカーとして
        # out_dir にも残す（Load Existing で由来を再判定できるように）
        if 'las_sources.json' in entry_map:
            members.append(entry_map['las_sources.json'])
        total = sum(zf.getinfo(m).file_size for m in members)
        done = 0
        for m in members:
            done = _safe_extract(zf, m, out_dir, progress, done, total)
    return abs_assets


class DerivativeWorker(QThread):
    """ZIP 展開と派生レイヤー生成をバックグラウンドで行う。"""
    # (step_no 1始まり, step_total, label, percent)
    progress = pyqtSignal(int, int, str, int)
    # (abs_assets, outputs{key: path}, results{key: 'ok'|'failed'|'skipped'},
    #  errors[(label, reason)], cancelled, fatal_error)
    done = pyqtSignal(object, object, object, object, bool, str)

    def __init__(self, source_path, is_zip, assets, out_dir, opts, parent=None):
        super().__init__(parent)
        self._source_path = source_path
        self._is_zip = is_zip
        self._assets = assets
        self._out_dir = out_dir
        self._steps = plan_steps(assets.keys(), opts)
        self._cancelled = False
        self.step_total = len(self._steps) + (1 if is_zip else 0)

    def cancel(self):
        self._cancelled = True

    def _progress_cb(self, step_no, label):
        last = {'pct': -1}

        def _cb(fraction):
            if self._cancelled:
                raise processor.Cancelled()
            pct = int(fraction * 100)
            if pct != last['pct']:
                last['pct'] = pct
                self.progress.emit(step_no, self.step_total, label, pct)
        return _cb

    def run(self):
        outputs = {s.key: os.path.join(self._out_dir, f'{s.key}.tif') for s in self._steps}
        results, errors = {}, []
        step_no = 0
        abs_assets = self._assets

        if self._is_zip:
            step_no += 1
            label = 'Extracting ZIP'
            self.progress.emit(step_no, self.step_total, label, 0)
            try:
                abs_assets = extract_assets(self._source_path, self._assets, self._out_dir,
                                            self._progress_cb(step_no, label))
            except processor.Cancelled:
                self.done.emit({}, outputs, results, errors, True, '')
                return
            except Exception as exc:
                self._log(label, exc)
                self.done.emit({}, outputs, results, errors, False,
                               f'Extracting ZIP failed: {describe_error(exc)}')
                return

        labels = {s.key: s.label for s in self._steps}
        for step in self._steps:
            if self._cancelled:
                self.done.emit(abs_assets, outputs, results, errors, True, '')
                return
            step_no += 1
            self.progress.emit(step_no, self.step_total, step.label, 0)
            failed_dep = next((d for d in step.deps if results.get(d) != 'ok'), None)
            if failed_dep:
                results[step.key] = 'skipped'
                errors.append((step.label, f'Skipped because "{labels[failed_dep]}" did not complete.'))
                continue
            try:
                _run_step(step.key, abs_assets, self._out_dir, outputs,
                          self._progress_cb(step_no, step.label))
                results[step.key] = 'ok'
            except processor.Cancelled:
                processor.remove_quietly(outputs[step.key])
                self.done.emit(abs_assets, outputs, results, errors, True, '')
                return
            except Exception as exc:
                # 壊れた途中結果を残さない（Load Existing が拾わないように）
                processor.remove_quietly(outputs[step.key])
                results[step.key] = 'failed'
                errors.append((step.label, describe_error(exc)))
                self._log(step.label, exc)

        self.done.emit(abs_assets, outputs, results, errors, False, '')

    @staticmethod
    def _log(label, exc):
        # 製作者向けの調査用ログ。UI表示は要約のみなので、詳細はここに残す。
        QgsMessageLog.logMessage(
            f'webodm_importer: {label} failed\n'
            + ''.join(traceback.format_exception(type(exc), exc, exc.__traceback__)),
            'webodm_importer', Qgis.MessageLevel.Warning)
