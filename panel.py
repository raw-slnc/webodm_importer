"""
WebODM Importer — panel window.
UI rules: one item per row; status/notes on the line below; columns where needed.
"""

import os
import re
import sys
import hashlib
import json
import shutil
import uuid
import tempfile
import subprocess  # nosec B404

from qgis.PyQt.QtWidgets import (
    QDialog, QVBoxLayout, QHBoxLayout, QWidget,
    QLabel, QLineEdit, QPushButton, QCheckBox, QFileDialog,
    QComboBox, QGroupBox, QProgressBar, QSpinBox, QMessageBox, QLayout,
)
from qgis.PyQt.QtCore import Qt, QThread, QTimer, pyqtSignal, QEventLoop, QEvent
from qgis.PyQt.QtGui import QIcon
import time
from qgis.core import QgsPointCloudLayer, QgsRasterLayer, QgsProject, QgsMessageLog, Qgis


class _CopcWorker(QThread):
    """PDAL による LAS → COPC 変換をバックグラウンドで実行する。"""
    finished = pyqtSignal(str, str)  # (copc_path, error_msg)  失敗時は ('', エラー文字列)

    def __init__(self, las_paths, copc_path, timeout_sec=600, parent=None):
        super().__init__(parent)
        self._las_paths = las_paths
        self._copc_path = copc_path
        self._timeout_sec = timeout_sec
        # タイムアウトは変換全体に掛ける（PDAL を複数回呼んでも合計で超えない）
        self._deadline = None
        self._cancelled = False
        self._proc = None

    def cancel(self):
        self._cancelled = True
        proc = self._proc
        if proc is not None:
            try:
                proc.kill()
            except Exception:  # nosec B110
                pass

    def _pdal(self, pipeline_json, win_flags):
        """pdal pipeline を実行し (returncode, stderr) を返す。
        キャンセル時は (None, '')、タイムアウト時は ('TIMEOUT', '') を返す。
        使える時間は締め切りまでの残り（結合で使った分は COPC 変換から差し引く）。"""
        remaining = self._deadline - time.monotonic()
        if remaining <= 0:
            return 'TIMEOUT', ''
        proc = subprocess.Popen(  # nosec B603, B607
            ['pdal', 'pipeline', '--stdin'],
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            text=True, encoding='utf-8', errors='replace',
            creationflags=win_flags,
        )
        self._proc = proc
        try:
            _, err = proc.communicate(input=pipeline_json, timeout=remaining)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.communicate()
            self._proc = None
            return 'TIMEOUT', ''
        self._proc = None
        if self._cancelled:
            return None, ''
        return proc.returncode, (err or '').strip()

    def run(self):
        self._deadline = time.monotonic() + self._timeout_sec
        copc_path = self._copc_path
        _uid = uuid.uuid4().hex[:8]
        _tmp_inputs = []  # Windows: 一時ハードリンク/コピーの管理

        def _short(p):
            """Windows: 日本語パスを 8.3 形式に変換して PDAL に渡す。"""
            if sys.platform != 'win32':
                return p
            try:
                import ctypes
                buf = ctypes.create_unicode_buffer(1024)
                ctypes.windll.kernel32.GetShortPathNameW(p, buf, 1024)
                return buf.value or p
            except Exception:
                return p

        def _safe_input(p, idx):
            """ASCII-safe な入力パスを返す。8.3 変換失敗時はハードリンク（同ドライブ）またはコピーで回避。"""
            short = _short(p)
            if sys.platform != 'win32' or all(ord(c) < 128 for c in short):
                return short
            ext = os.path.splitext(p)[1].lower()
            dst = os.path.join(tempfile.gettempdir(), f'pdal_in_{_uid}_{idx}{ext}')
            try:
                os.link(p, dst)
            except OSError:
                shutil.copy2(p, dst)
            _tmp_inputs.append(dst)
            return dst

        def _cleanup():
            for f in _tmp_inputs:
                try:
                    os.remove(f)
                except OSError:
                    pass

        # Windows: コマンドプロンプトウィンドウを開かない
        _win_flags = subprocess.CREATE_NO_WINDOW if sys.platform == 'win32' else 0

        # Windows: 出力先に ASCII 一時パスを使い、完了後に本来のパスへリネーム
        if sys.platform == 'win32':
            tmp_copc = os.path.join(tempfile.gettempdir(), f'pdal_tmp_{_uid}.copc.laz')
        else:
            tmp_copc = copc_path

        las_paths = [_safe_input(p, i) for i, p in enumerate(self._las_paths)]

        def _finalize_copc():
            """tmp_copc → copc_path へ移動（Windows のみ）。"""
            if tmp_copc != copc_path and os.path.isfile(tmp_copc):
                shutil.move(tmp_copc, copc_path)

        def _fail(error):
            """失敗時、出力先に残った不完全な変換結果を削除してから通知する。
            Linux 等は tmp_copc が copc_path と同一のため、放置すると次回起動時に
            壊れたファイルを変換済みキャッシュと誤認してしまう。"""
            if os.path.isfile(tmp_copc):
                try:
                    os.remove(tmp_copc)
                except OSError:
                    pass
            _cleanup()
            self.finished.emit('', error)

        _last_error = ''

        if len(las_paths) == 1:
            # 単一ファイルは直接 COPC 変換
            pipeline = {"pipeline": las_paths + [
                {"type": "writers.copc", "filename": tmp_copc},
            ]}
            rc, err = self._pdal(json.dumps(pipeline), _win_flags)
            if rc is None:
                _fail('CANCELLED'); return
            if rc == 'TIMEOUT':
                _fail('TIMEOUT'); return
            if rc == 0 and os.path.isfile(tmp_copc):
                _finalize_copc(); _cleanup(); self.finished.emit(copc_path, ''); return
            # フォールバック: Global Encoding WKT フラグ未設定の LAS 1.4 ファイル対策
            _last_error = err
            pipeline_nosrs = {"pipeline": [
                {"type": "readers.las", "filename": las_paths[0], "nosrs": True},
                {"type": "writers.copc", "filename": tmp_copc},
            ]}
            rc2, err2 = self._pdal(json.dumps(pipeline_nosrs), _win_flags)
            if rc2 is None:
                _fail('CANCELLED'); return
            if rc2 == 'TIMEOUT':
                _fail('TIMEOUT'); return
            if rc2 == 0 and os.path.isfile(tmp_copc):
                _finalize_copc(); _cleanup(); self.finished.emit(copc_path, ''); return
            _last_error = err2 or _last_error
        else:
            # 複数ファイル: 一旦 LAS にマージしてから COPC 化
            # （PDAL writers.copc の複数入力でオクツリー中心がずれるバグを回避）
            if sys.platform == 'win32':
                tmp_las = os.path.join(tempfile.gettempdir(), f'pdal_tmp_{_uid}.las')
            else:
                tmp_las = copc_path + '.tmp.las'
            try:
                merge_pipeline = {"pipeline": las_paths + [
                    {"type": "filters.merge"},
                    {"type": "writers.las", "filename": tmp_las},
                ]}
                rc, err = self._pdal(json.dumps(merge_pipeline), _win_flags)
                if rc is None:
                    _fail('CANCELLED'); return
                if rc == 'TIMEOUT':
                    _last_error = 'TIMEOUT'
                elif rc != 0 or not os.path.isfile(tmp_las):
                    _last_error = err
                else:
                    copc_pipeline = {"pipeline": [
                        tmp_las,
                        {"type": "writers.copc", "filename": tmp_copc},
                    ]}
                    rc2, err2 = self._pdal(json.dumps(copc_pipeline), _win_flags)
                    if rc2 is None:
                        _fail('CANCELLED'); return
                    if rc2 == 'TIMEOUT':
                        _last_error = 'TIMEOUT'
                    elif rc2 == 0 and os.path.isfile(tmp_copc):
                        _finalize_copc(); _cleanup(); self.finished.emit(copc_path, ''); return
                    else:
                        _last_error = err2
            finally:
                if os.path.isfile(tmp_las):
                    os.remove(tmp_las)

        _fail(_last_error)

from . import asset_detector, processor
from .derivative_worker import DerivativeWorker, LAYERS, REPORT_NAMES


def _pdal_available() -> bool:
    return shutil.which('pdal') is not None


class _AutoRefreshCombo(QComboBox):
    """ドロップダウンを開く直前にコールバックでリストを更新するコンボボックス。"""
    def __init__(self, refresh_fn, parent=None):
        super().__init__(parent)
        self._refresh_fn = refresh_fn

    def showPopup(self):
        self._refresh_fn()
        super().showPopup()


class _WrapLabel(QLabel):
    """折り返し表示のラベル。パネルは中身に合わせて大きさを固定しているが、
    Qt はその計算に折り返し後の高さを入れないため、文字が変わるたびに
    必要な高さを自分で求めて最低高さにする（行が増えればパネルが伸びる）。
    reserve_lines を指定すると、その行数分の高さを常に確保する（短い表示や
    空のときにパネルの高さが変わらない。超える長さのときだけ伸びる）。"""
    def __init__(self, text='', parent=None, reserve_lines=0):
        super().__init__(text, parent)
        self._reserve_lines = reserve_lines
        self.setWordWrap(True)
        self._fit()

    def setText(self, text):
        super().setText(text)
        self._fit()

    def setStyleSheet(self, style):
        super().setStyleSheet(style)
        self._fit()

    def resizeEvent(self, event):
        super().resizeEvent(event)
        self._fit()

    def _reserved_height(self):
        if self._reserve_lines <= 0:
            return 0
        self.ensurePolished()
        fm = self.fontMetrics()
        margins = self.contentsMargins()
        return (fm.height() + fm.lineSpacing() * (self._reserve_lines - 1)
                + margins.top() + margins.bottom() + 2 * self.margin())

    def _fit(self):
        width = self.contentsRect().width()
        text_height = self.heightForWidth(width) if (self.text() and width > 0) else 0
        height = max(text_height, self._reserved_height())
        if height != self.minimumHeight():
            self.setMinimumHeight(max(height, 0))
            # 固定サイズの再計算は通常1回遅れて反映されるため、その場で行う
            top_layout = self.window().layout()
            if top_layout is not None:
                top_layout.invalidate()
                top_layout.activate()


def _note_style(color='gray'):
    return f'color: {color}; font-size: 11px;'


class WebODMPanel(QDialog):
    # パネルの幅（高さは中身に合わせて自動で決まる）
    PANEL_WIDTH = 720

    def __init__(self, iface):
        # QGIS の子にせず独立したウィンドウにする（Alt+Tab・クリックで背面に回せる）。
        super().__init__(None)
        # 種類を Dialog のままにすると、X11 では親が無くてもアプリ全体の付属
        # ウィンドウとして扱われ、QGIS の上に居続ける。普通のウィンドウにする。
        self.setWindowFlags(
            Qt.WindowType.Window
            | Qt.WindowType.CustomizeWindowHint
            | Qt.WindowType.WindowTitleHint
            | Qt.WindowType.WindowSystemMenuHint
            | Qt.WindowType.WindowMinimizeButtonHint
            | Qt.WindowType.WindowCloseButtonHint
        )
        self.setWindowTitle('WebODM Importer')
        # タイトルバー・タスクバー・Alt+Tab の切替画面に出るアイコン
        self.setWindowIcon(QIcon(os.path.join(os.path.dirname(__file__), 'icon.png')))
        self.iface = iface
        self._source_path = None
        self._is_zip = False
        self._assets = {}
        self._is_fol_source = False

        main = QVBoxLayout(self)
        main.setContentsMargins(8, 8, 8, 8)
        main.setSpacing(6)
        # 大きさは中身に合わせて固定（利用者のドラッグでは変わらない）。
        # 表示倍率や文字サイズが違っても、必要な大きさを Qt が計算する。
        main.setSizeConstraint(QLayout.SizeConstraint.SetFixedSize)

        # ── Source ─────────────────────────────────
        grp = QGroupBox('Source')
        lay = QVBoxLayout(grp)
        lay.setSpacing(4)
        row_src = QHBoxLayout()
        self._src_edit = QLineEdit()
        self._src_edit.setPlaceholderText('Select ZIP…')
        self._src_edit.setReadOnly(True)
        btn_src = QPushButton('Select ZIP')
        btn_src.setFixedWidth(90)
        self._control_row_height = max(self._src_edit.sizeHint().height(), btn_src.sizeHint().height())
        btn_src.clicked.connect(self._select_source)
        row_src.addWidget(self._src_edit)
        row_src.addWidget(btn_src)
        lay.addLayout(row_src)
        self._lbl_source_status = QLabel()
        self._lbl_source_status.setStyleSheet(_note_style())
        lay.addWidget(self._lbl_source_status)
        main.addWidget(grp)

        # ── Detected Assets (2-column) ─────────────
        grp = QGroupBox('Detected Assets')
        lay = QVBoxLayout(grp)
        col_assets = QHBoxLayout()
        col_left  = QVBoxLayout()
        col_right = QVBoxLayout()
        col_left.setSpacing(2)
        col_right.setSpacing(2)

        self._asset_labels = {}
        _col_map = {'ortho': col_left, 'ept': col_left, 'laz': col_left,
                    'dsm':   col_right, 'dtm': col_right}
        for key, label in asset_detector.ASSET_LABELS.items():
            lbl = QLabel(f'— {label}')
            lbl.setStyleSheet(_note_style())
            _col_map[key].addWidget(lbl)
            self._asset_labels[key] = lbl

        col_assets.addLayout(col_left)
        col_assets.addLayout(col_right)
        lay.addLayout(col_assets)
        main.addWidget(grp)

        # ── Output ─────────────────────────────────
        grp = QGroupBox('Output')
        lay = QVBoxLayout(grp)
        self._lbl_out_path = _WrapLabel('(auto-set when project is saved)')
        self._lbl_out_path.setStyleSheet(_note_style())
        self._lbl_out_path.setWordWrap(True)
        lay.addWidget(self._lbl_out_path)
        main.addWidget(grp)

        # ── Options (2-column) ─────────────────────
        grp = QGroupBox('Options')
        lay = QVBoxLayout(grp)
        col_opt = QHBoxLayout()
        col_opt_left  = QVBoxLayout()
        col_opt_right = QVBoxLayout()
        col_opt_left.setSpacing(2)
        col_opt_right.setSpacing(2)

        self._chk_ortho = QCheckBox('Orthophoto')
        self._chk_ortho.setChecked(True)
        col_opt_left.addWidget(self._chk_ortho)

        self._chk_dsm = QCheckBox('Surface model (DSM + hillshade)')
        self._chk_dsm.setChecked(True)
        col_opt_left.addWidget(self._chk_dsm)

        row_cs_map = QHBoxLayout()
        self._chk_cs_map = QCheckBox('CS Map (terrain)')
        self._chk_cs_map.setChecked(True)
        row_cs_map.addWidget(self._chk_cs_map)
        self._lbl_cs_map_about = QLabel('<a href="cs_map_about">(about)</a>')
        self._lbl_cs_map_about.setStyleSheet('font-size: 11px;')
        self._lbl_cs_map_about.setTextInteractionFlags(Qt.TextInteractionFlag.LinksAccessibleByMouse)
        self._lbl_cs_map_about.setOpenExternalLinks(False)
        self._lbl_cs_map_about.linkActivated.connect(self._show_cs_map_about)
        row_cs_map.addWidget(self._lbl_cs_map_about)
        # (about) は CS Map のチェックが操作できるときだけ押せる
        self._chk_cs_map.installEventFilter(self)
        row_cs_map.addStretch()
        col_opt_left.addLayout(row_cs_map)

        self._chk_laz = QCheckBox('Point cloud')
        self._chk_laz.setChecked(True)
        col_opt_left.addWidget(self._chk_laz)

        self._chk_vegetation = QCheckBox('Vegetation index')
        self._chk_vegetation.setChecked(True)
        col_opt_right.addWidget(self._chk_vegetation)

        self._chk_hillshade = QCheckBox('Hillshade (terrain)')
        self._chk_hillshade.setChecked(True)
        col_opt_right.addWidget(self._chk_hillshade)

        self._chk_chm = QCheckBox('CHM')
        self._chk_chm.setChecked(True)
        col_opt_right.addWidget(self._chk_chm)

        self._chk_naturalness = QCheckBox('Forest Naturalness Index (Prototype)')
        self._chk_naturalness.setChecked(False)
        self._chk_naturalness.setVisible(False)  # ソース未選択時は非FOL扱いで隠す
        col_opt_right.addWidget(self._chk_naturalness)

        col_opt.addLayout(col_opt_left, 1)
        col_opt.addLayout(col_opt_right, 1)

        # ZIP を選ぶまでは、チェックは外した状態でグレーアウトしておく
        # （選ぶと検出結果に合わせて有効になる）
        self._option_checks = [
            self._chk_ortho, self._chk_dsm, self._chk_cs_map, self._chk_laz,
            self._chk_vegetation, self._chk_hillshade, self._chk_chm, self._chk_naturalness,
        ]
        for chk in self._option_checks:
            chk.setChecked(False)
            chk.setEnabled(False)
        lay.addLayout(col_opt)
        main.addWidget(grp)

        # ── Load Existing ──────────────────────────
        grp = QGroupBox('Load Existing')
        lay = QVBoxLayout(grp)
        row_existing = QHBoxLayout()
        self._combo_existing = _AutoRefreshCombo(self._refresh_existing_combo)
        self._combo_existing.setFixedHeight(self._control_row_height)
        row_existing.addWidget(self._combo_existing)
        self._btn_load = QPushButton('Load')
        self._btn_load.setFixedWidth(110)
        self._btn_load.setFixedHeight(self._control_row_height)
        self._btn_load.clicked.connect(self._load_existing)
        row_existing.addWidget(self._btn_load)
        lay.addLayout(row_existing)
        self._chk_convert_laz = QCheckBox('Convert LAS to COPC')
        self._chk_convert_laz.setEnabled(False)
        lay.addWidget(self._chk_convert_laz)
        self._lbl_existing_status = _WrapLabel(reserve_lines=2)
        self._lbl_existing_status.setStyleSheet(_note_style())
        self._lbl_existing_status.setWordWrap(True)
        lay.addWidget(self._lbl_existing_status)
        main.addWidget(grp)

        # ── Run ────────────────────────────────────
        grp = QGroupBox('Run')
        lay = QVBoxLayout(grp)

        # ステータス（実行中は処理内容と％、終了後は結果）
        self._lbl_progress_step = QLabel()
        self._lbl_progress_step.setStyleSheet('font-size: 11px;')
        self._lbl_progress_step.setVisible(False)
        lay.addWidget(self._lbl_progress_step)

        self._lbl_run_status = _WrapLabel()
        self._lbl_run_status.setWordWrap(True)
        self._lbl_run_status.setStyleSheet('font-size: 11px;')
        lay.addWidget(self._lbl_run_status)

        # 進捗バー｜Run/Stop
        row_run = QHBoxLayout()
        self._progress_bar = QProgressBar()
        self._progress_bar.setRange(0, 100)
        self._progress_bar.setValue(0)
        self._progress_bar.setTextVisible(False)
        self._progress_bar.setFixedHeight(self._control_row_height)
        row_run.addWidget(self._progress_bar)
        self._btn_run = QPushButton('Run')
        self._btn_run.setEnabled(False)
        self._btn_run.setFixedSize(110, self._control_row_height)
        self._btn_run.clicked.connect(self._run)
        row_run.addWidget(self._btn_run)
        self._btn_stop = QPushButton('Stop')
        self._btn_stop.setVisible(False)
        self._btn_stop.setFixedSize(110, self._control_row_height)
        self._btn_stop.clicked.connect(self._cancel)
        row_run.addWidget(self._btn_stop)
        lay.addLayout(row_run)
        main.addWidget(grp)

        lbl_credit = QLabel('Developed by Avid Tree Work')
        lbl_credit.setStyleSheet(_note_style())
        lbl_credit.setAlignment(Qt.AlignmentFlag.AlignCenter)
        main.addWidget(lbl_credit)

        # 幅をそろえるための見えない行（各欄が縦に並ぶので全体の幅になる）
        width_keeper = QWidget()
        width_keeper.setFixedSize(self.PANEL_WIDTH - 16, 0)
        main.addWidget(width_keeper)

        self._combo_existing.currentIndexChanged.connect(self._update_convert_laz_checkbox)
        self._refresh_existing_combo()

    # ── Helpers ─────────────────────────────────────
    def _source_hash(self) -> str:
        """ソースZIPの先頭2MBからMD5ハッシュを生成する（フォルダ時は空文字）。"""
        if not self._is_zip or not self._source_path:
            return ''
        h = hashlib.md5(usedforsecurity=False)
        with open(self._source_path, 'rb') as f:
            h.update(f.read(2 * 1024 * 1024))
        return h.hexdigest()

    def _save_meta(self, out_dir: str, generated_derivatives=None) -> None:
        meta = {'source': self._source_path, 'hash': self._source_hash()}
        if generated_derivatives is not None:
            meta['generated_derivatives'] = sorted(generated_derivatives)
        with open(os.path.join(out_dir, '.import_meta.json'), 'w') as f:
            json.dump(meta, f)

    def _load_meta(self, folder: str) -> dict:
        meta_path = os.path.join(folder, '.import_meta.json')
        if not os.path.isfile(meta_path):
            return {}
        try:
            with open(meta_path) as f:
                return json.load(f)
        except (OSError, json.JSONDecodeError):
            return {}

    def _load_meta_hash(self, folder: str) -> str:
        return self._load_meta(folder).get('hash', '')

    def _load_generated_derivatives(self, folder: str, derived_assets=None) -> set:
        meta = self._load_meta(folder)
        generated = meta.get('generated_derivatives')
        if generated is not None:
            return set(generated)
        if meta and derived_assets:
            # Legacy imports created these derived files in this plugin before
            # generation tracking was added.
            return set(derived_assets)
        return set()

    def _derived_layer_name(self, name: str, key: str, generated_derivatives) -> str:
        return name + '*' if key in generated_derivatives else name

    def _find_duplicate_import(self, base: str, group_name: str) -> bool:
        """base 以下の同名フォルダ（連番バリアント含む）にハッシュ一致の import があれば True。"""
        src_hash = self._source_hash()
        if not src_hash or not os.path.isdir(base):
            return False
        pattern = re.compile(rf'^{re.escape(group_name)}(_\d{{3}})?$')
        for entry in os.listdir(base):
            if pattern.match(entry):
                folder = os.path.join(base, entry)
                if os.path.isdir(folder) and self._load_meta_hash(folder) == src_hash:
                    return True
        return False

    def _short_path(self, full_path: str) -> str:
        """プロジェクトフォルダの親までを … に省略して返す。"""
        project_path = QgsProject.instance().absolutePath()
        if project_path:
            parent = os.path.dirname(project_path)
            if full_path.startswith(parent):
                return '…' + full_path[len(parent):]
        return full_path

    def _output_base(self):
        project_path = QgsProject.instance().absolutePath()
        if not project_path:
            return None
        return os.path.join(project_path, 'webodm_importer_data')

    def _group_name(self):
        src = self._src_edit.text()
        name = os.path.splitext(os.path.basename(src))[0]
        name = name.replace('-all', '').strip('-_')
        # Windows でフォルダ名に使えない文字 (<>:"/\|?* および制御文字) を除去
        name = re.sub(r'[<>:"/\\|?*\x00-\x1f]', '_', name).rstrip('. ')
        return name or 'import'

    def _resolve_output_dir(self, group_name):
        """Return unique output dir; appends _001, _002… if name already exists."""
        base = self._output_base()
        if not base:
            return None
        target = os.path.join(base, group_name)
        if not os.path.exists(target):
            return target
        i = 1
        while True:
            candidate = os.path.join(base, f'{group_name}_{i:03d}')
            if not os.path.exists(candidate):
                return candidate
            i += 1

    def _crs_from_las(self, las_path):
        """LAS VLR から CRS を読み取る。取得できなければ None を返す。"""
        try:
            import laspy
            from qgis.core import QgsCoordinateReferenceSystem
            las = laspy.read(las_path)
            crs = las.header.parse_crs()
            if crs is None:
                return None
            wkt = crs.to_wkt()
            if wkt:
                return QgsCoordinateReferenceSystem.fromWkt(wkt)
        except Exception:  # nosec B110
            pass
        return None

    def _crs_from_sibling_rasters(self, las_path):
        """LAS と同じタスクフォルダ内のラスターファイルから CRS を取得する。
        odm_georeferencing/ の親ディレクトリを起点に DSM → DTM → Orthophoto の順で確認する。"""
        try:
            from osgeo import gdal
            from qgis.core import QgsCoordinateReferenceSystem
            task_root = os.path.dirname(os.path.dirname(las_path))
            candidates = [
                os.path.join(task_root, 'odm_dem', 'dsm.tif'),
                os.path.join(task_root, 'odm_dem', 'dtm.tif'),
                os.path.join(task_root, 'odm_orthophoto', 'odm_orthophoto.tif'),
            ]
            for path in candidates:
                if not os.path.isfile(path):
                    continue
                ds = gdal.Open(path)
                if ds is None:
                    continue
                wkt = ds.GetProjection()
                ds = None
                if wkt:
                    crs = QgsCoordinateReferenceSystem.fromWkt(wkt)
                    if crs.isValid():
                        return crs
        except Exception:  # nosec B110
            pass
        return None

    def _load_point_cloud(self, laz_val, out_dir):
        """LAS/LAZ をポイントクラウドレイヤーとして返す。複数ファイルは結合 COPC に変換する。"""
        las_paths = laz_val if isinstance(laz_val, list) else [laz_val]
        crs = self._crs_from_las(las_paths[0])

        # 単一ファイルは直接読み込みを試みる
        if len(las_paths) == 1:
            for provider in ('pdal', 'copc'):
                layer = QgsPointCloudLayer(las_paths[0], 'Point Cloud', provider)
                if layer.isValid():
                    if crs and crs.isValid() and not layer.crs().isValid():
                        layer.setCrs(crs)
                    return layer

        copc_path = self._convert_to_copc(las_paths, out_dir)
        if copc_path:
            layer = QgsPointCloudLayer(copc_path, 'Point Cloud', 'copc')
            if layer.isValid():
                if crs and crs.isValid() and not layer.crs().isValid():
                    layer.setCrs(crs)
                return layer
        return None

    def _convert_to_copc(self, las_paths, out_dir):
        """PDAL CLI で LAS/LAZ（単数または複数）→ COPC 変換。変換済みなら再利用する。"""
        las_paths = las_paths if isinstance(las_paths, list) else [las_paths]
        pc_cache = os.path.join(out_dir, 'pc_cache')
        os.makedirs(pc_cache, exist_ok=True)
        base = os.path.splitext(os.path.basename(las_paths[0]))[0] if len(las_paths) == 1 else 'merged'
        copc_path = os.path.join(pc_cache, base + '.copc.laz')
        if os.path.isfile(copc_path):
            return copc_path

        _uid = uuid.uuid4().hex[:8]
        _tmp_inputs = []

        def _short(p):
            if sys.platform != 'win32':
                return p
            try:
                import ctypes
                buf = ctypes.create_unicode_buffer(1024)
                ctypes.windll.kernel32.GetShortPathNameW(p, buf, 1024)
                return buf.value or p
            except Exception:
                return p

        def _safe_input(p, idx):
            short = _short(p)
            if sys.platform != 'win32' or all(ord(c) < 128 for c in short):
                return short
            ext = os.path.splitext(p)[1].lower()
            dst = os.path.join(tempfile.gettempdir(), f'pdal_in_{_uid}_{idx}{ext}')
            try:
                os.link(p, dst)
            except OSError:
                shutil.copy2(p, dst)
            _tmp_inputs.append(dst)
            return dst

        def _cleanup():
            for f in _tmp_inputs:
                try:
                    os.remove(f)
                except OSError:
                    pass

        _win_flags = subprocess.CREATE_NO_WINDOW if sys.platform == 'win32' else 0

        if sys.platform == 'win32':
            tmp_copc = os.path.join(tempfile.gettempdir(), f'pdal_tmp_{_uid}.copc.laz')
        else:
            tmp_copc = copc_path

        safe_paths = [_safe_input(p, i) for i, p in enumerate(las_paths)]

        def _finalize():
            if tmp_copc != copc_path and os.path.isfile(tmp_copc):
                shutil.move(tmp_copc, copc_path)

        if len(safe_paths) == 1:
            pipeline = {"pipeline": safe_paths + [
                {"type": "writers.copc", "filename": tmp_copc},
            ]}
            try:
                r = subprocess.run(  # nosec B603, B607
                    ['pdal', 'pipeline', '--stdin'],
                    input=json.dumps(pipeline),
                    capture_output=True, text=True, encoding='utf-8', errors='replace', timeout=600,
                    creationflags=_win_flags,
                )
                if r.returncode == 0 and os.path.isfile(tmp_copc):
                    _finalize()
                    _cleanup()
                    return copc_path
                # フォールバック: Global Encoding WKT フラグ未設定の LAS 1.4 ファイル対策
                if r.returncode != 0:
                    pipeline_nosrs = {"pipeline": [
                        {"type": "readers.las", "filename": safe_paths[0], "nosrs": True},
                        {"type": "writers.copc", "filename": tmp_copc},
                    ]}
                    r2 = subprocess.run(  # nosec B603, B607
                        ['pdal', 'pipeline', '--stdin'],
                        input=json.dumps(pipeline_nosrs),
                        capture_output=True, text=True, encoding='utf-8', errors='replace', timeout=600,
                        creationflags=_win_flags,
                    )
                    if r2.returncode == 0 and os.path.isfile(tmp_copc):
                        _finalize()
                        _cleanup()
                        return copc_path
            except Exception:  # nosec B110
                pass
        else:
            if sys.platform == 'win32':
                tmp_las = os.path.join(tempfile.gettempdir(), f'pdal_tmp_{_uid}.las')
            else:
                tmp_las = copc_path + '.tmp.las'
            try:
                merge_pipeline = {"pipeline": safe_paths + [
                    {"type": "filters.merge"},
                    {"type": "writers.las", "filename": tmp_las},
                ]}
                r = subprocess.run(  # nosec B603, B607
                    ['pdal', 'pipeline', '--stdin'],
                    input=json.dumps(merge_pipeline),
                    capture_output=True, text=True, encoding='utf-8', errors='replace', timeout=600,
                    creationflags=_win_flags,
                )
                if r.returncode == 0 and os.path.isfile(tmp_las):
                    copc_pipeline = {"pipeline": [
                        tmp_las,
                        {"type": "writers.copc", "filename": tmp_copc},
                    ]}
                    r = subprocess.run(  # nosec B603, B607
                        ['pdal', 'pipeline', '--stdin'],
                        input=json.dumps(copc_pipeline),
                        capture_output=True, text=True, encoding='utf-8', errors='replace', timeout=600,
                        creationflags=_win_flags,
                    )
                    if r.returncode == 0 and os.path.isfile(tmp_copc):
                        _finalize()
                        _cleanup()
                        return copc_path
            except Exception:  # nosec B110
                pass
            finally:
                if os.path.isfile(tmp_las):
                    os.remove(tmp_las)
        _cleanup()
        return None

    def _add_to_group(self, layer, group):
        QgsProject.instance().addMapLayer(layer, False)
        group.addLayer(layer)
        node = group.findLayer(layer.id())
        if node:
            node.setExpanded(False)

    def _remove_group(self, name):
        root = QgsProject.instance().layerTreeRoot()
        group = root.findGroup(name)
        if not group:
            return
        layer_ids = [child.layerId() for child in group.findLayers()]
        root.removeChildNode(group)
        for lid in layer_ids:
            QgsProject.instance().removeMapLayer(lid)

    def _refresh_existing_combo(self):
        from qgis.PyQt.QtGui import QColor
        self._combo_existing.clear()
        self._combo_existing.addItem('— select —')
        base = self._output_base()
        if not base or not os.path.isdir(base):
            return
        root = QgsProject.instance().layerTreeRoot()
        for name in sorted(os.listdir(base)):
            if os.path.isdir(os.path.join(base, name)):
                self._combo_existing.addItem(name)
                group = root.findGroup(name)
                # 読み込み済みは選べない。ただし点群だけ欠けていて追加できるものは
                # 選べるようにし、点群だけを追加する（Add Point Cloud）。
                if group and not self._can_add_point_cloud(os.path.join(base, name), group):
                    idx = self._combo_existing.count() - 1
                    self._combo_existing.model().item(idx).setEnabled(False)
                    self._combo_existing.model().item(idx).setForeground(QColor('gray'))

    @staticmethod
    def _group_has_point_cloud(group):
        return any(isinstance(node.layer(), QgsPointCloudLayer) for node in group.findLayers())

    @staticmethod
    def _copc_cache_path(folder, laz_val):
        las_paths = laz_val if isinstance(laz_val, list) else [laz_val]
        base_name = 'merged' if len(las_paths) > 1 else os.path.splitext(os.path.basename(las_paths[0]))[0]
        return os.path.join(folder, 'pc_cache', base_name + '.copc.laz')

    def _point_cloud_source(self, folder):
        """('ept', path) / ('laz', paths) / None。LAS は変換済みキャッシュか PDAL が必要。"""
        assets = asset_detector.detect(folder)
        if 'ept' in assets:
            return 'ept', assets['ept']
        if 'laz' in assets:
            if _pdal_available() or os.path.isfile(self._copc_cache_path(folder, assets['laz'])):
                return 'laz', assets['laz']
        return None

    def _can_add_point_cloud(self, folder, group):
        return not self._group_has_point_cloud(group) and self._point_cloud_source(folder) is not None

    def _update_convert_laz_checkbox(self):
        self._chk_convert_laz.setText('Convert LAS to COPC')
        self._btn_load.setText('Load')
        self._btn_load.setEnabled(not getattr(self, '_running', False))
        selected = self._combo_existing.currentText()
        if selected == '— select —':
            self._chk_convert_laz.setEnabled(False)
            self._chk_convert_laz.setChecked(False)
            return
        base = self._output_base()
        if not base:
            self._chk_convert_laz.setEnabled(False)
            return
        folder = os.path.join(base, selected)
        group = QgsProject.instance().layerTreeRoot().findGroup(selected)
        if group:
            # 読み込み済み：点群だけ欠けていれば点群だけ追加する。そうでなければ何もしない
            if self._can_add_point_cloud(folder, group):
                self._btn_load.setText('Add Point Cloud')
            else:
                self._btn_load.setEnabled(False)
            self._chk_convert_laz.setEnabled(False)
            self._chk_convert_laz.setChecked(False)
            return
        assets = asset_detector.detect(folder)
        has_laz = 'laz' in assets and 'ept' not in assets
        if not has_laz:
            self._chk_convert_laz.setEnabled(False)
            self._chk_convert_laz.setChecked(False)
            return
        if not _pdal_available():
            self._chk_convert_laz.setText('Convert LAS to COPC (PDAL is required for LAZ conversion.)')
            self._chk_convert_laz.setEnabled(False)
            self._chk_convert_laz.setChecked(False)
            return
        if os.path.isfile(self._copc_cache_path(folder, assets['laz'])):
            self._chk_convert_laz.setEnabled(False)
            self._chk_convert_laz.setChecked(False)
        else:
            self._chk_convert_laz.setEnabled(True)
            self._chk_convert_laz.setChecked(False)

    def eventFilter(self, obj, event):
        if obj is self._chk_cs_map and event.type() == QEvent.Type.EnabledChange:
            enabled = self._chk_cs_map.isEnabled()
            self._lbl_cs_map_about.setEnabled(enabled)
            # リンクの色は無効状態でも変わらないため、押せないときは灰色の文字にする
            self._lbl_cs_map_about.setText(
                '<a href="cs_map_about">(about)</a>' if enabled
                else '<span style="color: gray;">(about)</span>')
        return super().eventFilter(obj, event)

    def center_on_main_window(self):
        """QGIS のメイン画面の中央に置く（初回表示用）。"""
        main_window = self.iface.mainWindow()
        if main_window is None:
            return
        self.adjustSize()
        frame = self.frameGeometry()
        frame.moveCenter(main_window.frameGeometry().center())
        self.move(frame.topLeft())

    # ── Slots ────────────────────────────────────────
    def _show_cs_map_about(self, _link=''):
        QMessageBox.information(
            self,
            'About CS Map',
            'CS topographic visualization was proposed by the Nagano Prefecture\n'
            'Forestry Research Center.\n\n'
            'This feature is an independent implementation inspired by that concept.\n\n'
            'Note: WebODM DTMs are derived from photogrammetry and have low\n'
            'ground-surface accuracy, especially under forest canopy.',
        )

    def _select_source(self):
        path, _ = QFileDialog.getOpenFileName(
            self, 'Select WebODM ZIP', '', 'ZIP (*.zip)')
        if not path:
            return
        self._source_path = path
        self._is_zip = True
        self._src_edit.setText(path)
        self._detect_assets()

    def _detect_assets(self):
        if self._is_zip:
            self._assets = asset_detector.detect_from_zip(self._source_path)
        else:
            self._assets = asset_detector.detect(self._source_path)
        self._is_fol_source = asset_detector.is_fol_source(self._source_path, self._is_zip)

        found = len(self._assets)
        total = len(asset_detector.ASSET_SPEC)
        self._lbl_source_status.setText(f'{found}/{total} assets detected')

        for key, lbl in self._asset_labels.items():
            if key in self._assets:
                lbl.setText(f'✅ {asset_detector.ASSET_LABELS[key]}')
                lbl.setStyleSheet(_note_style('green'))
            else:
                lbl.setText(f'❌ {asset_detector.ASSET_LABELS[key]}')
                lbl.setStyleSheet(_note_style())

        has_dsm = 'dsm' in self._assets
        has_dtm = 'dtm' in self._assets
        can_chm = has_dsm and has_dtm

        self._chk_ortho.setChecked('ortho' in self._assets)
        self._chk_ortho.setEnabled('ortho' in self._assets)

        self._chk_hillshade.setChecked(has_dtm)
        self._chk_hillshade.setEnabled(has_dtm)

        self._chk_dsm.setChecked(has_dsm)
        self._chk_dsm.setEnabled(has_dsm)

        self._chk_vegetation.setChecked('ortho' in self._assets)
        self._chk_vegetation.setEnabled('ortho' in self._assets)

        self._chk_chm.setEnabled(can_chm)
        self._chk_chm.setChecked(can_chm)
        self._chk_chm.setText('CHM' if can_chm else 'CHM (DSM or DTM missing)')

        self._chk_cs_map.setChecked(has_dtm)
        self._chk_cs_map.setEnabled(has_dtm)

        # 較正データ(FOL Virtual Shizuoka Export)以外では検証していないため、
        # UI自体を出さない(他由来のデータでは選択肢として見せない)
        self._chk_naturalness.setVisible(self._is_fol_source)
        can_naturalness = self._is_fol_source and can_chm and 'ortho' in self._assets
        self._chk_naturalness.setEnabled(can_naturalness)
        self._chk_naturalness.setChecked(False)
        self._chk_naturalness.setText('Forest Naturalness Index (Prototype)')

        has_pc = 'ept' in self._assets or 'laz' in self._assets
        laz_only = 'laz' in self._assets and 'ept' not in self._assets
        self._chk_laz.setText('Point cloud')
        if laz_only and not _pdal_available():
            self._chk_laz.setChecked(False)
            self._chk_laz.setEnabled(False)
        else:
            self._chk_laz.setChecked(has_pc)
            self._chk_laz.setEnabled(has_pc)

        base = self._output_base()
        if base:
            preview = self._resolve_output_dir(self._group_name())
            self._lbl_out_path.setText(self._short_path(preview))
            self._lbl_out_path.setToolTip(preview)
            self._lbl_out_path.setStyleSheet(_note_style())
        else:
            self._lbl_out_path.setText('Project not saved — output path unavailable')
            self._lbl_out_path.setStyleSheet(_note_style('orange'))

        self._btn_run.setEnabled(bool(self._assets and base))

    def _add_layers(self, group, paths, generated_derivatives, include_ortho=True):
        """LAYERS の順（上→下）にレイヤーを追加し、完了一覧に載せる名前を返す。
        paths: {key: ファイルパス}。無いキーは飛ばす。"""
        added = []
        for key, name, source, style, report in LAYERS:
            path = paths.get(key)
            if not path or not os.path.isfile(path):
                continue
            if key == 'ortho' and not include_ortho:
                continue
            if source == 'derived':
                name = self._derived_layer_name(name, key, generated_derivatives)
            layer = QgsRasterLayer(path, name)
            if not layer.isValid():
                continue
            if style == 'vegetation':
                processor.apply_vegetation_style(layer)
            self._add_to_group(layer, group)
            if report:
                added.append(REPORT_NAMES.get(key, name.rstrip('*')))
        return added

    def _load_existing(self):
        selected = self._combo_existing.currentText()
        if selected == '— select —':
            return
        base = self._output_base()
        if not base:
            return
        folder = os.path.join(base, selected)
        # 前回の結果（失敗・中断の表示など）を残さない
        self._set_status(self._lbl_existing_status, '', 'gray')
        root = QgsProject.instance().layerTreeRoot()
        existing = root.findGroup(selected)
        if existing is not None:
            self._add_point_cloud_only(folder, existing)
            return

        assets = asset_detector.detect(folder)
        derived_assets = asset_detector.detect_derived(folder)
        generated_derivatives = self._load_generated_derivatives(folder, derived_assets)

        group = root.insertGroup(0, selected)
        paths = {k: v for k, v in assets.items() if k in ('ortho', 'dsm', 'dtm')}
        paths.update(derived_assets)
        added = self._add_layers(group, paths, generated_derivatives)

        # Point Cloud (EPT preferred, LAZ fallback)
        if 'ept' in assets:
            layer = QgsPointCloudLayer(assets['ept'], 'Point Cloud', 'ept')
            self._add_to_group(layer, group)
            added.append('Point Cloud')
            self._finish_load_existing(group, added)
        elif 'laz' in assets and self._chk_convert_laz.isChecked():
            proceed, timeout_min = self._confirm_point_cloud_timeout('Skip LAS Conversion')
            if not proceed:
                self._finish_load_existing(group, added)
                return
            self._load_state = {'group': group, 'added': added, 'folder': folder}
            self._set_status(self._lbl_existing_status,
                             'Converting Point Cloud… (progress is shown under Run)', 'gray')
            self._begin_point_cloud_progress()
            self._start_copc_worker(assets['laz'], folder, on_done=self._on_load_copc_done,
                                    timeout_min=timeout_min)
        else:
            self._finish_load_existing(group, added)

    def _add_point_cloud_only(self, folder, group):
        """読み込み済みのグループに、点群だけを追加する（変換に失敗した後のやり直し用）。"""
        src = self._point_cloud_source(folder)
        if self._group_has_point_cloud(group) or src is None:
            self._lbl_existing_status.setText('No point cloud to add for this dataset.')
            self._lbl_existing_status.setStyleSheet(_note_style('orange'))
            return
        kind, value = src
        if kind == 'ept':
            layer = QgsPointCloudLayer(value, 'Point Cloud', 'ept')
            added = []
            if layer.isValid():
                self._add_to_group(layer, group)
                added.append('Point Cloud')
            self._finish_load_existing(group, added, pc_only=True)
            return
        proceed, timeout_min = self._confirm_point_cloud_timeout('Cancel')
        if not proceed:
            return
        self._load_state = {'group': group, 'added': [], 'folder': folder, 'pc_only': True}
        self._set_status(self._lbl_existing_status,
                         'Converting Point Cloud… (progress is shown under Run)', 'gray')
        self._begin_point_cloud_progress()
        self._start_copc_worker(value, folder, on_done=self._on_load_copc_done, timeout_min=timeout_min)

    def _set_running(self, running):
        self._running = running
        self._btn_run.setVisible(not running)
        self._btn_stop.setVisible(running)
        self._btn_stop.setEnabled(running)
        # バーは常に表示して行の配置を固定し、実行中だけ％を出す
        self._progress_bar.setTextVisible(running)
        self._lbl_progress_step.setVisible(running)
        if running:
            self._btn_load.setEnabled(False)
        else:
            self._progress_bar.setRange(0, 100)
            self._progress_bar.setValue(0)
            self._lbl_progress_step.setText('')
            self._update_convert_laz_checkbox()

    def _cancel(self):
        self._cancelled = True
        self._btn_stop.setEnabled(False)
        self._update_status('Cancelling…')
        worker = getattr(self, '_derivative_worker', None)
        if worker is not None and worker.isRunning():
            worker.cancel()
        if hasattr(self, '_copc_worker') and self._copc_worker.isRunning():
            self._copc_worker.cancel()

    def shutdown_workers(self):
        """プラグイン終了時：動いている処理を止めて待つ（スレッドを残さない）。"""
        for name in ('_derivative_worker', '_copc_worker'):
            worker = getattr(self, name, None)
            if worker is not None and worker.isRunning():
                worker.cancel()
                worker.wait(30000)

    def _update_status(self, text):
        from qgis.PyQt.QtWidgets import QApplication
        self._lbl_run_status.setText(text)
        QApplication.processEvents(QEventLoop.ProcessEventsFlag.ExcludeUserInputEvents)

    def _set_status(self, label, text, color):
        label.setText(text)
        label.setStyleSheet(f'color: {color}; font-size: 11px;')

    def _run(self):
        base = self._output_base()
        self._cancelled = False
        if not base:
            self._set_status(self._lbl_run_status, 'Save the project before running.', 'red')
            return

        group_name = self._group_name()
        if self._is_zip and self._find_duplicate_import(base, group_name):
            self._set_status(self._lbl_run_status,
                             f'Already imported: {group_name}\nUse Load Existing to reload.', 'orange')
            return

        out_dir = self._resolve_output_dir(group_name)
        os.makedirs(out_dir, exist_ok=True)

        opts = {
            'vegetation': self._chk_vegetation.isChecked(),
            'surface': self._chk_dsm.isChecked(),
            'hillshade': self._chk_hillshade.isChecked(),
            'cs_map': self._chk_cs_map.isChecked(),
            'chm': self._chk_chm.isChecked(),
            'naturalness': self._chk_naturalness.isChecked(),
        }
        self._run_ctx = {
            'out_dir': out_dir, 'group_name': group_name,
            'include_ortho': self._chk_ortho.isChecked(),
            'want_pc': self._chk_laz.isChecked(),
        }
        self._derivative_worker = DerivativeWorker(
            self._source_path, self._is_zip, dict(self._assets), out_dir, opts, parent=self)
        self._derivative_worker.progress.connect(self._on_worker_progress)
        self._derivative_worker.done.connect(self._on_worker_done)
        self._progress_bar.setRange(0, 100)
        self._progress_bar.setValue(0)
        self._set_running(True)
        self._set_status(self._lbl_run_status, '', 'black')
        self._lbl_progress_step.setText('Starting…')
        self._derivative_worker.start()

    def _on_worker_progress(self, step_no, step_total, label, pct):
        self._lbl_progress_step.setText(f'{step_no}/{step_total}  {label}…  {pct}%')
        self._progress_bar.setValue(pct)

    def _begin_point_cloud_progress(self):
        """点群変換は PDAL が進捗を出さないため、経過時間だけを表示する。"""
        self._progress_bar.setRange(0, 0)
        self._set_running(True)
        self._lbl_progress_step.setText('Converting Point Cloud…')

    def _on_worker_done(self, abs_assets, outputs, results, errors, cancelled, fatal):
        ctx = self._run_ctx
        if cancelled:
            self._set_status(self._lbl_run_status, 'Cancelled.', 'orange')
            self._set_running(False)
            return
        if fatal:
            self._set_status(self._lbl_run_status, fatal, 'red')
            self._set_running(False)
            return

        root = QgsProject.instance().layerTreeRoot()
        group = root.insertGroup(0, ctx['group_name'])
        generated = {k for k, v in results.items() if v == 'ok'}
        paths = {k: v for k, v in abs_assets.items() if k in ('ortho', 'dsm', 'dtm')}
        paths.update({k: outputs[k] for k in generated})
        added = self._add_layers(group, paths, generated, include_ortho=ctx['include_ortho'])

        # Point Cloud (EPT preferred, LAZ fallback)
        pc_layer = None
        if ctx['want_pc'] and 'ept' in abs_assets:
            ept_layer = QgsPointCloudLayer(abs_assets['ept'], 'Point Cloud', 'ept')
            if ept_layer.isValid():
                pc_layer = ept_layer

        self._run_state = {
            'group': group, 'added': added, 'out_dir': ctx['out_dir'],
            'root': root, 'pc_layer': pc_layer,
            'laz': abs_assets.get('laz') if ctx['want_pc'] else None,
            'generated_derivatives': generated,
            'errors': errors,
        }

        if pc_layer is None and self._run_state['laz']:
            proceed, timeout_min = self._confirm_point_cloud_timeout('Skip Point Cloud')
            if proceed:
                self._begin_point_cloud_progress()
                self._start_copc_worker(abs_assets['laz'], ctx['out_dir'], timeout_min=timeout_min)
                return
        self._on_copc_done(None, '')

    def _confirm_point_cloud_timeout(self, skip_label):
        """LAS→COPC 変換の確認ダイアログを表示する。
        戻り値: (続行するか, タイムアウト分数)。"""
        dlg = QDialog(self)
        dlg.setWindowTitle('Point Cloud')
        lay = QVBoxLayout(dlg)

        lay.addWidget(QLabel(
            'Converting large LAS files may take a long time.\n'
            'Do you want to continue?'
        ))
        lay.addSpacing(12)

        row = QHBoxLayout()
        row.addWidget(QLabel('Timeout:'))
        spin = QSpinBox()
        spin.setRange(1, 180)
        spin.setValue(10)
        spin.setSuffix(' min')
        row.addWidget(spin)
        lbl_range = QLabel(f'({spin.minimum()}–{spin.maximum()} min)')
        lbl_range.setStyleSheet('color: gray; font-size: 11px;')
        row.addWidget(lbl_range)
        row.addStretch()
        lay.addLayout(row)

        lbl_note = QLabel()
        lbl_note.setStyleSheet('color: gray; font-size: 11px;')

        def _update_note(value):
            lbl_note.setText(
                f'※ Conversion will be terminated if it does not complete within {value} minutes.'
            )
        spin.valueChanged.connect(_update_note)
        _update_note(spin.value())
        lay.addWidget(lbl_note)
        lay.addSpacing(12)

        btn_row = QHBoxLayout()
        btn_row.addStretch()
        btn_skip = QPushButton(skip_label)
        btn_continue = QPushButton('Continue')
        btn_continue.setDefault(True)
        btn_row.addWidget(btn_skip)
        btn_row.addWidget(btn_continue)
        lay.addLayout(btn_row)

        btn_continue.clicked.connect(dlg.accept)
        btn_skip.clicked.connect(dlg.reject)

        accepted = dlg.exec() == QDialog.DialogCode.Accepted
        return accepted, spin.value()

    def _start_copc_worker(self, laz_val, out_dir, on_done=None, timeout_min=10):
        las_paths = laz_val if isinstance(laz_val, list) else [laz_val]
        crs = self._crs_from_las(las_paths[0])
        # LAS に CRS がない場合は同フォルダの DSM/DTM/Orthophoto から取得
        if crs is None or not crs.isValid():
            crs = self._crs_from_sibling_rasters(las_paths[0])
        pc_cache = os.path.join(out_dir, 'pc_cache')
        os.makedirs(pc_cache, exist_ok=True)
        base = 'merged' if len(las_paths) > 1 else os.path.splitext(os.path.basename(las_paths[0]))[0]
        copc_path = os.path.join(pc_cache, base + '.copc.laz')
        callback = on_done if on_done is not None else self._on_copc_done
        if on_done is None:
            self._run_state['crs'] = crs
            self._run_state['timeout_min'] = timeout_min
        else:
            self._load_state['crs'] = crs
            self._load_state['timeout_min'] = timeout_min

        if os.path.isfile(copc_path):
            callback(copc_path, '')
            return

        las_total_mb = sum(os.path.getsize(p) for p in las_paths if os.path.isfile(p)) / 1024 / 1024
        self._copc_start_time = time.monotonic()
        self._copc_las_mb = las_total_mb

        self._copc_timer = QTimer(self)
        def _tick():
            elapsed = int(time.monotonic() - self._copc_start_time)
            m, s = divmod(elapsed, 60)
            self._lbl_progress_step.setText(
                f"Converting Point Cloud… {las_total_mb:.0f} MB — "
                f"{m}:{s:02d} / {timeout_min}:00 elapsed"
            )
        self._copc_timer.timeout.connect(_tick)
        self._copc_timer.start(1000)

        self._copc_worker = _CopcWorker(las_paths, copc_path, timeout_sec=timeout_min * 60, parent=self)
        self._copc_worker.finished.connect(callback)
        self._copc_worker.start()

    def _pc_error_message(self, error, timeout_min):
        if error == 'TIMEOUT':
            return (f'Point Cloud conversion timed out (exceeded {timeout_min} minutes). '
                    'The file may be too large. Use Load Existing → Add Point Cloud to try again '
                    'with a longer timeout.')
        if error == 'CANCELLED':
            return 'Point Cloud conversion was cancelled. Use Load Existing → Add Point Cloud to try again.'
        # 製作者向けの調査用ログ。UI表示は簡易メッセージのみなので、詳細はここに残す。
        QgsMessageLog.logMessage(
            f'webodm_importer PDAL error:\n{error}', 'webodm_importer', Qgis.MessageLevel.Warning
        )
        return f'Point Cloud conversion failed: {error[:200]}'

    def _on_copc_done(self, copc_path, error=''):
        # タイマー停止・処理速度を計算
        if hasattr(self, '_copc_timer') and self._copc_timer.isActive():
            self._copc_timer.stop()
        elapsed = time.monotonic() - getattr(self, '_copc_start_time', time.monotonic())
        las_mb = getattr(self, '_copc_las_mb', 0)
        if copc_path and elapsed > 0 and las_mb > 0:
            speed = las_mb / (elapsed / 60)
            em, es = divmod(int(elapsed), 60)
            speed_str = f"{las_mb:.0f} MB in {em}:{es:02d} — {speed:.0f} MB/min"
        else:
            speed_str = ""
        self._copc_las_mb = 0

        state = self._run_state
        group = state['group']
        added = state['added']
        root = state['root']
        pc_layer = state.get('pc_layer')
        lines = []

        if copc_path:
            pc_layer = QgsPointCloudLayer(copc_path, 'Point Cloud', 'copc')
            crs = state.get('crs')
            if pc_layer.isValid() and crs and crs.isValid() and not pc_layer.crs().isValid():
                pc_layer.setCrs(crs)
        elif error:
            # 点群だけの失敗・中断では、生成済みのレイヤーは残す（点群は後から追加できる）
            lines.append(self._pc_error_message(error, state.get('timeout_min', 10)))

        if pc_layer and pc_layer.isValid():
            self._add_to_group(pc_layer, group)
            added.append('Point Cloud')

        errors = state.get('errors', [])
        for label, reason in errors:
            lines.append(f'Not created — {label}: {reason}')

        if added:
            group.setExpanded(True)
            self._save_meta(state['out_dir'], state.get('generated_derivatives', set()))
            done_msg = 'Done: ' + ', '.join(added)
            if speed_str:
                done_msg += f'  |  {speed_str}'
            color = 'orange' if lines else 'green'
            self._set_status(self._lbl_run_status, '\n'.join([done_msg] + lines), color)
            self._refresh_existing_combo()
        else:
            root.removeChildNode(group)
            self._set_status(self._lbl_run_status, '\n'.join(['No layers added.'] + lines),
                             'red' if lines else 'orange')

        self._set_running(False)

    def _on_load_copc_done(self, copc_path, error=''):
        if hasattr(self, '_copc_timer') and self._copc_timer.isActive():
            self._copc_timer.stop()
        state = self._load_state
        group = state['group']
        added = state['added']
        err_msg = ''
        if copc_path:
            crs = state.get('crs')
            pc_layer = QgsPointCloudLayer(copc_path, 'Point Cloud', 'copc')
            if pc_layer.isValid():
                if crs and crs.isValid() and not pc_layer.crs().isValid():
                    pc_layer.setCrs(crs)
                self._add_to_group(pc_layer, group)
                added.append('Point Cloud')
        elif error:
            err_msg = self._pc_error_message(error, state.get('timeout_min', 10))
        self._finish_load_existing(group, added, pc_only=state.get('pc_only', False), error=err_msg)

    def _finish_load_existing(self, group, added, pc_only=False, error=''):
        root = QgsProject.instance().layerTreeRoot()
        if pc_only:
            # 既存グループには手を付けない（点群が追加できなくても消さない）
            if added:
                self._set_status(self._lbl_existing_status, 'Added: Point Cloud', 'green')
            else:
                self._set_status(self._lbl_existing_status,
                                 error or 'Point Cloud could not be loaded.', 'red')
        elif added:
            group.setExpanded(True)
            text = 'Loaded: ' + ', '.join(added)
            self._set_status(self._lbl_existing_status, text + (f'\n{error}' if error else ''),
                             'orange' if error else 'green')
        else:
            root.removeChildNode(group)
            self._set_status(self._lbl_existing_status, error or 'No valid layers found.',
                             'red' if error else 'orange')
        self._set_running(False)
        self._lbl_run_status.setText('')
