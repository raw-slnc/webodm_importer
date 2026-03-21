"""
WebODM Importer — right dock panel.
UI rules: one item per row; status/notes on the line below; columns where needed.
"""

import os
import zipfile
import hashlib
import json

from qgis.PyQt.QtWidgets import (
    QDockWidget, QWidget, QVBoxLayout, QHBoxLayout,
    QLabel, QLineEdit, QPushButton, QCheckBox, QFileDialog,
    QComboBox, QGroupBox, QProgressBar,
)
from qgis.PyQt.QtCore import Qt, QThread, pyqtSignal
from qgis.core import QgsPointCloudLayer, QgsRasterLayer, QgsProject


class _CopcWorker(QThread):
    """PDAL による LAS → COPC 変換をバックグラウンドで実行する。"""
    finished = pyqtSignal(str)  # copc_path（失敗時は空文字）

    def __init__(self, las_paths, copc_path, parent=None):
        super().__init__(parent)
        self._las_paths = las_paths
        self._copc_path = copc_path

    def run(self):
        import subprocess, json, tempfile
        copc_path = self._copc_path

        if len(self._las_paths) == 1:
            # 単一ファイルは直接 COPC 変換
            pipeline = {"pipeline": self._las_paths + [
                {"type": "writers.copc", "filename": copc_path},
            ]}
            try:
                r = subprocess.run(
                    ['pdal', 'pipeline', '--stdin'],
                    input=json.dumps(pipeline),
                    capture_output=True, text=True, timeout=600,
                )
                if r.returncode == 0 and os.path.isfile(copc_path):
                    self.finished.emit(copc_path)
                    return
            except Exception:
                pass
        else:
            # 複数ファイル: 一旦 LAS にマージしてから COPC 化
            # （PDAL writers.copc の複数入力でオクツリー中心がずれるバグを回避）
            tmp_las = copc_path + '.tmp.las'
            try:
                merge_pipeline = {"pipeline": self._las_paths + [
                    {"type": "filters.merge"},
                    {"type": "writers.las", "filename": tmp_las},
                ]}
                r = subprocess.run(
                    ['pdal', 'pipeline', '--stdin'],
                    input=json.dumps(merge_pipeline),
                    capture_output=True, text=True, timeout=600,
                )
                if r.returncode != 0 or not os.path.isfile(tmp_las):
                    self.finished.emit('')
                    return

                copc_pipeline = {"pipeline": [
                    tmp_las,
                    {"type": "writers.copc", "filename": copc_path},
                ]}
                r = subprocess.run(
                    ['pdal', 'pipeline', '--stdin'],
                    input=json.dumps(copc_pipeline),
                    capture_output=True, text=True, timeout=600,
                )
                if r.returncode == 0 and os.path.isfile(copc_path):
                    self.finished.emit(copc_path)
                    return
            except Exception:
                pass
            finally:
                if os.path.isfile(tmp_las):
                    os.remove(tmp_las)

        self.finished.emit('')

from . import asset_detector, processor


class _AutoRefreshCombo(QComboBox):
    """ドロップダウンを開く直前にコールバックでリストを更新するコンボボックス。"""
    def __init__(self, refresh_fn, parent=None):
        super().__init__(parent)
        self._refresh_fn = refresh_fn

    def showPopup(self):
        self._refresh_fn()
        super().showPopup()


def _note_style(color='gray'):
    return f'color: {color}; font-size: 11px;'


class WebODMPanel(QDockWidget):
    def __init__(self, iface):
        super().__init__('WebODM Importer')
        self.iface = iface
        self._source_path = None
        self._is_zip = False
        self._assets = {}

        self.setObjectName('WebODMImporterPanel')
        self.setAllowedAreas(Qt.RightDockWidgetArea | Qt.LeftDockWidgetArea)
        self.setMaximumHeight(620)

        root_widget = QWidget()
        self.setWidget(root_widget)
        main = QVBoxLayout(root_widget)
        main.setContentsMargins(8, 8, 8, 8)
        main.setSpacing(6)

        # ── Source ─────────────────────────────────
        grp = QGroupBox('Source')
        lay = QVBoxLayout(grp)
        lay.setSpacing(4)
        row_src = QHBoxLayout()
        self._src_edit = QLineEdit()
        self._src_edit.setPlaceholderText('Select ZIP or folder…')
        self._src_edit.setReadOnly(True)
        btn_src = QPushButton('Select ZIP')
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
        self._lbl_out_path = QLabel('(auto-set when project is saved)')
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

        self._chk_dsm = QCheckBox('Surface model (DSM)')
        self._chk_dsm.setChecked(True)
        col_opt_left.addWidget(self._chk_dsm)

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

        col_opt.addLayout(col_opt_left)
        col_opt.addLayout(col_opt_right)
        lay.addLayout(col_opt)
        main.addWidget(grp)

        # ── Load Existing ──────────────────────────
        grp = QGroupBox('Load Existing')
        lay = QVBoxLayout(grp)
        row_existing = QHBoxLayout()
        self._combo_existing = _AutoRefreshCombo(self._refresh_existing_combo)
        row_existing.addWidget(self._combo_existing)
        btn_load = QPushButton('Load')
        btn_load.setFixedWidth(48)
        btn_load.clicked.connect(self._load_existing)
        row_existing.addWidget(btn_load)
        lay.addLayout(row_existing)
        self._lbl_existing_status = QLabel()
        self._lbl_existing_status.setStyleSheet(_note_style())
        lay.addWidget(self._lbl_existing_status)
        main.addWidget(grp)

        # ── Run ────────────────────────────────────
        grp = QGroupBox('Run')
        lay = QVBoxLayout(grp)
        self._btn_run = QPushButton('Run')
        self._btn_run.setEnabled(False)
        self._btn_run.clicked.connect(self._run)
        lay.addWidget(self._btn_run)

        self._progress_bar = QProgressBar()
        self._progress_bar.setRange(0, 0)
        self._progress_bar.setVisible(False)
        lay.addWidget(self._progress_bar)

        self._lbl_run_status = QLabel()
        self._lbl_run_status.setWordWrap(True)
        self._lbl_run_status.setStyleSheet('font-size: 11px;')
        lay.addWidget(self._lbl_run_status)
        main.addWidget(grp)

        lbl_credit = QLabel('Developed by Avid Tree Work')
        lbl_credit.setStyleSheet(_note_style())
        lbl_credit.setAlignment(Qt.AlignCenter)
        main.addWidget(lbl_credit)

        main.addStretch()

        self._refresh_existing_combo()

    # ── Helpers ─────────────────────────────────────
    def _source_hash(self) -> str:
        """ソースZIPの先頭2MBからMD5ハッシュを生成する（フォルダ時は空文字）。"""
        if not self._is_zip or not self._source_path:
            return ''
        h = hashlib.md5()
        with open(self._source_path, 'rb') as f:
            h.update(f.read(2 * 1024 * 1024))
        return h.hexdigest()

    def _save_meta(self, out_dir: str) -> None:
        meta = {'source': self._source_path, 'hash': self._source_hash()}
        with open(os.path.join(out_dir, '.import_meta.json'), 'w') as f:
            json.dump(meta, f)

    def _load_meta_hash(self, folder: str) -> str:
        meta_path = os.path.join(folder, '.import_meta.json')
        if not os.path.isfile(meta_path):
            return ''
        with open(meta_path) as f:
            return json.load(f).get('hash', '')

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
        return name.replace('-all', '').strip('-_')

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
        except Exception:
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
        import subprocess, json
        las_paths = las_paths if isinstance(las_paths, list) else [las_paths]
        pc_cache = os.path.join(out_dir, 'pc_cache')
        os.makedirs(pc_cache, exist_ok=True)
        if len(las_paths) == 1:
            base = os.path.splitext(os.path.basename(las_paths[0]))[0]
        else:
            base = 'merged'
        copc_path = os.path.join(pc_cache, base + '.copc.laz')
        if os.path.isfile(copc_path):
            return copc_path
        if len(las_paths) == 1:
            pipeline = {"pipeline": las_paths + [
                {"type": "writers.copc", "filename": copc_path},
            ]}
            try:
                r = subprocess.run(
                    ['pdal', 'pipeline', '--stdin'],
                    input=json.dumps(pipeline),
                    capture_output=True, text=True, timeout=300,
                )
                if r.returncode == 0 and os.path.isfile(copc_path):
                    return copc_path
            except Exception:
                pass
        else:
            tmp_las = copc_path + '.tmp.las'
            try:
                merge_pipeline = {"pipeline": las_paths + [
                    {"type": "filters.merge"},
                    {"type": "writers.las", "filename": tmp_las},
                ]}
                r = subprocess.run(
                    ['pdal', 'pipeline', '--stdin'],
                    input=json.dumps(merge_pipeline),
                    capture_output=True, text=True, timeout=300,
                )
                if r.returncode == 0 and os.path.isfile(tmp_las):
                    copc_pipeline = {"pipeline": [
                        tmp_las,
                        {"type": "writers.copc", "filename": copc_path},
                    ]}
                    r = subprocess.run(
                        ['pdal', 'pipeline', '--stdin'],
                        input=json.dumps(copc_pipeline),
                        capture_output=True, text=True, timeout=300,
                    )
                    if r.returncode == 0 and os.path.isfile(copc_path):
                        return copc_path
            except Exception:
                pass
            finally:
                if os.path.isfile(tmp_las):
                    os.remove(tmp_las)
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
        from qgis.PyQt.QtCore import Qt
        self._combo_existing.clear()
        self._combo_existing.addItem('— select —')
        base = self._output_base()
        if not base or not os.path.isdir(base):
            return
        root = QgsProject.instance().layerTreeRoot()
        for name in sorted(os.listdir(base)):
            if os.path.isdir(os.path.join(base, name)):
                self._combo_existing.addItem(name)
                if root.findGroup(name):
                    idx = self._combo_existing.count() - 1
                    self._combo_existing.model().item(idx).setEnabled(False)
                    self._combo_existing.model().item(idx).setForeground(QColor('gray'))

    # ── Slots ────────────────────────────────────────
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

        self._chk_dsm.setChecked(has_dsm)
        self._chk_dsm.setEnabled(has_dsm)

        self._chk_vegetation.setChecked('ortho' in self._assets)
        self._chk_vegetation.setEnabled('ortho' in self._assets)

        self._chk_hillshade.setChecked(has_dtm)
        self._chk_hillshade.setEnabled(has_dtm)

        self._chk_chm.setEnabled(can_chm)
        self._chk_chm.setChecked(can_chm)
        self._chk_chm.setText('CHM' if can_chm else 'CHM\n(DSM or DTM missing)')

        has_pc = 'ept' in self._assets or 'laz' in self._assets
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

    def _load_existing(self):
        selected = self._combo_existing.currentText()
        if selected == '— select —':
            return
        base = self._output_base()
        if not base:
            return
        folder = os.path.join(base, selected)
        assets = asset_detector.detect(folder)

        self._remove_group(selected)

        root = QgsProject.instance().layerTreeRoot()
        group = root.insertGroup(0, selected)
        added = []

        # Orthophoto
        if 'ortho' in assets:
            layer = QgsRasterLayer(assets['ortho'], 'Orthophoto')
            if layer.isValid():
                self._add_to_group(layer, group)
                added.append('Orthophoto')

        # Vegetation
        veg_path = os.path.join(folder, 'vegetation.tif')
        if os.path.isfile(veg_path):
            layer = QgsRasterLayer(veg_path, 'Vegetation (VARI)')
            if layer.isValid():
                processor.apply_vegetation_style(layer)
                self._add_to_group(layer, group)
                added.append('Vegetation')

        hs_dtm_path = os.path.join(folder, 'hillshade_dtm.tif')
        comp_dsm_path = os.path.join(folder, 'surface_model.tif')
        comp_dtm_path = os.path.join(folder, 'terrain_model.tif')

        # Surface Model (baked composite RGB)
        if 'dsm' in assets and os.path.isfile(comp_dsm_path):
            layer = QgsRasterLayer(comp_dsm_path, 'Surface Model')
            if layer.isValid():
                self._add_to_group(layer, group)
            added.append('Surface Model')

        # Terrain Model (baked composite RGB)
        if 'dtm' in assets and os.path.isfile(comp_dtm_path):
            layer = QgsRasterLayer(comp_dtm_path, 'Terrain Model')
            if layer.isValid():
                self._add_to_group(layer, group)
            added.append('Terrain Model')

        # Standalone DSM
        if 'dsm' in assets:
            layer = QgsRasterLayer(assets['dsm'], 'DSM')
            if layer.isValid():
                self._add_to_group(layer, group)

        # Standalone DTM
        if 'dtm' in assets:
            layer = QgsRasterLayer(assets['dtm'], 'DTM')
            if layer.isValid():
                self._add_to_group(layer, group)

        # Standalone Hillshade (DTM)
        if os.path.isfile(hs_dtm_path):
            layer = QgsRasterLayer(hs_dtm_path, 'Hillshade (DTM)')
            if layer.isValid():
                self._add_to_group(layer, group)

        # CHM
        chm_path = os.path.join(folder, 'chm.tif')
        if os.path.isfile(chm_path):
            layer = QgsRasterLayer(chm_path, 'CHM')
            if layer.isValid():
                self._add_to_group(layer, group)
                added.append('CHM')

        # Point Cloud (EPT preferred, LAZ fallback)
        if 'ept' in assets:
            layer = QgsPointCloudLayer(assets['ept'], 'Point Cloud', 'ept')
            self._add_to_group(layer, group)
            added.append('Point Cloud')
        elif 'laz' in assets:
            layer = self._load_point_cloud(assets['laz'], folder)
            if layer:
                self._add_to_group(layer, group)
                added.append('Point Cloud')

        if added:
            group.setExpanded(True)
            self._lbl_existing_status.setText('Loaded: ' + ', '.join(added))
            self._lbl_existing_status.setStyleSheet(_note_style('green'))
        else:
            root.removeChildNode(group)
            self._lbl_existing_status.setText('No valid layers found.')
            self._lbl_existing_status.setStyleSheet(_note_style('orange'))

    def _set_running(self, running):
        self._btn_run.setVisible(not running)
        self._progress_bar.setVisible(running)

    def _update_status(self, text):
        from qgis.PyQt.QtWidgets import QApplication
        self._lbl_run_status.setText(text)
        QApplication.processEvents()

    def _run(self):
        base = self._output_base()
        if not base:
            self._lbl_run_status.setText('Save the project before running.')
            self._lbl_run_status.setStyleSheet('color: red; font-size: 11px;')
            return

        group_name = self._group_name()
        existing = os.path.join(base, group_name)
        if os.path.isdir(existing) and self._is_zip:
            if self._source_hash() == self._load_meta_hash(existing):
                self._lbl_run_status.setText(
                    f'Already imported: {group_name}\nUse Load Existing to reload.')
                self._lbl_run_status.setStyleSheet('color: orange; font-size: 11px;')
                return

        out_dir = self._resolve_output_dir(group_name)
        os.makedirs(out_dir, exist_ok=True)

        # ステップ数を事前に算出してプログレスバーを確定表示
        chk = self._chk_hillshade.isChecked()
        a = self._assets
        has_laz_conversion = self._chk_laz.isChecked() and 'laz' in a and 'ept' not in a
        steps = (
            (1 if self._is_zip else 0)
            + (1 if self._chk_ortho.isChecked() and 'ortho' in a else 0)
            + (1 if self._chk_vegetation.isChecked() and 'ortho' in a else 0)
            + (2 if chk and self._chk_dsm.isChecked() and 'dsm' in a else 0)
            + (2 if chk and 'dtm' in a else 0)
            + (1 if self._chk_chm.isChecked() and 'dsm' in a and 'dtm' in a else 0)
            + (2 if has_laz_conversion else 1 if self._chk_laz.isChecked() and 'ept' in a else 0)
        )
        self._progress_bar.setRange(0, max(steps, 1))
        self._progress_bar.setValue(0)
        self._set_running(True)
        self._update_status('Processing…')

        def _step(label):
            self._progress_bar.setValue(self._progress_bar.value() + 1)
            self._update_status(label)

        abs_assets = {}
        if self._is_zip:
            _step('Extracting ZIP…')
            with zipfile.ZipFile(self._source_path, 'r') as zf:
                all_names = zf.namelist()
                for key, rel in self._assets.items():
                    if key == 'ept':
                        for name in all_names:
                            if name.startswith('entwine_pointcloud/'):
                                zf.extract(name, out_dir)
                        abs_assets[key] = os.path.join(out_dir, rel)
                    elif isinstance(rel, list):
                        for r in rel:
                            zf.extract(r, out_dir)
                        abs_assets[key] = [os.path.join(out_dir, r) for r in rel]
                    else:
                        zf.extract(rel, out_dir)
                        abs_assets[key] = os.path.join(out_dir, rel)
        else:
            abs_assets = self._assets

        root = QgsProject.instance().layerTreeRoot()
        group = root.insertGroup(0, group_name)
        added = []

        # Orthophoto
        if self._chk_ortho.isChecked() and 'ortho' in abs_assets:
            _step('Loading Orthophoto…')
            layer = QgsRasterLayer(abs_assets['ortho'], 'Orthophoto')
            if layer.isValid():
                self._add_to_group(layer, group)
                added.append('Orthophoto')

        # Vegetation
        if self._chk_vegetation.isChecked() and 'ortho' in abs_assets:
            veg_path = os.path.join(out_dir, 'vegetation.tif')
            _step('Generating vegetation index…')
            processor.generate_vegetation_index(abs_assets['ortho'], veg_path)
            layer = QgsRasterLayer(veg_path, 'Vegetation (VARI)')
            if layer.isValid():
                processor.apply_vegetation_style(layer)
                self._add_to_group(layer, group)
                added.append('Vegetation')

        hs_dsm_path = os.path.join(out_dir, 'hillshade_dsm.tif')
        hs_dtm_path = os.path.join(out_dir, 'hillshade_dtm.tif')
        comp_dsm_path = os.path.join(out_dir, 'surface_model.tif')
        comp_dtm_path = os.path.join(out_dir, 'terrain_model.tif')

        # Surface Model (baked composite RGB)
        if self._chk_dsm.isChecked() and 'dsm' in abs_assets:
            if self._chk_hillshade.isChecked():
                _step('Generating hillshade (DSM)…')
                processor.generate_hillshade(abs_assets['dsm'], hs_dsm_path)
                _step('Rendering Surface Model…')
                processor.render_elevation_composite(abs_assets['dsm'], hs_dsm_path, comp_dsm_path)
                layer = QgsRasterLayer(comp_dsm_path, 'Surface Model')
                if layer.isValid():
                    self._add_to_group(layer, group)
                added.append('Surface Model')

        # Terrain Model (baked composite RGB)
        if 'dtm' in abs_assets:
            if self._chk_hillshade.isChecked():
                _step('Generating hillshade (DTM)…')
                processor.generate_hillshade(abs_assets['dtm'], hs_dtm_path)
                _step('Rendering Terrain Model…')
                processor.render_elevation_composite(abs_assets['dtm'], hs_dtm_path, comp_dtm_path)
                layer = QgsRasterLayer(comp_dtm_path, 'Terrain Model')
                if layer.isValid():
                    self._add_to_group(layer, group)
                added.append('Terrain Model')

        # Standalone DSM
        if 'dsm' in abs_assets:
            layer = QgsRasterLayer(abs_assets['dsm'], 'DSM')
            if layer.isValid():
                self._add_to_group(layer, group)

        # Standalone DTM
        if 'dtm' in abs_assets:
            layer = QgsRasterLayer(abs_assets['dtm'], 'DTM')
            if layer.isValid():
                self._add_to_group(layer, group)

        # Standalone Hillshade (DTM)
        if os.path.isfile(hs_dtm_path):
            layer = QgsRasterLayer(hs_dtm_path, 'Hillshade (DTM)')
            if layer.isValid():
                self._add_to_group(layer, group)

        # CHM
        if self._chk_chm.isChecked() and asset_detector.can_generate_chm(abs_assets):
            chm_path = os.path.join(out_dir, 'chm.tif')
            _step('Generating CHM…')
            processor.generate_chm(abs_assets['dsm'], abs_assets['dtm'], chm_path)
            layer = QgsRasterLayer(chm_path, 'CHM')
            if layer.isValid():
                self._add_to_group(layer, group)
                added.append('CHM')

        # Point Cloud (EPT preferred, LAZ fallback)
        pc_layer = None
        if self._chk_laz.isChecked():
            if 'ept' in abs_assets:
                _step('Loading Point Cloud…')
                ept_layer = QgsPointCloudLayer(abs_assets['ept'], 'Point Cloud', 'ept')
                if ept_layer.isValid():
                    pc_layer = ept_layer

        self._run_state = {
            'group': group, 'added': added, 'out_dir': out_dir,
            'root': root, 'pc_layer': pc_layer,
            'laz': abs_assets.get('laz') if self._chk_laz.isChecked() else None,
            'step': _step,
        }

        if pc_layer:
            self._on_copc_done(None)
        elif self._run_state['laz']:
            _step('Converting Point Cloud…')
            self._start_copc_worker(abs_assets['laz'], out_dir)
        else:
            self._on_copc_done(None)

    def _start_copc_worker(self, laz_val, out_dir):
        las_paths = laz_val if isinstance(laz_val, list) else [laz_val]
        crs = self._crs_from_las(las_paths[0])
        pc_cache = os.path.join(out_dir, 'pc_cache')
        os.makedirs(pc_cache, exist_ok=True)
        base = 'merged' if len(las_paths) > 1 else os.path.splitext(os.path.basename(las_paths[0]))[0]
        copc_path = os.path.join(pc_cache, base + '.copc.laz')
        self._run_state['crs'] = crs

        if os.path.isfile(copc_path):
            self._on_copc_done(copc_path)
            return

        self._copc_worker = _CopcWorker(las_paths, copc_path, self)
        self._copc_worker.finished.connect(self._on_copc_done)
        self._copc_worker.start()

    def _on_copc_done(self, copc_path):
        state = self._run_state
        group = state['group']
        added = state['added']
        out_dir = state['out_dir']
        root = state['root']
        pc_layer = state.get('pc_layer')

        if copc_path:
            from qgis.core import QgsCoordinateReferenceSystem
            state.get('step', lambda _: None)('Loading Point Cloud…')
            pc_layer = QgsPointCloudLayer(copc_path, 'Point Cloud', 'copc')
            crs = state.get('crs')
            if pc_layer.isValid() and crs and crs.isValid() and not pc_layer.crs().isValid():
                pc_layer.setCrs(crs)

        if pc_layer and pc_layer.isValid():
            self._add_to_group(pc_layer, group)
            added.append('Point Cloud')

        if added:
            group.setExpanded(True)
            self._save_meta(out_dir)
            self._lbl_run_status.setText('Done: ' + ', '.join(added))
            self._lbl_run_status.setStyleSheet('color: green; font-size: 11px;')
            self._refresh_existing_combo()
        else:
            root.removeChildNode(group)
            self._lbl_run_status.setText('No layers added.')
            self._lbl_run_status.setStyleSheet('color: orange; font-size: 11px;')

        self._set_running(False)
