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
    QComboBox, QGroupBox,
)
from qgis.PyQt.QtCore import Qt
from qgis.core import QgsPointCloudLayer, QgsRasterLayer, QgsProject

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
            layer = QgsPointCloudLayer(assets['laz'], 'Point Cloud', 'pdal')
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

        self._lbl_run_status.setText('Processing…')
        self._lbl_run_status.repaint()

        abs_assets = {}
        if self._is_zip:
            self._lbl_run_status.setText('Extracting ZIP…')
            self._lbl_run_status.repaint()
            with zipfile.ZipFile(self._source_path, 'r') as zf:
                all_names = zf.namelist()
                for key, rel in self._assets.items():
                    if key == 'ept':
                        for name in all_names:
                            if name.startswith('entwine_pointcloud/'):
                                zf.extract(name, out_dir)
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
            layer = QgsRasterLayer(abs_assets['ortho'], 'Orthophoto')
            if layer.isValid():
                self._add_to_group(layer, group)
                added.append('Orthophoto')

        # Vegetation
        if self._chk_vegetation.isChecked() and 'ortho' in abs_assets:
            veg_path = os.path.join(out_dir, 'vegetation.tif')
            self._lbl_run_status.setText('Generating vegetation index…')
            self._lbl_run_status.repaint()
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
                self._lbl_run_status.setText('Generating hillshade (DSM)…')
                self._lbl_run_status.repaint()
                processor.generate_hillshade(abs_assets['dsm'], hs_dsm_path)
                self._lbl_run_status.setText('Rendering Surface Model…')
                self._lbl_run_status.repaint()
                processor.render_elevation_composite(abs_assets['dsm'], hs_dsm_path, comp_dsm_path)
                layer = QgsRasterLayer(comp_dsm_path, 'Surface Model')
                if layer.isValid():
                    self._add_to_group(layer, group)
                added.append('Surface Model')

        # Terrain Model (baked composite RGB)
        if 'dtm' in abs_assets:
            if self._chk_hillshade.isChecked():
                self._lbl_run_status.setText('Generating hillshade (DTM)…')
                self._lbl_run_status.repaint()
                processor.generate_hillshade(abs_assets['dtm'], hs_dtm_path)
                self._lbl_run_status.setText('Rendering Terrain Model…')
                self._lbl_run_status.repaint()
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
            self._lbl_run_status.setText('Generating CHM…')
            self._lbl_run_status.repaint()
            processor.generate_chm(abs_assets['dsm'], abs_assets['dtm'], chm_path)
            layer = QgsRasterLayer(chm_path, 'CHM')
            if layer.isValid():
                self._add_to_group(layer, group)
                added.append('CHM')

        # Point Cloud (EPT preferred, LAZ fallback)
        if self._chk_laz.isChecked():
            if 'ept' in abs_assets:
                layer = QgsPointCloudLayer(abs_assets['ept'], 'Point Cloud', 'ept')
                self._add_to_group(layer, group)
                added.append('Point Cloud')
            elif 'laz' in abs_assets:
                layer = QgsPointCloudLayer(abs_assets['laz'], 'Point Cloud', 'pdal')
                self._add_to_group(layer, group)
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
