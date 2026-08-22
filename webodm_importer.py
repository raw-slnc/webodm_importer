import os
from qgis.PyQt.QtWidgets import QAction
from qgis.PyQt.QtGui import QIcon
from .panel import WebODMPanel


class WebODMImporter:
    def __init__(self, iface):
        self.iface = iface
        self.panel = None
        self.action = None

    def initGui(self):
        icon = QIcon(os.path.join(os.path.dirname(__file__), 'icon.png'))
        self.action = QAction(icon, 'WebODM Importer', self.iface.mainWindow())
        self.action.triggered.connect(self._show_panel)
        self.iface.addRasterToolBarIcon(self.action)
        self.iface.addPluginToRasterMenu('&WebODM Importer', self.action)

    def unload(self):
        self.iface.removeRasterToolBarIcon(self.action)
        self.iface.removePluginRasterMenu('WebODM Importer', self.action)
        if self.panel:
            self.panel.close()
            self.panel.deleteLater()
            self.panel = None

    def _show_panel(self):
        if self.panel is None:
            self.panel = WebODMPanel(self.iface)
            self.panel.destroyed.connect(self._on_panel_destroyed)
        self.panel.show()
        self.panel.raise_()
        self.panel.activateWindow()

    def _on_panel_destroyed(self):
        self.panel = None
