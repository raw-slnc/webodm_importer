import os
from qgis.PyQt.QtWidgets import QAction
from qgis.PyQt.QtGui import QIcon
from qgis.PyQt.QtCore import Qt
from .panel import WebODMPanel


class WebODMImporter:
    def __init__(self, iface):
        self.iface = iface
        self.panel = None
        self.action = None

    def initGui(self):
        icon = QIcon(os.path.join(os.path.dirname(__file__), 'icon.png'))
        self.action = QAction(icon, 'WebODM Importer', self.iface.mainWindow())
        self.action.triggered.connect(self._toggle_panel)
        self.iface.addRasterToolBarIcon(self.action)
        self.iface.addPluginToRasterMenu('&WebODM Importer', self.action)

    def unload(self):
        self.iface.removeRasterToolBarIcon(self.action)
        self.iface.removePluginRasterMenu('WebODM Importer', self.action)
        if self.panel:
            self.iface.removeDockWidget(self.panel)
            self.panel.deleteLater()
            self.panel = None

    def _toggle_panel(self):
        if self.panel is None:
            self.panel = WebODMPanel(self.iface)
            self.iface.addDockWidget(Qt.RightDockWidgetArea, self.panel)
        else:
            self.panel.setVisible(not self.panel.isVisible())
