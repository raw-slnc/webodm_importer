def classFactory(iface):
    from .webodm_importer import WebODMImporter
    return WebODMImporter(iface)
