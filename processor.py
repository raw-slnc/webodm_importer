"""
Processing pipeline: raster generation and styling.
"""

import os
import processing
import numpy as np
from osgeo import gdal
from qgis.core import (
    QgsRasterLayer,
    QgsRasterShader, QgsColorRampShader,
    QgsSingleBandPseudoColorRenderer,
)
from qgis.PyQt.QtGui import QColor

# Rainbow ramp stops shared by apply_elevation_style and render_elevation_composite
_ELEV_RAMP = [
    (0.0,  (0x8B, 0x00, 0xFF)),
    (0.2,  (0x00, 0x00, 0xFF)),
    (0.4,  (0x00, 0xFF, 0xFF)),
    (0.6,  (0x00, 0xFF, 0x00)),
    (0.8,  (0xFF, 0xFF, 0x00)),
    (1.0,  (0xFF, 0x00, 0x00)),
]


def generate_chm(dsm_path: str, dtm_path: str, output_path: str) -> str:
    """CHM = DSM - DTM"""
    processing.run('gdal:rastercalculator', {
        'INPUT_A': dsm_path, 'BAND_A': 1,
        'INPUT_B': dtm_path, 'BAND_B': 1,
        'FORMULA': 'A-B',
        'OUTPUT': output_path,
    })
    return output_path


def generate_vegetation_index(ortho_path: str, output_path: str) -> str:
    """VARI = (G - R) / (G + R - B)  from RGB orthophoto (bands 1=R, 2=G, 3=B)."""
    processing.run('gdal:rastercalculator', {
        'INPUT_A': ortho_path, 'BAND_A': 1,   # R
        'INPUT_B': ortho_path, 'BAND_B': 2,   # G
        'INPUT_C': ortho_path, 'BAND_C': 3,   # B
        'FORMULA': '(B*1.0-A)/(B+A-C+0.001)',
        'OUTPUT': output_path,
    })
    return output_path


def generate_hillshade(dtm_path: str, output_path: str) -> str:
    """Hillshade from DTM."""
    processing.run('gdal:hillshade', {
        'INPUT': dtm_path,
        'BAND': 1,
        'Z_FACTOR': 1.0,
        'SCALE': 1.0,
        'AZIMUTH': 315.0,
        'ALTITUDE': 45.0,
        'OUTPUT': output_path,
    })
    return output_path



def render_elevation_composite(elev_path: str, hs_path: str, output_path: str) -> str:
    """Bake elevation color ramp + hillshade (Multiply 70%) into a single RGB GeoTIFF."""
    ds_elev = gdal.Open(elev_path)
    band = ds_elev.GetRasterBand(1)
    elev = band.ReadAsArray().astype(np.float32)
    nodata = band.GetNoDataValue()

    valid = elev if nodata is None else elev[elev != nodata]
    lo, hi = float(np.nanmin(valid)), float(np.nanmax(valid))
    span = max(hi - lo, 1e-10)

    # Normalize elevation to 0-1 and apply rainbow ramp
    t = np.clip((elev - lo) / span, 0.0, 1.0)
    h, w = elev.shape
    rgb = np.zeros((h, w, 3), dtype=np.float32)
    for i in range(len(_ELEV_RAMP) - 1):
        t0, c0 = _ELEV_RAMP[i]
        t1, c1 = _ELEV_RAMP[i + 1]
        in_seg = (t >= t0) & (t <= t1)
        alpha = np.where(in_seg, (t - t0) / max(t1 - t0, 1e-10), 0.0)
        for ch in range(3):
            rgb[:, :, ch] += np.where(in_seg, (1 - alpha) * c0[ch] + alpha * c1[ch], 0.0)

    # Hillshade Multiply at 70% opacity: result = base * (hs * 0.7 + 0.3)
    ds_hs = gdal.Open(hs_path)
    hs = ds_hs.GetRasterBand(1).ReadAsArray().astype(np.float32) / 255.0
    for ch in range(3):
        rgb[:, :, ch] = np.clip(rgb[:, :, ch] * (hs * 0.7 + 0.3), 0, 255)

    # Write RGB GeoTIFF
    driver = gdal.GetDriverByName('GTiff')
    ds_out = driver.Create(output_path, w, h, 3, gdal.GDT_Byte)
    ds_out.SetGeoTransform(ds_elev.GetGeoTransform())
    ds_out.SetProjection(ds_elev.GetProjection())
    for ch in range(3):
        ds_out.GetRasterBand(ch + 1).WriteArray(rgb[:, :, ch].astype(np.uint8))
    ds_out.FlushCache()
    ds_out = ds_hs = ds_elev = None
    return output_path



def apply_vegetation_style(layer: QgsRasterLayer) -> None:
    """Red → white → green ramp for VARI index."""
    lo, hi = -1.0, 1.0
    ramp = QgsColorRampShader(lo, hi)
    ramp.setColorRampType(QgsColorRampShader.Interpolated)
    ramp.setColorRampItemList([
        QgsColorRampShader.ColorRampItem(-1.0, QColor('#d73027')),
        QgsColorRampShader.ColorRampItem(-0.1, QColor('#fc8d59')),
        QgsColorRampShader.ColorRampItem( 0.0, QColor('#ffffbf')),
        QgsColorRampShader.ColorRampItem( 0.2, QColor('#91cf60')),
        QgsColorRampShader.ColorRampItem( 1.0, QColor('#1a9641')),
    ])
    shader = QgsRasterShader(lo, hi)
    shader.setRasterShaderFunction(ramp)
    renderer = QgsSingleBandPseudoColorRenderer(layer.dataProvider(), 1, shader)
    renderer.setClassificationMin(lo)
    renderer.setClassificationMax(hi)
    layer.setRenderer(renderer)
    layer.triggerRepaint()
