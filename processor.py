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
    """CHM = DSM - DTM。DSM を DTM グリッドに合わせてから演算する。"""
    from osgeo import gdal
    import os

    dtm_ds = gdal.Open(dtm_path)
    gt = dtm_ds.GetGeoTransform()
    xmin = gt[0]
    ymax = gt[3]
    xmax = xmin + gt[1] * dtm_ds.RasterXSize
    ymin = ymax + gt[5] * dtm_ds.RasterYSize
    xres, yres = gt[1], abs(gt[5])
    srs = dtm_ds.GetProjection()
    dtm_ds = None

    tmp_dsm = output_path + '.tmp.tif'
    gdal.Warp(tmp_dsm, dsm_path,
              outputBounds=(xmin, ymin, xmax, ymax),
              xRes=xres, yRes=yres,
              dstSRS=srs, format='GTiff')

    processing.run('gdal:rastercalculator', {
        'INPUT_A': tmp_dsm, 'BAND_A': 1,
        'INPUT_B': dtm_path, 'BAND_B': 1,
        'FORMULA': 'A-B',
        'OUTPUT': output_path,
    })

    if os.path.isfile(tmp_dsm):
        os.remove(tmp_dsm)
    return output_path


def generate_vegetation_index(ortho_path: str, output_path: str) -> str:
    """VARI = (G - R) / (G + R - B)  from RGB orthophoto (bands 1=R, 2=G, 3=B).
    nodata 領域は -9999 をセットし QGIS で透明描画させる。"""
    from osgeo import gdal
    _NODATA = -9999.0
    ds = gdal.Open(ortho_path)
    r = ds.GetRasterBand(1).ReadAsArray().astype(np.float32)
    g = ds.GetRasterBand(2).ReadAsArray().astype(np.float32)
    b = ds.GetRasterBand(3).ReadAsArray().astype(np.float32)

    # nodata マスク：アルファバンド（Band 4）優先、なければ nodata 値、最後に全ゼロ画素
    if ds.RasterCount >= 4 and ds.GetRasterBand(4).GetColorInterpretation() == gdal.GCI_AlphaBand:
        alpha = ds.GetRasterBand(4).ReadAsArray()
        nodata_mask = (alpha == 0)
    else:
        nodata_val = ds.GetRasterBand(1).GetNoDataValue()
        if nodata_val is not None:
            nodata_mask = (r == nodata_val)
        else:
            nodata_mask = (r == 0) & (g == 0) & (b == 0)

    vari = (g - r) / (g + r - b + 0.001)
    vari = np.clip(vari, -1.0, 1.0)
    vari[nodata_mask] = _NODATA

    h, w = r.shape
    driver = gdal.GetDriverByName('GTiff')
    ds_out = driver.Create(output_path, w, h, 1, gdal.GDT_Float32)
    ds_out.SetGeoTransform(ds.GetGeoTransform())
    ds_out.SetProjection(ds.GetProjection())
    band = ds_out.GetRasterBand(1)
    band.WriteArray(vari)
    band.SetNoDataValue(_NODATA)
    ds_out.FlushCache()
    ds_out = ds = None
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
    valid = valid[np.isfinite(valid)]
    if valid.size == 0:
        lo, hi = 0.0, 1.0
    else:
        lo, hi = float(np.nanmin(valid)), float(np.nanmax(valid))
    span = max(hi - lo, 1e-10)

    # nodata マスク（透明化対象）
    nodata_mask = ~np.isfinite(elev)
    if nodata is not None:
        nodata_mask |= (elev == nodata)

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

    # Write RGBA GeoTIFF（nodata 領域はアルファ 0 で透明）
    driver = gdal.GetDriverByName('GTiff')
    ds_out = driver.Create(output_path, w, h, 4, gdal.GDT_Byte)
    ds_out.SetGeoTransform(ds_elev.GetGeoTransform())
    ds_out.SetProjection(ds_elev.GetProjection())
    for ch in range(3):
        ds_out.GetRasterBand(ch + 1).WriteArray(rgb[:, :, ch].astype(np.uint8))
    alpha_band = np.where(nodata_mask, 0, 255).astype(np.uint8)
    band4 = ds_out.GetRasterBand(4)
    band4.WriteArray(alpha_band)
    band4.SetColorInterpretation(gdal.GCI_AlphaBand)
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
