"""
Processing pipeline: raster generation and styling.

Generation functions run in a worker thread (see derivative_worker.py), so they
use GDAL directly instead of QGIS processing, read and write in row blocks to
keep memory use flat on large rasters, and accept an optional
progress(fraction) callback. The callback raises Cancelled to stop.
"""

import os

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

_RDYLGN_RAMP = [
    '#a50026',
    '#d73027',
    '#f46d43',
    '#fdae61',
    '#fee08b',
    '#ffffbf',
    '#d9ef8b',
    '#a6d96a',
    '#66bd63',
    '#1a9850',
    '#006837',
]

# 1ブロックあたりの画素数の目安（行単位で区切る）。float32 の作業配列が
# 十数枚あっても数百MBに収まる大きさ。
BLOCK_PIXELS = 4_000_000

# gdal_calc が Float32 出力で既定にする NoData 値（CHM の互換用）
_CALC_FLOAT32_NODATA = 3.402823466e38


class Cancelled(Exception):
    """利用者の中断。progress コールバックから送出される。"""


# ── GDAL helpers ────────────────────────────────────────
# gdal.UseExceptions() はプロセス全体（他プラグイン含む）に効くため使わず、
# 戻り値と直前のエラー（スレッドごと）を見て例外に変える。

def _gdal_error(default):
    msg = gdal.GetLastErrorMsg()
    return RuntimeError(msg or default)


def open_raster(path):
    gdal.ErrorReset()
    ds = gdal.Open(path)
    if ds is None:
        raise _gdal_error(f'Cannot open {os.path.basename(path)}')
    return ds


def create_raster(path, width, height, bands, data_type, gt, proj, options=None):
    gdal.ErrorReset()
    driver = gdal.GetDriverByName('GTiff')
    ds = driver.Create(path, width, height, bands, data_type,
                       options=['BIGTIFF=IF_NEEDED'] + list(options or []))
    if ds is None:
        raise _gdal_error(f'Cannot create {os.path.basename(path)}')
    ds.SetGeoTransform(gt)
    ds.SetProjection(proj)
    return ds


def write_block(band, arr, yoff):
    gdal.ErrorReset()
    if band.WriteArray(arr, 0, yoff) != 0:
        raise _gdal_error('Writing raster data failed')


def close_raster(ds):
    """書き込みを確定する。ディスク不足などはここで表面化することがある。"""
    gdal.ErrorReset()
    ds.FlushCache()
    failed = gdal.GetLastErrorType() >= gdal.CE_Failure
    msg = gdal.GetLastErrorMsg()
    ds = None
    if failed:
        raise RuntimeError(msg or 'Writing raster data failed')


def row_blocks(width, height):
    """(yoff, rows) を順に返す。"""
    rows = max(1, BLOCK_PIXELS // max(1, width))
    for yoff in range(0, height, rows):
        yield yoff, min(rows, height - yoff)


def report(progress, fraction):
    if progress is not None:
        progress(min(max(fraction, 0.0), 1.0))


def gdal_callback(progress, start=0.0, end=1.0):
    """GDAL の進捗コールバック。中断時は 0 を返して GDAL 側を止める。"""
    state = {'cancelled': False}

    def _cb(complete, _msg, _data):
        try:
            report(progress, start + (end - start) * complete)
        except Cancelled:
            state['cancelled'] = True
            return 0
        return 1
    return _cb, state


def remove_quietly(path):
    try:
        if path and os.path.isfile(path):
            os.remove(path)
    except OSError:
        pass


# ── Generators ──────────────────────────────────────────

def generate_chm(dsm_path: str, dtm_path: str, output_path: str, progress=None) -> str:
    """CHM = DSM - DTM。DSM を DTM グリッドに合わせてから演算する。
    NoData の扱いは以前使っていた gdal_calc（--calc "A-B" --type Float32）と同じ。"""
    dtm_ds = open_raster(dtm_path)
    gt = dtm_ds.GetGeoTransform()
    w, h = dtm_ds.RasterXSize, dtm_ds.RasterYSize
    xmin = gt[0]
    ymax = gt[3]
    xmax = xmin + gt[1] * w
    ymin = ymax + gt[5] * h
    xres, yres = gt[1], abs(gt[5])
    srs = dtm_ds.GetProjection()

    tmp_dsm = output_path + '.tmp.tif'
    try:
        cb, state = gdal_callback(progress, 0.0, 0.5)
        gdal.ErrorReset()
        warped = gdal.Warp(tmp_dsm, dsm_path,
                           outputBounds=(xmin, ymin, xmax, ymax),
                           xRes=xres, yRes=yres,
                           dstSRS=srs, format='GTiff',
                           creationOptions=['BIGTIFF=IF_NEEDED'],
                           callback=cb)
        if state['cancelled']:
            raise Cancelled()
        if warped is None:
            raise _gdal_error('Aligning DSM to DTM failed')
        warped = None

        dsm_ds = open_raster(tmp_dsm)
        band_a = dsm_ds.GetRasterBand(1)
        band_b = dtm_ds.GetRasterBand(1)
        ndv_a = band_a.GetNoDataValue()
        ndv_b = band_b.GetNoDataValue()
        out_ds = create_raster(output_path, w, h, 1, gdal.GDT_Float32,
                               dtm_ds.GetGeoTransform(), srs)
        out_band = out_ds.GetRasterBand(1)
        out_band.SetNoDataValue(_CALC_FLOAT32_NODATA)
        for yoff, rows in row_blocks(w, h):
            a = band_a.ReadAsArray(0, yoff, w, rows)
            b = band_b.ReadAsArray(0, yoff, w, rows)
            result = a - b
            ndvs = None
            for val, ndv in ((a, ndv_a), (b, ndv_b)):
                if ndv is not None:
                    if ndvs is None:
                        ndvs = np.zeros(val.shape)
                    ndvs = 1 * np.logical_or(ndvs == 1, val == ndv)
            if ndvs is not None:
                result = ((1 * (ndvs == 0)) * result) + (_CALC_FLOAT32_NODATA * ndvs)
            write_block(out_band, result, yoff)
            report(progress, 0.5 + 0.5 * (yoff + rows) / h)
        dsm_ds = None
        close_raster(out_ds)
    finally:
        dtm_ds = None
        remove_quietly(tmp_dsm)
    return output_path


def generate_vegetation_index(ortho_path: str, output_path: str, progress=None) -> str:
    """VARI = (G - R) / (G + R - B) from RGB orthophoto (bands 1=R, 2=G, 3=B).
    nodata 領域は -9999 をセットし QGIS で透明描画させる。"""
    _NODATA = -9999.0
    ds = open_raster(ortho_path)
    w, h = ds.RasterXSize, ds.RasterYSize
    band_r, band_g, band_b = (ds.GetRasterBand(i) for i in (1, 2, 3))

    # nodata マスク：アルファバンド（Band 4）優先、なければ nodata 値、最後に全ゼロ画素
    band_alpha = None
    if ds.RasterCount >= 4 and ds.GetRasterBand(4).GetColorInterpretation() == gdal.GCI_AlphaBand:
        band_alpha = ds.GetRasterBand(4)
    nodata_val = band_r.GetNoDataValue()

    out_ds = create_raster(output_path, w, h, 1, gdal.GDT_Float32,
                           ds.GetGeoTransform(), ds.GetProjection())
    out_band = out_ds.GetRasterBand(1)
    out_band.SetNoDataValue(_NODATA)
    for yoff, rows in row_blocks(w, h):
        r = band_r.ReadAsArray(0, yoff, w, rows).astype(np.float32)
        g = band_g.ReadAsArray(0, yoff, w, rows).astype(np.float32)
        b = band_b.ReadAsArray(0, yoff, w, rows).astype(np.float32)
        if band_alpha is not None:
            nodata_mask = (band_alpha.ReadAsArray(0, yoff, w, rows) == 0)
        elif nodata_val is not None:
            nodata_mask = (r == nodata_val)
        else:
            nodata_mask = (r == 0) & (g == 0) & (b == 0)

        with np.errstate(divide='ignore', invalid='ignore'):
            vari = np.nan_to_num((g - r) / (g + r - b)).astype(np.float32)
        vari[(vari > 1e30) | (vari < -1e30)] = _NODATA
        vari[nodata_mask] = _NODATA
        write_block(out_band, vari, yoff)
        report(progress, (yoff + rows) / h)
    close_raster(out_ds)
    ds = None
    return output_path


def generate_hillshade(dtm_path: str, output_path: str, progress=None) -> str:
    """Hillshade from DTM（gdaldem hillshade -z 1 -s 1 -az 315 -alt 45 と同じ）。"""
    cb, state = gdal_callback(progress)
    gdal.ErrorReset()
    ds = gdal.DEMProcessing(output_path, dtm_path, 'hillshade',
                            format='GTiff', band=1,
                            zFactor=1.0, scale=1.0, azimuth=315.0, altitude=45.0,
                            creationOptions=['BIGTIFF=IF_NEEDED'],
                            callback=cb)
    if state['cancelled']:
        ds = None
        raise Cancelled()
    if ds is None:
        raise _gdal_error('Hillshade generation failed')
    close_raster(ds)
    return output_path


def _elevation_rgb(elev, hs, lo, span):
    t = np.clip((elev - lo) / span, 0.0, 1.0)
    rgb = np.zeros(elev.shape + (3,), dtype=np.float32)
    for i in range(len(_ELEV_RAMP) - 1):
        t0, c0 = _ELEV_RAMP[i]
        t1, c1 = _ELEV_RAMP[i + 1]
        in_seg = (t >= t0) & (t <= t1)
        alpha = np.where(in_seg, (t - t0) / max(t1 - t0, 1e-10), 0.0)
        for ch in range(3):
            rgb[:, :, ch] += np.where(in_seg, (1 - alpha) * c0[ch] + alpha * c1[ch], 0.0)

    # Hillshade Multiply at 70% opacity: result = base * (hs * 0.7 + 0.3)
    for ch in range(3):
        rgb[:, :, ch] = np.clip(rgb[:, :, ch] * (hs * 0.7 + 0.3), 0, 255)
    return rgb


def render_elevation_composite(elev_path: str, hs_path: str, output_path: str, progress=None) -> str:
    """Bake elevation color ramp + hillshade (Multiply 70%) into a single RGB GeoTIFF."""
    ds_elev = open_raster(elev_path)
    ds_hs = open_raster(hs_path)
    w, h = ds_elev.RasterXSize, ds_elev.RasterYSize
    band = ds_elev.GetRasterBand(1)
    hs_band = ds_hs.GetRasterBand(1)
    nodata = band.GetNoDataValue()

    # 1周目: 全体の最小・最大（色の割り当て範囲）
    lo, hi = None, None
    for yoff, rows in row_blocks(w, h):
        elev = band.ReadAsArray(0, yoff, w, rows).astype(np.float32)
        valid = elev if nodata is None else elev[elev != nodata]
        valid = valid[np.isfinite(valid)]
        if valid.size:
            vmin, vmax = float(np.nanmin(valid)), float(np.nanmax(valid))
            lo = vmin if lo is None else min(lo, vmin)
            hi = vmax if hi is None else max(hi, vmax)
        report(progress, 0.3 * (yoff + rows) / h)
    if lo is None:
        lo, hi = 0.0, 1.0
    span = max(hi - lo, 1e-10)

    # 2周目: 着色・陰影合成・書き出し（Write RGBA GeoTIFF、nodata 領域はアルファ 0 で透明）
    ds_out = create_raster(output_path, w, h, 4, gdal.GDT_Byte,
                           ds_elev.GetGeoTransform(), ds_elev.GetProjection())
    out_bands = [ds_out.GetRasterBand(i + 1) for i in range(4)]
    out_bands[3].SetColorInterpretation(gdal.GCI_AlphaBand)
    for yoff, rows in row_blocks(w, h):
        elev = band.ReadAsArray(0, yoff, w, rows).astype(np.float32)
        hs = hs_band.ReadAsArray(0, yoff, w, rows).astype(np.float32) / 255.0
        nodata_mask = ~np.isfinite(elev)
        if nodata is not None:
            nodata_mask |= (elev == nodata)
        rgb = _elevation_rgb(elev, hs, lo, span)
        for ch in range(3):
            write_block(out_bands[ch], rgb[:, :, ch].astype(np.uint8), yoff)
        write_block(out_bands[3], np.where(nodata_mask, 0, 255).astype(np.uint8), yoff)
        report(progress, 0.3 + 0.7 * (yoff + rows) / h)
    close_raster(ds_out)
    ds_hs = ds_elev = None
    return output_path


def apply_vegetation_style(layer: QgsRasterLayer) -> None:
    """Apply WebODM-like RdYlGn styling to a raw, single-band vegetation index.

    Uses VARI's fixed (-1, 1) range, matching WebODM's own formulas.py
    definition, instead of a per-raster percentile stretch: a percentile
    stretch gets dragged by the long tail of near-zero-denominator outliers
    VARI produces over shadow, so real vegetation signal was being
    compressed into a narrow band at the red end of the ramp.
    """
    lo, hi = -1.0, 1.0
    ramp = QgsColorRampShader(lo, hi)
    ramp.setColorRampType(QgsColorRampShader.Type.Interpolated)
    step = (hi - lo) / (len(_RDYLGN_RAMP) - 1)
    ramp.setColorRampItemList([
        QgsColorRampShader.ColorRampItem(lo + step * i, QColor(color))
        for i, color in enumerate(_RDYLGN_RAMP)
    ])
    shader = QgsRasterShader(lo, hi)
    shader.setRasterShaderFunction(ramp)
    renderer = QgsSingleBandPseudoColorRenderer(layer.dataProvider(), 1, shader)
    renderer.setClassificationMin(lo)
    renderer.setClassificationMax(hi)
    layer.setRenderer(renderer)
    layer.triggerRepaint()
