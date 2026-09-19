"""
Custom Vegetation Index (Prototype) — experimental, opt-in, not validated
for general use. Kept in its own module, separate from processor.py's
established functions, so it can be dropped entirely without touching the
main pipeline.

numpy/GDAL only (no scipy). _sobel and _zoom reproduce scipy.ndimage's
sobel() and zoom() (order 0/1, default boundary handling) so results match
the scipy-based version this was calibrated with.
"""

import os

import numpy as np
from osgeo import gdal


def _correlate1d(a, kernel, axis):
    """Length-3 correlation along one axis, edge sample repeated at the
    border (scipy.ndimage 'reflect' == numpy 'symmetric')."""
    pad = [(0, 0), (0, 0)]
    pad[axis] = (1, 1)
    p = np.pad(a, pad, mode='symmetric')
    n = a.shape[axis]
    sl = [slice(None), slice(None)]
    out = None
    for j, w in enumerate(kernel):
        sl[axis] = slice(j, j + n)
        term = np.float32(w) * p[tuple(sl)]
        out = term if out is None else out + term
    return out


def _sobel(a, axis):
    """scipy.ndimage.sobel(a, axis): derivative along `axis`, smoothing
    ([1, 2, 1]) along the other axis."""
    smoothed = _correlate1d(a, (1, 2, 1), 1 - axis)
    return _correlate1d(smoothed, (-1, 0, 1), axis)


def _zoom_coords(in_n, out_n):
    """scipy.ndimage.zoom (grid_mode=False) maps output index i to input
    coordinate i * (in_n - 1) / (out_n - 1)."""
    return np.arange(out_n, dtype=np.float64) * ((in_n - 1) / (out_n - 1))


def _zoom_bilinear(a, out_h, out_w):
    """scipy.ndimage.zoom(a, ..., order=1) to an (out_h, out_w) grid."""
    in_h, in_w = a.shape
    ys = _zoom_coords(in_h, out_h)
    xs = _zoom_coords(in_w, out_w)
    y0 = np.floor(ys).astype(np.int64)
    y1 = np.minimum(y0 + 1, in_h - 1)
    wy = (ys - y0).astype(np.float32)
    x0 = np.floor(xs).astype(np.int64)
    x1 = np.minimum(x0 + 1, in_w - 1)
    wx = (xs - x0).astype(np.float32)
    rows = a[y0, :] * (1.0 - wy)[:, None] + a[y1, :] * wy[:, None]
    return rows[:, x0] * (1.0 - wx)[None, :] + rows[:, x1] * wx[None, :]


def _zoom_nearest(a, out_h, out_w):
    """scipy.ndimage.zoom(a, ..., order=0) to an (out_h, out_w) grid."""
    in_h, in_w = a.shape
    yi = np.clip(np.floor(_zoom_coords(in_h, out_h) + 0.5).astype(np.int64), 0, in_h - 1)
    xi = np.clip(np.floor(_zoom_coords(in_w, out_w) + 0.5).astype(np.int64), 0, in_w - 1)
    return a[yi][:, xi]


def generate_custom_vegetation_index(dsm_path: str, dtm_path: str, ortho_path: str,
                                     output_path: str) -> str:
    """Prototype (test feature, not validated for general use).

    Seamlessly pulls VARI toward a low floor value wherever the CHM
    (DSM-DTM) surface has a low local gradient, as a proxy for artificial
    flat ground (roads, solar panels, graded clearings) that VARI's color
    ratio alone cannot distinguish from real vegetation. Uses a per-pixel
    Sobel gradient, not a windowed statistic, so narrow features like
    roads aren't blurred out by averaging.

    Known limitation: gradient magnitude reacts to slope itself, so a
    uniformly tilted but smooth surface (e.g. a pitched roof) still scores
    as "rough" and is not suppressed. Thresholds were calibrated against a
    single solar-panel-vs-forest test site. Assumes DSM, DTM and
    orthophoto share the same extent, as standard WebODM output does.
    """
    _NODATA = -9999.0

    dtm_ds = gdal.Open(dtm_path)
    gt = dtm_ds.GetGeoTransform()
    xmin = gt[0]
    ymax = gt[3]
    xmax = xmin + gt[1] * dtm_ds.RasterXSize
    ymin = ymax + gt[5] * dtm_ds.RasterYSize
    xres, yres = gt[1], abs(gt[5])
    srs = dtm_ds.GetProjection()
    dtm_band = dtm_ds.GetRasterBand(1)
    dtm = dtm_band.ReadAsArray().astype(np.float32)
    dtm_nodata_val = dtm_band.GetNoDataValue()
    dtm_ds = None

    tmp_dsm = output_path + '.tmp_dsm.tif'
    gdal.Warp(tmp_dsm, dsm_path,
              outputBounds=(xmin, ymin, xmax, ymax),
              xRes=xres, yRes=yres,
              dstSRS=srs, format='GTiff')
    dsm_ds = gdal.Open(tmp_dsm)
    dsm_band = dsm_ds.GetRasterBand(1)
    dsm = dsm_band.ReadAsArray().astype(np.float32)
    dsm_nodata_val = dsm_band.GetNoDataValue()
    dsm_ds = None
    if os.path.isfile(tmp_dsm):
        os.remove(tmp_dsm)

    # DSM/DTM側のnodata(WebODM実測範囲外の飛行境界等)を検出し、方形埋めにしない
    chm_nodata_mask = ~np.isfinite(dsm) | ~np.isfinite(dtm)
    if dtm_nodata_val is not None:
        chm_nodata_mask |= (dtm == dtm_nodata_val)
    if dsm_nodata_val is not None:
        chm_nodata_mask |= (dsm == dsm_nodata_val)

    chm = dsm - dtm
    chm[~np.isfinite(chm)] = 0.0

    gx = _sobel(chm, axis=1) / 8.0
    gy = _sobel(chm, axis=0) / 8.0
    roughness = np.sqrt(gx ** 2 + gy ** 2).astype(np.float32)

    ortho_ds = gdal.Open(ortho_path)
    ortho_gt = ortho_ds.GetGeoTransform()
    vw, vh = ortho_ds.RasterXSize, ortho_ds.RasterYSize
    r = ortho_ds.GetRasterBand(1).ReadAsArray().astype(np.float32)
    g = ortho_ds.GetRasterBand(2).ReadAsArray().astype(np.float32)
    b = ortho_ds.GetRasterBand(3).ReadAsArray().astype(np.float32)
    ortho_proj = ortho_ds.GetProjection()

    # オルソ側のnodata検出(generate_vegetation_indexと同じ3段判定)
    if ortho_ds.RasterCount >= 4 and ortho_ds.GetRasterBand(4).GetColorInterpretation() == gdal.GCI_AlphaBand:
        alpha = ortho_ds.GetRasterBand(4).ReadAsArray()
        ortho_nodata_mask = (alpha == 0)
    else:
        ortho_nodata_val = ortho_ds.GetRasterBand(1).GetNoDataValue()
        if ortho_nodata_val is not None:
            ortho_nodata_mask = (r == ortho_nodata_val)
        else:
            ortho_nodata_mask = (r == 0) & (g == 0) & (b == 0)

    roughness_hi = _zoom_bilinear(roughness, vh, vw)
    chm_nodata_hi = _zoom_nearest(chm_nodata_mask, vh, vw)
    final_nodata_mask = ortho_nodata_mask | chm_nodata_hi

    with np.errstate(divide='ignore', invalid='ignore'):
        vari = np.nan_to_num((g - r) / (g + r - b)).astype(np.float32)
    vari = np.clip(vari, -1.0, 1.0)

    R_LO, R_HI = 0.029465389251708985, 0.11422117948532105  # ソーラーパネル vs 森林の実地較正値
    FLOOR = -0.3
    GRAD = 0.5
    weight = np.clip((roughness_hi - R_LO) / (R_HI - R_LO), 0.0, 1.0)
    suppressed_value = FLOOR + GRAD * vari
    hybrid = weight * vari + (1.0 - weight) * suppressed_value
    hybrid = np.clip(hybrid, -1.0, 1.0).astype(np.float32)
    hybrid[final_nodata_mask] = _NODATA

    driver = gdal.GetDriverByName('GTiff')
    ds_out = driver.Create(output_path, vw, vh, 1, gdal.GDT_Float32)
    ds_out.SetGeoTransform(ortho_gt)
    ds_out.SetProjection(ortho_proj)
    band = ds_out.GetRasterBand(1)
    band.WriteArray(hybrid)
    band.SetNoDataValue(_NODATA)
    ds_out.FlushCache()
    ds_out = ortho_ds = None
    return output_path
