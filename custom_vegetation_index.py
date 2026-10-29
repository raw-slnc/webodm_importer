"""
Forest Naturalness Index (Prototype) — experimental, opt-in, not validated
for general use. Kept in its own module, separate from processor.py's
established functions, so it can be dropped entirely without touching the
main pipeline.

numpy/GDAL only (no scipy). _sobel and _zoom reproduce scipy.ndimage's
sobel() and zoom() (order 0/1, default boundary handling) so results match
the scipy-based version this was calibrated with.
"""

import numpy as np
from osgeo import gdal

from . import processor


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


def _zoom_bilinear(a, out_h, out_w, y0=0, y1=None):
    """scipy.ndimage.zoom(a, ..., order=1) to an (out_h, out_w) grid.
    Only output rows y0:y1 are computed (same values as the full result)."""
    in_h, in_w = a.shape
    ys = _zoom_coords(in_h, out_h)[y0:y1]
    xs = _zoom_coords(in_w, out_w)
    y0i = np.floor(ys).astype(np.int64)
    y1i = np.minimum(y0i + 1, in_h - 1)
    wy = (ys - y0i).astype(np.float32)
    x0 = np.floor(xs).astype(np.int64)
    x1 = np.minimum(x0 + 1, in_w - 1)
    wx = (xs - x0).astype(np.float32)
    rows = a[y0i, :] * (1.0 - wy)[:, None] + a[y1i, :] * wy[:, None]
    return rows[:, x0] * (1.0 - wx)[None, :] + rows[:, x1] * wx[None, :]


def _zoom_nearest(a, out_h, out_w, y0=0, y1=None):
    """scipy.ndimage.zoom(a, ..., order=0) to an (out_h, out_w) grid.
    Only output rows y0:y1 are computed (same values as the full result)."""
    in_h, in_w = a.shape
    yi = np.clip(np.floor(_zoom_coords(in_h, out_h) + 0.5).astype(np.int64), 0, in_h - 1)[y0:y1]
    xi = np.clip(np.floor(_zoom_coords(in_w, out_w) + 0.5).astype(np.int64), 0, in_w - 1)
    return a[yi][:, xi]


def generate_forest_naturalness_index(dsm_path: str, dtm_path: str, ortho_path: str,
                                      output_path: str, progress=None) -> str:
    """Prototype (test feature, not validated for general use).

    Combines two signals to estimate how "natural" (vs. artificially flat or
    uniform) an area's vegetation structure is:

    - VARI colour ratio from the orthophoto, corrected for a shadow colour
      cast: open shade is lit mainly by scattered blue skylight rather than
      direct warm sunlight, which inflates the blue channel and shrinks
      VARI's denominator (G+R-B), making shadowed green read as more
      "healthy" than it is. A DSM-derived hillshade (same fixed-angle model
      as Surface Model*) estimates how shadowed each pixel likely is, and
      the blue channel's contribution is discounted proportionally. This
      hillshade does not need to match the real photographed shadow
      pixel-for-pixel (the ortho is a mosaic stitched from many capture
      times, so no single sun angle applies to the whole image) — it only
      needs to flag areas prone to this colour cast.
    - CHM (DSM-DTM) structural roughness (per-pixel Sobel gradient, not a
      windowed statistic, so narrow features aren't blurred out): flat areas
      are pulled toward a low floor value, as a proxy for artificial flat
      ground (roads, solar panels, graded clearings) that colour alone
      cannot distinguish from real vegetation; rough areas get a boost
      weighted by how much colour signal is already present, so it mainly
      reinforces areas with a real (non-neutral) colour reading rather than
      inventing one from geometry alone.

    Known limitations: gradient magnitude reacts to slope itself, so a
    uniformly tilted but smooth surface (e.g. a pitched roof) still scores
    as "rough". Thresholds were calibrated against a single solar-panel-vs-
    forest test site. Assumes DSM, DTM and orthophoto share the same
    extent, as standard WebODM output does.
    """
    _NODATA = -9999.0
    report = processor.report

    dtm_ds = processor.open_raster(dtm_path)
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
    tmp_shade = output_path + '.tmp_shade.tif'
    try:
        cb, state = processor.gdal_callback(progress, 0.0, 0.1)
        gdal.ErrorReset()
        warped = gdal.Warp(tmp_dsm, dsm_path,
                           outputBounds=(xmin, ymin, xmax, ymax),
                           xRes=xres, yRes=yres,
                           dstSRS=srs, format='GTiff',
                           creationOptions=['BIGTIFF=IF_NEEDED'],
                           callback=cb)
        if state['cancelled']:
            raise processor.Cancelled()
        if warped is None:
            raise RuntimeError(gdal.GetLastErrorMsg() or 'Aligning DSM to DTM failed')
        warped = None

        # Surface Model*と同じ固定角度hillshade（DTMグリッドに揃えたDSM上で計算）
        processor.generate_hillshade(
            tmp_dsm, tmp_shade,
            progress=(lambda f: report(progress, 0.1 + 0.1 * f)) if progress else None)
        shade_ds = processor.open_raster(tmp_shade)
        shade = shade_ds.GetRasterBand(1).ReadAsArray().astype(np.float32)
        shade_ds = None

        dsm_ds = processor.open_raster(tmp_dsm)
        dsm_band = dsm_ds.GetRasterBand(1)
        dsm = dsm_band.ReadAsArray().astype(np.float32)
        dsm_nodata_val = dsm_band.GetNoDataValue()
        dsm_ds = None
    finally:
        processor.remove_quietly(tmp_shade)
        processor.remove_quietly(tmp_dsm)

    # 本当の穴＝DTM自体が無い（地表そのものが不明）場合のみ。
    dtm_missing = ~np.isfinite(dtm)
    if dtm_nodata_val is not None:
        dtm_missing |= (dtm == dtm_nodata_val)
    chm_nodata_mask = dtm_missing

    # DSMだけ欠損（河川など写真測量が失敗しやすい箇所）はCHM=0(起伏なし)扱いにする。
    # DTM(地表)はあるので、「そこに構造物が無い」とみなす方が透明化するより実態に近い。
    dsm_missing = ~np.isfinite(dsm)
    if dsm_nodata_val is not None:
        dsm_missing |= (dsm == dsm_nodata_val)

    chm = dsm - dtm
    chm[dsm_missing & ~dtm_missing] = 0.0
    chm[~np.isfinite(chm)] = 0.0
    dsm = dtm = None

    gx = _sobel(chm, axis=1) / 8.0
    gy = _sobel(chm, axis=0) / 8.0
    roughness = np.sqrt(gx ** 2 + gy ** 2).astype(np.float32)
    gx = gy = chm = None
    report(progress, 0.25)

    ortho_ds = processor.open_raster(ortho_path)
    ortho_gt = ortho_ds.GetGeoTransform()
    vw, vh = ortho_ds.RasterXSize, ortho_ds.RasterYSize
    ortho_proj = ortho_ds.GetProjection()
    band_r, band_g, band_b = (ortho_ds.GetRasterBand(i) for i in (1, 2, 3))

    # オルソ側のnodata検出(generate_vegetation_indexと同じ3段判定)
    band_alpha = None
    if ortho_ds.RasterCount >= 4 and ortho_ds.GetRasterBand(4).GetColorInterpretation() == gdal.GCI_AlphaBand:
        band_alpha = ortho_ds.GetRasterBand(4)
    ortho_nodata_val = band_r.GetNoDataValue()

    ds_out = processor.create_raster(output_path, vw, vh, 1, gdal.GDT_Float32, ortho_gt, ortho_proj)
    out_band = ds_out.GetRasterBand(1)
    out_band.SetNoDataValue(_NODATA)

    # ここから先は画素ごとの計算なので、オルソの行ブロックごとに処理する
    # （DTM解像度の roughness/shade を該当行だけ拡大する。値は全体処理と同じ）。
    for y0, rows in processor.row_blocks(vw, vh):
        y1 = y0 + rows
        r = band_r.ReadAsArray(0, y0, vw, rows).astype(np.float32)
        g = band_g.ReadAsArray(0, y0, vw, rows).astype(np.float32)
        b = band_b.ReadAsArray(0, y0, vw, rows).astype(np.float32)
        if band_alpha is not None:
            ortho_nodata_mask = (band_alpha.ReadAsArray(0, y0, vw, rows) == 0)
        elif ortho_nodata_val is not None:
            ortho_nodata_mask = (r == ortho_nodata_val)
        else:
            ortho_nodata_mask = (r == 0) & (g == 0) & (b == 0)

        roughness_hi = _zoom_bilinear(roughness, vh, vw, y0, y1)
        shade_hi = _zoom_bilinear(shade, vh, vw, y0, y1)
        chm_nodata_hi = _zoom_nearest(chm_nodata_mask, vh, vw, y0, y1)
        final_nodata_mask = ortho_nodata_mask | chm_nodata_hi

        # 影は単に暗いだけでなく、直射光(暖色)が遮られ空からの散乱光(青みがかった光)が
        # 主になるため、Bチャンネルが相対的に強くなる色被りを起こす。VARIの分母(G+R-B)は
        # Bを引くため、この色被りだけで分母が縮み、影下の緑が実態より健全側(高VARI)に
        # 出てしまう。シェード(影らしさ)に応じてBの寄与を割り引き、この歪みを補正する。
        # 単純な明るさの一律な正規化は、r/g/bを同じ値で割るとVARIの比率計算で
        # 相殺されて無効なため使わない。仮値・未較正。
        B_CORRECTION = 0.3
        shadow_amount = np.clip(1.0 - shade_hi / 255.0, 0.0, 1.0)
        b_corrected = b * (1.0 - B_CORRECTION * shadow_amount)

        with np.errstate(divide='ignore', invalid='ignore'):
            vari = np.nan_to_num((g - r) / (g + r - b_corrected)).astype(np.float32)
        vari = np.clip(vari, -1.0, 1.0)

        # CHMの粗さで連続的に切り替え: 平坦地は健全度を引き下げ(suppress)、
        # 粗い地形は引き上げる(boost)。どちらも仮値・未較正。
        R_LO, R_HI = 0.029465389251708985, 0.11422117948532105  # ソーラーパネル vs 森林の実地較正値
        FLOOR = -0.3  # 平坦地での下限
        GRAD = 0.5    # 平坦地でのvariの効き具合
        BOOST = 0.15  # 粗い地形での加算量
        weight = np.clip((roughness_hi - R_LO) / (R_HI - R_LO), 0.0, 1.0)
        suppressed_value = FLOOR + GRAD * vari
        # 色が中立(白・グレー等、|vari|が小さい)な場所と、既に濃い
        # (|vari|が1に近い)場所の両方でブーストを弱める非対称の山型の重み。
        # ピークを|vari|=0.25付近の低めに置き、そこから先(1-|vari|)^3で
        # 急速に減衰させることで、中〜高VARI域(密な樹冠)が上限付近に
        # 圧縮されて階調が潰れるのを防ぐ。ピーク値は1になるよう正規化。
        av = np.abs(vari)
        color_confidence = (256.0 / 27.0) * av * (1.0 - av) ** 3
        boosted_value = vari + BOOST * color_confidence
        hybrid = weight * boosted_value + (1.0 - weight) * suppressed_value
        hybrid = np.clip(hybrid, -1.0, 1.0).astype(np.float32)
        hybrid[final_nodata_mask] = _NODATA
        processor.write_block(out_band, hybrid, y0)
        report(progress, 0.25 + 0.75 * y1 / vh)

    processor.close_raster(ds_out)
    ortho_ds = None
    return output_path
