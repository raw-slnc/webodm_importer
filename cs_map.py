"""
CS Map (CS-style terrain visualization) from DTM.

Ported from Forestry Operations Lite (terrain/cs_map.py, terrain/analysis.py)
so this plugin does not depend on FOL being installed. The colour preset and
the compositing steps are kept identical to FOL's.

numpy/GDAL only (no scipy). _gaussian_filter reproduces
scipy.ndimage.gaussian_filter(mode='nearest', truncate=4.0) so results match
FOL's scipy-based output.
"""

import numpy as np
from osgeo import gdal, osr

from . import processor

# DTM finer than this (m) is resampled to _RESAMPLE_TARGET before processing
# (same thresholds as FOL's CS Map Export).
_RESAMPLE_THRESHOLD = 0.1
_RESAMPLE_TARGET = 0.5

_PRESET = {
    "smooth_sigma_px": 1.0,
    "slope_percentiles": (3.0, 97.0),
    "elevation_percentiles": (2.0, 98.0),
    "curvature_percentile": 97.5,
    "ridge_gamma": 0.28,
    "valley_gamma": 0.28,
    "elevation_gray": (26.0, 114.0),
    "elevation_gamma": 1.78,
    "slope_brown_light": (228, 216, 190),
    "slope_brown_dark": (112, 62, 30),
    "slope_brown_alpha": 0.36,
    "warm_neutral": (166, 138, 118),
    "warm_neutral_alpha": 0.045,
    "curvature_neutral": (214, 194, 166),
    "valley_blue": (8, 42, 132),
    "valley_alpha": (0.90, 0.34),
    "valley_core_start": 0.28,
    "valley_core_light": (62, 78, 122),
    "valley_core_dark": (1, 14, 118),
    "valley_core_alpha": 0.40,
    "ridge_red": (132, 42, 24),
    "ridge_alpha": (1.12, 0.28),
    "ridge_slope_cutoff": 0.135,
    "ridge_slope_gate_gamma": 0.24,
    "ridge_valley_fade": 0.12,
    "slope_black": (18, 18, 18),
    "slope_black_alpha": 0.40,
    "contrast": (104.0, 1.14, 108.0),
    "plain_desaturate_cutoff": 0.22,
    "plain_desaturate_strength": 0.28,
    "plain_desaturate_gamma": 1.25,
    "color_mask_sigma_px": 0.9,
    "valley_spread_sigma_px": 3.25,
    "valley_ridge_fade": 0.08,
    "valley_depth_sigma_px": 8.0,
    "valley_depth_spread_sigma_px": 7.0,
    "valley_depth_percentiles": (34.0, 98.5),
    "valley_depth_gamma": 0.44,
    "valley_depth_floor": 0.66,
    "valley_depth_low_range": (0.06, 0.24),
    "valley_depth_mid_range": (0.24, 0.56),
    "valley_depth_high_range": (0.56, 0.86),
    "valley_depth_low_boost": 0.09,
    "valley_depth_mid_boost": 0.23,
    "valley_depth_peak_boost": 0.52,
    "valley_depth_support": 0.34,
    "valley_color_smooth_sigma_px": 1.10,
    "valley_convex_block": 0.92,
    "valley_plain_depth_range": (0.18, 0.42),
    "valley_plain_depth_support": 0.88,
    "valley_slope_gate_gamma": 0.28,
    "valley_slope_cutoff": 0.075,
    "final_saturation": 1.68,
    "final_luma_contrast": 1.32,
    "final_luma_center": 118.0,
    "final_luma_offset": 5.0,
    "final_density": 0.96,
}


def _slope_deg(dem, cell_size):
    """Horn法で傾斜角（度）を計算"""
    pad = np.pad(dem, 1, mode="edge")
    dzdx = (pad[1:-1, 2:] - pad[1:-1, :-2]) / (2.0 * cell_size)
    dzdy = (pad[2:, 1:-1] - pad[:-2, 1:-1]) / (2.0 * cell_size)
    result = np.degrees(np.arctan(np.sqrt(dzdx ** 2 + dzdy ** 2)))
    result[np.isnan(dem)] = np.nan
    return result


def _curvature(dem, cell_size):
    """平均曲率（ラプラシアン近似）。正値=凸地形（尾根）、負値=凹地形（谷）。"""
    pad = np.pad(dem, 1, mode="edge")
    r = (pad[1:-1, 2:] - 2.0 * dem + pad[1:-1, :-2]) / (cell_size ** 2)
    t = (pad[2:, 1:-1] - 2.0 * dem + pad[:-2, 1:-1]) / (cell_size ** 2)
    curv = -(r + t)
    curv[np.isnan(dem)] = np.nan
    return curv


def _gaussian_filter(data, sigma):
    """scipy.ndimage.gaussian_filter(mode='nearest', truncate=4.0) 相当の分離型畳み込み。"""
    radius = int(4.0 * sigma + 0.5)
    x = np.arange(-radius, radius + 1, dtype=np.float64)
    kernel = np.exp(-0.5 * (x / sigma) ** 2)
    kernel /= kernel.sum()
    out = np.asarray(data, dtype=np.float64)
    for axis in (0, 1):
        n = out.shape[axis]
        pad_width = [(0, 0), (0, 0)]
        pad_width[axis] = (radius, radius)
        padded = np.pad(out, pad_width, mode="edge")
        acc = np.zeros_like(out)
        for i, w in enumerate(kernel):
            acc += w * (padded[i:i + n, :] if axis == 0 else padded[:, i:i + n])
        out = acc
    return out


def _robust_unit(data, p_low=2.0, p_high=98.0):
    valid = np.isfinite(data)
    if not np.any(valid):
        return np.zeros(data.shape, dtype=np.float32)
    lo, hi = np.nanpercentile(data[valid], [p_low, p_high])
    if not np.isfinite(lo) or not np.isfinite(hi) or hi <= lo:
        return np.zeros(data.shape, dtype=np.float32)
    return np.clip((data - lo) / (hi - lo), 0.0, 1.0).astype(np.float32)


def _smooth_nan(data, sigma_px):
    valid = np.isfinite(data)
    if not np.any(valid) or sigma_px <= 0:
        return data
    smoothed = _gaussian_filter(np.where(valid, data, 0.0), sigma_px)
    weights = _gaussian_filter(valid.astype(np.float64), sigma_px)
    with np.errstate(invalid="ignore", divide="ignore"):
        out = smoothed / weights
    out[weights <= 0.0] = np.nan
    return out


def _lerp_color(a, b, t):
    t3 = np.clip(t, 0.0, 1.0)[..., None]
    ca = np.asarray(a, dtype=np.float32)
    cb = np.asarray(b, dtype=np.float32)
    return ca + (cb - ca) * t3


def _overlay(base, top, alpha):
    return base * (1.0 - alpha) + top * alpha


def _soft_alpha(t, strength, gamma=0.7):
    t = np.clip(t, 0.0, 1.0)
    return strength * np.power(t, gamma)[..., None]


def _smooth_range(data, edge0, edge1):
    if edge1 <= edge0:
        return np.where(data >= edge1, 1.0, 0.0).astype(np.float32)
    t = np.clip((data - edge0) / (edge1 - edge0), 0.0, 1.0)
    return (t * t * (3.0 - 2.0 * t)).astype(np.float32)


def compute_cs_map(dem, cell_size, progress=None):
    """DEM(NaN=nodata)から CS 立体図の RGBA (rows, cols, 4) uint8 を作る。
    progress(fraction) は段階ごとに呼ばれる（0〜1）。"""
    def _p(fraction):
        processor.report(progress, fraction)

    p = _PRESET
    smooth_dem = _smooth_nan(dem, p["smooth_sigma_px"])
    slope = _slope_deg(smooth_dem, cell_size)
    curv = _curvature(smooth_dem, cell_size)
    valid = np.isfinite(dem) & np.isfinite(slope) & np.isfinite(curv)
    _p(0.15)

    slope_u = _robust_unit(slope, *p["slope_percentiles"])
    elev_u = _robust_unit(smooth_dem, *p["elevation_percentiles"])
    elev_u = np.power(elev_u, p["elevation_gamma"])

    abs_curv = np.abs(curv)
    curv_valid = np.isfinite(abs_curv)
    cmax = np.nanpercentile(abs_curv[curv_valid], p["curvature_percentile"]) if np.any(curv_valid) else 0.0
    if not np.isfinite(cmax) or cmax <= 0:
        cmax = 1.0

    curv_s = np.clip(curv / cmax, -1.0, 1.0)
    ridge = np.clip(curv_s, 0.0, 1.0)
    valley = np.clip(-curv_s, 0.0, 1.0)
    ridge_mask = _smooth_nan(ridge, p["color_mask_sigma_px"])
    valley_mask = _smooth_nan(valley, p["valley_spread_sigma_px"])
    _p(0.3)
    valley_color_mask = valley_mask * (1.0 - p["valley_ridge_fade"] * np.clip(ridge_mask, 0.0, 1.0))
    depth_surface = _smooth_nan(smooth_dem, p["valley_depth_sigma_px"])
    _p(0.5)
    local_depth = np.clip(depth_surface - smooth_dem, 0.0, None)
    depth_u = _robust_unit(local_depth, *p["valley_depth_percentiles"])
    depth_u = np.power(depth_u, p["valley_depth_gamma"])
    depth_spread = np.clip(_smooth_nan(depth_u, p["valley_depth_spread_sigma_px"]), 0.0, 1.0)
    _p(0.65)
    depth_area = np.clip(0.30 * depth_u + 0.70 * depth_spread, 0.0, 1.0)
    depth_gate = (
        p["valley_depth_floor"]
        + p["valley_depth_low_boost"] * _smooth_range(depth_area, *p["valley_depth_low_range"])
        + p["valley_depth_mid_boost"] * _smooth_range(depth_area, *p["valley_depth_mid_range"])
        + p["valley_depth_peak_boost"] * _smooth_range(depth_area, *p["valley_depth_high_range"])
    )
    slope_gate = np.clip(
        (slope_u - p["valley_slope_cutoff"]) / (1.0 - p["valley_slope_cutoff"]), 0.0, 1.0)
    slope_gate = np.power(slope_gate, p["valley_slope_gate_gamma"])
    plain_depth_gate = _smooth_range(depth_area, *p["valley_plain_depth_range"])
    slope_gate = np.maximum(slope_gate, p["valley_plain_depth_support"] * plain_depth_gate)
    valley_color_mask = np.maximum(valley_color_mask, p["valley_depth_support"] * depth_area)
    valley_color_mask = valley_color_mask * depth_gate
    valley_color_mask = _smooth_nan(valley_color_mask, p["valley_color_smooth_sigma_px"])
    valley_color_mask *= 1.0 - p["valley_convex_block"] * np.clip(ridge, 0.0, 1.0)
    valley_color_mask = valley_color_mask * slope_gate
    ridge_t = np.power(np.clip(ridge_mask, 0.0, 1.0), p["ridge_gamma"])
    valley_t = np.power(np.clip(valley_color_mask, 0.0, 1.0), p["valley_gamma"])

    _p(0.75)
    gray_base, gray_scale = p["elevation_gray"]
    gray = (gray_base + gray_scale * elev_u).astype(np.float32)
    rgb = np.stack([gray, gray, gray], axis=-1)

    # 傾斜：白→茶の色付けと白→黒の陰影
    slope_brown = _lerp_color(p["slope_brown_light"], p["slope_brown_dark"], slope_u)
    rgb = _overlay(rgb, slope_brown, p["slope_brown_alpha"])
    rgb = _overlay(rgb, np.full_like(rgb, p["warm_neutral"], dtype=np.float32), p["warm_neutral_alpha"])

    # 尾根（凸）：赤茶
    curv_neutral = p["curvature_neutral"]
    ridge_red = _lerp_color(curv_neutral, p["ridge_red"], ridge_t)
    ridge_alpha = _soft_alpha(ridge_mask, *p["ridge_alpha"])
    ridge_slope_gate = np.clip(
        (slope_u - p["ridge_slope_cutoff"]) / (1.0 - p["ridge_slope_cutoff"]), 0.0, 1.0)
    ridge_alpha *= np.power(ridge_slope_gate, p["ridge_slope_gate_gamma"])[..., None]
    ridge_alpha *= 1.0 - p["ridge_valley_fade"] * np.clip(valley_mask, 0.0, 1.0)[..., None]
    rgb = _overlay(rgb, ridge_red, ridge_alpha)

    # 谷（凹）：青。範囲は窪みから柔らかく広げ、芯の強さは平滑化前の凹みに従う
    curv_blue = _lerp_color(curv_neutral, p["valley_blue"], valley_t)
    rgb = _overlay(rgb, curv_blue, _soft_alpha(valley_color_mask, *p["valley_alpha"]))
    core_start = p["valley_core_start"]
    valley_core = np.clip((valley - core_start) / (1.0 - core_start), 0.0, 1.0)
    valley_core *= 0.50 + 0.50 * depth_area
    core_blue = _lerp_color(p["valley_core_light"], p["valley_core_dark"], np.sqrt(valley_core))
    rgb = _overlay(rgb, core_blue, p["valley_core_alpha"] * valley_core[..., None])

    slope_black = _lerp_color((255, 255, 255), p["slope_black"], slope_u)
    rgb = _overlay(rgb, slope_black, p["slope_black_alpha"])

    contrast_center, contrast_factor, contrast_offset = p["contrast"]
    rgb = (rgb - contrast_center) * contrast_factor + contrast_offset
    luminance = 0.299 * rgb[..., 0:1] + 0.587 * rgb[..., 1:2] + 0.114 * rgb[..., 2:3]
    rgb = luminance + (rgb - luminance) * p["final_saturation"]
    plain_u = 1.0 - np.clip(slope_u / p["plain_desaturate_cutoff"], 0.0, 1.0)
    plain_u = np.power(plain_u, p["plain_desaturate_gamma"])
    plain_sat = 1.0 - p["plain_desaturate_strength"] * plain_u
    rgb = luminance + (rgb - luminance) * plain_sat[..., None]
    rgb = (rgb - p["final_luma_center"]) * p["final_luma_contrast"]
    rgb += p["final_luma_center"] + p["final_luma_offset"]
    rgb *= p["final_density"]

    rgba = np.zeros(dem.shape + (4,), dtype=np.uint8)
    for ch in range(3):
        rgba[..., ch] = np.clip(np.where(valid, rgb[..., ch], 0.0), 0, 255).astype(np.uint8)
    rgba[..., 3] = np.where(valid, 255, 0).astype(np.uint8)
    return rgba


def _read_dtm(dtm_path, progress=None):
    """DTMを読み込み (data[NaN=nodata], geotransform, projection) を返す。
    0.1m未満の解像度は 0.5m へ平均値で縮小する。"""
    ds = processor.open_raster(dtm_path)
    gt = ds.GetGeoTransform()
    srs = osr.SpatialReference(wkt=ds.GetProjection())
    if srs.IsGeographic():
        ds = None
        raise ValueError('CS Map requires a projected CRS (metres).')
    if abs(gt[1]) < _RESAMPLE_THRESHOLD:
        cb, state = processor.gdal_callback(progress)
        gdal.ErrorReset()
        warped = gdal.Warp('', ds, format='MEM', xRes=_RESAMPLE_TARGET, yRes=_RESAMPLE_TARGET,
                           resampleAlg='average', callback=cb)
        if state['cancelled']:
            raise processor.Cancelled()
        if warped is None:
            raise RuntimeError(gdal.GetLastErrorMsg() or 'Resampling the DTM failed')
        ds = warped
        gt = ds.GetGeoTransform()
    band = ds.GetRasterBand(1)
    data = band.ReadAsArray().astype(np.float32)
    nodata = band.GetNoDataValue()
    if nodata is not None:
        data[data == nodata] = np.nan
    data[~np.isfinite(data)] = np.nan
    proj = ds.GetProjection()
    ds = None
    return data, gt, proj


def generate_cs_map(dtm_path: str, output_path: str, progress=None) -> str:
    """DTMから CS 立体図の RGBA GeoTIFF を作る（データ外はアルファ0で透明）。"""
    dem, gt, proj = _read_dtm(
        dtm_path, progress=(lambda f: processor.report(progress, 0.2 * f)) if progress else None)
    rgba = compute_cs_map(
        dem, abs(gt[1]),
        progress=(lambda f: processor.report(progress, 0.2 + 0.7 * f)) if progress else None)
    rows, cols = dem.shape
    ds_out = processor.create_raster(output_path, cols, rows, 4, gdal.GDT_Byte, gt, proj,
                                     options=['COMPRESS=LZW', 'TILED=YES', 'PHOTOMETRIC=RGB', 'ALPHA=YES'])
    interp = [gdal.GCI_RedBand, gdal.GCI_GreenBand, gdal.GCI_BlueBand, gdal.GCI_AlphaBand]
    for i in range(4):
        band = ds_out.GetRasterBand(i + 1)
        processor.write_block(band, rgba[:, :, i], 0)
        band.SetColorInterpretation(interp[i])
    ds_out.SetMetadataItem(
        'METHOD_ATTRIBUTION',
        'CS-style terrain visualization method proposed by Nagano Prefecture Forestry Research Center.')
    ds_out.SetMetadataItem(
        'DATA_NOTE',
        'Photogrammetric (WebODM) DTM has low ground-surface accuracy, especially under canopy.')
    processor.close_raster(ds_out)
    processor.report(progress, 1.0)
    return output_path
