"""
OWP QC grading and HUC-level metrics for GFM scenes.

Implements the GFM Eval Metrics methodology with dynamic metadata handling:
per-HUC reliability (Data Quality Grade A/B/C/D), severity (Impact Score),
and scene-level aggregation. Uses mosaic-then-clip to avoid double-counting
when HUCs cross tile boundaries.
"""

import logging
import os
from typing import Any, Dict, List, Optional, Tuple, Union

logger = logging.getLogger(__name__)

import geopandas as gpd
import numpy as np
import rasterio
from rasterio.merge import merge
from rasterio.mask import mask
from shapely import Geometry

REQUIRED_LAYERS = [
    "ENSEMBLE_FLOOD",
    "ENSEMBLE_UNCERTAINTY",
    "ENSEMBLE_EXCLAYER",
    "ENSEMBLE_OBSWATER",
    "ADVFLAG",
    "POP",
]

# PUM Reference: 1 pixel = 20m * 20m = 0.0004 km2
# We use this for the area calculation formula.
REF_KM2_PER_PIXEL = 0.0004

def _resolve_layer_keys(
    s3_utils: Any,
    bucket_name: str,
    sent_ti_path: str,
    equi7tile: str,
) -> Dict[str, Optional[str]]:
    """Resolve S3 keys for all required layers for a specific equi7tile.

    Args:
        s3_utils: S3 utilities object with list_resources_with_string.
        bucket_name: S3 bucket name.
        sent_ti_path: S3 prefix for the scene (e.g. benchmark/rs/PI4/date/sent_ti/).
        equi7tile: Equi7 tile id (e.g. E078N027T3).

    Returns:
        Dict mapping layer name (e.g. ENSEMBLE_FLOOD) to S3 key or None if missing.
        On S3 listing failure, returns a dict with all layer keys mapped to None.
    """
    try:
        all_keys = s3_utils.list_resources_with_string(
            bucket_name, sent_ti_path, [equi7tile]
        )
    except Exception as e:
        logger.warning(
            "Failed to list S3 resources for tile %s: %s",
            equi7tile,
            e,
            exc_info=False,
        )
        return {ly: None for ly in REQUIRED_LAYERS}
    result = {}
    for layer in REQUIRED_LAYERS:
        found = [k for k in all_keys if layer in k]
        raster_found = [k for k in found if k.lower().endswith((".tif", ".tiff"))]
        result[layer] = raster_found[0] if raster_found else None
        if found and not raster_found:
            logger.debug(
                "No .tif/.tiff key for layer %s (tile %s); only non-raster keys matched.",
                layer,
                equi7tile,
            )
    return result

def _check_scene_completeness(
    s3_utils: Any,
    bucket_name: str,
    sent_ti_path: str,
    equi7tiles_list: List[str],
) -> bool:
    """Check whether at least one tile has all required layer files present and readable.

    Args:
        s3_utils: S3 utilities object with s3_client.
        bucket_name: S3 bucket name.
        sent_ti_path: S3 prefix for the scene.
        equi7tiles_list: List of equi7tile ids in the scene.

    Returns:
        True if at least one tile has all REQUIRED_LAYERS and each key passes head_object.
    """
    for equi7tile in equi7tiles_list:
        keys = _resolve_layer_keys(s3_utils, bucket_name, sent_ti_path, equi7tile)
        if not all(keys.get(ly) for ly in REQUIRED_LAYERS):
            continue
        try:
            for key in keys.values():
                if key:
                    s3_utils.s3_client.head_object(Bucket=bucket_name, Key=key)
            return True
        except Exception as e:
            logger.debug(
                "Scene completeness check failed for tile %s: %s",
                equi7tile,
                e,
                exc_info=False,
            )
            continue
    return False

def _get_raster_metadata(file_path: str) -> Optional[Dict[str, Any]]:
    """Read CRS, nodata, and resolution from a raster file.

    Args:
        file_path: Path to a GeoTIFF (local).

    Returns:
        Dict with keys "crs", "nodata", "res" (x_res, y_res in units of CRS),
        or None if the file cannot be opened.
    """
    try:
        with rasterio.open(file_path) as src:
            return {
                "crs": src.crs,
                "nodata": src.nodata,
                "res": src.res,
            }
    except (rasterio.RasterioIOError, OSError) as e:
        logger.warning("Failed to read raster metadata from %s: %s", file_path, e)
        return None

def _get_mosaic_only(
    layer_name: str,
    tile_files: Dict[str, str],
    detected_nodata: Optional[Union[int, float]],
) -> Optional[Tuple[np.ndarray, Any, Any, Optional[Union[int, float]]]]:
    """Build the full mosaic for one layer (open tiles, merge); no HUC mask.

    Args:
        layer_name: Layer identifier for logging (e.g. ENSEMBLE_FLOOD).
        tile_files: Dict mapping tile id to path (local or S3 URI) for this layer.
        detected_nodata: Nodata value for merge.

    Returns:
        (mosaic_ndarray, out_trans, crs, nodata) or None on failure.
        Mosaic has shape (count, height, width) from rasterio.merge.merge.
    """
    src_files_to_mosaic = []
    files_to_close = []
    work_nodata = detected_nodata if detected_nodata is not None else 255

    try:
        for fpath in tile_files.values():
            try:
                src = rasterio.open(fpath)
                files_to_close.append(src)
                src_files_to_mosaic.append(src)
            except (rasterio.RasterioIOError, OSError) as e:
                logger.debug("Could not open %s for mosaic: %s", fpath, e)
                continue

        if not src_files_to_mosaic:
            return None

        mosaic, out_trans = merge(src_files_to_mosaic, nodata=work_nodata)
        crs = src_files_to_mosaic[0].crs
        return (mosaic, out_trans, crs, work_nodata)
    except (rasterio.RasterioIOError, OSError, MemoryError) as e:
        logger.warning("Error processing mosaic for %s: %s", layer_name, e)
        return None
    except Exception as e:
        logger.warning("Unexpected error processing mosaic for %s: %s", layer_name, e)
        return None
    finally:
        for src in files_to_close:
            src.close()


def _mask_mosaic_to_geometry(
    mosaic: np.ndarray,
    transform: Any,
    crs: Any,
    nodata: Optional[Union[int, float]],
    geom: Geometry,
) -> Optional[np.ndarray]:
    """Mask a prebuilt mosaic to one geometry; return first band of masked array.

    Args:
        mosaic: Mosaic array (count, height, width).
        transform: Affine transform for the mosaic.
        crs: CRS of the mosaic.
        nodata: Nodata value.
        geom: Shapely geometry in the same CRS as the mosaic.

    Returns:
        First band of masked array (HUC clip), or None if mask fails.
    """
    work_nodata = nodata if nodata is not None else 255
    try:
        with rasterio.io.MemoryFile() as memfile:
            with memfile.open(
                driver="GTiff",
                height=mosaic.shape[1],
                width=mosaic.shape[2],
                count=mosaic.shape[0],
                dtype=mosaic.dtype,
                crs=crs,
                transform=transform,
                nodata=work_nodata,
            ) as dataset:
                dataset.write(mosaic)
                try:
                    out_image, _ = mask(
                        dataset,
                        [geom.__geo_interface__],
                        crop=True,
                        filled=True,
                    )
                    return out_image[0]
                except ValueError as e:
                    logger.debug(
                        "Mask failed (geometry may not overlap mosaic): %s",
                        e,
                    )
                    return None
    except (rasterio.RasterioIOError, OSError, MemoryError) as e:
        logger.warning("Error masking mosaic to geometry: %s", e)
        return None


def _metrics_from_layer_arrays(
    flood_arr: Optional[np.ndarray],
    unc_arr: Optional[np.ndarray],
    excl_arr: Optional[np.ndarray],
    adv_arr: Optional[np.ndarray],
    pop_arr: Optional[np.ndarray],
    obs_water_arr: Optional[np.ndarray],
    detected_nodata: Optional[Union[int, float]],
    metadata: Dict[str, Any],
    sent_ti_path: str,
) -> Dict[str, Any]:
    """Compute per-HUC metrics from the six layer arrays (masked to HUC).

    Args:
        flood_arr, unc_arr, excl_arr, adv_arr, pop_arr, obs_water_arr: Masked
            arrays for each layer; any may be None.
        detected_nodata: Nodata value for the rasters.
        metadata: Dict with "crs" (for WGS84 warning).
        sent_ti_path: Scene path for logging.

    Returns:
        Dict with flood_area_km2, uncertainty_mean, observability_pct,
        advisory_noise_pct, affected_pop, normalized_anomaly_ratio (and defaults).
    """
    metrics = {
        "flood_area_km2": 0.0,
        "affected_pop": 0.0,
        "observability_pct": 0.0,
        "uncertainty_mean": 0.0,
        "advisory_noise_pct": 0.0,
        "normalized_anomaly_ratio": 0.0,
    }

    if flood_arr is None:
        return metrics

    work_nodata = detected_nodata if detected_nodata is not None else 255
    valid_mask = (flood_arr != work_nodata)
    flood_pixels = (flood_arr == 1) & valid_mask
    flood_count = np.sum(flood_pixels)
    total_valid_pixels = np.sum(valid_mask)

    try:
        if metadata.get("crs") is not None and metadata["crs"].to_epsg() == 4326:
            logger.warning(
                "Scene %s appears to be WGS84. Area calcs using %s km2 constant may be invalid.",
                sent_ti_path,
                REF_KM2_PER_PIXEL,
            )
    except (AttributeError, TypeError):
        pass

    metrics["flood_area_km2"] = flood_count * REF_KM2_PER_PIXEL

    if unc_arr is not None and flood_count > 0 and unc_arr.shape == flood_arr.shape:
        unc_values = unc_arr[flood_pixels]
        if unc_values.size > 0:
            metrics["uncertainty_mean"] = float(np.mean(unc_values))

    if excl_arr is not None and total_valid_pixels > 0 and excl_arr.shape == flood_arr.shape:
        excl_pixels = (excl_arr == 1) & valid_mask
        excl_count = np.sum(excl_pixels)
        metrics["observability_pct"] = (1 - (excl_count / total_valid_pixels)) * 100.0

    if adv_arr is not None and flood_count > 0 and adv_arr.shape == flood_arr.shape:
        noisy_flood = flood_pixels & (adv_arr > 0)
        metrics["advisory_noise_pct"] = (np.sum(noisy_flood) / flood_count) * 100.0

    if pop_arr is not None and pop_arr.shape == flood_arr.shape:
        pop_valid = (pop_arr != work_nodata) & flood_pixels
        metrics["affected_pop"] = float(np.sum(pop_arr[pop_valid]))

    if obs_water_arr is not None and obs_water_arr.shape == flood_arr.shape:
        obs_water_count = np.sum((obs_water_arr == 1) & valid_mask)
        if obs_water_count > 0:
            metrics["normalized_anomaly_ratio"] = float(flood_count / obs_water_count)

    metrics["flood_area_km2"] = round(metrics["flood_area_km2"], 4)
    metrics["uncertainty_mean"] = round(metrics["uncertainty_mean"], 2)
    metrics["observability_pct"] = round(metrics["observability_pct"], 2)
    metrics["advisory_noise_pct"] = round(metrics["advisory_noise_pct"], 2)
    metrics["affected_pop"] = int(metrics["affected_pop"])
    metrics["normalized_anomaly_ratio"] = round(metrics["normalized_anomaly_ratio"], 4)
    return metrics

def _grade_qc(data_complete: bool, metrics: Dict[str, Any]) -> str:
    """Assign Data Quality Grade A/B/C/D using thresholds.

    Args:
        data_complete: True if all required layer files are present for the scene.
        metrics: Per-HUC metrics (observability_pct, advisory_noise_pct, etc.).

    Returns:
        "A", "B", "C", or "D".
    """
    if not data_complete:
        return "D"
    obs = metrics.get("observability_pct", 0.0)
    noise = metrics.get("advisory_noise_pct", 0.0)
    unc = metrics.get("uncertainty_mean", 0.0)
    flood_signal = (metrics.get("flood_area_km2", 0.0) or 0.0) > 0

    if obs < 50 or noise > 50: return "D"
    if unc > 75 and obs > 80 and noise < 5: return "A"
    if unc > 60 and obs > 60 and noise < 20: return "B"
    if flood_signal: return "C"
    return "D"

def _impact_score(flood_area: float, pop: float) -> str:
    """Assign Impact Score (High/Medium/Low) from flood area and affected population.

    Args:
        flood_area: Total flood area in km².
        pop: Affected population count.

    Returns:
        "High", "Medium", or "Low".
    """
    if pop > 100 or flood_area > 5.0:
        return "High"
    if pop > 10 or flood_area > 1.0:
        return "Medium"
    return "Low"

def compute_scene_qc(
    huc8_list: List[str],
    hucs_gdf: gpd.GeoDataFrame,
    sent_ti_path: str,
    equi7tiles_list: List[str],
    bucket_name: str,
    s3_utils: Any,
) -> Dict[str, Any]:
    """Compute OWP QC properties for a GFM scene (per-HUC metrics, grades, scene aggregation).

    Args:
        huc8_list: List of HUC8 ids that intersect the scene.
        hucs_gdf: GeoDataFrame with HUC8 column and geometry (CRS must be set).
        sent_ti_path: S3 prefix for the scene (e.g. benchmark/rs/PI4/date/sent_ti/).
        equi7tiles_list: List of equi7tile ids in the scene.
        bucket_name: S3 bucket name.
        s3_utils: S3 utilities (list_resources_with_string, s3_client, download_file).

    Returns:
        Dict to merge into STAC item properties: owp:qc_grade, owp:impact_score,
        owp:active_hucs, owp:total_flood_area_km2, owp:huc_summaries.
    """
    if not huc8_list or not equi7tiles_list:
        return _empty_owp_properties()

    # Pass CRS info to HUC GDF just in case it's missing (assuming 4326)
    if hucs_gdf.crs is None:
        logger.warning("HUC GeoDataFrame has no CRS. Assuming EPSG:4326.")
        hucs_gdf.set_crs("EPSG:4326", inplace=True)

    scene_data_complete = _check_scene_completeness(
        s3_utils, bucket_name, sent_ti_path, equi7tiles_list
    )

    huc_summaries = []
    total_flood_area_km2 = 0.0

    grade_rank = {"A": 4, "B": 3, "C": 2, "D": 1}
    impact_rank = {"High": 3, "Medium": 2, "Low": 1}
    scene_best_grade = "D"
    scene_highest_impact = "Low"
    active_hucs = []

    # Build S3 URIs once (scene-level)
    local_paths = {ly: {} for ly in REQUIRED_LAYERS}
    metadata = None
    for tile in equi7tiles_list:
        keys = _resolve_layer_keys(s3_utils, bucket_name, sent_ti_path, tile)
        if not keys["ENSEMBLE_FLOOD"]:
            continue
        for layer, key in keys.items():
            if key:
                local_paths[layer][tile] = f"s3://{bucket_name}/{key}"
        if metadata is None and local_paths["ENSEMBLE_FLOOD"].get(tile):
            metadata = _get_raster_metadata(local_paths["ENSEMBLE_FLOOD"][tile])
            if metadata is None:
                logger.debug(
                    "Could not read raster metadata from %s",
                    local_paths["ENSEMBLE_FLOOD"][tile],
                )

    if metadata is None:
        logger.debug("No valid flood raster metadata for scene %s", sent_ti_path)
        return _empty_owp_properties()

    try:
        if metadata["crs"] is not None and metadata["crs"].to_epsg() == 4326:
            logger.warning(
                "Scene %s appears to be WGS84. Area calcs using %s km2 constant may be invalid.",
                sent_ti_path,
                REF_KM2_PER_PIXEL,
            )
    except (AttributeError, TypeError):
        pass

    detected_nodata = metadata["nodata"] if metadata["nodata"] is not None else 255
    raster_crs = metadata["crs"]

    # Build mosaics once per layer (scene-level)
    mosaics = {}
    for layer in REQUIRED_LAYERS:
        result = _get_mosaic_only(layer, local_paths[layer], detected_nodata)
        mosaics[layer] = result

    # Per-HUC: mask prebuilt mosaics and compute metrics
    for huc8_id in huc8_list:
        try:
            row = hucs_gdf[hucs_gdf["HUC8"].astype(str) == str(huc8_id)]
            if row.empty:
                continue
            huc_geom = row.geometry.iloc[0]
            if huc_geom is None or huc_geom.is_empty:
                continue
        except Exception as e:
            logger.debug(
                "Skipping HUC %s (lookup or geometry error): %s",
                huc8_id,
                e,
                exc_info=False,
            )
            continue

        try:
            huc_gdf = gpd.GeoDataFrame(geometry=[huc_geom], crs=hucs_gdf.crs)
            huc_projected = huc_gdf.to_crs(raster_crs).geometry.iloc[0]
            if huc_projected.is_empty:
                continue
        except Exception as e:
            logger.debug("CRS/projection error for HUC %s: %s", huc8_id, e)
            continue

        flood_arr = None
        unc_arr = None
        excl_arr = None
        adv_arr = None
        pop_arr = None
        obs_water_arr = None
        if mosaics["ENSEMBLE_FLOOD"]:
            mosaic, trans, crs, nodata = mosaics["ENSEMBLE_FLOOD"]
            flood_arr = _mask_mosaic_to_geometry(mosaic, trans, crs, nodata, huc_projected)
        if mosaics["ENSEMBLE_UNCERTAINTY"]:
            mosaic, trans, crs, nodata = mosaics["ENSEMBLE_UNCERTAINTY"]
            unc_arr = _mask_mosaic_to_geometry(mosaic, trans, crs, nodata, huc_projected)
        if mosaics["ENSEMBLE_EXCLAYER"]:
            mosaic, trans, crs, nodata = mosaics["ENSEMBLE_EXCLAYER"]
            excl_arr = _mask_mosaic_to_geometry(mosaic, trans, crs, nodata, huc_projected)
        if mosaics["ADVFLAG"]:
            mosaic, trans, crs, nodata = mosaics["ADVFLAG"]
            adv_arr = _mask_mosaic_to_geometry(mosaic, trans, crs, nodata, huc_projected)
        if mosaics["POP"]:
            mosaic, trans, crs, nodata = mosaics["POP"]
            pop_arr = _mask_mosaic_to_geometry(mosaic, trans, crs, nodata, huc_projected)
        if mosaics["ENSEMBLE_OBSWATER"]:
            mosaic, trans, crs, nodata = mosaics["ENSEMBLE_OBSWATER"]
            obs_water_arr = _mask_mosaic_to_geometry(mosaic, trans, crs, nodata, huc_projected)

        metrics = _metrics_from_layer_arrays(
            flood_arr, unc_arr, excl_arr, adv_arr, pop_arr, obs_water_arr,
            detected_nodata, metadata, sent_ti_path,
        )

        grade = _grade_qc(scene_data_complete, metrics)
        impact = _impact_score(metrics["flood_area_km2"], metrics["affected_pop"])

        huc_summaries.append({
            "huc8_id": str(huc8_id),
            "qc_grade": grade,
            "impact": impact,
            "metrics": metrics,
        })

        total_flood_area_km2 += metrics["flood_area_km2"]

        if grade_rank[grade] > grade_rank[scene_best_grade]:
            scene_best_grade = grade
        if impact_rank[impact] > impact_rank[scene_highest_impact]:
            scene_highest_impact = impact
        if grade in ("A", "B"):
            active_hucs.append(str(huc8_id))

    return {
        "owp:qc_grade": scene_best_grade,
        "owp:impact_score": scene_highest_impact,
        "owp:active_hucs": active_hucs,
        "owp:total_flood_area_km2": round(total_flood_area_km2, 4),
        "owp:huc_summaries": huc_summaries,
    }

def _empty_owp_properties() -> Dict[str, Any]:
    """Return minimal OWP properties when QC cannot be computed (e.g. no HUCs or tiles)."""
    return {
        "owp:qc_grade": "D",
        "owp:impact_score": "Low",
        "owp:active_hucs": [],
        "owp:total_flood_area_km2": 0.0,
        "owp:huc_summaries": [],
    }


# python -m ingest.gfm_exp.gfm_exp_col \
#   --bucket_name fimc-data \
#   --catalog_path scratch/biplov.bhandari/gfm-stac-test/stac/ \
#   --asset_object_key scratch/biplov.bhandari/gfm-stac-test/data-gfm-exp/ \
#   --hucs_object_key benchmark/stac-bench-cat/assets/WBDHU8_webproj.gpkg \
#   --derived_metadata_path scratch/biplov.bhandari/gfm-stac-test/stac/assets/derived-asset-data/gfm_expanded_collection.parquet \
#   --workers  6 \
#   --profile Data \
#   --skip-owp-qc
