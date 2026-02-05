"""
OWP QC grading and HUC-level metrics for GFM scenes.

Implements the GFM Eval Metrics methodology with dynamic metadata handling:
per-HUC reliability (Data Quality Grade A/B/C/D), severity (Impact Score),
and scene-level aggregation. Uses mosaic-then-clip to avoid double-counting
when HUCs cross tile boundaries.
"""

import logging
import os
from typing import Any, Dict, List, Optional, Union

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

def _get_mosaic_masked_array(
    layer_name: str,
    tile_files: Dict[str, str],
    huc_geom_projected: Geometry,
    detected_nodata: Optional[Union[int, float]],
) -> Optional[np.ndarray]:
    """Mosaic multiple tiles for one layer, then clip to HUC geometry.

    Avoids double-counting when a HUC crosses tile boundaries by merging
    overlapping rasters before clipping.

    Args:
        layer_name: Layer identifier for logging (e.g. ENSEMBLE_FLOOD).
        tile_files: Dict mapping tile id to local file path for this layer.
        huc_geom_projected: HUC polygon in the same CRS as the rasters.
        detected_nodata: Nodata value from the raster; used for merge and mask.

    Returns:
        First band of the masked array (HUC clip) as numpy array, or None on error.
    """
    src_files_to_mosaic = []
    files_to_close = []

    # Standardize NoData for processing (use detected, or default to 255 if missing)
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

        # Mosaic (Handle Overlaps)
        # Passing nodata ensures rasterio knows what represents 'transparency' in the inputs
        mosaic, out_trans = merge(src_files_to_mosaic, nodata=work_nodata)

        # Virtual Clip
        with rasterio.io.MemoryFile() as memfile:
            with memfile.open(
                driver='GTiff',
                height=mosaic.shape[1],
                width=mosaic.shape[2],
                count=mosaic.shape[0],
                dtype=mosaic.dtype,
                crs=src_files_to_mosaic[0].crs, # Use CRS from first source
                transform=out_trans,
                nodata=work_nodata
            ) as dataset:
                dataset.write(mosaic)

                # Mask to HUC geometry
                try:
                    out_image, _ = mask(
                        dataset,
                        [huc_geom_projected.__geo_interface__],
                        crop=True,
                        filled=True
                    )
                    return out_image[0] # Return 1st band
                except ValueError as e:
                    logger.debug(
                        "Mask failed for layer %s (HUC may not overlap mosaic): %s",
                        layer_name,
                        e,
                    )
                    return None

    except (rasterio.RasterioIOError, OSError, MemoryError) as e:
        logger.warning("Error processing mosaic for %s: %s", layer_name, e)
        return None
    except Exception as e:
        logger.warning("Unexpected error processing mosaic for %s: %s", layer_name, e)
        return None
    finally:
        for src in files_to_close:
            src.close()

    return None

def _compute_huc_metrics(
    bucket_name: str,
    sent_ti_path: str,
    huc8_id: str,
    huc_geom_original: Geometry,
    equi7tiles_list: List[str],
    s3_utils: Any,
    huc_crs: Any,
) -> Dict[str, Any]:
    """Compute per-HUC reliability and severity metrics (mosaic then clip to HUC).

    Opens required layer COGs from S3 via s3:// URIs (streaming), mosaics them,
    clips to the HUC polygon in raster CRS, then computes: flood area (km²),
    mean uncertainty, observability %, advisory noise %, affected population,
    normalized anomaly ratio.

    Args:
        bucket_name: S3 bucket name.
        sent_ti_path: S3 prefix for the scene.
        huc8_id: HUC8 identifier (e.g. 12060101).
        huc_geom_original: HUC polygon in huc_crs (e.g. EPSG:4326).
        equi7tiles_list: List of equi7tile ids in the scene.
        s3_utils: S3 utilities (list).
        huc_crs: CRS of huc_geom_original (e.g. GeoDataFrame.crs).

    Returns:
        Dict with keys flood_area_km2, affected_pop, observability_pct, uncertainty_mean,
        advisory_noise_pct, normalized_anomaly_ratio (and defaults if data missing).
    """
    metrics = {
        "flood_area_km2": 0.0,
        "affected_pop": 0.0,
        "observability_pct": 0.0,
        "uncertainty_mean": 0.0,
        "advisory_noise_pct": 0.0,
        "normalized_anomaly_ratio": 0.0
    }

    # 1. Build S3 URIs for all layers/tiles; get CRS/NoData from first valid FLOOD.
    metadata = None
    local_paths = {ly: {} for ly in REQUIRED_LAYERS}

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
        logger.debug("No valid flood raster metadata for HUC %s", huc8_id)
        return metrics

    # 2. Dynamic Reprojection
    # HUC geometry is in huc_crs; project to the raster's CRS for masking.
    try:
        raster_crs = metadata["crs"]
        huc_gdf = gpd.GeoDataFrame(geometry=[huc_geom_original], crs=huc_crs)
        huc_projected = huc_gdf.to_crs(raster_crs).geometry.iloc[0]

        # Check for empty projection result
        if huc_projected.is_empty:
            return metrics

    except Exception as e:
        logger.error(
            "CRS mismatch or projection error for HUC %s: %s",
            huc8_id,
            e,
            exc_info=False,
        )
        return metrics

    # 3. Process Layers
    detected_nodata = metadata["nodata"]

    # -- Flood --
    flood_arr = _get_mosaic_masked_array("ENSEMBLE_FLOOD", local_paths["ENSEMBLE_FLOOD"], huc_projected, detected_nodata)
    if flood_arr is None: return metrics

    valid_mask = (flood_arr != detected_nodata)
    flood_pixels = (flood_arr == 1) & valid_mask
    flood_count = np.sum(flood_pixels)
    total_valid_pixels = np.sum(valid_mask)

    # Warn if raster is geographic (e.g. WGS84)
    try:
        if metadata["crs"] is not None and metadata["crs"].to_epsg() == 4326:
            logger.warning(
                "Scene %s appears to be WGS84. Area calcs using %s km2 constant may be invalid.",
                sent_ti_path,
                REF_KM2_PER_PIXEL,
            )
    except (AttributeError, TypeError):
        pass

    metrics["flood_area_km2"] = flood_count * REF_KM2_PER_PIXEL

    # -- Uncertainty --
    unc_arr = _get_mosaic_masked_array("ENSEMBLE_UNCERTAINTY", local_paths["ENSEMBLE_UNCERTAINTY"], huc_projected, detected_nodata)
    if unc_arr is not None and flood_count > 0:
        if unc_arr.shape == flood_arr.shape:
            # GFM Uncertainty is 0-100
            unc_values = unc_arr[flood_pixels]
            if unc_values.size > 0:
                metrics["uncertainty_mean"] = float(np.mean(unc_values))

    # -- Exclusion (Observability) --
    excl_arr = _get_mosaic_masked_array("ENSEMBLE_EXCLAYER", local_paths["ENSEMBLE_EXCLAYER"], huc_projected, detected_nodata)
    if excl_arr is not None and total_valid_pixels > 0:
        excl_pixels = (excl_arr == 1) & valid_mask
        excl_count = np.sum(excl_pixels)
        metrics["observability_pct"] = (1 - (excl_count / total_valid_pixels)) * 100.0

    # -- Advisory Noise --
    adv_arr = _get_mosaic_masked_array("ADVFLAG", local_paths["ADVFLAG"], huc_projected, detected_nodata)
    if adv_arr is not None and flood_count > 0:
        if adv_arr.shape == flood_arr.shape:
            noisy_flood = flood_pixels & (adv_arr > 0)
            metrics["advisory_noise_pct"] = (np.sum(noisy_flood) / flood_count) * 100.0

    # -- Population --
    pop_arr = _get_mosaic_masked_array("POP", local_paths["POP"], huc_projected, detected_nodata)
    if pop_arr is not None:
        if pop_arr.shape == flood_arr.shape:
            pop_valid = (pop_arr != detected_nodata) & flood_pixels
            metrics["affected_pop"] = float(np.sum(pop_arr[pop_valid]))

    # -- Normalized Anomaly Ratio --
    obs_water_arr = _get_mosaic_masked_array("ENSEMBLE_OBSWATER", local_paths["ENSEMBLE_OBSWATER"], huc_projected, detected_nodata)
    if obs_water_arr is not None:
        obs_water_count = np.sum((obs_water_arr == 1) & valid_mask)
        if obs_water_count > 0:
            metrics["normalized_anomaly_ratio"] = float(flood_count / obs_water_count)

    # Rounding
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

    for huc8_id in huc8_list:
        try:
            row = hucs_gdf[hucs_gdf["HUC8"].astype(str) == str(huc8_id)]
            if row.empty:
                continue
            huc_geom = row.geometry.iloc[0]

            # Explicit empty check
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

        metrics = _compute_huc_metrics(
            bucket_name, sent_ti_path, huc8_id, huc_geom,
            equi7tiles_list, s3_utils, huc_crs=hucs_gdf.crs
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
#   --profile Data \
#   --skip-owp-qc
