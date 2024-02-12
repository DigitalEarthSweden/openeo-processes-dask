import gc
import logging
import traceback
from typing import Callable, Optional, Union

import dask.array as da
import geopandas as gpd
import numpy as np
import pandas as pd
import rasterio
import shapely
import xarray as xr
import xvec
from joblib import Parallel, delayed
from openeo_pg_parser_networkx.pg_schema import (TemporalInterval,
                                                 TemporalIntervals)
from openeo_processes_dask.process_implementations.data_model import (
    RasterCube, VectorCube)
from openeo_processes_dask.process_implementations.exceptions import (
    DimensionNotAvailable,
    TooManyDimensions,
)

__all__ = ["aggregate_temporal", "aggregate_temporal_period", "aggregate_spatial"]

logger = logging.getLogger(__name__)


def aggregate_temporal(
    data: RasterCube,
    intervals: Union[TemporalIntervals, list[TemporalInterval], list[Optional[str]]],
    reducer: Callable,
    labels: Optional[list] = None,
    dimension: Optional[str] = None,
    context: Optional[dict] = None,
    **kwargs,
) -> RasterCube:
    temporal_dims = data.openeo.temporal_dims

    if dimension is not None:
        if dimension not in data.dims:
            raise DimensionNotAvailable(
                f"A dimension with the specified name: {dimension} does not exist."
            )
        applicable_temporal_dimension = dimension
    else:
        if not temporal_dims:
            raise DimensionNotAvailable(
                f"No temporal dimension detected on dataset. Available dimensions: {data.dims}"
            )
        if len(temporal_dims) > 1:
            raise TooManyDimensions(
                f"The data cube contains multiple temporal dimensions: {temporal_dims}. The parameter `dimension` must be specified."
            )
        applicable_temporal_dimension = temporal_dims[0]

    aggregated_data = data.groupby_bins(
        group=applicable_temporal_dimension, labels=labels
    )

    raise NotImplementedError("aggregate_temporal is currently not implemented")


def aggregate_temporal_period(
    data: RasterCube,
    reducer: Callable,
    period: str,
    dimension: Optional[str] = None,
) -> RasterCube:
    temporal_dims = data.openeo.temporal_dims

    if dimension is not None:
        if dimension not in data.dims:
            raise DimensionNotAvailable(
                f"A dimension with the specified name: {dimension} does not exist."
            )
        applicable_temporal_dimension = dimension
    else:
        if not temporal_dims:
            raise DimensionNotAvailable(
                f"No temporal dimension detected on dataset. Available dimensions: {data.dims}"
            )
        if len(temporal_dims) > 1:
            raise TooManyDimensions(
                f"The data cube contains multiple temporal dimensions: {temporal_dims}. The parameter `dimension` must be specified."
            )
        applicable_temporal_dimension = temporal_dims[0]

    periods_to_frequency = {
        "hour": "H",
        "day": "D",
        "week": "W",
        "month": "M",
        "season": "QS-DEC",
        "year": "AS",
    }

    if period in periods_to_frequency.keys():
        frequency = periods_to_frequency[period]
    else:
        raise NotImplementedError(
            f"The provided period '{period})' is not implemented yet. The available ones are {list(periods_to_frequency.keys())}."
        )

    resampled_data = data.resample({applicable_temporal_dimension: frequency})

    positional_parameters = {"data": 0}
    return resampled_data.reduce(
        reducer, keep_attrs=True, positional_parameters=positional_parameters
    )


<<<<<<< HEAD
=======
def _aggregate_geometry(
    data: RasterCube,
    geom,
    transform,
    reducer: Callable,
):
    data_dims = list(data.dims)
    y_dim = data.openeo.y_dim
    x_dim = data.openeo.x_dim
    t_dim = data.openeo.temporal_dims
    t_dim = None if len(t_dim) == 0 else t_dim[0]
    b_dim = data.openeo.band_dims
    b_dim = None if len(b_dim) == 0 else b_dim[0]

    y_dim_size = data.sizes[y_dim]
    x_dim_size = data.sizes[x_dim]

    # Create a GeoSeries from the geometry
    geo_series = gpd.GeoSeries(geom)

    # Convert the GeoSeries to a GeometryArray
    geometry_array = geo_series.geometry.array

    mask = rasterio.features.geometry_mask(
        geometry_array, out_shape=(y_dim_size, x_dim_size), transform=transform
    )
    # data is (band,times,y,x)                                        # RISE data_dims = ['bands', 't', 'y', 'x']
                                                                      # RISE                0       1    2    3
    # Maks gets default (band, y,times,x)                             # RISE see below:
    if t_dim is not None:
        # mask = np.expand_dims(mask, axis=data_dims.index(t_dim))    # -> (36,1,38) (y,time,x) Borde varit 0 här ?
        mask = np.expand_dims(mask, axis=0)                           # -> RISE (1,36,38) (time,y,x) add dummy dimension to the left
    if b_dim is not None: 
        #mask = np.expand_dims(mask, axis=data_dims.index(b_dim))     # -> (1,36,1,38) (band,y,time,x) Borde varit 0 här också (vilket det är)
        mask = np.expand_dims(mask, axis=0)                           # -> RISE (1, 1,36,38) (time,y,x) add dummy dimension to the left
 
    # Do above instead pf reshaped_mask = np.transpose(mask, (0, 2, 1, 3))                   # RISE
    masked_data = data * mask
    del mask, data
    gc.collect()                                                      # RISE

    positional_parameters = {"data": 0}

    stat_within_polygon = masked_data.reduce(
        reducer,
        axis=(data_dims.index(y_dim), data_dims.index(x_dim)),
        keep_attrs=True,
        ignore_nodata=True,
        positional_parameters=positional_parameters,
    )
    result = stat_within_polygon.values

    del masked_data, stat_within_polygon
    gc.collect()
    return result.T


>>>>>>> 1e6a4fa (aggregate spatial now emits better JSON with data in data_vars)
def aggregate_spatial(
    data: RasterCube,
    geometries,
    reducer: Callable,
    chunk_size: int = 2,
) -> VectorCube:
    x_dim = data.openeo.x_dim
    y_dim = data.openeo.y_dim
    DEFAULT_CRS = "EPSG:4326"

    if isinstance(geometries, str):
        # Allow importing geometries from url (e.g. github raw)
        import json
        from urllib.request import urlopen

        response = urlopen(geometries)
        geometries = json.loads(response.read())
    if isinstance(geometries, dict):
        # Get crs from geometries
        if "features" in geometries:
            for feature in geometries["features"]:
                if "properties" not in feature:
                    feature["properties"] = {}
                elif feature["properties"] is None:
                    feature["properties"] = {}
            if isinstance(geometries.get("crs", {}), dict):
                DEFAULT_CRS = (
                    geometries.get("crs", {})
                    .get("properties", {})
                    .get("name", DEFAULT_CRS)
                )
            else:
                DEFAULT_CRS = int(geometries.get("crs", {}))
            logger.info(f"CRS in geometries: {DEFAULT_CRS}.")

        if "type" in geometries and geometries["type"] == "FeatureCollection":
            gdf = gpd.GeoDataFrame.from_features(geometries, crs=DEFAULT_CRS)
        elif "type" in geometries and geometries["type"] in ["Polygon"]:
            polygon = shapely.geometry.Polygon(geometries["coordinates"][0])
            gdf = gpd.GeoDataFrame(geometry=[polygon])
            gdf.crs = DEFAULT_CRS

    if isinstance(geometries, xr.Dataset):
        if hasattr(geometries, "xvec"):
            gdf = geometries.xvec.to_geodataframe()

    if isinstance(geometries, gpd.GeoDataFrame):
        gdf = geometries

    gdf = gdf.to_crs(data.rio.crs)
    geometries = gdf.geometry.values

<<<<<<< HEAD
    positional_parameters = {"data": 0}
    vec_cube = data.xvec.zonal_stats(
        geometries,
        x_coords=x_dim,
        y_coords=y_dim,
        method="iterate",
        stats=reducer,
        positional_parameters=positional_parameters,
    )
=======
    geometry_chunks = [
        geometries[i : i + chunk_size] for i in range(0, len(geometries), chunk_size)
    ]

    computed_results = []
    logger.info(f"Running aggregate_spatial process")
    try:
        for i, chunk in enumerate(geometry_chunks):
            # Create a list of delayed objects for the current chunk
            chunk_results = Parallel(n_jobs=-1)(
                delayed(_aggregate_geometry)(
                    data, geom, transform=transform, reducer=reducer
                )
                for geom in chunk
            )
            computed_results.extend(chunk_results)
    except Exception as e:
        logger.debug(f"Running process failed at {(i+1) *2} geometry")

    logger.info(f"Finish aggregate_spatial process for {len(geometries)}")

    final_results = np.stack(computed_results)
 
    del chunk_results, geometry_chunks, computed_results
    gc.collect()

    df = pd.DataFrame()
    keys_items = {}
    if b_dim: 
        for idx, b in enumerate(data[b_dim].values):
            columns = []
            if t_dim:
                for t in range(len(data[t_dim])):
                    columns.append(f"{b}_time{t+1}")
                aggregated_data = final_results[:, idx, :]
            else:
                columns.append(f"{b}")
                aggregated_data = final_results[:, idx]

            keys_items[b] = columns

            # Create a new DataFrame with the current data and columns
            aggregated_data = final_results.reshape(1, -1)                   # RISE
            band_df = pd.DataFrame(aggregated_data, columns=columns)  
            # Concatenate the new DataFrame with the existing DataFrame
            df = pd.concat([df, band_df], axis=1)
    else:  # There are no band but possibly a time dimension # RISE (whole else block)
        columns = []
        if t_dim:
            for t in range(len(data[t_dim])):
                columns.append(f"data_time{t+1}")
        else:
            columns.append(f"data")
            aggregated_data = final_results[:, idx]
        keys_items['values'] = columns 
        aggregated_data = final_results.reshape(1, -1)                   # RISE
        df = pd.DataFrame(aggregated_data, columns=columns)

    df = gpd.GeoDataFrame(df, geometry=gdf.geometry)

    data_vars = {}
    for key in keys_items.keys():
        data_vars[key] = (["geometry", t_dim], df[keys_items[key]])

    ## Create VectorCube
    if t_dim:
        times = list(data[t_dim].values)
        vec_cube = xr.Dataset(
            data_vars=data_vars, coords={"geometry": df.geometry, t_dim: times}
        ).xvec.set_geom_indexes("geometry", crs=df.crs)
    else:
        vec_cube = xr.Dataset(
            data_vars=data_vars, coords=dict(geometry=df.geometry)
        ).xvec.set_geom_indexes("geometry", crs=df.crs)

>>>>>>> 1e6a4fa (aggregate spatial now emits better JSON with data in data_vars)
    return vec_cube
