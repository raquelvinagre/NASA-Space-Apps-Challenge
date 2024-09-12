from fastapi import FastAPI, Request
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import RedirectResponse


import openeo  # for processing/retrieving data from Copernicus
import xarray  # for reading netCDF data
import geopandas as gpd  # for working with shapefiles and GeoJSON

from os.path import isfile

app = FastAPI()

origins = [
    "http://localhost.tiangolo.com",
    "https://localhost.tiangolo.com",
    "http://localhost",
    "http://localhost:8080",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


def get_datacube(
    collection,
    temporal_extent,
    spatial_extent,
    bands,
    max_cloud_cover=50,
    mask_clouds=False,
    spatial_resolution=None,
    temporal_reducer=None,
):
    # connect to the Copernicus Data Space Ecosystem
    connection = openeo.connect(url="openeo.dataspace.copernicus.eu")
    connection.authenticate_oidc()

    # load data
    data = connection.load_collection(
        collection,
        temporal_extent=temporal_extent,
        spatial_extent=spatial_extent,
        bands=bands,
        max_cloud_cover=max_cloud_cover,
    )

    # https://documentation.dataspace.copernicus.eu/notebook-samples/openeo/Load_Collection.html#cloud-masking
    # For Sentinel-2, use the Scene Classification layer (adjust if using other data sources)
    if mask_clouds and "SCL" in bands:
        SCL = data.band("SCL")

        # build a binary cloud mask from the SCL values 3 (cloud shadows), 8 (cloud medium probability) and 9 (cloud high probability)
        cloud_mask = (SCL == 3) | (SCL == 8) | (SCL == 9)
        # need to resample, as the SCL layer has a ground sample distance of 20 meter, while it is 10 meter for the B02, B03 and B04 bands
        cloud_mask = cloud_mask.resample_cube_spatial(data)

        data = data.mask(cloud_mask)

    if spatial_resolution:
        data = data.resample_spatial(resolution=spatial_resolution)

    if temporal_reducer:
        data = data.reduce_temporal(reducer=temporal_reducer)

    return data


def get_spectral_indices(ds, shapefile, reproject=True):
    if reproject:
        ds = ds.rio.reproject("EPSG:4326")

    # calculate indices
    green, red, nir = "B03", "B04", "B08"
    ds["NDWI"] = (ds[green] - ds[nir]) / (ds[green] + ds[nir]).rio.write_crs(
        "EPSG:4326"
    )
    ds["NDVI"] = (ds[nir] - ds[red]) / (ds[nir] + ds[red]).rio.write_crs("EPSG:4326")

    # GeoDataFrame
    df = ds.to_dataframe().reset_index()
    gdf = gpd.GeoDataFrame(df, geometry=gpd.points_from_xy(df.x, df.y), crs="EPSG:4326")
    gdf = gdf[["NDWI", "NDVI", "geometry"]].dropna()

    # read shapefile
    geo = gpd.read_file(shapefile)
    geo = geo[["localId", "geometry"]]

    # aggregate indices per zones in shapefile
    indices = (
        geo.sjoin(gdf, predicate="contains")[["localId", "NDVI", "NDWI"]]
        .groupby("localId")
        .agg("mean")
    )
    indices = geo.join(indices, on="localId")

    return indices.to_geo_dict()


@app.get("/ria.json")
async def get_ndvi_nwdi():
    if not isfile("data/eo_data.nc"):
        temporal_extent = ("2019-01-01", "2019-01-31")
        spatial_extent = {
            "west": -8.84,
            "south": 40.43,
            "east": -8.56,
            "north": 40.88,
            "crs": "EPSG:4326",
        }
        bands = ["B02", "B03", "B04", "B08", "SCL"]

        eo_cube = get_datacube(
            "SENTINEL2_L2A",
            temporal_extent,
            spatial_extent,
            bands,
            spatial_resolution=100,
            temporal_reducer="mean",
            mask_clouds=True,
        )
        eo_cube.download("data/eo_data.nc")

    ds = xarray.load_dataset("data/eo_data.nc", decode_coords="all")

    out = get_spectral_indices(ds, "RiaAveiro_WGS84/RiaAveiro_WGS84.shp")

    return out


app.mount("/static", StaticFiles(directory="html"), name="static")


@app.get("/")
async def default():
    return RedirectResponse(url="/static/ria.html")
