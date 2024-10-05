import geopandas as gpd
from shapely.geometry import Polygon

'''Create new shapefiles by dividing into a grid'''

shapefile_path = "╡rea de Estudo/╡rea de Estudo.shp" 
gdf = gpd.read_file(shapefile_path)

import numpy as np

# grid size
n_rows, n_cols = 50, 50

# geometry
minx, miny, maxx, maxy = gdf.total_bounds

# grid
x_step = (maxx - minx) / n_cols
y_step = (maxy - miny) / n_rows


polygons = []
for i in range(n_cols):
    for j in range(n_rows):
        poly = Polygon([
            (minx + i * x_step, miny + j * y_step),
            (minx + (i + 1) * x_step, miny + j * y_step),
            (minx + (i + 1) * x_step, miny + (j + 1) * y_step),
            (minx + i * x_step, miny + (j + 1) * y_step),
            (minx + i * x_step, miny + j * y_step)
        ])
        
        # intersects
        if gdf.geometry.intersects(poly).any():
            polygons.append(poly)

grid_gdf = gpd.GeoDataFrame({'geometry': polygons}, crs=gdf.crs)

grid_gdf.to_file('new_shapes/new_shape1.shp')
