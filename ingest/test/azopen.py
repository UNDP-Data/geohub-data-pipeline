import os

from osgeo import gdal
from ingest.config import setup_env_vars
setup_env_vars()
fpath = '/vsiaz/userdata/a85516c81c0b78d3e89d3f00099b8b15/raw/HYP_HR_SR_W_20230313134627.tif'
fpath = '/vsizip/vsiaz/userdata/a85516c81c0b78d3e89d3f00099b8b15/raw/ne_10m_airports_20230314095422.zip'


print(gdal.Info(fpath))



