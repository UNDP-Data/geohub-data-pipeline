from dotenv import load_dotenv
from osgeo import gdal
load_dotenv('../../.env')
fpath = '/vsiaz/userdata/a85516c81c0b78d3e89d3f00099b8b15/raw/HYP_HR_SR_W_20230313134627.tif'
fpath = '/vsizip/vsiaz/userdata/a85516c81c0b78d3e89d3f00099b8b15/raw/ne_10m_airports_20230314095422.zip'
from ingest.config import (
    account_url,
    connection_string,
    container_name,
    datasets_folder,
    raw_folder,
)

dataset = gdal.OpenEx(fpath, gdal.GA_ReadOnly)

print(dataset)



