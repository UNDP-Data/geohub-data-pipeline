import asyncio
import io
import multiprocessing
import os.path
import json
import threading

from osgeo import gdal, osr, ogr
from pmtiles.reader import Reader, MmapSource
import typing
import tempfile
from ingest.config import gdal_configs
from rio_cogeo import cog_validate
import threading
import logging
import time
from ingest.utils import download_blob

from traceback import print_exc
# from ingest.utils import upload_error_blob
gdal.UseExceptions()
import subprocess

logger = logging.getLogger(__name__)

config, output_profile = gdal_configs()

class IngestError(Exception):
    pass


for varname, varval in config.items():
    logger.debug(f'setting {varname}={varval}')
    gdal.SetConfigOption(str(varname), str(varval))


def should_reproject(src_srs: osr.SpatialReference = None, dst_srs: osr.SpatialReference = None):
    auth_code_func_name = ".".join([osr.SpatialReference.GetAuthorityCode.__module__, osr.SpatialReference.GetAuthorityCode.__name__])
    is_same_func_name = ".".join([osr.SpatialReference.IsSame.__module__, osr.SpatialReference.IsSame.__name__])
    if int(dst_srs.GetAuthorityCode(None)) == 3857 or int(dst_srs.GetAuthorityCode(None)) == 4326:return False
    try:

        proj_are_equal = int(src_srs.GetAuthorityCode(None)) == int(dst_srs.GetAuthorityCode(None))
    except Exception as evpe:
        logger.error(
            f'Failed to compare src and dst projections using {auth_code_func_name}. Trying using {is_same_func_name}')
        try:
            proj_are_equal = bool(src_srs.IsSame(dst_srs))
        except Exception as evpe1:
            logger.error(
                f'Failed to compare src and dst projections using {is_same_func_name}. Error is \n {evpe1}')
            raise evpe1

    return not proj_are_equal



def tippecanoe(tippecanoe_cmd:str =None, event=None):
    """
    tippecanoe is a bit peculiar. It redirects the status and live logging to stderr
    see https://github.com/mapbox/tippecanoe/issues/874

    As a result the line buffering  has to be enabled (bufsize=1) and the output is set as text (universal_new_line)
    This allows to follow the conversion logs in real time.
    @param tippecanoe_cmd: str, the
    @param timeout_secs:
    @return:
    """
    logger.debug(' '.join(tippecanoe_cmd))
    with subprocess.Popen(tippecanoe_cmd, stdout=subprocess.PIPE,
                          stderr=subprocess.STDOUT,
                          start_new_session=True,
                          universal_newlines=True,
                          bufsize=1
                          ) as proc:
        # the error is going to show up on stdout as it is redirected in the Popen
        err = None
        with proc.stdout:
            start = time.time()
            stream = io.open(proc.stdout.fileno()) # this will really make it streamabale
            while proc.poll() is None:
                output = stream.readline().strip('\r').strip('\n')
                if output:
                    logger.debug(output)
                    if err != output:err = output
                if event and event.is_set():
                    logger.error(f'tippecanoe process has been signalled to stop ')
                    proc.terminate()
                    raise subprocess.TimeoutExpired(cmd=tippecanoe_cmd,timeout=None)


        if proc.returncode and proc.returncode != 0:
            raise Exception(err)






def dataset2fgb(fgb_dir: str = None,
                src_ds: typing.Union[gdal.Dataset, ogr.DataSource] = None,
                layers: typing.List[str] = None,
                dst_prj_epsg: int = 4326,
                event=None):
    """
    Convert one or more layers from src_ds into FlatGeobuf format in a (temporary) directory featuring dst_prj_epsg
    projection. The layer is possibly reprojected. In case errors are encountered an error blob is uploaded for now
    #TODO
    @param fgb_dir: the abs path to a directory where the FGB files will be created
    @param src_ds: GDAL Dataset  or OGR Datasource instance where the layers will be read from
    @param layers: list of layer name ot be converted
    @param dst_prj_epsg: the  target projection as an EPSG code
    @param timeout_secs: the max number or seconds the GDAL translate function is allowed to run before timing out
    @return:
    """
    dst_srs = osr.SpatialReference()
    dst_srs.ImportFromEPSG(dst_prj_epsg)
    src_path = os.path.abspath(src_ds.GetDescription())
    converted_layers = dict()
    for lname in layers:
        try:
            #if '_' in lname:raise Exception(f'Simulated exception on {lname}')
            dst_path = os.path.join(fgb_dir, f'{lname}.fgb')
            layer = src_ds.GetLayerByName(lname)
            layer_srs = layer.GetSpatialRef()

            if layer_srs is None:
                logger.error(f'Layer {lname} does not feature a projection and will not be ingested')
                continue

            fgb_opts = [
                '-f FlatGeobuf',
                '-preserve_fid',
                '-skipfailures',

            ]
            reproject = should_reproject(src_srs=layer_srs, dst_srs=dst_srs)
            if reproject:
                fgb_opts.append(f'-t_srs EPSG:{dst_prj_epsg}')
            fgb_opts.append(lname)
            logger.debug(f'Converting {lname} from {src_path} into {dst_path}')

            fgb_ds = gdal.VectorTranslate(destNameOrDestDS=dst_path,
                                          srcDS=src_ds,
                                          reproject=reproject,
                                          options=' '.join(fgb_opts),
                                          callback=gdal_callback,
                                          callback_data=event
                                          )
            logger.debug(json.dumps(gdal.Info(fgb_ds, format='json'), indent=4))
            logger.info(f'Converted {lname} from {src_path} into {dst_path}')
            converted_layers[lname] = dst_path
            del fgb_ds
        except (RuntimeError, Exception) as re:
            if 'user terminated' in str(re):
                logger.info(f'Conversion of {lname} from {src_path} to FlatGeobuf has timed out')
            else:
                with io.StringIO() as m:
                    print_exc(
                        file=m
                    )  # exc is extracted using system.exc_info
                    error_message = m.getvalue()
                    msg = f'Failed to convert {lname} from {src_path} to FlatGeobuf. \n {error_message}'
                    logger.error(msg)
                    #TODO upload error blob

    return converted_layers

def fgb2pmtiles(fgb_layers=None, pmtiles_file_name=None, event=None):


    if pmtiles_file_name is None:
        for layer_name, fgb_layer_path in fgb_layers.items():
            try:
                layer_pmtiles_path = fgb_layer_path.replace('.fgb', '.pmtiles')


                tippecanoe_cmd = [
                    "tippecanoe",
                    "-o",
                    layer_pmtiles_path,
                    f'-l{layer_name}', # no space is allowed here between -l and layer name
                    "--no-feature-limit",
                    #"-zg" if '_' not in layer_name else '-zt',
                    "-zg",
                    "--simplify-only-low-zooms",
                    "--detect-shared-borders",
                    "--read-parallel",
                    "--no-tile-size-limit",
                    "--no-tile-compression",
                    "--force",
                    fgb_layer_path,
                ]
                tippecanoe(tippecanoe_cmd=tippecanoe_cmd,event=event)
                with open(layer_pmtiles_path, 'r+b') as f:
                    reader = Reader(MmapSource(f))
                    mdict = reader.metadata()
                    assert layer_name in  [vl["id"] for vl in mdict["vector_layers"]], f'{layer_name} is not present in {layer_pmtiles_path} PMTiles file.'
                logger.info(f'Created single layer PMtiles file {layer_pmtiles_path}')
                #upload layer_pmtiles_path to azure

            except subprocess.TimeoutExpired as te:
                logger.error(f'Conversion of layer {layer_name} from {fgb_layer_path} to PMtiles  has timed out.')
                if len(fgb_layers) > 1: logger.error(f'Moving to next layer')
            except Exception as e:
                with io.StringIO() as m:
                    print_exc(
                        file=m
                    )  # exc is extracted using system.exc_info
                    error_message = m.getvalue()
                    logger.error(f'Failed to convert FlatGeobuf {fgb_layer_path} to PMtiles. {error_message}')
                #TODO upload error file

                if len(fgb_layers) > 1: logger.error(f'Moving to next layer')


    else:
        try:
            assert pmtiles_file_name != '', f'Invalid PMtiles path {pmtiles_file_name}'
            fgb_sources = list()
            fgb_dir = None
            for layer_name, fgb_layer_path in fgb_layers.items():
                fgb_sources.append(f'--named-layer={layer_name}:{fgb_layer_path}')
                if fgb_dir is None:
                    fgb_dir, _ = os.path.split(fgb_layer_path)
            pmtiles_path = os.path.join(fgb_dir, f'{pmtiles_file_name}.pmtiles')
            tippecanoe_cmd = [
                "tippecanoe",
                "-o",
                pmtiles_path,
                "--no-feature-limit",
                "-zg",
                "--simplify-only-low-zooms",
                "--detect-shared-borders",
                "--read-parallel",
                "--no-tile-size-limit",
                "--no-tile-compression",
                "--force",

            ]

            tippecanoe_cmd += fgb_sources
            tippecanoe(tippecanoe_cmd=tippecanoe_cmd, event=event)
            with open(pmtiles_path, 'r+b') as f:
                reader = Reader(MmapSource(f))
                mdict = reader.metadata()
                assert len(fgb_layers) ==  len([vl["id"] for vl in mdict["vector_layers"]]), f'{layer_name} is not present in {pmtiles_path} PMTiles file.'
            logger.info(f'Created multilayer PMtiles file {pmtiles_path}')
        except subprocess.TimeoutExpired as te:
            logger.error(f'Conversion of layers {",".join(fgb_layers)} from {fgb_dir} has timed out.')

        except Exception as e:
            with io.StringIO() as m:
                print_exc(
                    file=m
                )  # exc is extracted using system.exc_info
                error_message = m.getvalue()
                logger.error(f'Failed to convert {",".join(fgb_layers)} from {fgb_dir} to PMtiles. {error_message}')

            #upload error file






def dataset2pmtiles(src_ds=None, layers=None, pmtiles_file_name=None, event=None):


    with tempfile.TemporaryDirectory() as temp_dir:
        fgb_layers = dataset2fgb(fgb_dir=temp_dir, src_ds=src_ds, layers=layers, event=event)
        if fgb_layers:
            fgb2pmtiles(fgb_layers=fgb_layers, pmtiles_file_name=pmtiles_file_name, event=event)



def gdal_callback(complete, message, cb_data):
    logger.debug(f'{complete*100:.2f}')
    if cb_data and cb_data.is_set():
        logger.info(f'GDAL received timeout signal')
        return 0


def dataset2cog(src_ds:gdal.Dataset=None, bands:typing.List[int]=None, event=None):
    """
    Convert a GDAL dataset or a subdataset to a COG
    @param src_ds: an instance of gdal.Dataset
    @param bands: list of band numbers
    @param timeout_secs: int,
    @return:
    """
    src_path = os.path.abspath(src_ds.GetDescription())

    try:
        with tempfile.TemporaryDirectory() as temp_dir:

            band = bands[0] if bands and len(bands) == 1 else None
            cog_path = prepare_cog_path1(src_path=src_path, dst_folder=temp_dir, band=band)
            band_word = f'band {band}' if band else 'bands'
            cog_ds = gdal.Translate(
                destName=cog_path,
                srcDS=src_ds,
                format="COG",
                bandList=bands,
                creationOptions=[
                    "BLOCKSIZE=256",
                    "OVERVIEWS=IGNORE_EXISTING",
                    "COMPRESS=ZSTD",
                    "PREDICTOR = YES",
                    "OVERVIEW_RESAMPLING=NEAREST",
                    "BIGTIFF=YES",
                    "TARGET_SRS=EPSG:3857",
                    "RESAMPLING=NEAREST",
                ],
                callback=gdal_callback,
                callback_data=event
            )

            del cog_ds
            is_valid, errors, warnings = cog_validate(src_path=cog_path, quiet=True)
            if not is_valid:
                sep = '\n'
                raise Exception(f'Invalid COG {cog_path}. Errors are {f"{sep}".join(errors)}')
            logger.info(f'Created COG {cog_path} from {src_path}')
            #TODO upload COG
    except (RuntimeError, Exception) as re:
        if 'user terminated' in str(re).lower():
            logger.info(f'Conversion of {src_path} to COG has timed out')
        else:
            with io.StringIO() as m:
                print_exc(
                    file=m
                )  # exc is extracted using system.exc_info
                error_message = m.getvalue()
                msg = f'Failed to convert {band_word} from {src_path} to COG. \n {error_message}'
                logger.error(msg)
                #uploa error blob




def prepare_cog_path1(src_path: str = None, dst_folder:str=None, band=None):

    folders, fname = os.path.split(src_path)
    fname_without_ext, ext = os.path.splitext(fname)
    if src_path.count(':') == 2:
        _, rpath, fname_without_ext = src_path.split(':')
        folders, _ = os.path.split(rpath)
        if '"' in fname_without_ext: fname_without_ext = fname_without_ext.replace('"', '')
        if "'" in fname_without_ext: fname_without_ext = fname_without_ext.replace("'", '')

    if not band:
        return f'{os.path.join(dst_folder, f"{fname_without_ext}.tif")}'
    else:
        return f'{os.path.join(dst_folder, f"{fname_without_ext}_band{band}.tif")}'



def gdal_open_safe(path:str=None, data_type=None)-> gdal.Dataset:
    return  gdal.OpenEx(path, data_type )


def process_geo_file(vsiaz_blob_path: str = None, join_vector_tiles=None, conn_string=None, event:multiprocessing.Event=None):
    assert vsiaz_blob_path not in ['', None], f'Invalid geospatial data file path: {vsiaz_blob_path}'

    with tempfile.TemporaryDirectory() as temp_dir:
        try:
            temp_data_file = asyncio.run(download_blob(temp_dir=temp_dir,
                                      conn_string=conn_string,
                                      blob_path=vsiaz_blob_path,
                                      event=event, in_chunks=True)
                        )
            vsiaz_blob_path = temp_data_file
            #download_blob_sync(conn_string=AZ_STORAGE_CONN_STR,blob_path=container_blob_path)
            # handle vectors first
            logger.info(f'Opening {vsiaz_blob_path}')
            try:
                vdataset = gdal.OpenEx(vsiaz_blob_path, gdal.OF_VECTOR )
            except RuntimeError as ioe:
                if 'supported' in str(ioe):
                    vdataset = None
                else:
                    raise

            if vdataset is not None:
                logger.info(f'Opened {vsiaz_blob_path} with {vdataset.GetDriver().ShortName} vector driver')
                logger.info(f'Found {vdataset.GetLayerCount()} vector layers')
                layer_names = [vdataset.GetLayerByIndex(i).GetName() for i in range(vdataset.GetLayerCount())]
                if not join_vector_tiles:
                    for layer_name in layer_names:
                        logger.info(f'Ingesting vector layer "{layer_name}"')
                        dataset2pmtiles(src_ds=vdataset, layers=[layer_name],event=event)
                else:

                    logger.info(f'Ingesting all vector layers into one multilayer PMtiles file')
                    _, file_name = os.path.split(vdataset.GetDescription())
                    fname, ext = os.path.splitext(file_name)
                    dataset2pmtiles(src_ds=vdataset, layers=layer_names, pmtiles_file_name=fname, event=event)

                del vdataset
            else:
                logger.info(f"{vsiaz_blob_path} does not contain vector GIS data")


            try:
                rdataset = gdal.OpenEx(vsiaz_blob_path, gdal.OF_RASTER)
            except RuntimeError as ioe:
                if 'supported' in str(ioe):
                    rdataset = None
                else:
                    raise

            if rdataset is None:
                logger.info(f"{vsiaz_blob_path} does not contain raster GIS data")
                return
            logger.info(f'Opening {vsiaz_blob_path} with {rdataset.GetDriver().ShortName} raster driver')
            # some formats will have subdatasets like ESRI geodatabase (according to docs) or NetCDF
            nraster_bands = rdataset.RasterCount


            # Driver.getMetadataItem(gdal.DCAP_SUBTADASETS) is not reliable so it is better to try

            for sdb in rdataset.GetSubDatasets():
                subdataset_path, subdataset_descr = sdb
                subds = gdal.Open(subdataset_path.replace('\"', ''))
                #logger.info(f'Opening raster subdataset {subdataset_descr} featuring {subds.RasterCount} bands')
                subds_bands = [b + 1 for b in range(subds.RasterCount)]
                subds_colorinterp = []
                if subds_bands:
                    subds_colorinterp = [subds.GetRasterBand(b).GetColorInterpretation() for b in subds_bands]
                subds_photometric = subds.GetMetadataItem('PHOTOMETRIC')
                subds_no_colorinterp_bands = len(subds_colorinterp)

                # create cog_path, usually  it is a temp
                if subds_no_colorinterp_bands >= 3 or subds_photometric is not None:
                    logger.info(f'Ingesting multiband subdataset {subdataset_path}')
                    dataset2cog(src_ds=subds,event=event)
                else:
                    for band_no in subds_bands:
                        logger.info(f'Ingesting band {band_no} from {subdataset_path}')
                        dataset2cog(src_ds=subds, bands=[band_no], event=event)


                del subds

            if nraster_bands:  # raster data is located at root
                bands = [b + 1 for b in range(nraster_bands)]
                colorinterp = []
                if bands:
                    colorinterp = [rdataset.GetRasterBand(b).GetColorInterpretation() for b in bands]
                no_colorinterp_bands = len(colorinterp)
                photometric = rdataset.GetMetadataItem('PHOTOMETRIC')
                if max(colorinterp) >= 3 or photometric is not None:
                    logger.info(f'Ingesting multiband dataset {vsiaz_blob_path}')
                    dataset2cog(src_ds=rdataset, event=event)


                else:
                    logger.info(f'Found {nraster_bands} rasters')
                    for band_no in bands:
                        #cog_path = prepare_cog_path(path=vsiaz_blob_path, band=band_no)
                        logger.info(f'Ingesting band {band_no} from {vsiaz_blob_path}')
                        dataset2cog(src_ds=rdataset, bands=[band_no], event=event)

            del rdataset

        except Exception as e:
            if 'vdataset' in locals(): del vdataset
            if 'rdataset' in locals(): del rdataset
            raise


