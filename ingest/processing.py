import os.path
import json
from osgeo import gdal, osr, ogr
from osgeo.gdal import Dataset, Driver
from pmtiles.reader import Reader, MmapSource
import typing
import tempfile
from ingest.config import gdal_configs
from rio_cogeo import cog_info
from rio_cogeo.models import Info
import logging
import time
from timeoutcontext import timeout
import shutil
# from ingest.utils import upload_error_blob
gdal.UseExceptions()
import subprocess

logger = logging.getLogger(__name__)

config, output_profile = gdal_configs()

for varname, varval in config.items():
    gdal.SetConfigOption(str(varname), str(varval))


def should_reproject(src_srs: osr.SpatialReference = None, dst_srs: osr.SpatialReference = None):
    try:

        proj_are_equal = int(src_srs.GetAuthorityCode(None)) == int(dst_srs.GetAuthorityCode(None))
    except Exception as evpe:
        logger.error(
            f'Failed to compare src and dst projections using {osr.SpatialReference.GetAuthorityCode}. Trying using {osr.SpatialReference.IsSame} \n {evpe}')
        try:
            proj_are_equal = bool(src_srs.IsSame(dst_srs))
        except Exception as evpe1:
            logger.error(
                f'Failed to compare src and dst projections using {osr.SpatialReference.IsSame}. Error is \n {evpe1}')
            raise evpe1

    return not proj_are_equal


def fgbdir2pmtiles(fgb_dir=None, pmtiles_file_name=None,  timeout_secs=3600*3):
    """

    """
    fgb_file_names = os.listdir(fgb_dir)
    if pmtiles_file_name is None:
        for file_name in fgb_file_names:
            src_layer_path = os.path.join(fgb_dir, file_name)
            fname, ext = os.path.splitext(file_name)
            layer_pmtiles_path = os.path.join(fgb_dir, f'{fname}.pmtiles')

            tippecanoe_cmd = [
                "tippecanoe",
                "-o",
                layer_pmtiles_path,
                f'-l {fname}',
                "--no-feature-limit",
                "-zg",
                "--simplify-only-low-zooms",
                "--detect-shared-borders",
                "--read-parallel",
                "--no-tile-size-limit",
                "--no-tile-compression",
                "--force",
                src_layer_path,
            ]

            try:
                # TODO ADD TIMER
                # with subprocess.Popen(shlex.split(tippecanoe_cmd), stdout=subprocess.PIPE, start_new_session=True) as proc:
                with subprocess.Popen(tippecanoe_cmd, stdout=subprocess.PIPE, start_new_session=True) as proc:
                    start = time.time()
                    while proc.poll() is None:
                        output = proc.stdout.readline()
                        if output:
                            logger.info(output.strip())
                        if timeout:
                            if time.time() - start > timeout_secs:
                                proc.terminate()
                                raise subprocess.TimeoutExpired(tippecanoe_cmd, timeout=timeout_secs)
            except subprocess.TimeoutExpired as te:
                logger.error(f'Conversion of FlatGeobuf {src_layer_path} to PMtiles has timed out.')
                if len(fgb_file_names)>1:logger.error(f'Moving to next layer')
            except Exception as e:
                logger.error(f'Failed to convert FlatGeobuf {src_layer_path} to PMtiles. {e}')
                if len(fgb_file_names)>1:logger.error(f'Moving to next layer')

            with open(layer_pmtiles_path, 'r+b') as f:
                reader = Reader(MmapSource(f))
                logger.info(json.dumps(reader.metadata(), indent=4))

    else:
        assert pmtiles_file_name != '', f'Invalid PMtiles path {pmtiles_file_name}'
        fgb_sources = list()
        for file_name in fgb_file_names:
            src_layer_path = os.path.join(fgb_dir, file_name)
            fname, ext = os.path.splitext(file_name)
            fgb_sources.append(f'--named-layer={fname}:{src_layer_path}')

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

        logger.info(' '.join(tippecanoe_cmd))
        with subprocess.Popen(tippecanoe_cmd, stdout=subprocess.PIPE, start_new_session=True) as proc:
            start = time.time()
            while proc.poll() is None:
                output = proc.stdout.readline()
                if output:
                    logger.info(output.strip())
                if timeout:
                    if time.time() - start > timeout_secs:
                        proc.terminate()
                        raise subprocess.TimeoutExpired(tippecanoe_cmd, timeout=timeout_secs)

        shutil.copy(pmtiles_path, '/data/out')
        with open(pmtiles_path, 'r+b') as f:
            reader = Reader(MmapSource(f))
            logger.debug(json.dumps(reader.metadata(), indent=4))







def dataset2fgb( fgb_dir:str = None,
                src_ds: typing.Union[gdal.Dataset, ogr.DataSource] = None,
                layers: typing.List[str] = None,
                dst_prj_epsg: int = 4326, timeout_secs=3600):
    """
    Convert one or more layers from src_ds into FlatGeobuf format
    in a temporary directory featuring dst_prj_epsg projection.
    The layer is possibly reprojected
    """
    dst_srs = osr.SpatialReference()
    dst_srs.ImportFromEPSG(dst_prj_epsg)
    src_path = os.path.abspath(src_ds.GetDescription())


    for lname in layers:
        dst_path = os.path.join(fgb_dir, f'{lname}.fgb')
        layer = src_ds.GetLayerByName(lname)
        layer_srs = layer.GetSpatialRef()
        if layer_srs is None:
            logger.error(f'Layer {lname} does not feature a projection and will not be ingested')
            continue

        fgb_opts = [
            '-f FlatGeobuf',
            #'-overwrite',
            '-preserve_fid',
            '-skipfailures',

        ]
        reproject = should_reproject(src_srs=layer_srs, dst_srs=dst_srs)
        if reproject:
            fgb_opts.append(f'-t_srs EPSG:{dst_prj_epsg}')
        fgb_opts.append(lname)
        logger.debug(f'Converting {lname} from {src_path} into {dst_path}')
        try:
            with timeout(timeout_secs):
                fgb_ds = gdal.VectorTranslate(destNameOrDestDS=dst_path,
                                              srcDS=src_ds,
                                              # layers=lname,
                                              reproject=reproject,
                                              options=' '.join(fgb_opts)
                                              )
                logger.debug(json.dumps(gdal.Info(fgb_ds, format='json'), indent=4))
                logger.info(f'Converted {lname} from {src_path} into {dst_path}')

        except TimeoutError:
            logger.error(f'Failed to convert layer {lname} from {src_path} to FlatGeobuf in {timeout_secs} seconds')




def dataset2pmtiles(src_ds=None, layers=None, pmtiles_file_name=None ):
    src_path = os.path.join(src_ds.GetDescription())
    try:
        with tempfile.TemporaryDirectory() as temp_dir:
            dataset2fgb(fgb_dir=temp_dir, src_ds=src_ds, layers=layers)
            fgbdir2pmtiles(fgb_dir=temp_dir, pmtiles_file_name=pmtiles_file_name)

    except Exception as e:
        logger.error(f'Failed to convert {src_path} to PMtiles. {e}')
        raise


def dataset2cog(src_ds=None, bands=None, cog_path=None, timeout_secs=3600):
    """
    Convert a GDAL Dataset to COG
    """
    src_path = os.path.abspath(src_ds.GetDesciption())
    try:

        gdal.Translate(
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
        )

        logger.debug(json.dumps(json.loads(cog_info(cog_path).json()), indent=4))
    except TimeoutError:
        logger.error(f'Failed to convert  from {src_path} to FlatGeobuf in {timeout_secs} seconds')

    except Exception as e:
        logger.error(f'Failed to convert {src_path} to COG. {e}')
        # TODO upload error blob


def prepare_cog_path(path: str = None, band=None):
    folders, fname = os.path.split(path)
    fname_without_ext, ext = os.path.splitext(fname)
    if path.count(':') == 2:
        _, rpath, fname_without_ext = path.split(':')
        folders, _ = os.path.split(rpath)
        if '"' in fname_without_ext: fname_without_ext = fname_without_ext.replace('"', '')
        if "'" in fname_without_ext: fname_without_ext = fname_without_ext.replace("'", '')

    if not band:
        return f'{os.path.join(folders, "out", f"{fname_without_ext}.tif")}'
    else:
        return f'{os.path.join(folders, "out", f"{fname_without_ext}_band{band}.tif")}'


def process_geo_file(vsiaz_blob_path:str=None, join_vector_tiles=True):

    assert vsiaz_blob_path not in ['', None], f'Invalid geospatial data file path: {vsiaz_blob_path}'
    try:
        # handle vectors first
        vdataset = gdal.OpenEx(vsiaz_blob_path, gdal.OF_VECTOR)
        logger.info(f'Opening {vsiaz_blob_path} with {vdataset.GetDriver().ShortName} vector driver')
        if vdataset is not None:
            logger.info(f'Found {vdataset.GetLayerCount()} vector layers')
            layer_names = [vdataset.GetLayerByIndex(i).GetName() for i in range(vdataset.GetLayerCount())]
            if not join_vector_tiles:
                for layer_name in layer_names:
                    logger.info(f'Ingesting vector layer {layer_name}')
                    try:
                        dataset2pmtiles(src_ds=vdataset, layers=[layer_name])
                    except Exception as e:
                        continue
            else:

                logger.info(f'Ingesting all vector layers into one multilayer PMtiles file')
                _, file_name = os.path.split(vdataset.GetDescription())
                fname, ext = os.path.splitext(file_name)
                dataset2pmtiles(src_ds=vdataset, layers=layer_names, pmtiles_file_name=fname)

            del vdataset
        else:
            logger.info(f"{vsiaz_blob_path} does not contain vector GIS data")
        exit()
        rdataset = gdal.OpenEx(vsiaz_blob_path, gdal.OF_RASTER)
        logger.info(f'Opening {vsiaz_blob_path} with {rdataset.GetDriver().ShortName} raster driver')
        if rdataset is None:
            logger.info(f"{vsiaz_blob_path} does not contain raster GIS data")
            return
        # some formats will have subdatasets like ESRI geodatabase (according to docs) or NetCDF
        nraster_bands = rdataset.RasterCount

        bands = [b + 1 for b in range(nraster_bands)]
        colorinterp = []
        if bands:
            colorinterp = [rdataset.GetRasterBand(b).GetColorInterpretation() for b in bands]
        no_colorinterp_bands = len(colorinterp)
        photometric = rdataset.GetMetadataItem('PHOTOMETRIC')
        # Driver.getMetadataItem(gdal.DCAP_SUBTADASETS) is not reliable so it is better to try

        for sdb in rdataset.GetSubDatasets():
            subdataset_path, subdataset_descr = sdb
            subds = gdal.Open(subdataset_path.replace('\"', ''))
            logger.info(f'Opening raster subdataset {subdataset_descr} featuring {subds.RasterCount} bands')
            subds_bands = [b + 1 for b in range(subds.RasterCount)]
            subds_colorinterp = []
            if subds_bands:
                subds_colorinterp = [subds.GetRasterBand(b).GetColorInterpretation() for b in subds_bands]
            subds_photometric = subds.GetMetadataItem('PHOTOMETRIC')
            subds_no_colorinterp_bands = len(colorinterp)

            # create cog_path, usually  it is a temp
            if subds_no_colorinterp_bands >= 3 or subds_photometric is not None:
                cog_path = prepare_cog_path(path=subdataset_path)
                try:
                    dataset2cog(src_ds=subds, cog_path=cog_path)
                except Exception as e:
                    continue

            else:
                for band_no in subds_bands:
                    cog_path = prepare_cog_path(path=subdataset_path, band=band_no)
                    logger.info(f'Ingesting band {band_no} from {subdataset_path} into {cog_path}')
                    try:
                        dataset2cog(src_ds=subds, bands=[band_no], cog_path=cog_path)
                    except Exception as e:
                        continue

            del subds

        if nraster_bands:  # this means we have a simple format
            if max(colorinterp) >= 3 or photometric is not None:
                nrasters = 1
                logger.info(f'Found {nrasters} RGB rasters')
                cog_path = prepare_cog_path(path=vsiaz_blob_path)
                try:
                    dataset2cog(src_ds=rdataset, cog_path=cog_path)
                except Exception as e:
                    pass

            else:
                logger.info(f'Found {nraster_bands} rasters')
                for band_no in bands:
                    cog_path = prepare_cog_path(path=vsiaz_blob_path, band=band_no)
                    logger.info(f'Ingesting band {band_no} into {cog_path}')
                    try:
                        dataset2cog(src_ds=rdataset, bands=[band_no], cog_path=cog_path)
                    except Exception as e:
                        continue



    except Exception as e:
        if 'vdataset' in locals(): del vdataset
        if 'rdataset' in locals(): del rdataset
        logger.error(e)
        raise

    # try:
    #
    #     dataset = gdal.OpenEx(vsiaz_blob_path, gdal.OF_RASTER, open_options=['LIST_ALL_TABLES=NO'])
    #     logger.info(f'Opening {vsiaz_blob_path} with {dataset.GetDriver().ShortName} driver')
    #     if dataset is None:
    #         logger.error(f"{vsiaz_blob_path} does not contain GIS data")
    #         #await upload_error_blob(vsiaz_blob_path, f"{vsiaz_blob_path} is not a GIS data file")
    #     nrasters, nvectors = dataset.RasterCount, dataset.GetLayerCount()
    #     bands = [b+1 for b in range(dataset.RasterCount)]
    #     colorinterp = []
    #     if bands:
    #         colorinterp = [dataset.GetRasterBand(b).GetColorInterpretation() for b in bands]
    #     logger.info(f'i found {nvectors} vectors and  {nrasters} rasters {len(dataset.GetSubDatasets())} subdatasets')
    #     # Driver.getMetadataItem(gdal.DCAP_SUBTADASETS) is not reliable so it is better to try
    #     rg = dataset.GetRootGroup()
    #     print(rg)
    #
    #     for sdb in dataset.GetSubDatasets():
    #         subdataset_path, subdataset_descr = sdb
    #         subds = gdal.Open(subdataset_path.replace('\"', ''))
    #         logger.info(f'Opening raster subdataset {subdataset_descr} featuring {subds.RasterCount} bands')
    #         subds_bands = [b+1 for b in range(subds.RasterCount)]
    #         subds_colorinterp = []
    #         if subds_bands:
    #             subds_colorinterp = [subds.GetRasterBand(b).GetColorInterpretation() for b in subds_bands]
    #         # create cog_path, usually  it is a temp
    #         if subds_colorinterp:
    #             band2cog(dataset=subds, band_index=subds_bands, cog_path=None)
    #         else:
    #             for band_index in subds_bands:
    #                 band2cog(dataset=subds,band_index=band_index, cog_path=None)
    #         del subds
    #
    #     if nrasters:
    #         if colorinterp:
    #             band2cog(dataset=dataset,band_index=bands, cog_path=None)
    #         else:
    #             for band_index in bands:
    #                 #b = dataset.GetRasterBand(band_index)
    #                 # print(b.GetColorInterp())
    #                 band2cog(dataset=dataset,band_index=band_index, cog_path=None)
    #
    #     # if nvectors:
    #     #
    #     #     for vindex in range(nvectors):
    #     #         l = dataset.GetLayer(vindex)
    #     #         logger.info(l.GetName())
    #
    # finally:
    #     dataset = None
