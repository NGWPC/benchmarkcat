# s3 paths to access flowlines and retrodata to create flowfiles 
nwm_streams = 'hand_fim/inputs/nwm_hydrofabric/nwm_flows.gpkg'

# need the bucket name for url_conus since isn't fimc-data
url_conus = 's3://noaa-nwm-retrospective-3-0-pds/CONUS/zarr/chrtout.zarr'

#place to write the hwm flowfiles too. Make sure there isn't a leading or trailing "/" for s3 upload.
flowfile_dir = "benchmark/high_water_marks/usgs/flowfiles"

# different conditions to create wkt strings for events depending on crs/datum used
def create_wkt_string(horizontalDatumName, verticalDatumName):
    # Assuming the datum will always be some variant of WGS 84, NAD 83, NAD 27, CSRS 2017.5, or a local control point
    if "WGS84" in horizontalDatumName:
        horizontal_wkt = 'GEOGCS["WGS 84",' \
                         'DATUM["WGS_1984",' \
                         'SPHEROID["WGS 84", 6378137, 298.257223563]],' \
                         'PRIMEM["Greenwich", 0],' \
                         'UNIT["degree", 0.0174532925199433],' \
                         'AXIS["Latitude", NORTH],' \
                         'AXIS["Longitude", EAST]]'
    elif "NAD83" in horizontalDatumName or "NAD 83" in horizontalDatumName:
        horizontal_wkt = 'GEOGCS["NAD83",' \
                         'DATUM["North_American_Datum_1983",' \
                         'SPHEROID["GRS 1980", 6378137, 298.257222101]],' \
                         'PRIMEM["Greenwich", 0],' \
                         'UNIT["degree", 0.0174532925199433],' \
                         'AXIS["Latitude", NORTH],' \
                         'AXIS["Longitude", EAST]]'
    elif "NAD27" in horizontalDatumName or "NAD 27" in horizontalDatumName:
        horizontal_wkt = 'GEOGCS["NAD27",' \
                         'DATUM["North_American_Datum_1927",' \
                         'SPHEROID["Clarke 1866", 6378206.4, 294.9786982139006]],' \
                         'PRIMEM["Greenwich", 0],' \
                         'UNIT["degree", 0.0174532925199433],' \
                         'AXIS["Latitude", NORTH],' \
                         'AXIS["Longitude", EAST]]'
    elif "CSRS 2017.5" in horizontalDatumName or "CSRS2017.5" in horizontalDatumName:
        horizontal_wkt = 'GEOGCS["CSRS 2017.5",' \
                         'DATUM["Canadian_Spatial_Reference_System_2017.5",' \
                         'SPHEROID["GRS 1980", 6378137, 298.257222101]],' \
                         'PRIMEM["Greenwich", 0],' \
                         'UNIT["degree", 0.0174532925199433],' \
                         'AXIS["Latitude", NORTH],' \
                         'AXIS["Longitude", EAST]]'
    elif "local control point" in horizontalDatumName.lower():
        horizontal_wkt = 'GEOGCS["local control point",' \
                         'DATUM["local control point",' \
                         'SPHEROID["Null", 0, 0]],' \
                         'PRIMEM["Null", 0],' \
                         'UNIT["degree", 0.0174532925199433],' \
                         'AXIS["Latitude", NORTH],' \
                         'AXIS["Longitude", EAST]]'
    else:
        raise ValueError(f"Unsupported horizontal datum: {horizontalDatumName}")
        
    # Define the vertical CRS part
    vertical_wkt = f'VERT_CS["{verticalDatumName} height",' \
                   f'VERT_DATUM["{verticalDatumName}"],' \
                   f'UNIT["ft", 1],' \
                   f'AXIS["Up", UP]]'
    
    # Combine into a compound CRS
    wkt = f'COMPD_CS["{horizontalDatumName} + {verticalDatumName} height",' \
          f'{horizontal_wkt},' \
          f'{vertical_wkt}]'

    # Replace double quotes with single quotes to avoid "/" issues
    wkt = wkt.replace('"', "'")

    return wkt

# flowfile column info
columns_list = [{
                "feature_id": {
                    "Column description": "feature id that identifies the stream segment being modeled or measured",
                    "Column data source": "NWM 3.0 hydrofabric",
                    "data_href": "https://water.noaa.gov/resources/downloads/nwm/NWM_channel_hydrofabric.tar.gz"
                },
                "discharge": {
                    "Column description": "Discharge in m^3/s",
                    "Column data source": "NWM 3.0 retrospective discharge data",
                    "data_href": "https://registry.opendata.aws/nwm-archive/"
                }
            }]

albers_crs = """PROJCRS["USA_Contiguous_Albers_Equal_Area_Conic_USGS_version",
        BASEGEOGCRS["NAD83",
            DATUM["North American Datum 1983",
                ELLIPSOID["GRS 1980",6378137,298.257222101004,
                    LENGTHUNIT["metre",1]]],
            PRIMEM["Greenwich",0,
                ANGLEUNIT["degree",0.0174532925199433]],
            ID["EPSG",4269]],
        CONVERSION["unnamed",
            METHOD["Albers Equal Area",
                ID["EPSG",9822]],
            PARAMETER["Latitude of 1st standard parallel",29.5,
                ANGLEUNIT["degree",0.0174532925199433],
                ID["EPSG",8823]],
            PARAMETER["Latitude of 2nd standard parallel",45.5,
                ANGLEUNIT["degree",0.0174532925199433],
                ID["EPSG",8824]],
            PARAMETER["Latitude of false origin",23,
                ANGLEUNIT["degree",0.0174532925199433],
                ID["EPSG",8821]],
            PARAMETER["Longitude of false origin",-96,
                ANGLEUNIT["degree",0.0174532925199433],
                ID["EPSG",8822]],
            PARAMETER["Easting at false origin",0,
                LENGTHUNIT["metre",1],
                ID["EPSG",8826]],
            PARAMETER["Northing at false origin",0,
                LENGTHUNIT["metre",1],
                ID["EPSG",8827]]],
        CS[Cartesian,2],
            AXIS["easting",east,
                ORDER[1],
                LENGTHUNIT["metre",1,
                    ID["EPSG",9001]]],
            AXIS["northing",north,
                ORDER[2],
                LENGTHUNIT["metre",1,
                    ID["EPSG",9001]]]]"""
