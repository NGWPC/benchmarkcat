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

    # Replace double quotes with single quotes
    wkt = wkt.replace('"', "'")

    return wkt
