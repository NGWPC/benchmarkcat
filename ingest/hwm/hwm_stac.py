def create_wkt_string(horizontalDatumName, verticalDatumName):
    # assuming the datum will always be some variant of WGS 84 or NAD 83
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
    
    return wkt
