from shapely.geometry import Point, Polygon, LineString

def create_geometry(geo_obj):
    if isinstance(geo_obj, Point):
        return geo_obj
    elif isinstance(geo_obj, dict):
        geo_type = geo_obj['type'].lower()
        coords = geo_obj['coordinates']
        
        if geo_type == 'point' and len(coords) >= 2:
            return Point(coords[0], coords[1])
        elif geo_type == 'polygon':
            return Polygon(coords[0])
        elif geo_type == 'linestring':
            return LineString(coords)
    return None