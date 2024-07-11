import os
from typing import Literal, Any, Dict, List, Optional, Union
import pystac
from pystac.extensions.base import PropertiesExtension, ExtensionManagementMixin

# Define constants for the USGS-FIM extension
homedir = os.path.expanduser("~")
SCHEMA_URI: str = f"file://{homedir}/benchmarkcat/schemas/hec_ras/v1.0.0/hec_ras.json"
PREFIX: str = "hec-ras:"

class HECRASExtension(
    PropertiesExtension, 
    ExtensionManagementMixin[Union[pystac.Item, pystac.Collection]]
):
    name: Literal["hec-ras"] = "hec-ras"

    def __init__(self, item: pystac.Item) -> None:
        self.item = item
        self.properties = item.properties

    def apply(
        self,
        huc8: Optional[int] = None,
        gauge: Optional[str] = None,
        magnitude: Optional[Dict[str, Any]] = None,
        extent_area: Optional[Dict[str, Any]] = None,
        attributes: Optional[Dict[str, Any]] = None,
        flowfile: Optional[Dict[str, Any]] = None,
        rating_curve: Optional[Dict[str, Any]] = None,
        model_resolution: Optional[List[int]] = None,
        terrain_resolution: Optional[List[int]] = None,
        model_dimension: Optional[List[int]] = None,
        flow_type: Optional[str] = None,
        categorical: Optional[List[str]] = None,
        continuous: Optional[List[str]] = None,
        elevation_source: Optional[Dict[str, Any]] = None
    ) -> None:
        if huc8 is not None:
            self.huc8 = huc8
        if gauge is not None:
            self.gauge = gauge
        if magnitude is not None:
            self.magnitude = magnitude
        if extent_area is not None:
            self.extent_area = extent_area
        if attributes is not None:
            self.attributes = attributes
        if flowfile is not None:
            self.flowfile = flowfile
        if rating_curve is not None:
            self.rating_curve = rating_curve
        if model_resolution is not None:
            self.model_resolution = model_resolution
        if terrain_resolution is not None:
            self.terrain_resolution = terrain_resolution
        if model_dimension is not None:
            self.model_dimension = model_dimension
        if flow_type is not None:
            self.flow_type = flow_type
        if categorical is not None:
            self.categorical = categorical
        if continuous is not None:
            self.continuous = continuous
        if elevation_source is not None:
            self.elevation_source = elevation_source

    @property
    def huc8(self) -> Optional[int]:
        return self._get_property(f"{PREFIX}huc8", int)

    @huc8.setter
    def huc8(self, v: Optional[int]) -> None:
        self._set_property(f"{PREFIX}huc8", v)

    @property
    def gauge(self) -> Optional[str]:
        return self._get_property(f"{PREFIX}gauge", str)

    @gauge.setter
    def gauge(self, v: Optional[str]) -> None:
        self._set_property(f"{PREFIX}gauge", v)

    @property
    def magnitude(self) -> Optional[Dict[str, Any]]:
        return self._get_property(f"{PREFIX}magnitude", Dict[str, Any])

    @magnitude.setter
    def magnitude(self, v: Optional[Dict[str, Any]]) -> None:
        self._set_property(f"{PREFIX}magnitude", v)

    @property
    def extent_area(self) -> Optional[Dict[str, Any]]:
        return self._get_property(f"{PREFIX}extent_area", Dict[str, Any])

    @extent_area.setter
    def extent_area(self, v: Optional[Dict[str, Any]]) -> None:
        self._set_property(f"{PREFIX}extent_area", v)

    @property
    def attributes(self) -> Optional[Dict[str, Any]]:
        return self._get_property(f"{PREFIX}attributes", Dict[str, Any])

    @attributes.setter
    def attributes(self, v: Optional[Dict[str, Any]]) -> None:
        self._set_property(f"{PREFIX}attributes", v)

    @property
    def flowfile(self) -> Optional[Dict[str, Any]]:
        return self._get_property(f"{PREFIX}flowfile", Dict[str, Any])

    @flowfile.setter
    def flowfile(self, v: Optional[Dict[str, Any]]) -> None:
        self._set_property(f"{PREFIX}flowfile", v)

    @property
    def rating_curve(self) -> Optional[Dict[str, Any]]:
        return self._get_property(f"{PREFIX}rating_curve", Dict[str, Any])

    @rating_curve.setter
    def rating_curve(self, v: Optional[Dict[str, Any]]) -> None:
        self._set_property(f"{PREFIX}rating_curve", v)

    @property
    def model_resolution(self) -> Optional[List[int]]:
        return self._get_property(f"{PREFIX}model_resolution", List[int])

    @model_resolution.setter
    def model_resolution(self, v: Optional[List[int]]) -> None:
        self._set_property(f"{PREFIX}model_resolution", v)

    @property
    def terrain_resolution(self) -> Optional[List[int]]:
        return self._get_property(f"{PREFIX}terrain_resolution", List[int])

    @terrain_resolution.setter
    def terrain_resolution(self, v: Optional[List[int]]) -> None:
        self._set_property(f"{PREFIX}terrain_resolution", v)

    @property
    def model_dimension(self) -> Optional[List[int]]:
        return self._get_property(f"{PREFIX}model_dimension", List[int])

    @model_dimension.setter
    def model_dimension(self, v: Optional[List[int]]) -> None:
        self._set_property(f"{PREFIX}model_dimension", v)

    @property
    def flow_type(self) -> Optional[str]:
        return self._get_property(f"{PREFIX}flow_type", str)

    @flow_type.setter
    def flow_type(self, v: Optional[str]) -> None:
        self._set_property(f"{PREFIX}flow_type", v)

    @property
    def categorical(self) -> Optional[List[str]]:
        return self._get_property(f"{PREFIX}categorical", List[str])

    @categorical.setter
    def categorical(self, v: Optional[List[str]]) -> None:
        self._set_property(f"{PREFIX}categorical", v)

    @property
    def continuous(self) -> Optional[List[str]]:
        return self._get_property(f"{PREFIX}continuous", List[str])

    @continuous.setter
    def continuous(self, v: Optional[List[str]]) -> None:
        self._set_property(f"{PREFIX}continuous", v)

    @property
    def elevation_source(self) -> Optional[Dict[str, Any]]:
        return self._get_property(f"{PREFIX}elevation_source", Dict[str, Any])

    @elevation_source.setter
    def elevation_source(self, v: Optional[Dict[str, Any]]) -> None:
        self._set_property(f"{PREFIX}elevation_source", v)

    @classmethod
    def get_schema_uri(cls) -> str:
        return SCHEMA_URI

    @classmethod
    def ext(cls, obj: Union[pystac.Item, pystac.Collection], add_if_missing: bool = True) -> "HECRASExtension":
        if isinstance(obj, (pystac.Item, pystac.Collection)):
            cls.ensure_has_extension(obj, add_if_missing)
            return HECRASExtension(obj)
        else:
            raise pystac.ExtensionTypeError(
                f"HECRASExtension does not apply to type '{type(obj).__name__}'"
            )

if __name__ == "__main__":
    # small test to make sure can apply extension to an item
    item = pystac.read_file("https://raw.githubusercontent.com/radiantearth/stac-spec/master/examples/core-item.json")
    print("Item properties before applying extension:", item.properties)
    
    usgs_fim_ext = HECRASExtension.ext(item, add_if_missing=True)
    usgs_fim_ext.apply(
        huc8=12345678,
        gauge="ABCD1",
        magnitude={"minor": "minor_asset_id", "moderate": "moderate_asset_id"},
        extent_area={"frequency": "annual", "area": 100.0},
        attributes={"value": "value_description"},
        flowfile={
            "assets": ["flowfile_asset_id"],
            "columns": [
                {"name": "column1", "description": "description1", "type": "string"},
                {"name": "column2", "description": "description2", "type": "integer"}
            ]
        },
        rating_curve={
            "assets": ["rating_curve_asset_id"],
            "columns": [
                {"name": "column1", "description": "description1", "type": "string"},
                {"name": "column2", "description": "description2", "type": "float"}
            ]
        },
        model_resolution=[10, 20],
        terrain_resolution=[5, 10],
        model_dimension=[1, 2],
        flow_type="observed",
        categorical=["cat_asset_id"],
        continuous=["cont_asset_id"],
        elevation_source={"source": "DEM","accuracy": "RMSE"}
    )

    print("Item properties after applying extension:", item.properties)
    print("Schema URI:", HECRASExtension.get_schema_uri())
    print("Implements Extension:", HECRASExtension.has_extension(item))
