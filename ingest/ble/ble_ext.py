import os
from typing import Literal
import pystac
from pystac.extensions.base import PropertiesExtension, ExtensionManagementMixin
from typing import Any, Dict, List, Optional, Union
import pdb

# Define constants for the BLE extension
homedir = os.path.expanduser("~")
SCHEMA_URI: str = f"file://{homedir}/benchmarkcat/schemas/ble/v1.0.0/ble.json"
PREFIX: str = "ble:"

class BLEExtension(
    PropertiesExtension, 
    ExtensionManagementMixin[Union[pystac.Item, pystac.Collection]]
):
    name: Literal["ble"] = "ble"

    def __init__(self, item: pystac.Item) -> None:
        self.item = item
        self.properties = item.properties

    def apply(
        self,
        extent_area: Optional[Dict[str, Any]] = None,
        model_dimension: Optional[List[int]] = None,
        magnitude: Optional[List[int]] = None,
        huc8: Optional[int] = None,
        flow_type: Optional[str] = None,
        continuous: Optional[List[str]] = None,
        model_resolution: Optional[List[int]] = None,
        terrain_resolution: Optional[List[int]] = None,
        categorical: Optional[List[str]] = None,
        elevation_source: Optional[Dict[str, Any]] = None
    ) -> None:
        if extent_area is not None:
            self.extent_area = extent_area
        if model_dimension is not None:
            self.model_dimension = model_dimension
        if magnitude is not None:
            self.magnitude = magnitude
        if huc8 is not None:
            self.huc8 = huc8
        if flow_type is not None:
            self.flow_type = flow_type
        if continuous is not None:
            self.continuous = continuous
        if model_resolution is not None:
            self.model_resolution = model_resolution
        if terrain_resolution is not None:
            self.terrain_resolution = terrain_resolution
        if categorical is not None:
            self.categorical = categorical
        if elevation_source is not None:
            self.elevation_source = elevation_source

    @property
    def extent_area(self) -> Optional[Dict[str, Any]]:
        return self._get_property(f"{PREFIX}extent_area", Dict[str, Any])

    @extent_area.setter
    def extent_area(self, v: Optional[Dict[str, Any]]) -> None:
        self._set_property(f"{PREFIX}extent_area", v)

    @property
    def model_dimension(self) -> Optional[List[int]]:
        return self._get_property(f"{PREFIX}model_dimension", List[int])

    @model_dimension.setter
    def model_dimension(self, v: Optional[List[int]]) -> None:
        self._set_property(f"{PREFIX}model_dimension", v)

    @property
    def magnitude(self) -> Optional[List[int]]:
        return self._get_property(f"{PREFIX}magnitude", List[int])

    @magnitude.setter
    def magnitude(self, v: Optional[List[int]]) -> None:
        self._set_property(f"{PREFIX}magnitude", v)

    @property
    def huc8(self) -> Optional[int]:
        return self._get_property(f"{PREFIX}huc8", int)

    @huc8.setter
    def huc8(self, v: Optional[int]) -> None:
        self._set_property(f"{PREFIX}huc8", v)

    @property
    def flow_type(self) -> Optional[str]:
        return self._get_property(f"{PREFIX}flow_type", str)

    @flow_type.setter
    def flow_type(self, v: Optional[str]) -> None:
        self._set_property(f"{PREFIX}flow_type", v)

    @property
    def continuous(self) -> Optional[List[str]]:
        return self._get_property(f"{PREFIX}continuous", List[str])

    @continuous.setter
    def continuous(self, v: Optional[List[str]]) -> None:
        self._set_property(f"{PREFIX}continuous", v)

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
    def categorical(self) -> Optional[List[str]]:
        return self._get_property(f"{PREFIX}categorical", List[str])

    @categorical.setter
    def categorical(self, v: Optional[List[str]]) -> None:
        self._set_property(f"{PREFIX}categorical", v)

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
    def ext(cls, obj:pystac.Item, add_if_missing: bool = True) -> "BLEExtension":
        if isinstance(obj, pystac.Item):
            cls.ensure_has_extension(obj, add_if_missing)
            return BLEExtension(obj)
        else:
            raise pystac.ExtensionTypeError(
                f"OrderExtension does not apply to type '{type(obj).__name__}'"
            )

# 
# item = pystac.read_file(
#     "https://raw.githubusercontent.com/radiantearth/stac-spec/master/examples/core-item.json"
# )
# item.properties
# print(item.stac_extensions)
# print(f"BLEExtesions uri: {BLEExtension.get_schema_uri()}")
# print(f"Implements Extension: {BLEExtension.has_extension(item)}")
# # pdb.set_trace()
# order_ext = BLEExtension.ext(item, add_if_missing=True)

# print(f"Implements Extension: {BLEExtension.has_extension(item)}")
