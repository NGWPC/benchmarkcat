import pystac
from pystac.extensions.base import PropertiesExtension, ExtensionManagementMixin
from typing import Any, Dict, List, Optional, Union

# Define constants for the BLE extension
SCHEMA_URI = "https://gitlab.sh.nextgenwaterprediction.com/NGWPC/fim-c/benchmarkcat/-/raw/main/schemas/BLE/ble.json"
PREFIX = "ble:"

# Define the BLEExtension class
class BLEExtension(
    PropertiesExtension, 
    ExtensionManagementMixin[Union[pystac.Item, pystac.Collection]]
):
    def __init__(self, obj: Union[pystac.Item, pystac.Collection]) -> None:
        self.obj = obj
        self.properties = obj.properties if isinstance(obj, pystac.Item) else obj.extra_fields

    def apply(
        self,
        extent_area: Optional[Dict[str, Any]] = None,
        model_dimension: Optional[int] = None,
        magnitude: Optional[List[int]] = None,
        huc8: Optional[int] = None,
        flow_type: Optional[List[str]] = None,
        continuous: Optional[List[str]] = None,
        model_resolution: Optional[int] = None,
        terrain_resolution: Optional[int] = None,
        categorical: Optional[str] = None,
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
    def model_dimension(self) -> Optional[int]:
        return self._get_property(f"{PREFIX}model_dimension", int)

    @model_dimension.setter
    def model_dimension(self, v: Optional[int]) -> None:
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
    def flow_type(self) -> Optional[List[str]]:
        return self._get_property(f"{PREFIX}flow_type", List[str])

    @flow_type.setter
    def flow_type(self, v: Optional[List[str]]) -> None:
        self._set_property(f"{PREFIX}flow_type", v)

    @property
    def continuous(self) -> Optional[List[str]]:
        return self._get_property(f"{PREFIX}continuous", List[str])

    @continuous.setter
    def continuous(self, v: Optional[List[str]]) -> None:
        self._set_property(f"{PREFIX}continuous", v)

    @property
    def model_resolution(self) -> Optional[int]:
        return self._get_property(f"{PREFIX}model_resolution", int)

    @model_resolution.setter
    def model_resolution(self, v: Optional[int]) -> None:
        self._set_property(f"{PREFIX}model_resolution", v)

    @property
    def terrain_resolution(self) -> Optional[int]:
        return self._get_property(f"{PREFIX}terrain_resolution", int)

    @terrain_resolution.setter
    def terrain_resolution(self, v: Optional[int]) -> None:
        self._set_property(f"{PREFIX}terrain_resolution", v)

    @property
    def categorical(self) -> Optional[str]:
        return self._get_property(f"{PREFIX}categorical", str)

    @categorical.setter
    def categorical(self, v: Optional[str]) -> None:
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
    def ext(cls, obj: Union[pystac.Item, pystac.Collection], add_if_missing: bool = False) -> "BLEExtension":
        if isinstance(obj, pystac.Item):
            cls.ensure_has_extension(obj, add_if_missing)
            return BLEExtension(obj)
        else:
            raise pystac.ExtensionTypeError(
                f"BLEExtension does not apply to type '{type(obj).__name__}'"
            )
