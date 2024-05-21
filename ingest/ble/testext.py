from typing import Literal
from datetime import datetime, timedelta
from pprint import pprint
from typing import Any, Dict, List, Optional, Union
from uuid import uuid4

import pystac
from pystac.utils import (
    StringEnum,
    datetime_to_str,
    get_required,
    map_opt,
    str_to_datetime,
)
from pystac.extensions.base import PropertiesExtension, ExtensionManagementMixin

class OrderEventType(StringEnum):
    SUBMITTED = "submitted"
    STARTED_PROCESSING = "started_processing"
    DELIVERED = "delivered"
    CANCELLED = "cancelled"


class OrderEvent:
    properties: Dict[str, Any]

    def __init__(self, properties: Dict[str, Any]) -> None:
        self.properties = properties

    @property
    def event_type(self) -> OrderEventType:
        return get_required(self.properties.get("type"), self, "event_type")

    @event_type.setter
    def event_type(self, v: OrderEventType) -> None:
        self.properties["type"] = str(v)

    @property
    def timestamp(self) -> datetime:
        return str_to_datetime(
            get_required(self.properties.get("timestamp"), self, "timestamp")
        )

    @timestamp.setter
    def timestamp(self, v: datetime) -> None:
        self.properties["timestamp"] = datetime_to_str(v)

    def __repr__(self) -> str:
        return "<OrderEvent " f"type={self.event_type} " f"timestamp={self.timestamp}>"

    def apply(
        self,
        event_type: OrderEventType,
        timestamp: datetime,
    ) -> None:
        self.event_type = event_type
        self.timestamp = timestamp

    @classmethod
    def create(
        cls,
        event_type: OrderEventType,
        timestamp: datetime,
    ) -> "OrderEvent":
        oe = cls({})
        oe.apply(event_type=event_type, timestamp=timestamp)
        return oe

    def to_dict(self) -> Dict[str, Any]:
        return self.properties

SCHEMA_URI: str = "https://example.com/image-order/v1.0.0/schema.json"
PREFIX: str = "order:"
ID_PROP: str = PREFIX + "id"
HISTORY_PROP: str = PREFIX + "history"


class OrderExtension(
    PropertiesExtension, ExtensionManagementMixin[Union[pystac.Item, pystac.Collection]]
):
    name: Literal["order"] = "order"

    def __init__(self, item: pystac.Item):
        self.item = item
        self.properties = item.properties

    def apply(
        self, order_id: str = None, history: Optional[List[OrderEvent]] = None
    ) -> None:
        self.order_id = order_id
        self.history = history

    @property
    def order_id(self) -> str:
        return get_required(self._get_property(ID_PROP, str), self, ID_PROP)

    @order_id.setter
    def order_id(self, v: str) -> None:
        self._set_property(ID_PROP, v, pop_if_none=False)

    @property
    def history(self) -> Optional[List[OrderEvent]]:
        return map_opt(
            lambda history: [OrderEvent(d) for d in history],
            self._get_property(HISTORY_PROP, List[OrderEvent]),
        )

    @history.setter
    def history(self, v: Optional[List[OrderEvent]]) -> None:
        self._set_property(
            HISTORY_PROP,
            map_opt(lambda history: [event.to_dict() for event in history], v),
            pop_if_none=True,
        )

    @classmethod
    def get_schema_uri(cls) -> str:
        return SCHEMA_URI

    @classmethod
    def ext(cls, obj: pystac.Item, add_if_missing: bool = False) -> "OrderExtension":
        if isinstance(obj, pystac.Item):
            cls.ensure_has_extension(obj, add_if_missing)
            return OrderExtension(obj)
        else:
            raise pystac.ExtensionTypeError(
                f"OrderExtension does not apply to type '{type(obj).__name__}'"
            )

item = pystac.read_file(
    "https://raw.githubusercontent.com/radiantearth/stac-spec/master/examples/core-item.json"
)
item.properties

print(f"Implements Extension: {OrderExtension.has_extension(item)}")
print(f"Order ID: {item.properties.get(ID_PROP)}")
print("History:")
for event in item.properties.get(HISTORY_PROP, []):
    pprint(event)

order_ext = OrderExtension.ext(item, add_if_missing=True)

# Create a unique string ID for the order ID
order_ext.order_id = str(uuid4())

# Create some fake order history and set it using the extension
event_1 = OrderEvent.create(
    event_type=OrderEventType.SUBMITTED, timestamp=datetime.now() - timedelta(days=1)
)
event_2 = OrderEvent.create(
    event_type=OrderEventType.STARTED_PROCESSING,
    timestamp=datetime.now() - timedelta(hours=12),
)
event_3 = OrderEvent.create(
    event_type=OrderEventType.DELIVERED, timestamp=datetime.now() - timedelta(hours=1)
)

print(f"Implements Extension: {OrderExtension.has_extension(item)}")
print(f"Order ID: {item.properties.get(ID_PROP)}")
print("History:")
for event in item.properties.get(HISTORY_PROP, []):
    pprint(event)
