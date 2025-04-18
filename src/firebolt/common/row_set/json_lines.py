from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, List, Union

from firebolt.common._types import RawColType
from firebolt.common.row_set.types import Statistics
from firebolt.utils.exception import OperationalError


class MessageType(Enum):
    start = "START"
    data = "DATA"
    success = "FINISH_SUCCESSFULLY"
    error = "FINISH_WITH_ERRORS"


@dataclass
class Column:
    name: str
    type: str


@dataclass
class StartRecord:
    message_type: MessageType
    result_columns: List[Column]
    query_id: str
    query_label: str
    request_id: str


@dataclass
class DataRecord:
    message_type: MessageType
    data: List[List[RawColType]]


@dataclass
class ErrorRecord:
    message_type: MessageType
    errors: List[Dict[str, Any]]
    query_id: str
    query_label: str
    request_id: str
    statistics: Statistics


@dataclass
class SuccessRecord:
    message_type: MessageType
    statistics: Statistics


JSONLinesRecord = Union[StartRecord, DataRecord, ErrorRecord, SuccessRecord]


def parse_json_lines_record(record: dict) -> JSONLinesRecord:
    """
    Parse a JSON lines record into its corresponding data class.

    Args:
        record (dict): The JSON lines record to parse.

    Returns:
        JSONLinesRecord: The parsed JSON lines record.

    Raises:
        OperationalError: If the JSON line message_type is unknown or if it contains
            a record of invalid format.
    """
    if "message_type" not in record:
        raise OperationalError("Invalid JSON lines record format: missing message_type")

    message_type = MessageType(record["message_type"])

    try:
        if message_type == MessageType.start:
            result_columns = [Column(**col) for col in record.pop("result_columns")]
            return StartRecord(result_columns=result_columns, **record)
        elif message_type == MessageType.data:
            return DataRecord(**record)
        elif message_type == MessageType.error:
            statistics = Statistics(**record.pop("statistics"))
            return ErrorRecord(statistics=statistics, **record)
        elif message_type == MessageType.success:
            statistics = Statistics(**record.pop("statistics"))
            return SuccessRecord(statistics=statistics, **record)
        raise OperationalError(f"Unknown message type: {message_type}")
    except (TypeError, KeyError) as e:
        raise OperationalError(f"Invalid JSON lines {message_type} record format: {e}")
