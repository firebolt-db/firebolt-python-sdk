import json
from unittest.mock import MagicMock

import pytest
from httpx import Response


@pytest.fixture
def mock_decimal_response_streaming() -> Response:
    """Create a mock Response with decimal data."""
    mock = MagicMock(spec=Response)

    # Create JSON record strings with properly formatted data
    start_record_data = {
        "message_type": "START",
        "result_columns": [
            {"name": "col1", "type": "int"},
            {"name": "col2", "type": "string"},
            {"name": "col3", "type": "numeric(10, 2)"},
        ],
        "query_id": "q1",
        "query_label": "l1",
        "request_id": "r1",
    }

    data_record_data = {
        "message_type": "DATA",
        "data": [
            [1, "one", "1231232.123459999990457054844258706536"],
            [2, "two", "1231232.123459999990457054844258706536"],
        ],
    }

    success_record_data = {
        "message_type": "FINISH_SUCCESSFULLY",
        "statistics": {
            "elapsed": 0.1,
            "rows_read": 10,
            "bytes_read": 100,
            "time_before_execution": 0.01,
            "time_to_execute": 0.09,
        },
    }

    # Generate the JSON strings
    start_record = json.dumps(start_record_data)
    data_record = json.dumps(data_record_data)

    # Replace the decimal string with a float to simulate the behavior of FB 1.0
    # for one of the rows
    data_record = data_record.replace(
        '"1231232.123459999990457054844258706536"',
        "1231232.123459999990457054844258706536",
        1,
    )

    success_record = json.dumps(success_record_data)

    mock.iter_lines.return_value = iter([start_record, data_record, success_record])

    async def async_iter():
        for item in [
            start_record.encode("utf-8"),
            data_record.encode("utf-8"),
            success_record.encode("utf-8"),
        ]:
            yield item

    mock.aiter_lines.side_effect = async_iter
    mock.is_closed = False
    return mock


@pytest.fixture
def mock_decimal_bytes_stream() -> Response:
    """Create a mock bytes stream with decimal data."""
    mock = MagicMock(spec=Response)
    data = iter(
        [
            json.dumps(
                {
                    "meta": [
                        {"name": "col1", "type": "int"},
                        {"name": "col2", "type": "string"},
                        {"name": "col3", "type": "Decimal(10, 2)"},
                    ],
                    "data": [
                        [1, "one", "1231232.123459999990457054844258706536"],
                        [2, "two", "1231232.123459999990457054844258706536"],
                    ],
                    "statistics": {
                        "elapsed": 0.1,
                        "rows_read": 10,
                        "bytes_read": 100,
                        "time_before_execution": 0.01,
                        "time_to_execute": 0.09,
                    },
                }
            )
            # Replace the decimal string with a float to simulate the behavior of FB 1.0
            # for one of the rows
            .replace(
                '"1231232.123459999990457054844258706536"',
                "1231232.123459999990457054844258706536",
                1,
            ).encode("utf-8")
        ]
    )
    mock.iter_bytes.return_value = data

    async def async_iter():
        for item in data:
            yield item

    mock.aiter_bytes.side_effect = async_iter
    mock.is_closed = False
    return mock
