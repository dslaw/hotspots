from datetime import datetime

import pytest
from pydantic import BaseModel

from src.validation import Validator, _model_cls_mapping


@pytest.fixture
def test_schema_name():
    class Model(BaseModel):
        a: int
        b: str | None = None
        c: datetime

    schema_name = "test_model"
    _model_cls_mapping[schema_name] = Model
    yield schema_name

    _model_cls_mapping.pop(schema_name)
    return


class TestValidator:
    def test_validate(self, test_schema_name):
        records = [
            {"a": 1, "b": "present", "c": datetime(2025, 1, 1)},
            {"a": 2, "b": None, "c": datetime(2025, 1, 2)},
            {"a": 3, "c": datetime(2025, 1, 3)},
            {"a": 3, "b": None},
        ]
        expected = [
            {"a": 1, "b": "present", "c": datetime(2025, 1, 1)},
            {"a": 2, "b": None, "c": datetime(2025, 1, 2)},
            # Default value is injected.
            {"a": 3, "b": None, "c": datetime(2025, 1, 3)},
            # Record with missing required field `c` is filtered out.
        ]

        validator = Validator(test_schema_name)
        actual = validator.validate(iter(records))
        assert list(actual) == expected
