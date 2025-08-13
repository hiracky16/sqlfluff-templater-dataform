import re
from pathlib import Path

from .helpers import assert_sql_is_equal
from .constants import (
    TEST_DATASET_ID,
    TEST_PROJECT_ID,
    TEST_INPUT_FILE_SEPARATOR,
    PATH_TEST_INPUTS,
)
import pytest


@pytest.mark.parametrize(
    "input_sql, expected_sql",
    [
        (
            r"SELECT * FROM ${hoge}, ${hoge.fuga('another')}",
            r"SELECT * FROM 'hoge', 'hoge.fuga(\'another\')'",
        ),
        (
            """SELECT * FROM ${
            hoge
            }, ${
            hoge.fuga('another')
            }""",
            """SELECT * FROM 'hoge', 'hoge.fuga(\\'another\\')'""",
        ),
        (
            # Ensure a recursive pattern is allowed
            """pre_operations {
                SELECT * FROM ${ref('test')}
                }""",
            f"SELECT * FROM `{TEST_PROJECT_ID}.{TEST_DATASET_ID}.test`;",
        ),
    ],
)
def test_replace_templates(templater, input_sql: str, expected_sql: re.Pattern | str):
    result = templater.templatize_sql(input_sql)

    assert_sql_is_equal(
        expected_sql=expected_sql,
        actual_sql=result,
        ignore_whitespace=True,
    )


@pytest.mark.parametrize(
    "test_input_path",
    list(PATH_TEST_INPUTS.glob("templatize_*.sqlx")),
)
def test_templatize_file(templater, test_input_path: Path):
    input_sql, expected_sql = test_input_path.read_text().split(
        TEST_INPUT_FILE_SEPARATOR
    )

    result = templater.templatize_sql(input_sql)

    try:
        assert_sql_is_equal(
            expected_sql=expected_sql,
            actual_sql=result,
            ignore_whitespace=True,
        )
    except AssertionError as exc:
        raise AssertionError(
            f"Assertion failed for test input file: {test_input_path.name!r}"
        ) from exc
