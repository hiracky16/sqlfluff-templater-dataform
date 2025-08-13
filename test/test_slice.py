"""Slicing tests."""

import re
from typing import Callable, List, Tuple, Literal, TypeAlias, Collection
from operator import eq as op_equal

from sqlfluff.core.templaters.base import (
    RawTemplater,
    TemplatedFile,
    large_file_check,
    RawFileSlice,
    TemplatedFileSlice,
)

from .helpers import assert_sql_is_equal

ALIAS_TEMPLATE_TYPE: TypeAlias = Literal["templated", "literal"]
ALIAS_SLICE_EXPECTED_SQL_SNIPPET_AND_FUNC: TypeAlias = (
    Tuple[Callable[..., bool], str] | str
)


def assert_slices(
    slices_raw_actual: Collection[RawFileSlice],
    slices_template_actual: Collection[TemplatedFileSlice],
    slices_template_type: Collection[ALIAS_TEMPLATE_TYPE],
    slices_expected_sql_snippet_and_func: Collection[
        ALIAS_SLICE_EXPECTED_SQL_SNIPPET_AND_FUNC
    ],
):
    assert (
        len(slices_raw_actual)
        == len(slices_template_actual)
        == len(slices_template_type)
        == len(slices_expected_sql_snippet_and_func)
    ), "The length of the slices are not the same."

    for (
        slice_raw_actual,
        slice_template_actual,
        slice_template_type,
        _slice_expected,
    ) in zip(
        slices_raw_actual,
        slices_template_actual,
        slices_template_type,
        *zip(slices_expected_sql_snippet_and_func),
    ):
        slice_raw_actual: RawFileSlice
        slice_template_actual: TemplatedFileSlice
        slice_template_type: ALIAS_TEMPLATE_TYPE
        _slice_expected: ALIAS_SLICE_EXPECTED_SQL_SNIPPET_AND_FUNC

        slice_expected_sql_func, slice_expected_sql_snippet = (
            _slice_expected
            if isinstance(_slice_expected, tuple)
            else (op_equal, _slice_expected)
        )
        # ^ Assume that if the item is not a tuple, that its a bare string and it should be equal

        assert slice_expected_sql_func(slice_raw_actual.raw, slice_expected_sql_snippet)
        assert slice_template_actual.slice_type == slice_template_type
        # assert slice_raw_actual.slice_type == slice_template_type, (
        #     "Template type is not the same"
        # )


def test_slice_sqlx_template_with_config_and_ref(templater):
    input_sqlx = """config {
    type: "table"
}
SELECT 1 AS value FROM ${ref('test')} WHERE true
"""
    expected_sql = "\nSELECT 1 AS value FROM `my_project.my_dataset.test` WHERE true\n"
    replaced_sql, raw_slices, templated_slices = templater.slice_sqlx_template(
        input_sqlx
    )
    assert_sql_is_equal(expected_sql=expected_sql, actual_sql=replaced_sql)

    assert_slices(
        slices_raw_actual=raw_slices,
        slices_template_actual=templated_slices,
        slices_template_type=["templated", "literal", "templated", "literal"],
        slices_expected_sql_snippet_and_func=[
            (str.startswith, "config"),
            (str.startswith, "\nSELECT 1"),
            (str.startswith, "${ref"),
            (op_equal, " WHERE true\n"),
        ],
    )


def test_slice_sqlx_template_with_multiple_refs(templater):
    input_sqlx = """config {
    type: "view",
    columns: {
        "test" : "test",
        "value:: "value"
    }
}
SELECT * FROM ${ref('test')} JOIN ${ref('other_table')} ON test.id = other_table.id
"""
    expected_sql = "\nSELECT * FROM `my_project.my_dataset.test` JOIN `my_project.my_dataset.other_table` ON test.id = other_table.id\n"

    replaced_sql, raw_slices, templated_slices = templater.slice_sqlx_template(
        input_sqlx
    )

    assert_sql_is_equal(expected_sql=expected_sql, actual_sql=replaced_sql)

    assert_slices(
        slices_raw_actual=raw_slices,
        slices_template_actual=templated_slices,
        slices_template_type=[
            "templated",
            "literal",
            "templated",
            "literal",
            "templated",
            "literal",
        ],
        slices_expected_sql_snippet_and_func=[
            (str.startswith, "config"),
            (str.startswith, "\nSELECT *"),
            (str.startswith, "${ref"),
            (str.startswith, " JOIN"),
            (str.startswith, "${ref"),
            (str.endswith, " ON test.id = other_table.id\n"),
        ],
    )


def test_slice_sqlx_template_with_full_expression_query(templater):
    input_sqlx = """config {
    type: "view",
    columns: {
        "test" : "test",
        "value:: "value"
    }
}
js {
    var hoge = "fuga"
}
pre_operations {
    CREATE TEMP FUNCTION AddFourAndDivide(x INT64, y INT64)
      RETURNS FLOAT64
      AS ((x + 4) / y);
}
post_operations {
  GRANT `roles/bigquery.dataViewer`
      ON
      TABLE ${self()}
      TO "group:allusers@example.com", "user:otheruser@example.com"
}

SELECT * FROM ${ref('test')} JOIN ${ref("other_dataset", "other_table")} ON test.id = other_table.id AND test.name = ${hoge}
"""
    expected_sql = re.compile(
        r"\s+SELECT \* FROM `my_project\.my_dataset\.test` JOIN `my_project\.other_dataset\.other_table` ON test\.id = other_table\.id AND test\.name = a.+\n"
    )

    replaced_sql, raw_slices, templated_slices = templater.slice_sqlx_template(
        input_sqlx
    )

    assert_sql_is_equal(expected_sql=expected_sql, actual_sql=replaced_sql)

    assert_slices(
        slices_raw_actual=raw_slices,
        slices_template_actual=templated_slices,
        slices_template_type=[
            "templated",
            "literal",
            "templated",
            "literal",
            "templated",
            "literal",
            "templated",
            "literal",
            "templated",
            "literal",
            "templated",
            "literal",
            "templated",
            "literal",
        ],
        slices_expected_sql_snippet_and_func=[
            (str.startswith, "config"),
            (str.startswith, "js"),
            (str.startswith, "pre_operations"),
            (str.startswith, "post_operations"),
            (str.startswith, "\n\nSELECT *"),
            (str.startswith, "${ref"),
            (str.startswith, " ON test.id = other_table.id AND test.name = "),
            (str.startswith, " JOIN"),
            (str.startswith, "${ref"),
            (str.startswith, "${hoge}"),
            (str.startswith, "\n"),
        ],
    )


def test_slice_sqlx_template_with_no_ref(templater):
    input_sqlx = """SELECT * FROM my_table WHERE true
"""
    expected_sql = "SELECT * FROM my_table WHERE true\n"

    replaced_sql, raw_slices, templated_slices = templater.slice_sqlx_template(
        input_sqlx
    )
    assert_sql_is_equal(expected_sql=expected_sql, actual_sql=replaced_sql)

    assert_slices(
        slices_raw_actual=raw_slices,
        slices_template_actual=templated_slices,
        slices_template_type=[
            "literal",
        ],
        slices_expected_sql_snippet_and_func=["SELECT * FROM my_table WHERE true\n"],
    )


def test_slice_sqlx_template_with_incremental_table(templater):
    input_sqlx = """config {
    type: "incremental",
    columns: {
        "test" : "test",
        "value:: "value"
    }
}
SELECT * FROM ${ref('test')} JOIN ${ref('other_table')}
  ON test.id = other_table.id
${when(incremental(), "WHERE updated_at > '2020-01-01'")}
GROUP BY test
"""
    expected_sql = """

SELECT * FROM `my_project.my_dataset.test` JOIN `my_project.my_dataset.other_table`
  ON test.id = other_table.id
WHERE updated_at > '2020-01-01'
GROUP BY test
"""

    replaced_sql, list_raw_slice_actual, templated_slices = (
        templater.slice_sqlx_template(input_sqlx)
    )

    assert_sql_is_equal(expected_sql=expected_sql, actual_sql=replaced_sql)

    assert_slices(
        slices_raw_actual=list_raw_slice_actual,
        slices_template_actual=templated_slices,
        slices_template_type=[
            "templated",
            "literal",
            "templated",
            "literal",
            "templated",
            "literal",
        ],
        slices_expected_sql_snippet_and_func=[
            (str.startswith, "config"),
            (str.startswith, "\nSELECT *"),
            (str.startswith, "${ref"),
            (str.startswith, " JOIN"),
            (str.startswith, "${ref"),
            (str.endswith, " ON test.id = other_table.id\n"),
            (op_equal, "WHERE updated_at > '2020-01-01'"),
            (str.startswith, "\nGROUP BY"),
        ],
    )
