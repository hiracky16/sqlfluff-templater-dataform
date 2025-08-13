"""Slicing tests."""

from pathlib import Path


from .helpers import (
    assert_sql_is_equal,
    SliceExpected,
    assert_slices,
)
from .constants import TEST_INPUT_FILE_SEPARATOR


def test_slice_sqlx_template_with_config_and_ref(templater):
    input_sqlx = """config {
    type: "table"
}
SELECT 1 AS value FROM ${ref('test')} WHERE true
"""
    expected_sql = (
        "\n\nSELECT 1 AS value FROM `my_project.my_dataset.test` WHERE true\n"
    )
    replaced_sql, raw_slices, templated_slices = templater.slice_sqlx_template(
        input_sqlx
    )
    assert_sql_is_equal(expected_sql=expected_sql, actual_sql=replaced_sql)

    assert_slices(
        slices_raw_actual=raw_slices,
        slices_template_actual=templated_slices,
        slices_expected=[
            SliceExpected(
                "config", sql_slice_type="templated", sql_comparison_func=str.startswith
            ),
            SliceExpected(
                "\nSELECT 1",
                sql_slice_type="literal",
                sql_comparison_func=str.startswith,
            ),
            SliceExpected(
                "${ref", sql_slice_type="templated", sql_comparison_func=str.startswith
            ),
            SliceExpected(" WHERE true\n", sql_slice_type="literal"),
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
    expected_sql = "\n\nSELECT * FROM `my_project.my_dataset.test` JOIN `my_project.my_dataset.other_table` ON test.id = other_table.id\n"

    replaced_sql, raw_slices, templated_slices = templater.slice_sqlx_template(
        input_sqlx
    )

    assert_sql_is_equal(expected_sql=expected_sql, actual_sql=replaced_sql)

    assert_slices(
        slices_raw_actual=raw_slices,
        slices_template_actual=templated_slices,
        slices_expected=[
            SliceExpected(
                sql_comparison_func=str.startswith,
                sql_expected="config",
                sql_slice_type="templated",
            ),
            SliceExpected(
                sql_comparison_func=str.startswith,
                sql_expected="\nSELECT *",
                sql_slice_type="literal",
            ),
            SliceExpected(
                sql_comparison_func=str.startswith,
                sql_expected="${ref",
                sql_slice_type="templated",
            ),
            SliceExpected(
                sql_comparison_func=str.startswith,
                sql_expected=" JOIN",
                sql_slice_type="literal",
            ),
            SliceExpected(
                sql_comparison_func=str.startswith,
                sql_expected="${ref",
                sql_slice_type="templated",
            ),
            SliceExpected(
                sql_comparison_func=str.endswith,
                sql_expected=" ON test.id = other_table.id\n",
                sql_slice_type="literal",
            ),
        ],
    )


def test_slice_sqlx_template_with_full_expression_query(
    templater, test_inputs_dir_path: Path
):
    input_sqlx, expected_sql = (
        (test_inputs_dir_path / "templatize_slice_2.sqlx")
        .read_text()
        .split(TEST_INPUT_FILE_SEPARATOR)
    )

    replaced_sql, raw_slices, templated_slices = templater.slice_sqlx_template(
        input_sqlx
    )

    assert_sql_is_equal(
        expected_sql=expected_sql, actual_sql=replaced_sql, ignore_whitespace=True
    )

    assert_slices(
        slices_raw_actual=raw_slices,
        slices_template_actual=templated_slices,
        slices_expected=[
            SliceExpected(
                sql_slice_type="templated",
                sql_comparison_func=str.startswith,
                sql_expected="config",
            ),
            SliceExpected(
                sql_slice_type="literal",
                sql_comparison_func=str.startswith,
                sql_expected="\n",
            ),
            SliceExpected(
                sql_slice_type="templated",
                sql_comparison_func=str.startswith,
                sql_expected="js",
            ),
            SliceExpected(
                sql_slice_type="literal",
                sql_comparison_func=str.startswith,
                sql_expected="\n",
            ),
            SliceExpected(
                sql_slice_type="templated",
                sql_comparison_func=str.startswith,
                sql_expected="pre_operations",
            ),
            SliceExpected(
                sql_slice_type="literal",
                sql_comparison_func=str.startswith,
                sql_expected="\n",
            ),
            SliceExpected(
                sql_slice_type="templated",
                sql_comparison_func=str.startswith,
                sql_expected="post_operations",
            ),
            SliceExpected(
                sql_slice_type="literal",
                sql_comparison_func=str.startswith,
                sql_expected="\n\nSELECT *",
            ),
            SliceExpected(
                sql_slice_type="templated",
                sql_comparison_func=str.startswith,
                sql_expected="${ref",
            ),
            SliceExpected(
                sql_slice_type="literal",
                sql_comparison_func=str.startswith,
                sql_expected=" JOIN",
            ),
            SliceExpected(
                sql_slice_type="templated",
                sql_comparison_func=str.startswith,
                sql_expected="${ref",
            ),
            SliceExpected(
                sql_slice_type="literal",
                sql_comparison_func=str.startswith,
                sql_expected=" ON test.id = other_table.id AND test.name = ",
            ),
            SliceExpected(
                sql_slice_type="templated",
                sql_comparison_func=str.startswith,
                sql_expected="${hoge}",
            ),
            SliceExpected(
                sql_slice_type="literal",
                sql_comparison_func=str.startswith,
                sql_expected="\n",
            ),
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
        slices_expected=[
            SliceExpected(
                sql_slice_type="literal",
                sql_expected="SELECT * FROM my_table WHERE true\n",
            )
        ],
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
        slices_expected=[
            SliceExpected(
                sql_slice_type="templated",
                sql_comparison_func=str.startswith,
                sql_expected="config",
            ),
            SliceExpected(
                sql_slice_type="literal",
                sql_comparison_func=str.startswith,
                sql_expected="\nSELECT *",
            ),
            SliceExpected(
                sql_slice_type="templated",
                sql_comparison_func=str.startswith,
                sql_expected="${ref",
            ),
            SliceExpected(
                sql_slice_type="literal",
                sql_comparison_func=str.startswith,
                sql_expected=" JOIN",
            ),
            SliceExpected(
                sql_slice_type="templated",
                sql_comparison_func=str.startswith,
                sql_expected="${ref",
            ),
            SliceExpected(
                sql_slice_type="literal",
                sql_comparison_func=str.endswith,
                sql_expected=" ON test.id = other_table.id\n",
            ),
            SliceExpected(
                sql_slice_type="templated",
                sql_comparison_func=str.startswith,
                sql_expected="${when(",
            ),
            SliceExpected(
                sql_slice_type="literal",
                sql_comparison_func=str.startswith,
                sql_expected="\nGROUP BY",
            ),
        ],
    )
