"""Slicing tests."""

from pathlib import Path

import pytest


from .helpers import (
    assert_sql_is_equal,
    SliceExpected,
    assert_slices,
)
from .constants import TEST_INPUT_FILE_SEPARATOR


@pytest.mark.parametrize(
    [
        "file_name",
        "test_description",
        "slices_expected",
    ],
    [
        (
            "templatize_slice_8.sqlx",
            "config_and_ref",
            [
                SliceExpected(
                    "config",
                    sql_slice_type="templated",
                    sql_comparison_func=str.startswith,
                ),
                SliceExpected(
                    "\nSELECT 1",
                    sql_slice_type="literal",
                    sql_comparison_func=str.startswith,
                ),
                SliceExpected(
                    "${ref",
                    sql_slice_type="templated",
                    sql_comparison_func=str.startswith,
                ),
                SliceExpected(" WHERE true\n", sql_slice_type="literal"),
            ],
        ),
        (
            "templatize_slice_7.sqlx",
            "multiple_refs",
            [
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
        ),
        (
            "templatize_slice_2.sqlx",
            "full_expression_query",
            [
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
        ),
        (
            "templatize_slice_5.sqlx",
            "incremental_table",
            [
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
                    sql_expected=" ON test.id = other_table.id\n    ",
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
        ),
        (
            "templatize_slice_4.sqlx",
            "config_and_ref",
            [
                SliceExpected(
                    sql_expected="config",
                    sql_slice_type="templated",
                    sql_comparison_func=str.startswith,
                ),
                SliceExpected(
                    sql_expected="\nSELECT *",
                    sql_slice_type="literal",
                    sql_comparison_func=str.startswith,
                ),
                SliceExpected(
                    sql_expected="${ref(",
                    sql_slice_type="templated",
                    sql_comparison_func=str.startswith,
                ),
                SliceExpected(
                    sql_expected=" WHERE true",
                    sql_slice_type="literal",
                    sql_comparison_func=str.startswith,
                ),
            ],
        ),
        (
            "templatize_slice_3.sqlx",
            "multiple_refs",
            [
                SliceExpected(
                    sql_expected="config",
                    sql_slice_type="templated",
                    sql_comparison_func=str.startswith,
                ),
                SliceExpected(
                    sql_expected="\n\n\nSELECT *",
                    sql_slice_type="literal",
                    sql_comparison_func=str.startswith,
                ),
                SliceExpected(
                    sql_expected="${ref(",
                    sql_slice_type="templated",
                    sql_comparison_func=str.startswith,
                ),
                SliceExpected(
                    sql_expected=" JOIN",
                    sql_slice_type="literal",
                    sql_comparison_func=str.startswith,
                ),
                SliceExpected(
                    sql_expected="${ref(",
                    sql_slice_type="templated",
                    sql_comparison_func=str.startswith,
                ),
                SliceExpected(
                    sql_expected=" ON test.id = other_table.id",
                    sql_slice_type="literal",
                    sql_comparison_func=str.startswith,
                ),
            ],
        ),
        (
            "templatize_slice_1.sqlx",
            "post_pre_operations_config_and_ref",
            [
                SliceExpected(
                    sql_expected="config",
                    sql_slice_type="templated",
                    sql_comparison_func=str.startswith,
                ),
                SliceExpected(
                    sql_expected="\n",
                    sql_slice_type="literal",
                    sql_comparison_func=str.startswith,
                ),
                SliceExpected(
                    sql_expected="pre_operations",
                    sql_slice_type="templated",
                    sql_comparison_func=str.startswith,
                ),
                SliceExpected(
                    sql_expected="\n",
                    sql_slice_type="literal",
                    sql_comparison_func=str.startswith,
                ),
                SliceExpected(
                    sql_expected="post_operations",
                    sql_slice_type="templated",
                    sql_comparison_func=str.startswith,
                ),
                SliceExpected(
                    sql_expected="\nSELECT *",
                    sql_slice_type="literal",
                    sql_comparison_func=str.startswith,
                ),
                SliceExpected(
                    sql_expected="${ref(",
                    sql_slice_type="templated",
                    sql_comparison_func=str.startswith,
                ),
                SliceExpected(
                    sql_expected=" WHERE true",
                    sql_slice_type="literal",
                    sql_comparison_func=str.startswith,
                ),
            ],
        ),
        (
            "templatize_slice_9.sqlx",
            "no_ref",
            [
                SliceExpected(
                    sql_slice_type="literal",
                    sql_expected="SELECT * FROM my_table WHERE true\n",
                )
            ],
        ),
    ],
)
def test_slicing(
    templater,
    test_inputs_dir_path: Path,
    file_name: str,
    test_description: str,
    slices_expected: list[SliceExpected],
):
    input_sqlx, expected_sql = (
        (test_inputs_dir_path / file_name).read_text().split(TEST_INPUT_FILE_SEPARATOR)
    )
    replaced_sql, raw_slices, templated_slices = templater.slice_sqlx_template(
        input_sqlx
    )

    try:
        assert_sql_is_equal(
            expected_sql=expected_sql, actual_sql=replaced_sql, ignore_whitespace=True
        )

        assert_slices(
            slices_raw_actual=raw_slices,
            slices_template_actual=templated_slices,
            slices_expected=slices_expected,
        )
    except AssertionError as exc:
        raise AssertionError(f"Failed test: {file_name} - {test_description}") from exc
