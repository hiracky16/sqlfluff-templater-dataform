from pathlib import Path

from .helpers import assert_sql_is_equal, assert_slices, SliceExpected
from .constants import TEST_INPUT_FILE_SEPARATOR


def test_process_sqlx_with_config_and_ref(templater, test_inputs_dir_path):
    input_sqlx, expected_sql = (
        (test_inputs_dir_path / "templatize_slice_4.sqlx")
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
        slices_expected=[
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
        slices_raw_actual=raw_slices,
        slices_template_actual=templated_slices,
    )


def test_process_sqlx_with_multiple_refs(templater, test_inputs_dir_path: Path):
    input_sqlx, expected_sql = (
        (test_inputs_dir_path / "templatize_slice_3.sqlx")
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
        slices_expected=[
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
        slices_raw_actual=raw_slices,
        slices_template_actual=templated_slices,
    )


def test_process_sqlx_with_post_pre_operations_config_and_ref(
    templater,
    test_inputs_dir_path: Path,
):
    input_sqlx, expected_sql = (
        (test_inputs_dir_path / "templatize_slice_1.sqlx")
        .read_text()
        .split(TEST_INPUT_FILE_SEPARATOR)
    )

    slices_expected = [
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
    ]

    replaced_sql, raw_slices, templated_slices = templater.slice_sqlx_template(
        input_sqlx
    )

    assert_sql_is_equal(
        expected_sql=expected_sql, actual_sql=replaced_sql, ignore_whitespace=True
    )

    assert_slices(
        slices_expected=slices_expected,
        slices_raw_actual=raw_slices,
        slices_template_actual=templated_slices,
    )
