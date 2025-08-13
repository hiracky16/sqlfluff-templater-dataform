from pathlib import Path

from pytest import mark

from .helpers import assert_sql_is_equal, assert_slices, SliceExpected


def test_process_sqlx_with_config_and_ref(templater):
    input_sqlx = """config {
    type: "table",
    columns: {
        "test" : "test",
        "value:: "value"
    }
}
SELECT * FROM ${ref('test')} WHERE true
"""
    expected_sql = "\n\nSELECT * FROM `my_project.my_dataset.test` WHERE true\n"

    replaced_sql, raw_slices, templated_slices = templater.slice_sqlx_template(
        input_sqlx
    )
    assert_sql_is_equal(expected_sql=expected_sql, actual_sql=replaced_sql)

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


def test_process_sqlx_with_multiple_refs(templater):
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


@mark.parametrize(
    "test_input_filename, test_output_filename, slices_expected",
    [
        (
            "config_pre_post_ref__raw.sqlx",
            "config_pre_post_ref__expected.sqlx",
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
        )
    ],
)
def test_process_sqlx_with_post_pre_operations_config_and_ref(
    templater,
    test_inputs_dir_path: Path,
    test_input_filename: str,
    test_output_filename: str,
    slices_expected,
):
    input_sqlx = (test_inputs_dir_path / test_input_filename).read_text()
    expected_sql = (test_inputs_dir_path / test_output_filename).read_text()

    replaced_sql, raw_slices, templated_slices = templater.slice_sqlx_template(
        input_sqlx
    )

    assert_sql_is_equal(expected_sql=expected_sql, actual_sql=replaced_sql)

    assert_slices(
        slices_expected=slices_expected,
        slices_raw_actual=raw_slices,
        slices_template_actual=templated_slices,
    )
