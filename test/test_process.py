from pathlib import Path

from pytest import mark

from .helpers import assert_sql_is_equal


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
    assert_sql_is_equal(
        expected_sql=expected_sql, actual_sql=replaced_sql
    )

    assert len(raw_slices) == 4
    assert raw_slices[0].raw.startswith("config")
    assert raw_slices[1].raw.startswith("\nSELECT *")
    assert raw_slices[2].raw.startswith("${ref")
    assert raw_slices[3].raw == " WHERE true\n"

    assert len(templated_slices) == 4
    assert templated_slices[0].slice_type == "templated"
    assert templated_slices[1].slice_type == "literal"
    assert templated_slices[2].slice_type == "templated"
    assert templated_slices[3].slice_type == "literal"


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

    assert_sql_is_equal(
        expected_sql=expected_sql, actual_sql=replaced_sql
    )

    assert len(raw_slices) == 6
    assert raw_slices[0].raw.startswith("config")
    assert raw_slices[1].raw.startswith("\nSELECT *")
    assert raw_slices[2].raw.startswith("${ref")
    assert raw_slices[3].raw.startswith(" JOIN")
    assert raw_slices[4].raw.startswith("${ref")
    assert raw_slices[5].raw.endswith(" ON test.id = other_table.id\n")

    assert len(templated_slices) == 6
    assert templated_slices[0].slice_type == "templated"
    assert templated_slices[1].slice_type == "literal"
    assert templated_slices[2].slice_type == "templated"
    assert templated_slices[3].slice_type == "literal"
    assert templated_slices[4].slice_type == "templated"
    assert templated_slices[5].slice_type == "literal"


@mark.parametrize(
    "test_input_filename, test_output_filename, expected",
    [
        (
            "config_pre_post_ref__raw.sqlx",
            "config_pre_post_ref__expected.sqlx",
            {
                "raw_slices_starts": [
                    "config",
                    "\n",
                    "pre_operations",
                    "\n",
                    "post_operations",
                    "\nSELECT *",
                    "${ref",
                    " WHERE true",
                ],
                "templated_slices": [
                    "templated",
                    "literal",
                    "templated",
                    "literal",
                    "templated",
                    "literal",
                    "templated",
                    "literal",
                ],
            },
        )
    ],
)
def test_process_sqlx_with_post_pre_operations_config_and_ref(
    templater, test_inputs_dir_path: Path, test_input_filename: str, test_output_filename: str, expected
):
    input_sqlx = (test_inputs_dir_path / test_input_filename).read_text()
    expected_sql = (test_inputs_dir_path / test_output_filename).read_text()

    replaced_sql, raw_slices, templated_slices = templater.slice_sqlx_template(
        input_sqlx
    )

    assert_sql_is_equal(
        expected_sql=expected_sql, actual_sql=replaced_sql
    )

    for (
        template_type_expected,
        template_type_actual,
        raw_slice_expected_start,
        raw_slice_actual,
    ) in zip(
        expected["templated_slices"],
        templated_slices,
        expected["raw_slices_starts"],
        raw_slices,
    ):
        _debug = "\n".join(
            [
                f"Raw slice actual: {raw_slice_actual.raw}",
                f"Raw slice expected start: {raw_slice_expected_start}",
            ]
        )

        assert template_type_expected == template_type_actual.slice_type, (
            f"Expected {template_type_expected} but got {template_type_actual.slice_type}."
            f"\n{_debug}"
        )
        assert raw_slice_actual.raw.startswith(raw_slice_expected_start), (
            f"Expected raw slice to start with {raw_slice_expected_start} but got {raw_slice_actual.raw}"
        )

    assert len(raw_slices) == len(expected["raw_slices_starts"]), (
        "The length of raw slices did not match."
    )
    assert len(templated_slices) == len(expected["templated_slices"]), (
        "The length of templated slices did not match."
    )
