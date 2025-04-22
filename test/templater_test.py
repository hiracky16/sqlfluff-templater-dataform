"""Tests for the dataform templater."""
from pytest import mark

def test_has_js_block(templater):
    has_js_sql = """config {
    type: "table",
    columns: {
        "test" : "test",
        "value:: "value"
    }
}
js {
    const myVar = "test";
}
SELECT * FROM my_table"""
    not_has_js_sql = """config {
    type: "table",
    columns: {
        "test" : "test",
        "value:: "value"
    }
}
SELECT * FROM my_table"""
    assert templater.has_js_block(has_js_sql) == True
    assert templater.has_js_block(not_has_js_sql) == False

def test_replace_ref_with_bq_table_single_ref(templater):
    input_sql = "SELECT * FROM ${ref('test')}"
    expected_sql = "SELECT * FROM `my_project.my_dataset.test`"
    result = templater.replace_ref_with_bq_table(input_sql)
    assert result == expected_sql

def test_replace_ref_with_bq_table_single_ref_double_quotes(templater):
    input_sql = 'SELECT * FROM ${ref("test")}'
    expected_sql = "SELECT * FROM `my_project.my_dataset.test`"
    result = templater.replace_ref_with_bq_table(input_sql)
    assert result == expected_sql

def test_replace_ref_with_bq_table_with_dataset(templater):
    input_sql = "SELECT * FROM ${ref('other_dataset', 'test')}"
    expected_sql = "SELECT * FROM `my_project.other_dataset.test`"
    result = templater.replace_ref_with_bq_table(input_sql)
    assert result == expected_sql

def test_replace_ref_with_bq_table_with_dataset_double_quotes(templater):
    input_sql = 'SELECT * FROM ${ref("other_dataset", "test")}'
    expected_sql = "SELECT * FROM `my_project.other_dataset.test`"
    result = templater.replace_ref_with_bq_table(input_sql)
    assert result == expected_sql

def test_replace_ref_with_bq_table_multiple_refs(templater):
    input_sql = "SELECT * FROM ${ref('test')}, ${ref('another')}"
    expected_sql = "SELECT * FROM `my_project.my_dataset.test`, `my_project.my_dataset.another`"
    result = templater.replace_ref_with_bq_table(input_sql)
    assert result == expected_sql


@mark.parametrize(
    "test_block, replaced_sql",
    [
        (
                "",
                ""
        ),
        (
                """config {
                type: "table",
                columns: {
                    "test" : "test",
                    "value:: "value"
                }
                }""",
                ""
        ),
        (
                """pre_operations {
                CREATE TEMP FUNCTION AddFourAndDivide(x INT64, y INT64)
                    RETURNS FLOAT64
                    AS ((x + 4) / y);
                }""",
                ""
        ),
        (
                """post_operations {
                GRANT `roles/bigquery.dataViewer`
                ON
                TABLE ${self()}
                TO "group:allusers@example.com", "user:otheruser@example.com"
                }""",
                ""
        ),
        (
                """config {
                type: "table",
                columns: {
                    "test" : "test",
                    "value:: "value"
                }
                } pre_operations {
                CREATE TEMP FUNCTION AddFourAndDivide(x INT64, y INT64)
                    RETURNS FLOAT64
                    AS ((x + 4) / y);
                } post_operations {
                GRANT `roles/bigquery.dataViewer`
                ON
                TABLE ${self()}
                TO "group:allusers@example.com", "user:otheruser@example.com"
                }""",
                "  "
        ),
    ]
)
def test_replace_blocks(templater, test_block, replaced_sql):
    sql_body = "SELECT * FROM my_table"
    input_sql = f"{test_block} {sql_body}"
    expected_sql = f"{replaced_sql} {sql_body}"
    result = templater.replace_blocks(input_sql)
    assert result == expected_sql


# slice_sqlx_template のテスト
def test_slice_sqlx_template_with_config_and_ref(templater):
    input_sqlx = """config {
    type: "table"
}
SELECT 1 AS value FROM ${ref('test')} WHERE true
"""
    expected_sql = "\nSELECT 1 AS value FROM `my_project.my_dataset.test` WHERE true\n"
    replaced_sql, raw_slices, templated_slices = templater.slice_sqlx_template(input_sqlx)
    assert replaced_sql == expected_sql

    assert len(raw_slices) == 4
    assert raw_slices[0].raw.startswith("config")
    assert raw_slices[1].raw.startswith("\nSELECT 1")
    assert raw_slices[2].raw.startswith('${ref')
    assert raw_slices[3].raw == " WHERE true\n"

    assert len(templated_slices) == 4
    assert templated_slices[0].slice_type == "templated"
    assert templated_slices[1].slice_type == "literal"
    assert templated_slices[2].slice_type == "templated"
    assert templated_slices[3].slice_type == "literal"

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

    replaced_sql, raw_slices, templated_slices = templater.slice_sqlx_template(input_sqlx)

    assert replaced_sql == expected_sql

    assert len(raw_slices) == 6
    assert raw_slices[0].raw.startswith("config")
    assert raw_slices[1].raw.startswith("\nSELECT *")
    assert raw_slices[2].raw.startswith('${ref')
    assert raw_slices[3].raw.startswith(' JOIN')
    assert raw_slices[4].raw.startswith('${ref')
    assert raw_slices[5].raw.endswith(" ON test.id = other_table.id\n")

    assert len(templated_slices) == 6
    assert templated_slices[0].slice_type == "templated"
    assert templated_slices[1].slice_type == "literal"
    assert templated_slices[2].slice_type == "templated"
    assert templated_slices[3].slice_type == "literal"
    assert templated_slices[4].slice_type == "templated"
    assert templated_slices[5].slice_type == "literal"

def test_slice_sqlx_template_with_no_ref(templater):
    input_sqlx = """SELECT * FROM my_table WHERE true
"""
    expected_sql = "SELECT * FROM my_table WHERE true\n"

    replaced_sql, raw_slices, templated_slices = templater.slice_sqlx_template(input_sqlx)
    assert replaced_sql == expected_sql

    assert len(raw_slices) == 1
    assert raw_slices[0].raw == "SELECT * FROM my_table WHERE true\n"

    assert len(templated_slices) == 1
    assert templated_slices[0].slice_type == "literal"

def test_slice_sqlx_template_with_incremental_table(templater):
    input_sqlx = """config {
    type: "incremantal",
    columns: {
        "test" : "test",
        "value:: "value"
    }
}
SELECT * FROM ${ref('test')} JOIN ${ref('other_table')}
  ON test.id = other_table.id
${when(incremantal(), "WHERE updated_at > '2020-01-01'")}
GROUP BY test
"""
    expected_sql = """
SELECT * FROM `my_project.my_dataset.test` JOIN `my_project.my_dataset.other_table`
  ON test.id = other_table.id

GROUP BY test
"""

    replaced_sql, raw_slices, templated_slices = templater.slice_sqlx_template(input_sqlx)
    assert replaced_sql == expected_sql

    assert len(raw_slices) == 8
    assert raw_slices[0].raw.startswith("config")
    assert raw_slices[1].raw.startswith("\nSELECT *")
    assert raw_slices[2].raw.startswith('${ref')
    assert raw_slices[3].raw.startswith(' JOIN')
    assert raw_slices[4].raw.startswith('${ref')
    assert raw_slices[5].raw.endswith(" ON test.id = other_table.id\n")
    assert raw_slices[6].raw.startswith("${when")
    assert raw_slices[7].raw.startswith("\nGROUP BY")

    assert len(templated_slices) == 8
    assert templated_slices[0].slice_type == "templated"
    assert templated_slices[1].slice_type == "literal"
    assert templated_slices[2].slice_type == "templated"
    assert templated_slices[3].slice_type == "literal"
    assert templated_slices[4].slice_type == "templated"
    assert templated_slices[5].slice_type == "literal"
    assert templated_slices[6].slice_type == "templated"
    assert templated_slices[7].slice_type == "literal"

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
    expected_sql = "\nSELECT * FROM `my_project.my_dataset.test` WHERE true\n"

    replaced_sql, raw_slices, templated_slices = templater.slice_sqlx_template(input_sqlx)
    assert replaced_sql == expected_sql

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
    expected_sql = "\nSELECT * FROM `my_project.my_dataset.test` JOIN `my_project.my_dataset.other_table` ON test.id = other_table.id\n"

    replaced_sql, raw_slices, templated_slices = templater.slice_sqlx_template(input_sqlx)

    assert replaced_sql == expected_sql

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
    "test_input_filename, expected",
    [
        (
            "config_pre_post_ref.sqlx",
            {
                "expected_sql": "\n\n\nSELECT * FROM `my_project.my_dataset.test` WHERE true\n",
                "raw_slices": {
                    "len": 8,
                    "raw_starts": [
                        "config",
                        "\n",
                        "pre_operations",
                        "\n",
                        "post_operations",
                        "\nSELECT *",
                        "${ref",
                        " WHERE true",
                    ]
                },
                "templated_slices": {
                    "len": 8,
                    "templated_types": [
                        "templated",
                        "literal",
                        "templated",
                        "literal",
                        "templated",
                        "literal",
                        "templated",
                        "literal",
                    ]
                }
            }

        )
    ]
)
def test_process_sqlx_with_post_pre_operations_config_and_ref(templater, test_inputs_dir_path, test_input_filename, expected):

    input_sqlx_path = test_inputs_dir_path / test_input_filename
    input_sqlx = input_sqlx_path.read_text()

    replaced_sql, raw_slices, templated_slices = templater.slice_sqlx_template(input_sqlx)

    assert replaced_sql ==  expected["expected_sql"]

    assert len(raw_slices) == expected["raw_slices"]["len"]
    for i, expected_raw_starts in enumerate(expected["raw_slices"]["raw_starts"]):
        assert raw_slices[i].raw.startswith(expected_raw_starts)

    assert len(templated_slices) == expected["templated_slices"]["len"]
    for i, templated_types in enumerate(expected["templated_slices"]["templated_types"]):
        assert templated_slices[i].slice_type == templated_types

