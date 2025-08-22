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
                "CREATE TEMP FUNCTION AddFourAndDivide(x INT64, y INT64)\n                    RETURNS FLOAT64\n                    AS ((x + 4) / y);"
        ),
        (
                """post_operations {
                GRANT `roles/bigquery.dataViewer`
                ON
                TABLE ${self()}
                TO "group:allusers@example.com", "user:otheruser@example.com"
                }""",
                "GRANT `roles/bigquery.dataViewer`\n                ON\n                TABLE `my_project.my_dataset.CURRENT_TABLE`\n                TO \"group:allusers@example.com\", \"user:otheruser@example.com\""
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
                " CREATE TEMP FUNCTION AddFourAndDivide(x INT64, y INT64)\n                    RETURNS FLOAT64\n                    AS ((x + 4) / y); GRANT `roles/bigquery.dataViewer`\n                ON\n                TABLE `my_project.my_dataset.CURRENT_TABLE`\n                TO \"group:allusers@example.com\", \"user:otheruser@example.com\""
        ),
    ]
)
def test_replace_blocks(templater, test_block, replaced_sql):
    sql_body = "SELECT * FROM my_table"
    input_sql = f"{test_block} {sql_body}"
    expected_sql = f"{replaced_sql} {sql_body}"
    result = templater.replace_blocks(input_sql)
    assert result == expected_sql


def test_replace_self_reference(templater):
    """Test that ${self()} references are replaced with unique placeholders."""
    input_sql = """GRANT `roles/bigquery.dataViewer`
    ON
    TABLE ${self()}
    TO "group:allusers@example.com" """
    
    expected_sql = """GRANT `roles/bigquery.dataViewer`
    ON
    TABLE `my_project.my_dataset.CURRENT_TABLE`
    TO "group:allusers@example.com" """
    
    result = templater.replace_self_reference(input_sql)
    assert result == expected_sql


def test_replace_self_reference_multiple_occurrences(templater):
    """Test that multiple ${self()} references are all replaced."""
    input_sql = """SELECT * FROM ${self()} 
    WHERE table_name = (SELECT name FROM ${self()})"""
    
    expected_sql = """SELECT * FROM `my_project.my_dataset.CURRENT_TABLE` 
    WHERE table_name = (SELECT name FROM `my_project.my_dataset.CURRENT_TABLE`)"""
    
    result = templater.replace_self_reference(input_sql)
    assert result == expected_sql


def test_replace_self_reference_no_occurrences(templater):
    """Test that SQL without ${self()} is unchanged."""
    input_sql = """SELECT * FROM my_table WHERE true"""
    
    result = templater.replace_self_reference(input_sql)
    assert result == input_sql


# slice_sqlx_template のテスト
def test_slice_sqlx_template_with_self_reference(templater):
    """Test that ${self()} references are handled in the full slicing pipeline."""
    input_sqlx = """config {
    type: "table"
}
GRANT `roles/bigquery.dataViewer`
    ON
    TABLE ${self()}
    TO "group:allusers@example.com"
SELECT * FROM ${ref('test')} WHERE true"""
    
    expected_sql = """\nGRANT `roles/bigquery.dataViewer`
    ON
    TABLE `my_project.my_dataset.CURRENT_TABLE`
    TO "group:allusers@example.com"
SELECT * FROM `my_project.my_dataset.test` WHERE true"""
    
    replaced_sql, raw_slices, templated_slices = templater.slice_sqlx_template(input_sqlx)
    assert replaced_sql == expected_sql


def test_slice_sqlx_template_with_config_and_ref(templater):
    input_sqlx = """config {
    type: "table"
}
SELECT 1 AS value FROM ${ref('test')} WHERE true
"""
    expected_sql = "\nSELECT 1 AS value FROM `my_project.my_dataset.test` WHERE true\n"
    replaced_sql, raw_slices, templated_slices = templater.slice_sqlx_template(input_sqlx)
    assert replaced_sql == expected_sql

    assert len(raw_slices) == 3
    assert raw_slices[0].raw.startswith("\nSELECT 1")
    assert raw_slices[1].raw.startswith('${ref')
    assert raw_slices[2].raw == " WHERE true\n"

    assert len(templated_slices) == 3
    assert templated_slices[0].slice_type == "literal"
    assert templated_slices[1].slice_type == "templated"
    assert templated_slices[2].slice_type == "literal"

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

    assert len(raw_slices) == 5
    assert raw_slices[0].raw.startswith("\nSELECT *")
    assert raw_slices[1].raw.startswith('${ref')
    assert raw_slices[2].raw.startswith(' JOIN')
    assert raw_slices[3].raw.startswith('${ref')
    assert raw_slices[4].raw.endswith(" ON test.id = other_table.id\n")

    assert len(templated_slices) == 5
    assert templated_slices[0].slice_type == "literal"
    assert templated_slices[1].slice_type == "templated"
    assert templated_slices[2].slice_type == "literal"
    assert templated_slices[3].slice_type == "templated"
    assert templated_slices[4].slice_type == "literal"

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

    assert len(raw_slices) == 5
    assert raw_slices[0].raw.startswith("\nSELECT *")
    assert raw_slices[1].raw.startswith('${ref')
    assert raw_slices[2].raw.startswith(' JOIN')
    assert raw_slices[3].raw.startswith('${ref')
    assert raw_slices[4].raw.endswith("GROUP BY test\n")

    assert len(templated_slices) == 5
    assert templated_slices[0].slice_type == "literal"
    assert templated_slices[1].slice_type == "templated"
    assert templated_slices[2].slice_type == "literal"
    assert templated_slices[3].slice_type == "templated"
    assert templated_slices[4].slice_type == "literal"

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

    assert len(raw_slices) == 3
    assert raw_slices[0].raw.startswith("\nSELECT *")
    assert raw_slices[1].raw.startswith("${ref")
    assert raw_slices[2].raw == " WHERE true\n"

    assert len(templated_slices) == 3
    assert templated_slices[0].slice_type == "literal"
    assert templated_slices[1].slice_type == "templated"
    assert templated_slices[2].slice_type == "literal"

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

    assert len(raw_slices) == 5
    assert raw_slices[0].raw.startswith("\nSELECT *")
    assert raw_slices[1].raw.startswith("${ref")
    assert raw_slices[2].raw.startswith(" JOIN")
    assert raw_slices[3].raw.startswith("${ref")
    assert raw_slices[4].raw.endswith(" ON test.id = other_table.id\n")

    assert len(templated_slices) == 5
    assert templated_slices[0].slice_type == "literal"
    assert templated_slices[1].slice_type == "templated"
    assert templated_slices[2].slice_type == "literal"
    assert templated_slices[3].slice_type == "templated"
    assert templated_slices[4].slice_type == "literal"


@mark.parametrize(
    "test_input_filename, expected",
    [
        (
            "config_pre_post_ref.sqlx",
            {
                                    "expected_sql": "\nCREATE TEMP FUNCTION AddFourAndDivide(x INT64, y INT64)\nRETURNS FLOAT64\nAS ((x + 4) / y);\nGRANT `roles/bigquery.dataViewer`\n    ON\n    TABLE `my_project.my_dataset.CURRENT_TABLE`\n    TO \"group:allusers@example.com\", \"user:otheruser@example.com\"\nSELECT * FROM `my_project.my_dataset.test` WHERE true\n",
                "raw_slices": {
                    "len": 3,
                    "raw_starts": [
                        "\nCREATE TEMP",
                        "${ref",
                        " WHERE true",
                    ]
                },
                "templated_slices": {
                    "len": 3,
                    "templated_types": [
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
    for i, expected_templated_types in enumerate(expected["templated_slices"]["templated_types"]):
        assert templated_slices[i].slice_type == expected_templated_types

