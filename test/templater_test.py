"""Tests for the dataform templater."""
import re

def test_replace_ref_with_bq_table_single_ref(templater):
    input_sql = "SELECT * FROM ${ref('test')}"
    expected_sql = "SELECT * FROM `my_project.my_dataset.test`"
    result = templater.replace_ref_with_bq_table(input_sql)
    assert result == expected_sql

def test_replace_ref_with_bq_table_with_dataset(templater):
    input_sql = "SELECT * FROM ${ref('other_dataset', 'test')}"
    expected_sql = "SELECT * FROM `my_project.other_dataset.test`"
    result = templater.replace_ref_with_bq_table(input_sql)
    assert result == expected_sql

def test_replace_ref_with_bq_table_multiple_refs(templater):
    input_sql = "SELECT * FROM ${ref('test')}, ${ref('another')}"
    expected_sql = "SELECT * FROM `my_project.my_dataset.test`, `my_project.my_dataset.another`"
    result = templater.replace_ref_with_bq_table(input_sql)
    assert result == expected_sql

def test_replace_templates_without_linebreak(templater):
    input_sql = "SELECT * FROM ${hoge}, ${hoge.fuga('another')}"
    expected_sql = r"SELECT \* FROM a.+, a.+"
    result = templater.replace_templates(input_sql)
    assert re.match(expected_sql, result)

def test_replace_templates_with_linebreak(templater):
    input_sql = """SELECT * FROM ${
    hoge
    }, ${
    hoge.fuga('another')
    }"""
    expected_sql = r"""SELECT \* FROM a.+, a.+"""
    result = templater.replace_templates(input_sql)
    assert re.match(expected_sql, result)


def test_replace_blocks_single_block_config(templater):
    input_sql = """config {
    type: "table",
    columns: {
        "test" : "test",
        "value:: "value"
    }
}
SELECT * FROM my_table"""

    expected_sql = "\nSELECT * FROM my_table"
    result = templater.replace_blocks(input_sql, "config")
    assert result == expected_sql

def test_replace_blocks_single_block_js(templater):
    input_sql = """js {
    var hoge = "fuga"
}
SELECT * FROM my_table"""

    expected_sql = "\nSELECT * FROM my_table"
    result = templater.replace_blocks(input_sql, "js")
    assert result == expected_sql

def test_replace_blocks_single_block_pre_operations(templater):
    input_sql = """pre_operations {
CREATE TEMP FUNCTION AddFourAndDivide(x INT64, y INT64)
      RETURNS FLOAT64
      AS ((x + 4) / y);
}
SELECT * FROM my_table"""

    expected_sql = "\nSELECT * FROM my_table"
    result = templater.replace_blocks(input_sql, "pre_operations")
    assert result == expected_sql

def test_replace_blocks_single_block_post_operations(templater):
    input_sql = """post_operations {
GRANT `roles/bigquery.dataViewer`
      ON
      TABLE ${self()}
      TO "group:allusers@example.com", "user:otheruser@example.com"
}
SELECT * FROM my_table"""

    expected_sql = "\nSELECT * FROM my_table"
    result = templater.replace_blocks(input_sql, "post_operations")
    assert result == expected_sql

def test_replace_blocks_no_block(templater):
    input_sql = "SELECT * FROM my_table"
    expected_sql = "SELECT * FROM my_table"
    result = templater.replace_blocks(input_sql, "config")
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
    expected_sql = r"\s+SELECT \* FROM `my_project\.my_dataset\.test` JOIN `my_project\.other_dataset\.other_table` ON test\.id = other_table\.id AND test\.name = a.+\n"

    replaced_sql, raw_slices, templated_slices = templater.slice_sqlx_template(input_sqlx)

    assert re.match(expected_sql, replaced_sql)

    assert len(raw_slices) == 14
    assert raw_slices[0].raw.startswith("config")
    assert raw_slices[2].raw.startswith("js")
    assert raw_slices[4].raw.startswith("pre_operations")
    assert raw_slices[6].raw.startswith("post_operations")
    assert raw_slices[7].raw.startswith("\n\nSELECT *")
    assert raw_slices[8].raw.startswith('${ref')
    assert raw_slices[9].raw.startswith(' JOIN')
    assert raw_slices[10].raw.startswith('${ref')
    assert raw_slices[11].raw.startswith(" ON test.id = other_table.id AND test.name = ")
    assert raw_slices[12].raw.startswith('${hoge}')
    assert raw_slices[13].raw.startswith('\n')

    assert len(templated_slices) == 14
    assert templated_slices[0].slice_type == "templated"
    assert templated_slices[1].slice_type == "literal"
    assert templated_slices[2].slice_type == "templated"
    assert templated_slices[3].slice_type == "literal"
    assert templated_slices[4].slice_type == "templated"
    assert templated_slices[5].slice_type == "literal"
    assert templated_slices[6].slice_type == "templated"
    assert templated_slices[7].slice_type == "literal"
    assert templated_slices[8].slice_type == "templated"
    assert templated_slices[9].slice_type == "literal"
    assert templated_slices[10].slice_type == "templated"
    assert templated_slices[11].slice_type == "literal"
    assert templated_slices[12].slice_type == "templated"
    assert templated_slices[13].slice_type == "literal"


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