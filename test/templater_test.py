"""Tests for the dataform templater."""
import pytest

def test_replace_ref_with_bq_table_single_ref(templater):
    input_sql = "SELECT * FROM ${ref('test')}"
    expected_sql = "SELECT * FROM `my_project.my_dataset.test`"
    result = templater._replace_ref_with_bq_table(input_sql)
    assert result == expected_sql

def test_replace_ref_with_bq_table_with_dataset(templater):
    input_sql = "SELECT * FROM ${ref('other_dataset', 'test')}"
    expected_sql = "SELECT * FROM `my_project.other_dataset.test`"
    result = templater._replace_ref_with_bq_table(input_sql)
    assert result == expected_sql

def test_replace_ref_with_bq_table_multiple_refs(templater):
    input_sql = "SELECT * FROM ${ref('test')}, ${ref('another')}"
    expected_sql = "SELECT * FROM `my_project.my_dataset.test`, `my_project.my_dataset.another`"
    result = templater._replace_ref_with_bq_table(input_sql)
    assert result == expected_sql

# _replace_blocks のテスト
def test_replace_blocks_single_block(templater):
    input_sql = """config {
    type: "table",
    columns: {
        "test" : "test",
        "value:: "value"
    }
}
SELECT * FROM my_table"""

    expected_sql = "\nSELECT * FROM my_table"
    result = templater._replace_blocks(input_sql)
    assert result == expected_sql

def test_replace_blocks_multiple_blocks(templater):
    input_sql = """config {
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

    expected_sql = "\n\nSELECT * FROM my_table"
    result = templater._replace_blocks(input_sql)
    assert result == expected_sql

def test_replace_blocks_no_block(templater):
    input_sql = "SELECT * FROM my_table"
    expected_sql = "SELECT * FROM my_table"
    result = templater._replace_blocks(input_sql)
    assert result == expected_sql

# slice_sqlx_template のテスト
def test_slice_sqlx_template_with_config_and_ref(templater):
    input_sqlx = """config {
    type: "table"
}
SELECT 1 AS value FROM ${ref('test')} WHERE true
"""
    expected_sql = "\nSELECT 1 AS value FROM `my_project.my_dataset.test` WHERE true\n"
    # テンプレートを置換したSQLとスライス結果を取得
    replaced_sql, raw_slices, templated_slices = templater.slice_sqlx_template(input_sqlx)
    # テンプレートを置換したSQLが期待通りであるかを確認
    assert replaced_sql == expected_sql

    # RawFileSlice の検証
    assert len(raw_slices) == 4
    assert raw_slices[0].raw.startswith("config")
    assert raw_slices[1].raw.startswith("\nSELECT 1")
    assert raw_slices[2].raw.startswith('${ref')
    assert raw_slices[3].raw == " WHERE true\n"

    # TemplatedFileSlice の検証
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

    # テンプレートを置換したSQLとスライス結果を取得
    replaced_sql, raw_slices, templated_slices = templater.slice_sqlx_template(input_sqlx)

    # テンプレートを置換したSQLが期待通りであるかを確認
    assert replaced_sql == expected_sql

    # RawFileSlice の検証
    assert len(raw_slices) == 6
    assert raw_slices[0].raw.startswith("config")
    assert raw_slices[1].raw.startswith("\nSELECT *")
    assert raw_slices[2].raw.startswith('${ref')
    assert raw_slices[3].raw.startswith(' JOIN')
    assert raw_slices[4].raw.startswith('${ref')
    assert raw_slices[5].raw.endswith(" ON test.id = other_table.id\n")

    # TemplatedFileSlice の検証
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

    # テンプレートを置換したSQLとスライス結果を取得
    replaced_sql, raw_slices, templated_slices = templater.slice_sqlx_template(input_sqlx)

    # テンプレートを置換したSQLが期待通りであるかを確認
    assert replaced_sql == expected_sql

    # RawFileSlice の検証
    assert len(raw_slices) == 1
    assert raw_slices[0].raw == "SELECT * FROM my_table WHERE true\n"

    # TemplatedFileSlice の検証
    assert len(templated_slices) == 1
    assert templated_slices[0].slice_type == "literal"

# process (slice_sqlx_template) のテスト
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

    # テンプレートを置換したSQLとスライス結果を取得
    replaced_sql, raw_slices, templated_slices = templater.slice_sqlx_template(input_sqlx)
    # テンプレートを置換したSQLが期待通りであるかを確認
    assert replaced_sql == expected_sql

    # RawFileSlice の検証
    assert len(raw_slices) == 4
    assert raw_slices[0].raw.startswith("config")
    assert raw_slices[1].raw.startswith("\nSELECT *")
    assert raw_slices[2].raw.startswith("${ref")
    assert raw_slices[3].raw == " WHERE true\n"

    # TemplatedFileSlice の検証
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

    # テンプレートを置換したSQLとスライス結果を取得
    replaced_sql, raw_slices, templated_slices = templater.slice_sqlx_template(input_sqlx)

    # テンプレートを置換したSQLが期待通りであるかを確認
    assert replaced_sql == expected_sql

    # RawFileSlice の検証
    assert len(raw_slices) == 6
    assert raw_slices[0].raw.startswith("config")
    assert raw_slices[1].raw.startswith("\nSELECT *")
    assert raw_slices[2].raw.startswith("${ref")
    assert raw_slices[3].raw.startswith(" JOIN")
    assert raw_slices[4].raw.startswith("${ref")
    assert raw_slices[5].raw.endswith(" ON test.id = other_table.id\n")

    # TemplatedFileSlice の検証
    assert len(templated_slices) == 6
    assert templated_slices[0].slice_type == "templated"
    assert templated_slices[1].slice_type == "literal"
    assert templated_slices[2].slice_type == "templated"
    assert templated_slices[3].slice_type == "literal"
    assert templated_slices[4].slice_type == "templated"
    assert templated_slices[5].slice_type == "literal"