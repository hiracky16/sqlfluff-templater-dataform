"""Tests for replacing a JS BigQuery refrence with the actual BigQuery fully qualified name."""


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
    expected_sql = (
        "SELECT * FROM `my_project.my_dataset.test`, `my_project.my_dataset.another`"
    )
    result = templater.replace_ref_with_bq_table(input_sql)
    assert result == expected_sql
