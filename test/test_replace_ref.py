"""Tests for replacing a JS BigQuery refrence with the actual BigQuery fully qualified name."""

from .helpers import assert_sql_is_equal

import pytest


@pytest.mark.parametrize(
    "input_sql, expected_sql",
    [
        ("SELECT * FROM ${ref('test')}", "SELECT * FROM `my_project.my_dataset.test`"),
        ('SELECT * FROM ${ref("test")}', "SELECT * FROM `my_project.my_dataset.test`"),
        (
            "SELECT * FROM ${ref('other_dataset', 'test')}",
            "SELECT * FROM `my_project.other_dataset.test`",
        ),
        (
            'SELECT * FROM ${ref("other_dataset", "test")}',
            "SELECT * FROM `my_project.other_dataset.test`",
        ),
        (
            "SELECT * FROM ${ref('test')}, ${ref('another')}",
            "SELECT * FROM `my_project.my_dataset.test`, `my_project.my_dataset.another`",
        ),
    ],
)
def test_template_ref_with_bq_table(templater, input_sql, expected_sql):
    result = templater.replace_ref_with_bq_table(input_sql)

    assert_sql_is_equal(expected_sql, result)
