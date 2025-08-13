import re

from .helpers import assert_sql_is_equal


def test_replace_templates_without_linebreak(templater):
    input_sql = "SELECT * FROM ${hoge}, ${hoge.fuga('another')}"
    expected_sql = r"SELECT \* FROM [A-z]+\.[A-z]+, [A-z]+\.[A-z]+"
    result = templater.replace_templates(input_sql)
    assert re.match(expected_sql, result)


def test_replace_templates_with_linebreak(templater):
    input_sql = """SELECT * FROM ${
    hoge
    }, ${
    hoge.fuga('another')
    }"""
    expected_sql = r"""SELECT \* FROM [A-z]+\.[A-z]+, [A-z]+\.[A-z]+"""
    result = templater.replace_templates(input_sql)
    assert re.match(expected_sql, result)


def test_replace_js_block(templater):
    input_sql = """js {
    const some_var = 1;
    }"""
    expected_sql = "\n"

    assert_sql_is_equal(
        expected_sql=expected_sql,
        actual_sql=templater.templatize_sql(input_sql),
    )
