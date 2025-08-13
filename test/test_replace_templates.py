import re


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
