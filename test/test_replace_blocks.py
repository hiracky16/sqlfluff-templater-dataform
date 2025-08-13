"""Tests for the dataform templater."""

from pytest import mark

from .helpers import assert_sql_is_equal


@mark.parametrize(
    "test_block, replaced_sql",
    [
        ("", ""),
        (
            """config {
                type: "table",
                columns: {
                    "test" : "test",
                    "value:: "value"
                }
                }""",
            "",
        ),
        (
            """pre_operations {
                CREATE TEMP FUNCTION AddFourAndDivide(x INT64, y INT64)
                    RETURNS FLOAT64
                    AS ((x + 4) / y);
                }""",
            "",
        ),
        (
            """post_operations {
                GRANT `roles/bigquery.dataViewer`
                ON
                TABLE ${self()}
                TO "group:allusers@example.com", "user:otheruser@example.com"
                }""",
            "",
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
            "  ",
        ),
    ],
)
def test_replace_blocks(templater, test_block, replaced_sql):
    sql_body = "SELECT * FROM my_table"
    input_sql = f"{test_block} {sql_body}"
    expected_sql = f"{replaced_sql} {sql_body}"
    result = templater.replace_blocks(input_sql)
    assert_sql_is_equal(
        expected_sql=expected_sql,
        actual_sql=result,
    )


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
