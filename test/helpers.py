import re


def assert_sql_is_equal(
    expected_sql: str | re.Pattern,
    actual_sql: str,
):
    """Assert that the expected SQL is equal to the raw SQL.

    :raises AssertionError: If the SQL statements do not match
    """

    if isinstance(expected_sql, re.Pattern):
        assert re.match(expected_sql, actual_sql), "Expected to match Regex pattern"

    assert expected_sql == actual_sql, (
        f"EXPECTED:\n{expected_sql!r}\nACTUAL:\n{actual_sql!r}"
    )
