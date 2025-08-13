import pytest
from typing import Literal
import re

from sqlfluff_templater_dataform import (
    PATTERN_BLOCK_CONFIG,
    PATTERN_BLOCK_OPERATION,
    PATTERN_BLOCK_JS,
    PATTERN_REFERENCE,
    PATTERN_INCREMENTAL_CONDITION,
    PATTERN_INTERPOLATION,
)


@pytest.mark.parametrize(
    "pattern,text,expected",
    [
        (PATTERN_BLOCK_CONFIG, "config { key = 'value' }", None),
        (PATTERN_BLOCK_CONFIG, "config{\n  nested { key = 'value' }\n}", None),
        (
            PATTERN_BLOCK_OPERATION,
            "pre_operations { op1(); op2(); }",
            " op1(); op2(); ",
        ),
        (
            PATTERN_BLOCK_OPERATION,
            "pre_operations{\n  nested { foo } \n}",
            "\n  nested { foo } \n",
        ),
        (PATTERN_BLOCK_OPERATION, "post_operations { op1(); }", " op1(); "),
        (
            PATTERN_BLOCK_OPERATION,
            "post_operations{\n nested { bar } }",
            "\n nested { bar } ",
        ),
        (PATTERN_BLOCK_JS, "js { console.log('hello'); }", None),
        (PATTERN_BLOCK_JS, "js{ nested { inner(); } }", None),
        (PATTERN_REFERENCE, "${ref('table')}", ("table", None)),
        (PATTERN_REFERENCE, "${ref('dataset', 'table')}", ("dataset", "table")),
        (
            PATTERN_INCREMENTAL_CONDITION,
            "${when(is_incremental(), 'col > last_run')}",
            {"SQL", "col > last_run"},
        ),
        (
            PATTERN_INCREMENTAL_CONDITION,
            "${when(is_incremental(), `col > \\`last_run\\``)}",
            {"SQL", "col > `last_run`"},
        ),
        (
            PATTERN_INCREMENTAL_CONDITION,
            '${when(is_incremental(), "col > last_run")}',
            {"SQL", "col > last_run"},
        ),
        (PATTERN_INTERPOLATION, r'${ref("something")}', False),
        (PATTERN_INTERPOLATION, r"${some_variable}", {"variable": "some_variable"}),
        (PATTERN_INTERPOLATION, r"\${some_var}", False),
        (PATTERN_INTERPOLATION, r"${some_js_var + 1}", {"variable": "some_js_var + 1"}),
    ],
)
def test_regex_pattern(
    pattern: re.Pattern,
    text: str,
    expected: tuple[str] | dict[str, str] | str | None | Literal[False],
):
    """Test that a pattern will match.

    :param pattern: The regex pattern to test
    :param text: The text to test
    :param expected: Expected value
        If None, just check for a match
        If a string, assume it should be the entire match
        If a tuple, assume that those items are the expected values for each group
        If `False`, assume no match
        If a dict, assume Capture Group Name => Expected Group Content
    """
    match = pattern.search(text)

    if expected is False:
        assert match is None
        return

    assert match is not None

    if isinstance(expected, str):
        # Convenience method to make back into a tuple
        expected = (expected,)

    if isinstance(expected, tuple):
        i = 0
        for _expected_item, _matched_item in zip(expected, match.groups()):
            assert _expected_item == _matched_item, (
                f"Expected {_expected_item!r}, for group {i}, but got {_matched_item!r}"
            )
            i += 1

    if isinstance(expected, dict):
        for _group_name, _expected_item in expected.items():
            assert match.group(_group_name) is not None
            assert _expected_item == match.group(_group_name)
