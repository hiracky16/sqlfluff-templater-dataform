"""Matching regex patterns for the Dataform templater plugin."""

import re

PATTERN_BLOCK_CONFIG = re.compile(
    r"config\s*\{(?:[^{}]|\{(?:[^{}]|\{[^{}]*\})*\})*\}",
    flags=re.DOTALL,
)
"""Match config blocks. i.e. `config { ... }`"""

PATTERN_BLOCK_OPERATION = re.compile(
    r"""
    (?:pre_operations|post_operations)\s*
    (?<!\\)\{           # Match `{` and don't allow an escape
    (?P<SQL>
        # (?:[^\\}]|\\})+?
        (?:
            [^{}]|
            \{
                (?:
                    [^{}]|
                    \{[^{}]*\}
                )*
            \}
        )*
    )
    (?<!\\)\}           # Match `}` and don't allow an escape
    """,
    flags=re.DOTALL | re.VERBOSE,
)
"""Match pre_operations blocks. i.e. `pre_operations { ... }` or `post_operations { ... }`

Note: the first group inside will be the content of the block
"""

PATTERN_BLOCK_JS = re.compile(r"js\s*\{(?:[^{}]|\{[^{}]*\})*\}", flags=re.DOTALL)
"""Match js blocks. i.e. `js { ... }`"""

PATTERN_REFERENCE = re.compile(
    r"""
    \$\{\s*
    ref\(\s*                # Match ref()
    [\'"]([^\'"]+)[\'"]
    (?:\s*,\s*[\'"]
        ([^\'"]+)
        [\'"])?
    \s*\)\s*\}
    """,
    flags=re.DOTALL | re.VERBOSE,
)
"""Match ref patterns. i.e. `${ref('dataset', 'model')}`"""

PATTERN_REFERENCE_SELF = re.compile(
    r"""
    \$\{\s*
    self\(\s*\)                # Match self()
    \s*\}
    """,
    flags=re.DOTALL | re.VERBOSE,
)
"""Match self ref patterns. i.e. `${self()}`"""

PATTERN_INTERPOLATION = re.compile(
    r"""
    (?<!\\)\$\{                  # Match unescaped ${
    (?!ref)                      # Negative lookahead: not ref
    (?P<variable>                    # Capture the entire expression body
        (?:
            [^\\}`]+
                # ^ Match normal content
                #   (everything but closing curly brace and backtick)
            | \\[}`]             # Allow escaped } and `
            | `(?:
                # ^ Match backtick-quoted string (for JS string interpolation)
                [^`\\]*          #   Normal inside content
                (?:\\.[^`\\]*)*  #   Handle escapes inside backtick
                (?:              #   Inner `${...}` pattern
                    (?<!\\)\$\{
                    (?P<inner>[^{}]*)  # (optional) capture inner var, no nesting
                    \}
                )?
            )`
        )*
    )
    (?<!\\)\}                    # Closing unescaped }
    """,
    flags=re.DOTALL | re.VERBOSE,
)
"""Match JS variables. i.e. `${some_js_var + 1}`"""

PATTERN_INCREMENTAL_CONDITION = re.compile(
    r"""
    \$\{when\(\s*[\w]+\(\),\s*
    (?<!\\)                     # no backslash before opening quote
    (?P<quote>["'`])            # opening quote/backtick
    (?P<SQL>.*)            # capture content
    (?<!\\)(?P=quote)           # closing quote, also not escaped
    \)}
    """,
    flags=re.DOTALL | re.VERBOSE,
)
"""Match incremental condition patterns. i.e. `${when(condition, 'value')}`"""

DICT_PATTERN = {
    "config": PATTERN_BLOCK_CONFIG,
    "operations": PATTERN_BLOCK_OPERATION,
    "js": PATTERN_BLOCK_JS,
    "ref": PATTERN_REFERENCE,
    "ref_self": PATTERN_REFERENCE_SELF,
    "incremental_condition": PATTERN_INCREMENTAL_CONDITION,
    "interpolation": PATTERN_INTERPOLATION,
}

__all__ = [
    "PATTERN_BLOCK_CONFIG",
    "PATTERN_BLOCK_OPERATION",
    "PATTERN_BLOCK_JS",
    "PATTERN_REFERENCE",
    "PATTERN_REFERENCE_SELF",
    "PATTERN_INCREMENTAL_CONDITION",
    "PATTERN_INTERPOLATION",
    "DICT_PATTERN",
]
