"""Constants for the Dataform templater plugin."""

import re

PATTERN_BLOCK_CONFIG = re.compile(
    r"config\s*\{(?:[^{}]|\{(?:[^{}]|\{[^{}]*\})*\})*\}",
    flags=re.DOTALL,
)
"""Match config blocks. i.e. `config { ... }`"""

PATTERN_BLOCK_PRE_OPERATION = re.compile(
    r"pre_operations\s*\{(?P<SQL>([^{}]|\{(?:[^{}]|\{[^{}]*\})*\})*)\}",
    flags=re.DOTALL,
)
"""Match pre_operations blocks. i.e. `pre_operations { ... }`

Note: the first group inside will be the content of the block
"""

PATTERN_BLOCK_POST_OPERATION = re.compile(
    r"post_operations\s*\{(?P<SQL>([^{}]|\{(?:[^{}]|\{[^{}]*\})*\})*)\}",
    flags=re.DOTALL,
)
"""Match post_operations blocks. i.e. `post_operations { ... }`

Note: the first group inside will be the content of the block
"""

PATTERN_BLOCK_JS = re.compile(r"js\s*\{(?:[^{}]|\{[^{}]*\})*\}", flags=re.DOTALL)
"""Match js blocks. i.e. `js { ... }`"""

PATTERN_REFERENCE = re.compile(
    r'''\$\{\s*
        ref\(\s*                # Match ref()
        [\'"]([^\'"]+)[\'"]
        (?:\s*,\s*[\'"]
            ([^\'"]+)
            [\'"])?
        \s*\)\s*\}''',
    flags=re.DOTALL | re.VERBOSE,
)
"""Match ref patterns. i.e. `${ref('dataset', 'model')}`"""

PATTERN_INTERPOLATION = re.compile(
    r'''
    (?<!\\)\$           # Match `$` and don't allow an escape
    (?<!\\)\{           # Match `{` and don't allow an escape
    (?!ref)             # Don't match ref()
    (?P<variable>.+)    # Get the variable content (for hashing)
    (?<!\\)\}           # Match `}` and don't allow an escape
    ''',
    flags=re.DOTALL | re.VERBOSE
)
"""Match JS variables. i.e. `${some_js_var + 1}`"""

# PATTERN_INCREMENTAL_CONDITION = re.compile(
#     r'\$\{when\(\s*[\w]+\(\),\s*(?P<SQL>(?:`[^`]*`)|(?:\"[^\"]*\")|(?:\'[^\']*\')|[^{}]*)\)}',
#     flags=re.DOTALL,
# )

# PATTERN_INCREMENTAL_CONDITION = re.compile(
#     r'''
#     \$\{when\(\s*[\w]+\(\),\s*
#     (?P<SQL>
#         (?:`[^`]*`)
#         |(?:\"[^\"]*\")
#         |(?:\'[^\']*\')
#         |[^{}]*
#     )
#     \)}
#     ''',
#     flags=re.DOTALL | re.VERBOSE,
# )

PATTERN_INCREMENTAL_CONDITION = re.compile(
    r'''
    \$\{when\(\s*[\w]+\(\),\s*
    (?<!\\)                     # no backslash before opening quote
    (?P<quote>["'`])            # opening quote/backtick
    (?P<SQL>.*)            # capture content
    (?<!\\)(?P=quote)           # closing quote, also not escaped
    \)}
    ''',
    flags=re.DOTALL | re.VERBOSE,
)
"""Match incremental condition patterns. i.e. `${when(condition, 'value')}`"""

DICT_PATTERN = {
    "config": PATTERN_BLOCK_CONFIG,
    "pre_operations": PATTERN_BLOCK_PRE_OPERATION,
    "post_operations": PATTERN_BLOCK_POST_OPERATION,
    "js": PATTERN_BLOCK_JS,
    "ref": PATTERN_REFERENCE,
    "incremental_condition": PATTERN_INCREMENTAL_CONDITION,
    "interpolation": PATTERN_INTERPOLATION
}

__all__ = [
    "PATTERN_BLOCK_CONFIG",
    "PATTERN_BLOCK_PRE_OPERATION",
    "PATTERN_BLOCK_POST_OPERATION",
    "PATTERN_BLOCK_JS",
    "PATTERN_REFERENCE",
    "PATTERN_INCREMENTAL_CONDITION",
    "PATTERN_INTERPOLATION",
    "DICT_PATTERN",
]
