import logging
import os
import re
import string
import random
from typing import List, Optional, Tuple, overload
from typing_extensions import override, deprecated
import operator


# SQLFluff imports
from sqlfluff.core.templaters.base import (
    RawTemplater,
    TemplatedFile,
    large_file_check,
    RawFileSlice,
    TemplatedFileSlice,
)
from sqlfluff.core import FluffConfig
from sqlfluff.core.formatter import FormatterInterface

from .patterns import (
    PATTERN_BLOCK_CONFIG,
    PATTERN_BLOCK_OPERATION,
    PATTERN_BLOCK_JS,
    PATTERN_REFERENCE,
    PATTERN_INCREMENTAL_CONDITION,
    PATTERN_INTERPOLATION,
    DICT_PATTERN,
)

# Instantiate the templater logger
_LOGGER = logging.getLogger("sqlfluff.templater")


class DataformTemplater(RawTemplater):
    """A templater using dataform."""

    name = "dataform"
    sequential_fail_limit = 3

    def __init__(self, **kwargs):
        self.sqlfluff_config = None
        self.formatter = None
        self.project_id = None
        self.dataset_id = None
        self.working_dir = os.getcwd()
        self._sequential_fails = 0
        super().__init__(**kwargs)

    @override
    def sequence_files(
        self,
        fnames: List[str],
        config: FluffConfig | None = None,
        formatter: FormatterInterface | None = None,
    ) -> List[str]:
        self.sqlfluff_config = config

        assert isinstance(self.sqlfluff_config, FluffConfig), (
            "DataformTemplater requires a FluffConfig instance to set project_id and dataset_id."
        )

        # NOTE: The sqlfluff_config will be introduced at this stage, so the default project_id and dataset_id will be set.
        self.project_id = self.sqlfluff_config.get_section(
            (self.templater_selector, self.name, "project_id")
        )
        self.dataset_id = self.sqlfluff_config.get_section(
            (self.templater_selector, self.name, "dataset_id")
        )
        return fnames

    @large_file_check
    @override
    def process(
        self,
        *,
        fname: str,
        in_str: Optional[str] = None,
        config: FluffConfig | None = None,
        formatter: FormatterInterface | None = None,
    ):
        if in_str is None:
            return TemplatedFile(source_str="", fname=fname), []

        templated_sql, raw_slices, templated_slices = self.slice_sqlx_template(in_str)

        return TemplatedFile(
            source_str=in_str,
            templated_str=templated_sql,
            fname=fname,
            sliced_file=templated_slices,
            raw_sliced=raw_slices,
        ), []

    @deprecated("Use `re.search(PATTERN_BLOCK_JS)` instead.")
    def has_js_block(self, sql: str) -> bool:
        return bool(PATTERN_BLOCK_JS.search(sql))

    @deprecated("Use `templatize_sql` instead to replace block patterns.")
    def replace_blocks(self, in_str: str, *list_pattern_name: str) -> str:
        """Replace block patterns with empty strings.

        :param in_str: The input string to process.
        :param list_pattern_name: Optional block names to replace.
            If not provided, all blocks will be replaced.

        i.e.
        ```
        config { ... }
        pre_operations { ... }
        post_operations { ... }

        SELECT * FROM table;
        ```
        **becomes**
        ```
        SELECT * FROM table;
        ```
        """
        patterns = DICT_PATTERN.values()
        if list_pattern_name:
            # If specific block names are provided, use those patterns only
            patterns = [DICT_PATTERN[name] for name in list_pattern_name]

        for block_pattern in patterns:
            # pattern = re.compile(block_pattern, re.DOTALL)
            in_str = re.sub(block_pattern, "", in_str)

        return in_str

    @deprecated("Use `self.templatize_sql` instead.")
    @staticmethod
    def _recalculate_block_end(text: str, block_start: int, block_end: int) -> int:
        """Recalculate block end index for a slice."""
        num_brackets = 1
        num_chars = block_end - block_start
        for char in text[block_end:]:
            if char == "{":
                num_brackets += 1
            elif char == "}":
                num_brackets -= 1
            num_chars += 1
            if num_brackets == 0:
                break

        return num_chars

    @deprecated("Use `self.templatize_sql` instead.")
    def extract_blocks(
        self, text, block_name: str
    ) -> Tuple[str, int, int] | Tuple[None, None, None]:
        """Extract a block of text that starts with the given block_name.

        :returns: A tuple containing the extracted block, the start index, and the end index.
            If no block is found, returns `None` for all.
        """
        block_start_match = re.search(block_name + r"\s*\{", text)
        if not block_start_match:
            return None, None, None

        block_start, block_end = block_start_match.span()
        block_end_recalculated = block_start + self._recalculate_block_end(
            text, block_start, block_end
        )

        return (
            text[block_start:block_end_recalculated],
            block_start,
            block_end_recalculated,
        )

    def _ref_to_table(self, match: re.Match) -> str:
        """Convert a ref pattern to a BigQuery table name."""
        if match.group(2):
            dataset = match.group(1)
            model_name = match.group(2)
        else:
            dataset = self.dataset_id
            model_name = match.group(1)
        return f"`{self.project_id}.{dataset}.{model_name}`"

    @deprecated("Use `self.templatize_sql` instead.")
    def replace_ref_with_bq_table(self, sql: str) -> str:
        """Search SQL and replace ref patterns with BigQuery table names."""
        return re.sub(PATTERN_REFERENCE, self._ref_to_table, sql)

    @deprecated("Use `self.templatize_sql` instead.")
    def replace_incremental_condition(self, sql: str) -> str:
        return re.sub(PATTERN_INCREMENTAL_CONDITION, "", sql)

    @deprecated("Use `self.templatize_sql` instead.")
    def extract_templates(self, sql):
        expressions = []
        current_idx = 0
        while True:
            expression, start, end = self.extract_blocks(sql[current_idx:], "\\$")
            if expression is None:
                break
            expressions.append(expression)
            if "${" in expression[2 : len(expression) - 1]:
                nested_expressions = self.extract_templates(
                    expression[2 : len(expression) - 1]
                )
                expressions.extend(nested_expressions)

            current_idx += end or 0

        return sorted(expressions, key=len, reverse=True)

    @deprecated("Use `self.templatize_sql` instead.")
    def replace_templates(self, sql):
        replaced_text = sql
        expressions = self.extract_templates(sql)
        for expression in expressions:
            # https://github.com/sqlfluff/sqlfluff/issues/1540#issuecomment-1110835283
            mask_string = (
                "MASKED_"
                + "".join(random.choices(string.ascii_lowercase, k=8))
                + "."
                + "".join(random.choices(string.ascii_lowercase, k=8))
            )
            replaced_text = replaced_text.replace(expression, mask_string)
        return replaced_text

    def templatize_sql(self, sql: str) -> str:
        """Get the template SQL by applying all necessary transformations.

        :returns: A new preprocessed SQL string.

        This will:
        - Remove `js { ... }` and `config { ... }` blocks
        - Replace `pre_operations { ... }` and `post_operations { ... }`
            blocks with the content of their inner blocks
        - Replace `${ref(<table_name>)}` with the fully qualified table name
        """
        return self.slice_sqlx_template(sql)[0]

    @overload
    def _get_templated_sql(self, pattern: None, match: None) -> str: ...
    @overload
    def _get_templated_sql(self, pattern: re.Pattern, match: re.Match) -> str: ...
    def _get_templated_sql(self, pattern, match):
        if operator.xor(
            pattern is None,
            match is None,
        ):
            raise ValueError(
                "Both pattern and match must be provided to _get_templated_sql."
            )

        rtn_templated_sql = ""

        if pattern and match:
            if pattern in [
                PATTERN_BLOCK_OPERATION,
                PATTERN_INCREMENTAL_CONDITION,
            ]:
                rtn_templated_sql = match.group("SQL")

                if pattern == PATTERN_BLOCK_OPERATION and not re.sub(
                    r"\s+", "", rtn_templated_sql
                ).endswith(";"):
                    rtn_templated_sql = rtn_templated_sql.rstrip() + ";"

            elif pattern == PATTERN_REFERENCE:
                rtn_templated_sql = self._ref_to_table(match=match)

            elif pattern == PATTERN_INTERPOLATION:
                rtn_templated_sql = (
                    "'" + match.group("variable").strip().replace("'", "\\'") + "'"
                )
                # ^ replace with a single-quoted string, escaping single quotes within the variable

            elif pattern in [
                PATTERN_BLOCK_JS,
                PATTERN_BLOCK_CONFIG,
            ]:
                # For patterns that are just config or JS, just replace with a newline
                rtn_templated_sql = "\n"

            else:
                raise NotImplementedError(
                    f"Pattern {pattern!r} ({pattern.__doc__}) does not have a _get_templated_sql implementation."
                    f" Implement what should happen when this pattern is found in `_get_templated_sql`"
                    " or provide a `SQL` group in the pattern."
                )

        while True:
            internal_match: re.Match | None = None
            internal_pattern: re.Pattern | None = None

            internal_match, internal_pattern = self._find_next_match(rtn_templated_sql)

            if internal_match and internal_pattern:
                _LOGGER.debug(
                    f"Found internal match {internal_match.group(0)!r} with pattern {internal_pattern.__doc__!r}"
                )
                rtn_templated_sql = (
                    rtn_templated_sql[: internal_match.start()]
                    + self._get_templated_sql(
                        pattern=internal_pattern,
                        match=internal_match,
                    )
                    + rtn_templated_sql[internal_match.end() :]
                )

            if not internal_match or not internal_pattern:
                break

        return rtn_templated_sql

    def _find_next_match(
        self, sql_snippet: str
    ) -> Tuple[re.Match, re.Pattern] | Tuple[None, None]:
        """Find the next match in the SQL snippet.

        :returns: The match and pattern used to match or `None` if no match
        """
        next_match: re.Match | None = None
        next_pattern: re.Pattern | None = None

        for _pattern_name, pattern in DICT_PATTERN.items():
            # Find the next immediate match
            match = re.search(pattern, sql_snippet)
            if match and (not next_match or next_match.start() > match.start()):
                next_match = match
                next_pattern = pattern

        if not next_match or not next_pattern:
            return None, None

        return next_match, next_pattern

    def slice_sqlx_template(
        self, sql: str
    ) -> Tuple[str, List[RawFileSlice], List[TemplatedFileSlice]]:
        """Slices SQLX and return RawFileSlice and TemplatedFileSlice at the same time.

        :returns: The templated sql, the raw slices, and the templated slices.
        """

        # Execution Plan
        # Use a current index integer to figure out where I am in the raw and the template
        # Iterate through the raw, find the next match (nearest to index) to change (a block, ref, etc)
        # When matched, replace with correct template
        # Move index of raw to current += length of match
        # Move index of template to current += len of replacement sub

        raw_slices: list[RawFileSlice] = []
        templated_slices: list[TemplatedFileSlice] = []

        raw_idx = 0  # Current parsing/cursor index
        templated_idx = 0  # Current templated index
        block_idx = 0  # Current block index
        sql_templated = ""

        # Go though the SQL string
        while raw_idx < len(sql):
            next_match, next_pattern = self._find_next_match(sql[raw_idx:])

            if not next_match or not next_pattern:
                _LOGGER.debug(f"No more matches found past index {raw_idx}")
                # If no more patterns matches, add the rest as literals
                raw_slices.append(
                    RawFileSlice(
                        raw=sql[raw_idx:],
                        slice_type="literal",
                        source_idx=raw_idx,
                        block_idx=block_idx,
                    )
                )
                templated_slices.append(
                    TemplatedFileSlice(
                        slice_type="literal",
                        source_slice=slice(raw_idx, len(sql)),
                        templated_slice=slice(
                            templated_idx, templated_idx + len(sql) - raw_idx
                        ),
                    )
                )
                sql_templated += sql[raw_idx:]
                break

            if next_match.start() > 0:
                """
                If the next pattern starts before the current index our cursor is at,
                assume that between our current position and the match is literal SQL
                ```
                SELECT * FROM ${ ... }
                ^             ^<some_matched_block>
                ^<current_cursor>
                ```
                """
                raw_slices.append(
                    RawFileSlice(
                        raw=sql[raw_idx : next_match.start() + raw_idx],
                        slice_type="literal",
                        source_idx=raw_idx,
                        block_idx=block_idx,
                    )
                )
                templated_slices.append(
                    TemplatedFileSlice(
                        slice_type="literal",
                        source_slice=slice(raw_idx, next_match.start() + raw_idx),
                        templated_slice=slice(
                            templated_idx, templated_idx + next_match.start()
                        ),
                    )
                )
                templated_idx += next_match.start()
                sql_templated += sql[raw_idx : next_match.start() + raw_idx]
                block_idx += 1

            assert next_pattern is not None

            _sql_templated_snippet = self._get_templated_sql(
                pattern=next_pattern, match=next_match
            )

            raw_slices.append(
                RawFileSlice(
                    raw=next_match.group(0),
                    slice_type="templated",
                    source_idx=raw_idx + next_match.start(),
                    # block_idx=block_idx,
                )
            )
            templated_slices.append(
                TemplatedFileSlice(
                    slice_type="templated",
                    source_slice=slice(
                        raw_idx + next_match.start(), raw_idx + next_match.end()
                    ),
                    templated_slice=slice(
                        templated_idx,
                        templated_idx + len(_sql_templated_snippet),
                    ),
                ),
            )

            templated_idx += len(_sql_templated_snippet)
            raw_idx = raw_idx + next_match.end()
            sql_templated += _sql_templated_snippet

            block_idx += 1

        return sql_templated, raw_slices, templated_slices


__all__ = [
    "DataformTemplater",
]
