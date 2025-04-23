import logging
import os
import os.path
import re
from typing import (
    Iterator,
    List,
    Optional,
    Tuple,
)
from sqlfluff.core.templaters.base import RawTemplater, TemplatedFile, large_file_check, RawFileSlice, TemplatedFileSlice
from sqlfluff.cli.formatters import OutputStreamFormatter
from sqlfluff.core import FluffConfig
from sqlfluff.core.errors import SQLFluffSkipFile


# Instantiate the templater logger
templater_logger = logging.getLogger("sqlfluff.templater")

# regex pattern for config block and js block
CONFIG_BLOCK_PATTERN = r'config\s*\{(?:[^{}]|\{(?:[^{}]|\{[^{}]*\})*\})*\}'
PRE_OPERATION_BLOCK_PATTERN = r'pre_operations\s*\{(?:[^{}]|\{(?:[^{}]|\{[^{}]*\})*\})*\}'
POST_OPERATION_BLOCK_PATTERN = r'post_operations\s*\{(?:[^{}]|\{(?:[^{}]|\{[^{}]*\})*\})*\}'
JS_BLOCK_PATTERN = r'js\s*\{(?:[^{}]|\{[^{}]*\})*\}'
REF_PATTERN = r'\$\{\s*ref\(\s*[\'"]([^\'"]+)[\'"](?:\s*,\s*[\'"]([^\'"]+)[\'"])?\s*\)\s*\}'
INCREMENTAL_CONDITION_PATTERN = r'\$\{when\(\s*[\w]+\(\),\s*(?:(`[^`]*`)|("[^"]*")|(\'[^\']*\')|[^{}]*)\)}'

class UsedJSBlockError(SQLFluffSkipFile):
    """ This package does not support dataform js block """
    """ When js block used, skip linting a file."""
    pass

class DataformTemplater(RawTemplater):
    """A templater using dataform."""

    name = "dataform"
    sequential_fail_limit = 3
    adapters = {}

    def __init__(self, **kwargs):
        self.sqlfluff_config = None
        self.formatter = None
        self.project_id = None
        self.dataset_id = None
        self.working_dir = os.getcwd()
        self._sequential_fails = 0
        super().__init__(**kwargs)

    def sequence_files(
        self, fnames: List[str], config=None, formatter=None
    ) -> List[str]:
        self.sqlfluff_config = config
        # NOTE: The sqlfluff_config will be introduced at this stage, so the default project_id and dataset_id will be set.
        self.project_id = self.sqlfluff_config.get_section(
            (self.templater_selector, self.name, "project_id")
        )
        self.dataset_id = self.sqlfluff_config.get_section(
            (self.templater_selector, self.name, "dataset_id")
        )
        return fnames

    @large_file_check
    def process(
        self,
        *,
        fname: str,
        in_str: Optional[str] = None,
        config: Optional["FluffConfig"] = None,
        formatter: Optional["OutputStreamFormatter"] = None,
    ):
        if in_str is None:
          return TemplatedFile(source_str='', fname=fname), []

        templater_logger.info(in_str)
        if in_str and self.has_js_block(in_str):
            raise UsedJSBlockError("JavaScript block is not supported.")

        templated_sql, raw_slices, templated_slices = self.slice_sqlx_template(in_str)

        return TemplatedFile(
            source_str=in_str,
            templated_str=templated_sql,
            fname=fname,
            sliced_file=templated_slices,
            raw_sliced=raw_slices,
        ), []

    def has_js_block(self, sql: str) -> bool:
        pattern = re.compile(JS_BLOCK_PATTERN, re.DOTALL)
        return bool(pattern.search(sql))

    def replace_blocks(self, in_str: str) -> str:
        for block_pattern in [
            CONFIG_BLOCK_PATTERN,
            PRE_OPERATION_BLOCK_PATTERN,
            POST_OPERATION_BLOCK_PATTERN
        ]:
            pattern = re.compile(block_pattern, re.DOTALL)
            in_str = re.sub(pattern, '', in_str)

        return in_str

    def replace_ref_with_bq_table(self, sql):
        """ A regular expression to handle ref function calls that include spaces. """
        pattern = re.compile(REF_PATTERN)
        def ref_to_table(match):
            if match.group(2):
                dataset = match.group(1)
                model_name = match.group(2)
            else:
                dataset = self.dataset_id
                model_name = match.group(1)
            return f"`{self.project_id}.{dataset}.{model_name}`"

        return re.sub(pattern, ref_to_table, sql)

    def replace_incremental_condition(self, sql: str):
      pattern = re.compile(INCREMENTAL_CONDITION_PATTERN, re.DOTALL)
      return re.sub(pattern, '', sql)

    def slice_sqlx_template(self, sql: str) -> Tuple[str, List[RawFileSlice], List[TemplatedFileSlice]]:
        """ A function that slices SQLX and returns both RawFileSlice and TemplatedFileSlice simultaneously. """
        replaced_sql = self.replace_blocks(sql)
        replaced_sql = self.replace_ref_with_bq_table(replaced_sql)
        replaced_sql = self.replace_incremental_condition(replaced_sql)

        # A regular expression pattern that matches the structure of SQLX.
        patterns = [
            (CONFIG_BLOCK_PATTERN, 'templated'),
            (PRE_OPERATION_BLOCK_PATTERN, 'templated'),
            (POST_OPERATION_BLOCK_PATTERN, 'templated'),
            # (JS_BLOCK_PATTERN, 'templated'),
            (REF_PATTERN, 'templated'),
            (INCREMENTAL_CONDITION_PATTERN, 'templated'),
        ]

        raw_slices = []
        templated_slices = []
        current_idx = 0
        templated_idx = 0
        block_idx = 0

        while current_idx < len(sql):
            next_match = None
            next_match_type = 'templated'

            for pattern, match_type in patterns:
                match = re.search(pattern, sql[current_idx:])
                if match:
                    match_start = current_idx + match.start()
                    if not next_match or match_start < next_match.start():
                        next_match = match
                        next_match_type = match_type

            if not next_match:
                raw_slices.append(RawFileSlice(
                    raw=sql[current_idx:],
                    slice_type='literal',
                    source_idx=current_idx,
                    block_idx=block_idx
                ))
                templated_slices.append(TemplatedFileSlice(
                    slice_type='literal',
                    source_slice=slice(current_idx, len(sql)),
                    templated_slice=slice(templated_idx, templated_idx + len(sql) - current_idx)
                ))
                break

            if next_match.start() > 0:
                raw_slices.append(RawFileSlice(
                    raw=sql[current_idx:next_match.start() + current_idx],
                    slice_type='literal',
                    source_idx=current_idx,
                    block_idx=block_idx
                ))
                templated_slices.append(TemplatedFileSlice(
                    slice_type='literal',
                    source_slice=slice(current_idx, next_match.start() + current_idx),
                    templated_slice=slice(templated_idx, templated_idx + next_match.start())
                ))
                templated_idx += next_match.start()
                block_idx += 1

            if next_match_type == 'templated' and r"ref(" in next_match.group(0):
                ref_replaced = self.replace_ref_with_bq_table(next_match.group(0))
                raw_slices.append(RawFileSlice(
                    raw=next_match.group(0),
                    slice_type='templated',
                    source_idx=current_idx + next_match.start(),
                    block_idx=block_idx
                ))
                templated_slices.append(TemplatedFileSlice(
                    slice_type=next_match_type,
                    source_slice=slice(current_idx + next_match.start(), current_idx + next_match.end()),
                    templated_slice=slice(templated_idx, templated_idx + len(ref_replaced))
                ))
                templated_idx += len(ref_replaced)
            else:
                raw_slices.append(RawFileSlice(
                    raw=next_match.group(0),
                    slice_type=next_match_type,
                    source_idx=current_idx + next_match.start(),
                    block_idx=block_idx
                ))
                templated_slices.append(TemplatedFileSlice(
                    slice_type=next_match_type,
                    source_slice=slice(current_idx + next_match.start(), current_idx + next_match.end()),
                    templated_slice=slice(templated_idx, templated_idx)
                ))

            current_idx = current_idx + next_match.end()
            block_idx += 1

        return replaced_sql, raw_slices, templated_slices
