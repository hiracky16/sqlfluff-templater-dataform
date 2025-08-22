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
SELF_REFERENCE_PATTERN = r'\$\{self\(\)\}'
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
        # Remove config blocks (metadata)
        in_str = re.sub(re.compile(CONFIG_BLOCK_PATTERN, re.DOTALL), '', in_str)
        
        # Extract SQL content from pre_operations and post_operations blocks, removing the block syntax
        def extract_and_process_block_content(match):
            content = match.group(0)[match.group(0).find('{')+1:match.group(0).rfind('}')].strip()
            # Also process ${self()} references in the extracted content
            return self.replace_self_reference(content)
        
        # Replace "pre_operations { ... }" with just the content inside braces
        in_str = re.sub(re.compile(PRE_OPERATION_BLOCK_PATTERN, re.DOTALL), 
                       extract_and_process_block_content, in_str)
        
        # Replace "post_operations { ... }" with just the content inside braces
        in_str = re.sub(re.compile(POST_OPERATION_BLOCK_PATTERN, re.DOTALL), 
                       extract_and_process_block_content, in_str)
        
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

    def replace_self_reference(self, sql: str) -> str:
        """Replace ${self()} with a unique, deterministic placeholder.
        
        Since we can't reliably determine the current table name from filename or config,
        we use a consistent placeholder that can be identified and replaced later if needed.
        """
        # Replace ${self()} with a unique placeholder that includes project, dataset, and a generic table name
        placeholder = f"`{self.project_id}.{self.dataset_id}.CURRENT_TABLE`"
        return re.sub(r'\$\{self\(\)\}', placeholder, sql)

    def slice_sqlx_template(self, sql: str) -> Tuple[str, List[RawFileSlice], List[TemplatedFileSlice]]:
        """ A function that slices SQLX and returns both RawFileSlice and TemplatedFileSlice simultaneously. """
        replaced_sql = self.replace_blocks(sql)
        # Don't process ref calls here - they're handled during slicing for individual blocks
        replaced_sql = self.replace_incremental_condition(replaced_sql)

        # A regular expression pattern that matches the structure of SQLX.
        patterns = [
            # Config blocks, pre_operations, and post_operations are processed in replace_blocks
            # (JS_BLOCK_PATTERN, 'templated'),
            (REF_PATTERN, 'templated'),
            (INCREMENTAL_CONDITION_PATTERN, 'templated'),
        ]

        raw_slices = []
        templated_slices = []
        current_idx = 0
        templated_idx = 0
        block_idx = 0

        while current_idx < len(replaced_sql):
            next_match = None
            next_match_type = 'templated'

            for pattern, match_type in patterns:
                match = re.search(pattern, replaced_sql[current_idx:])
                if match:
                    match_start = current_idx + match.start()
                    if not next_match or match_start < next_match.start():
                        next_match = match
                        next_match_type = match_type

            if not next_match:
                raw_slices.append(RawFileSlice(
                    raw=replaced_sql[current_idx:],
                    slice_type='literal',
                    source_idx=current_idx,
                    block_idx=block_idx
                ))
                templated_slices.append(TemplatedFileSlice(
                    slice_type='literal',
                    source_slice=slice(current_idx, len(replaced_sql)),
                    templated_slice=slice(templated_idx, templated_idx + len(replaced_sql) - current_idx)
                ))
                break

            if next_match.start() > 0:
                raw_slices.append(RawFileSlice(
                    raw=replaced_sql[current_idx:next_match.start() + current_idx],
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

        # Build the final templated SQL from the slices
        final_sql = ""
        for templated_slice in templated_slices:
            if templated_slice.slice_type == 'templated':
                # For templated slices, we need to get the raw content and process it
                raw_content = None
                for raw_slice in raw_slices:
                    if (raw_slice.source_idx == templated_slice.source_slice.start and 
                        raw_slice.source_idx + len(raw_slice.raw) == templated_slice.source_slice.stop):
                        raw_content = raw_slice.raw
                        break
                
                if raw_content:
                    # Process ref calls in the raw content
                    processed_content = self.replace_ref_with_bq_table(raw_content)
                    final_sql += processed_content
                else:
                    final_sql += replaced_sql[templated_slice.source_slice.start:templated_slice.source_slice.stop]
            else:
                # For literal slices, just add the content
                final_sql += replaced_sql[templated_slice.source_slice.start:templated_slice.source_slice.stop]

        # Replace any remaining ${self()} references in the final SQL
        final_sql = self.replace_self_reference(final_sql)

        return final_sql, raw_slices, templated_slices
        
