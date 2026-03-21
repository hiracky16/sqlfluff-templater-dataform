import logging
import os
import os.path
import re
from typing import (
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
JS_BLOCK_PATTERN = r'\s*js\s*\{(?:[^{}]|\{[^{}]*\})*\}'
JS_EXPRESSION_PATTERN_IN_SQL = r'\$\{'
REF_PATTERN = r'\$\{\s*ref\((.*?)\)\s*\}'
SELF_PATTERN = r'\$\{\s*self\(\s*\)\s*\}'
INCREMENTAL_CONDITION_PATTERN = r'\$\{\s*when\((.*?)\)\s*\}'

class DataformTemplater(RawTemplater):
    """A templater for Dataform SQLX files.

    This templater processes Dataform .sqlx files by:

    1. Removing Dataform-specific blocks (config, pre_operations, post_operations, js)
    2. Replacing Dataform expressions (${ref()}, ${self()}) with BigQuery table references
    3. Handling incremental conditions and JavaScript expressions in SQL

    Block Handling:
    Dataform blocks can contain deeply nested braces in JavaScript code. The templater
    uses a brace-counting algorithm to correctly identify block boundaries regardless
    of nesting depth, ensuring proper removal of blocks even when they contain complex
    JavaScript with multiple levels of nested functions, conditionals, or objects.

    For example, this post_operations block with deep nesting is handled correctly:
        post_operations {
          if (condition) {
            const result = ${self()};
            if (result) {
              console.log({ nested: { object: true } });
            }
          }
        }

    The brace-counting approach ensures that the entire block is identified and removed,
    preventing slicing errors that could occur with regex-based approaches limited to
    fixed nesting depths.
    """

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

    def _setup_config(self, config: Optional["FluffConfig"] = None):
        """Set up configuration for the templater."""
        if config:
            self.sqlfluff_config = config
            
        if self.sqlfluff_config:
            self.project_id = self.sqlfluff_config.get(
                "project_id", section=(self.templater_selector, self.name)
            )
            self.dataset_id = self.sqlfluff_config.get(
                "dataset_id", section=(self.templater_selector, self.name)
            )

    def sequence_files(
        self, fnames: List[str], config=None, formatter=None
    ) -> List[str]:
        self._setup_config(config)
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

        self._setup_config(config)

        templated_sql, raw_slices, templated_slices = self.slice_sqlx_template(in_str)

        return TemplatedFile(
            source_str=in_str,
            templated_str=templated_sql,
            fname=fname,
            sliced_file=templated_slices,
            raw_sliced=raw_slices,
        ), []

    def replace_blocks(self, in_str: str) -> str:
        """Remove all Dataform blocks from the SQL string.

        This method identifies and removes config, pre_operations, post_operations,
        and js blocks from the input string. It uses a brace-counting approach to
        handle blocks with arbitrary nesting depth, ensuring that blocks containing
        complex JavaScript code with deeply nested braces are correctly identified
        and removed in their entirety.

        The algorithm:
        1. Search for block start patterns (keyword followed by {)
        2. Use find_block_end() to locate the matching closing brace
        3. Remove the entire block (including nested content)
        4. Repeat until no more blocks are found

        This approach is robust against JavaScript code with unlimited nesting levels,
        unlike regex-based methods that are limited to fixed recursion depths.

        Args:
            in_str: The input SQLX string containing Dataform blocks

        Returns:
            The input string with all Dataform blocks removed
        """
        block_keywords = ['config', 'js']
        for keyword in block_keywords:
            pattern = rf'{re.escape(keyword)}\s*\{{'
            while True:
                match = re.search(pattern, in_str)
                if not match:
                    break
                start = match.end() - 1  # position of {
                end = self.find_block_end(in_str, start)
                if end != -1:
                    in_str = in_str[:match.start()] + in_str[end:]
                else:
                    break  # invalid, stop
        return in_str

    def extract_operation_blocks(self, in_str: str) -> str:
        """Extract SQL from pre/post_operations blocks and append a semicolon if missing."""
        block_keywords = ['pre_operations', 'post_operations']
        for keyword in block_keywords:
            pattern = rf'{re.escape(keyword)}\s*\{{'
            while True:
                match = re.search(pattern, in_str)
                if not match:
                    break
                start = match.end() - 1  # position of {
                end = self.find_block_end(in_str, start)
                if end != -1:
                    inner_sql = in_str[start+1:end-1].strip()
                    if inner_sql and not inner_sql.endswith(";"):
                        inner_sql += ";"
                    replacement = f"\n{inner_sql}\n" if inner_sql else ""
                    in_str = in_str[:match.start()] + replacement + in_str[end:]
                else:
                    break  # invalid, stop
        return in_str

    def find_block_end(self, sql: str, start: int) -> int:
        """Find the end of a block starting with { at position start.

        This method implements a brace-counting algorithm to find the matching
        closing brace for a block that starts with an opening brace at the given
        position. It correctly handles arbitrary nesting levels by maintaining
        a counter that increments for each '{' and decrements for each '}'.

        The algorithm ensures that nested blocks are properly traversed, making
        it robust for complex JavaScript code with deeply nested structures like:
        - Nested function calls
        - Conditional statements within conditionals
        - Object literals with nested objects
        - Array literals with complex expressions

        Args:
            sql: The SQL string to search in
            start: The position of the opening brace '{' that starts the block

        Returns:
            The position after the matching closing brace '}', or -1 if no
            matching brace is found (malformed input)
        """
        brace_count = 0
        i = start
        while i < len(sql):
            if sql[i] == '{':
                brace_count += 1
            elif sql[i] == '}':
                brace_count -= 1
                if brace_count == 0:
                    return i + 1  # include the }
            i += 1
        return -1  # not found

    def find_expression_end(self, sql: str, start: int) -> int:
        """Find the end of a JavaScript expression starting with ${ at position start.

        This method implements a brace-counting algorithm to find the matching
        closing brace for a JavaScript expression that starts with ${ at the given
        position. It correctly handles arbitrary nesting levels by maintaining
        a counter that increments for each '{' and decrements for each '}'.

        The algorithm ensures that nested expressions are properly traversed, making
        it robust for complex JavaScript expressions with deeply nested structures like:
        - Nested function calls with object parameters
        - Conditional expressions within expressions
        - Object literals with nested objects
        - Array literals with complex expressions

        Args:
            sql: The SQL string to search in
            start: The position of the opening { in ${ that starts the expression

        Returns:
            The position after the matching closing brace '}', or -1 if no
            matching brace is found (malformed input)
        """
        brace_count = 0
        i = start
        while i < len(sql):
            if sql[i:i+2] == '${':
                brace_count += 1
                i += 1  # skip the $
            elif sql[i] == '{':
                brace_count += 1
            elif sql[i] == '}':
                brace_count -= 1
                if brace_count == 0:
                    return i + 1  # include the }
            i += 1
        return -1  # not found

    def replace_ref_with_bq_table(self, sql):
        """ A regular expression to handle ref function calls that include spaces. """
        pattern = re.compile(REF_PATTERN)
        def ref_to_table(match):
            # Extract the content inside ref() using the captured group
            ref_content = match.group(1)  # Use the captured group instead of manual extraction
            
            # Check if it's object notation: { name: "name", schema: "schema", database: "database" }
            if ref_content.strip().startswith('{') and ref_content.strip().endswith('}'):
                # Parse object notation with simpler string parsing
                obj_content = ref_content.strip()[1:-1]  # Remove { and }
                parts = {}
                
                # Split by commas and parse each key-value pair
                for pair in obj_content.split(','):
                    pair = pair.strip()
                    if ':' in pair:
                        # Find the first colon and split
                        colon_pos = pair.find(':')
                        key = pair[:colon_pos].strip()
                        value = pair[colon_pos + 1:].strip()
                        # Remove quotes if present
                        if (value.startswith('"') and value.endswith('"')) or \
                           (value.startswith("'") and value.endswith("'")):
                            value = value[1:-1]
                        parts[key] = value
                
                # Debug: if parsing failed, return original
                if not parts:
                    return match.group(0)
                
                # Extract values with fallbacks
                project_id = parts.get('database', self.project_id)
                dataset = parts.get('schema', self.dataset_id)
                model_name = parts.get('name', '')
                
            else:
                # Handle variadic arguments: "database", "schema", "name" or "schema", "name" or "name"
                # Split by commas, trim whitespace, and remove quotes
                parts = [part.strip().strip('"\'') for part in ref_content.split(',')]
                
                if len(parts) == 3:
                    # 3 elements: database, schema, name
                    project_id = parts[0]
                    dataset = parts[1]
                    model_name = parts[2]
                elif len(parts) == 2:
                    # 2 elements: schema, name
                    dataset = parts[0]
                    model_name = parts[1]
                    project_id = self.project_id
                else:
                    # 1 element: name only
                    model_name = parts[0]
                    dataset = self.dataset_id
                    project_id = self.project_id
            
            # Ensure we have a valid model_name
            if not model_name:
                return match.group(0)  # Return original if no valid name found
            
            # Sanitize identifiers to ensure they're valid for BigQuery
            # BigQuery identifiers can contain letters, numbers, and underscores
            # They must start with a letter or underscore
            def sanitize_identifier(identifier):
                if not identifier:
                    return identifier
                # Replace invalid characters with underscores
                sanitized = re.sub(r'[^a-zA-Z0-9_-]', '_', str(identifier))
                # Ensure it starts with a letter or underscore
                if sanitized and sanitized[0].isdigit():
                    sanitized = '_' + sanitized
                return sanitized
            
            project_id = sanitize_identifier(project_id)
            dataset = sanitize_identifier(dataset)
            model_name = sanitize_identifier(model_name)
                
            result = f"`{project_id}.{dataset}.{model_name}`"
            return result

        return re.sub(pattern, ref_to_table, sql)

    def replace_self_with_bq_table(self, sql):
        """ A regular expression to handle self function calls. """
        pattern = re.compile(SELF_PATTERN)
        def self_to_table(match):
            return f"`{self.project_id}.{self.dataset_id}.self`"

        return re.sub(pattern, self_to_table, sql)

    def replace_incremental_condition(self, sql: str):
        pattern = re.compile(INCREMENTAL_CONDITION_PATTERN, re.DOTALL)
        def replace_when(match):
            # For linting purposes, we assume non-incremental mode
            # when(condition, value) with one param: return empty (non-incremental)
            # when(condition, value1, value2) with two params: return value2 (fallback)
            
            content = match.group(1)  # Everything inside when(...)
            
            # Split by comma, but be careful with quoted strings
            params = []
            current_param = ''
            in_backtick = False
            in_double_quote = False
            in_single_quote = False
            paren_depth = 0
            
            i = 0
            while i < len(content):
                char = content[i]
                if char == '`' and not in_double_quote and not in_single_quote:
                    in_backtick = not in_backtick
                elif char == '"' and not in_backtick and not in_single_quote:
                    in_double_quote = not in_double_quote
                elif char == "'" and not in_backtick and not in_double_quote:
                    in_single_quote = not in_single_quote
                elif char == '(' and not in_backtick and not in_double_quote and not in_single_quote:
                    paren_depth += 1
                elif char == ')' and not in_backtick and not in_double_quote and not in_single_quote:
                    paren_depth -= 1
                elif char == ',' and not in_backtick and not in_double_quote and not in_single_quote and paren_depth == 0:
                    params.append(current_param.strip())
                    current_param = ''
                    i += 1
                    continue
                current_param += char
                i += 1
            
            if current_param.strip():
                params.append(current_param.strip())
            
            # Remove the condition (first parameter)
            if len(params) > 1:
                value_params = params[1:]
            else:
                value_params = params
            
            if len(value_params) == 0:
                return ''  # No value parameters
            elif len(value_params) == 1:
                # Single value parameter: return empty (non-incremental mode)
                return ''
            else:
                # Multiple value parameters: return the last one (fallback)
                return value_params[-1]
        
        return re.sub(pattern, replace_when, sql)

    def replace_js_expressions(self, sql: str) -> str:
        """Replace JavaScript expressions with placeholders.
        
        This method uses brace-counting to properly handle expressions
        with nested braces like ${func({param: 'value'})}.
        """
        result = []
        i = 0
        while i < len(sql):
            if sql[i:i+2] == '${' and not (
                sql[i:i+6] == '${ref(' or 
                sql[i:i+7] == '${self(' or
                sql[i:i+6] == '${when('
            ):
                # Found a JS expression, find its end
                expr_start = i + 1  # position of {
                end = self.find_expression_end(sql, expr_start)
                if end != -1:
                    result.append("js_expression")
                    i = end
                    continue
            result.append(sql[i])
            i += 1
        return ''.join(result)

    def slice_sqlx_template(self, sql: str) -> Tuple[str, List[RawFileSlice], List[TemplatedFileSlice]]:
        """Slice SQLX content into raw and templated components.

        This method processes the input SQLX string and creates corresponding slices
        that map between the original source and the templated SQL. It handles:

        1. Dataform blocks (config, pre_operations, post_operations, js) - marked as 'templated'
        2. Dataform expressions (${ref()}, ${self()}, ${when()}) - replaced and marked as 'templated'
        3. JavaScript expressions in SQL (${...}) - replaced with placeholders
        4. Literal SQL content - preserved as-is

        The slicing ensures that sqlfluff can map formatting changes back to the
        original source file. For blocks with deeply nested braces, the brace-counting
        algorithm in find_block_end() ensures accurate block boundary detection.

        The method processes patterns in order of specificity, ensuring that nested
        expressions within blocks are handled correctly. Block detection uses
        brace-counting rather than regex recursion to support unlimited nesting depths.

        Args:
            sql: The raw SQLX string to slice

        Returns:
            A tuple of (templated_sql, raw_slices, templated_slices) where:
            - templated_sql: The SQL with all Dataform elements processed/replaced
            - raw_slices: List of RawFileSlice objects representing source segments
            - templated_slices: List of TemplatedFileSlice objects for mapping
        """
        replaced_sql = self.replace_blocks(sql)
        replaced_sql = self.extract_operation_blocks(replaced_sql)
        replaced_sql = self.replace_self_with_bq_table(replaced_sql)
        replaced_sql = self.replace_ref_with_bq_table(replaced_sql)
        replaced_sql = self.replace_incremental_condition(replaced_sql)
        replaced_sql = self.replace_js_expressions(replaced_sql)

        # Block keywords that start blocks
        block_keywords = ['config', 'pre_operations', 'post_operations', 'js']

        # A regular expression pattern that matches the structure of SQLX.
        patterns = [
            (CONFIG_BLOCK_PATTERN, 'templated'),
            (PRE_OPERATION_BLOCK_PATTERN, 'templated'),
            (POST_OPERATION_BLOCK_PATTERN, 'templated'),
            (JS_BLOCK_PATTERN, 'templated'),
            (REF_PATTERN, 'templated'),
            (SELF_PATTERN, 'templated'),
            (INCREMENTAL_CONDITION_PATTERN, 'templated'),
            (JS_EXPRESSION_PATTERN_IN_SQL, 'templated'), # Add this line
        ]

        raw_slices = []
        templated_slices = []
        current_idx = 0
        templated_idx = 0
        block_idx = 0

        while current_idx < len(sql):
            next_match = None
            next_match_type = 'templated'
            next_match_start = None
            next_match_end = None

            for pattern, match_type in patterns:
                if pattern in [CONFIG_BLOCK_PATTERN, PRE_OPERATION_BLOCK_PATTERN, POST_OPERATION_BLOCK_PATTERN, JS_BLOCK_PATTERN]:
                    # Special handling for blocks: find start, then find matching }
                    start_pattern = r'(config|pre_operations|post_operations|js)\s*\{'
                    match = re.search(start_pattern, sql[current_idx:])
                    if match and match.group(1) in block_keywords:
                        match_start = current_idx + match.start()
                        brace_start = current_idx + match.end() - 1  # position of {
                        end = self.find_block_end(sql, brace_start)
                        if end != -1:
                            match_end = end
                        else:
                            continue  # invalid block, skip
                    else:
                        continue
                elif pattern == JS_EXPRESSION_PATTERN_IN_SQL:
                    # Special handling for JavaScript expressions: find ${, then find matching }
                    match = re.search(r'\$\{', sql[current_idx:])
                    if match:
                        match_start = current_idx + match.start()
                        expr_start = current_idx + match.end() - 1  # position of {
                        end = self.find_expression_end(sql, expr_start)
                        if end != -1:
                            match_end = end
                        else:
                            continue  # invalid expression, skip
                    else:
                        continue
                else:
                    match = re.search(pattern, sql[current_idx:])
                    if match:
                        match_start = current_idx + match.start()
                        match_end = current_idx + match.end()
                    else:
                        continue

                if not next_match or match_start < next_match_start:
                    next_match = match
                    next_match_type = match_type
                    next_match_start = match_start
                    next_match_end = match_end

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

            if next_match_start > current_idx:
                raw_slices.append(RawFileSlice(
                    raw=sql[current_idx:next_match_start],
                    slice_type='literal',
                    source_idx=current_idx,
                    block_idx=block_idx
                ))
                templated_slices.append(TemplatedFileSlice(
                    slice_type='literal',
                    source_slice=slice(current_idx, next_match_start),
                    templated_slice=slice(templated_idx, templated_idx + (next_match_start - current_idx))
                ))
                templated_idx += (next_match_start - current_idx)
                block_idx += 1

            match_raw = sql[next_match_start:next_match_end]

            if next_match_type == 'templated' and re.match(r'^(pre_operations|post_operations)\s*\{', match_raw):
                brace_start = match_raw.find('{')
                inner_sql = match_raw[brace_start+1:-1].strip()
                if inner_sql and not inner_sql.endswith(";"):
                    inner_sql += ";"
                
                op_replaced = inner_sql
                if op_replaced:
                    op_replaced = self.replace_self_with_bq_table(op_replaced)
                    op_replaced = self.replace_ref_with_bq_table(op_replaced)
                    op_replaced = self.replace_incremental_condition(op_replaced)
                    op_replaced = self.replace_js_expressions(op_replaced)
                    op_replaced = f"\n{op_replaced}\n"
                
                raw_slices.append(RawFileSlice(
                    raw=match_raw,
                    slice_type='templated',
                    source_idx=next_match_start,
                    block_idx=block_idx
                ))
                templated_slices.append(TemplatedFileSlice(
                    slice_type=next_match_type,
                    source_slice=slice(next_match_start, next_match_end),
                    templated_slice=slice(templated_idx, templated_idx + len(op_replaced))
                ))
                templated_idx += len(op_replaced)
            elif next_match_type == 'templated' and match_raw.startswith('${') and 'ref(' in match_raw:
                ref_replaced = self.replace_ref_with_bq_table(match_raw)
                raw_slices.append(RawFileSlice(
                    raw=match_raw,
                    slice_type='templated',
                    source_idx=next_match_start,
                    block_idx=block_idx
                ))
                templated_slices.append(TemplatedFileSlice(
                    slice_type=next_match_type,
                    source_slice=slice(next_match_start, next_match_end),
                    templated_slice=slice(templated_idx, templated_idx + len(ref_replaced))
                ))
                templated_idx += len(ref_replaced)
            elif next_match_type == 'templated' and match_raw.startswith('${') and 'self(' in match_raw:
                self_replaced = self.replace_self_with_bq_table(match_raw)
                raw_slices.append(RawFileSlice(
                    raw=match_raw,
                    slice_type='templated',
                    source_idx=next_match_start,
                    block_idx=block_idx
                ))
                templated_slices.append(TemplatedFileSlice(
                    slice_type=next_match_type,
                    source_slice=slice(next_match_start, next_match_end),
                    templated_slice=slice(templated_idx, templated_idx + len(self_replaced))
                ))
                templated_idx += len(self_replaced)
            elif next_match_type == 'templated' and match_raw.startswith('${') and 'when(' in match_raw:
                when_replaced = self.replace_incremental_condition(match_raw)
                raw_slices.append(RawFileSlice(
                    raw=match_raw,
                    slice_type='templated',
                    source_idx=next_match_start,
                    block_idx=block_idx
                ))
                templated_slices.append(TemplatedFileSlice(
                    slice_type=next_match_type,
                    source_slice=slice(next_match_start, next_match_end),
                    templated_slice=slice(templated_idx, templated_idx + len(when_replaced))
                ))
                templated_idx += len(when_replaced)
            elif next_match_type == 'templated' and match_raw.startswith('${') and "when(" not in match_raw and 'ref(' not in match_raw and 'self(' not in match_raw:
                js_replaced = self.replace_js_expressions(match_raw)
                raw_slices.append(RawFileSlice(
                    raw=match_raw,
                    slice_type='templated',
                    source_idx=next_match_start,
                    block_idx=block_idx
                ))
                templated_slices.append(TemplatedFileSlice(
                    slice_type=next_match_type,
                    source_slice=slice(next_match_start, next_match_end),
                    templated_slice=slice(templated_idx, templated_idx + len(js_replaced))
                ))
                templated_idx += len(js_replaced)
            else:
                # This is for blocks (config, pre_operations, post_operations, js)
                raw_slices.append(RawFileSlice(
                    raw=match_raw,
                    slice_type=next_match_type,
                    source_idx=next_match_start,
                    block_idx=block_idx
                ))
                templated_slices.append(TemplatedFileSlice(
                    slice_type=next_match_type,
                    source_slice=slice(next_match_start, next_match_end),
                    templated_slice=slice(templated_idx, templated_idx)
                ))

            current_idx = next_match_end
            block_idx += 1

        return replaced_sql, raw_slices, templated_slices
