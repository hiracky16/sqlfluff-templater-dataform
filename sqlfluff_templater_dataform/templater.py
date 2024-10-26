import logging
import os
import os.path
import re
import uuid
from typing import (
    Iterator,
    List,
    Optional, Tuple, )

from sqlfluff.cli.formatters import OutputStreamFormatter
from sqlfluff.core import FluffConfig
from sqlfluff.core.templaters.base import RawTemplater, TemplatedFile, large_file_check, RawFileSlice, \
    TemplatedFileSlice

# Instantiate the templater logger
templater_logger = logging.getLogger("sqlfluff.templater")

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

    def sequence_files(
        self, fnames: List[str], config=None, formatter=None
    ) -> Iterator[str]:
        self.sqlfluff_config = config
        # sqlfluff_config がこの段階で入ってくるのでデフォルトの project_id と dataset_id をセット
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
        templated_sql, raw_slices, templated_slices = self.slice_sqlx_template(in_str)

        return TemplatedFile(
            source_str=in_str,
            templated_str=templated_sql,
            fname=fname,
            sliced_file=templated_slices,
            raw_sliced=raw_slices,
        ), []

    def extract_blocks(self, text, block_name: str):

        block_start_match = re.search(block_name + r"\s*\{", text)
        if block_start_match is None:
            return None, None, None

        block_start_start, block_start_end = block_start_match.span()
        num_brackets = 1
        num_chars = block_start_end - block_start_start
        for char in text[block_start_end:]:
            if char == "{":
                num_brackets += 1
            elif char == "}":
                num_brackets -= 1
            num_chars += 1
            if num_brackets == 0:
                break
        return text[block_start_start:block_start_start + num_chars], block_start_start, block_start_start + num_chars

    def replace_blocks(self, in_str: str, block_name: str) -> str:
        match_text, block_start_start, block_start_end = self.extract_blocks(in_str, block_name)
        if match_text is None:
            return in_str

        block_start_match = re.search(block_name + r"\s+\{", in_str)
        if block_start_match is None:
            return in_str

        return in_str.replace(match_text, '')

    def replace_ref_with_bq_table(self, sql):
        # スペースを含む ref 関数呼び出しに対応する正規表現
        pattern = re.compile(r"\$\{\s*ref\(\s*['\"]([^'\"]+)['\"](?:\s*,\s*['\"]([^'\"]+)['\"])?\s*\)\s*\}")
        def ref_to_table(match):
            if match.group(2):
                dataset = match.group(1)
                model_name = match.group(2)
            else:
                dataset = self.dataset_id
                model_name = match.group(1)
            return f"`{self.project_id}.{dataset}.{model_name}`"

        return re.sub(pattern, ref_to_table, sql)

    def extract_templates(self, sql):
        pattern = re.compile(r'\$\s*\{(?:[^{}]|\{[^{}]*})*}', re.DOTALL)
        return [m.group() for m in re.finditer(pattern, sql)]

    def replace_templates(self, sql):
        remaining_text = sql
        expressions = self.extract_templates(sql)
        for expression in expressions:
            # https://github.com/sqlfluff/sqlfluff/issues/1540#issuecomment-1110835283
            mask_string = "a" + str(uuid.uuid1()).replace("-", ".a")
            remaining_text = remaining_text.replace(expression, mask_string)
        return remaining_text

    # SQLX をスライスして、RawFileSlice と TemplatedFileSlice を同時に返す関数
    def slice_sqlx_template(self, sql: str) -> Tuple[str, List[RawFileSlice], List[TemplatedFileSlice]]:
        # config や js ブロックを改行に置換
        replaced_sql = sql
        for block_name in ['config', 'js', 'pre_operations', 'post_operations']:
            replaced_sql = self.replace_blocks(replaced_sql, block_name)

        # ref 関数をBigQueryテーブル名に置換
        replaced_sql = self.replace_ref_with_bq_table(replaced_sql)

        # ${} を置換
        replaced_sql = self.replace_templates(replaced_sql)

        # SQLX の構造に対応するブロック名
        blocks = [
            ('config', 'templated'),   # config ブロック
            ('js', 'templated'),       # js ブロック
            ('pre_operations', 'templated'),  # pre_operations ブロック
            ('post_operations', 'templated'),  # post_operations ブロック
            ('\\$', 'templated'), # $関数
        ]

        raw_slices = []  # RawFileSlice のリスト
        templated_slices = []  # TemplatedFileSlice のリスト
        current_idx = 0
        templated_idx = 0  # テンプレート後のインデックス
        block_idx = 0

        # SQLX 全体をスキャンしてスライスを作成
        while current_idx < len(sql):
            next_match_text = None
            next_match_type = None
            next_match_start = 0
            next_match_end = 0

            # 各パターンで最初にマッチする箇所を探す
            for block_name, match_type in blocks:

                match_text, start, end = self.extract_blocks(sql[current_idx:], block_name)
                if match_text:
                    match_start = current_idx + start
                    if not next_match_text or match_start < next_match_start:
                        next_match_text = match_text
                        next_match_type = match_type
                        next_match_start = start
                        next_match_end = end

            # マッチするものがない場合、残りはリテラルとして追加
            if not next_match_text:
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

            # リテラル部分を追加（マッチした部分の手前までの内容を追加）
            if next_match_start > 0:
                raw_slices.append(RawFileSlice(
                    raw=sql[current_idx:next_match_start + current_idx],
                    slice_type='literal',
                    source_idx=current_idx,
                    block_idx=block_idx
                ))
                templated_slices.append(TemplatedFileSlice(
                    slice_type='literal',
                    source_slice=slice(current_idx, next_match_start + current_idx),
                    templated_slice=slice(templated_idx, templated_idx + next_match_start)
                ))
                templated_idx += next_match_start
                block_idx += 1

            # `ref` 関数の置換を適用する
            if next_match_type == 'templated' and next_match_text.startswith("${") and r"ref(" in next_match_text:
                ref_replaced = self.replace_ref_with_bq_table(next_match_text)
                raw_slices.append(RawFileSlice(
                    raw=next_match_text,
                    slice_type='templated',
                    source_idx=current_idx + next_match_start,
                    block_idx=block_idx
                ))
                templated_slices.append(TemplatedFileSlice(
                    slice_type=next_match_type,
                    source_slice=slice(current_idx + next_match_start, current_idx + next_match_end),
                    templated_slice=slice(templated_idx, templated_idx + len(ref_replaced))
                ))
                templated_idx += len(ref_replaced)
            elif next_match_type == 'templated' and next_match_text.startswith("${"):
                raw_slices.append(RawFileSlice(
                    raw=next_match_text,
                    slice_type='templated',
                    source_idx=current_idx + next_match_start,
                    block_idx=block_idx
                ))
                templated_slices.append(TemplatedFileSlice(
                    slice_type=next_match_type,
                    source_slice=slice(current_idx + next_match_start, current_idx + next_match_end),
                    templated_slice=slice(templated_idx, templated_idx + 41)
                ))
                templated_idx += 41
            else:
                raw_slices.append(RawFileSlice(
                    raw=next_match_text,
                    slice_type=next_match_type,
                    source_idx=current_idx + next_match_start,
                    block_idx=block_idx
                ))
                templated_slices.append(TemplatedFileSlice(
                    slice_type=next_match_type,
                    source_slice=slice(current_idx + next_match_start, current_idx + next_match_end),
                    templated_slice=slice(templated_idx, templated_idx)
                ))

            # インデックスを次のマッチの終わりに移動
            current_idx = current_idx + next_match_end
            block_idx += 1

        # 置換済みのSQLと、スライス情報を返す
        return replaced_sql, raw_slices, templated_slices
