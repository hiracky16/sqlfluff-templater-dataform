import re

from pprint import pformat
from typing import Callable, List, Literal, TypeAlias
from operator import eq as op_equal
from dataclasses import dataclass, field

from sqlfluff.core.templaters.base import (
    RawFileSlice,
    TemplatedFileSlice,
)

ALIAS_TEMPLATE_TYPE: TypeAlias = Literal["templated", "literal"]


def assert_sql_is_equal(
    expected_sql: str | re.Pattern,
    actual_sql: str,
    ignore_whitespace: bool = False,
):
    """Assert that the expected SQL is equal to the raw SQL.

    :param expected_sql: The expected SQL statement (can be a regex pattern)
    :param actual_sql: The actual SQL statement to compare against
    :param ignore_whitespace: Whether to ignore whitespace differences.
        This is ignored if `expected_sql` is a regex pattern.

    :raises AssertionError: If the SQL statements do not match
    """

    if isinstance(expected_sql, re.Pattern):
        assert re.match(expected_sql, actual_sql), (
            f"Expected {actual_sql} to match Regex pattern: {expected_sql}"
        )
        return

    if ignore_whitespace:
        expected_sql = re.sub(r"\s+", " ", expected_sql).strip()
        actual_sql = re.sub(r"\s+", " ", actual_sql).strip()

    # expected_sql = re.sub(r"(?<!')''(?!')", r"'", expected_sql)
    # ^ When comparing strings that come from SQL files, the escape sequence is doubled single-quote,
    #   This needs to be converted to an escaped single quote in python

    assert expected_sql == actual_sql, (
        f"EXPECTED:\n{expected_sql!r}\nACTUAL:\n{actual_sql!r}"
    )


@dataclass
class SliceExpected:
    """Container to hold testing expectations for SQL slices."""

    sql_expected: str
    """Raw SQL that should be expected from the slice"""

    sql_slice_type: ALIAS_TEMPLATE_TYPE
    """Templated or literal slice type.

    A **templated** slice is one that has been modified.
        i.e. `${ref("some_table")}`
    A **literal** slice is just raw SQL.
        i.e. `SELECT * FROM table;`
    """

    sql_comparison_func: Callable[..., bool] = field(default=op_equal)
    """Function to use when comparing expected and actual SQL"""

    def slice_matches_expected(self, slice_raw: RawFileSlice) -> bool:
        return self.sql_comparison_func(slice_raw.raw, self.sql_expected)

    def assert_slice_matches_expected(self, slice_raw: RawFileSlice):
        assert self.slice_matches_expected(slice_raw=slice_raw), (
            f"Raw slice {slice_raw} does not match expected slice"
            f" {self.sql_expected!r} with function {self.sql_comparison_func!r}"
        )


def assert_slices(
    slices_raw_actual: List[RawFileSlice],
    slices_template_actual: List[TemplatedFileSlice],
    slices_expected: List[SliceExpected],
):
    for slice_raw_actual, slice_template_actual, slice_expected in zip(
        slices_raw_actual, slices_template_actual, slices_expected
    ):
        slice_raw_actual: RawFileSlice
        slice_template_actual: TemplatedFileSlice

        slice_expected.assert_slice_matches_expected(slice_raw=slice_raw_actual)
        assert slice_template_actual.slice_type == slice_expected.sql_slice_type, (
            f"Slice {slice_template_actual} does not match expected slice type {slice_expected!r}"
        )

    extra_raw_slices = slices_raw_actual[len(slices_expected) :]
    extra_expected_slices = slices_expected[len(slices_raw_actual) :]
    extra_error_info = (
        f"Extra raw slices: {pformat(extra_raw_slices)},\nlast expected slice: {slices_expected[-1]}"
        if extra_raw_slices
        else f"Extra expected slices: {pformat(extra_expected_slices)},\nlast raw slice {slices_raw_actual[-1]}"
    )

    assert (
        len(slices_raw_actual) == len(slices_template_actual) == len(slices_expected)
    ), (
        "The length of the slices are not the same."
        # f"\n{pformat(list(zip(slices_expected, slices_raw_actual)))}"
        + extra_error_info
    )

    # Make sure the slices are contiguous
    # i.e. the end of one slice should be the start of the next
    for _slice_a, _slice_b in zip(
        slices_template_actual[:-1], slices_template_actual[1:]
    ):
        assert _slice_a.source_slice.stop == _slice_b.source_slice.start, (
            f"Raw slices are not contiguous: {_slice_a} and {_slice_b}"
        )
        assert _slice_a.templated_slice.stop == _slice_b.templated_slice.start, (
            f"Template slices are not contiguous: {_slice_a} and {_slice_b}"
        )


__all__ = [
    "assert_sql_is_equal",
    "assert_slices",
    "SliceExpected",
    "ALIAS_TEMPLATE_TYPE",
]
