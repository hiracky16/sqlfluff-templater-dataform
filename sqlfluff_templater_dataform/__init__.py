"""Defines the hook endpoints for the dataform templater plugin."""

from sqlfluff.core.plugin import hookimpl


from .constants import *  # ruff
from .templater import DataformTemplater


@hookimpl
def get_templaters():
    """Get templaters."""
    return [DataformTemplater]
