"""Defines the hook endpoints for the dataform templater plugin."""

from sqlfluff.core.plugin import hookimpl
from sqlfluff_templater_dataform.templater import DataformTemplater


@hookimpl
def get_templaters():
    """Get templaters."""
    return [DataformTemplater]
