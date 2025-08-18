"""Configuration fixtures for testing.

NOTE: You must install the current repo for the plugin manager
    to work correctly and pick up the Dataform templater.
"""

from pathlib import Path

import pytest


from sqlfluff_templater_dataform import DataformTemplater
from sqlfluff.core import FluffConfig

from .constants import TEST_DATASET_ID, TEST_PROJECT_ID, PATH_TEST_INPUTS


@pytest.fixture
def config():
    return FluffConfig(overrides={"dialect": "bigquery", "templater": "dataform"})


@pytest.fixture
def templater(config: FluffConfig):
    _templater_instance = config.get_templater()

    assert isinstance(_templater_instance, DataformTemplater), (
        "Expected DataformTemplater instance"
    )

    # NOTE: The patch will be applied as it is not being updated.
    _templater_instance.project_id = TEST_PROJECT_ID
    _templater_instance.dataset_id = TEST_DATASET_ID
    return _templater_instance


@pytest.fixture
def test_inputs_dir_path() -> Path:
    return PATH_TEST_INPUTS
