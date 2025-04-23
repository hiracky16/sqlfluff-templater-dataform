from pathlib import Path
import pytest

from sqlfluff.core import FluffConfig

@pytest.fixture
def config():
    return FluffConfig(overrides={
        "dialect": "bigquery",
        "templater": "dataform"
    })

@pytest.fixture
def templater(config):
    temp = config.get_templater()
    # NOTE: The patch will be applied as it is not being updated.
    temp.project_id = 'my_project'
    temp.dataset_id = 'my_dataset'
    return temp

@pytest.fixture
def test_inputs_dir_path():
    return Path(__file__).parent.resolve() / "test_inputs"
