from itertools import chain


from sqlfluff.core.config import FluffConfig


def test_fluff_config_templaters(config: FluffConfig):
    """Verify that the plugin has been registered, and the dataform templater is available."""

    assert config.get_templater() is not None

    templater_lookup = {
        templater.name: templater
        for templater in chain.from_iterable(
            config._plugin_manager.hook.get_templaters()
        )
    }

    assert "dataform" in templater_lookup.keys()
