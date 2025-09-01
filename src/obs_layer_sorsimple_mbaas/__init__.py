# read version from installed package
from importlib.metadata import version
__version__ = version("obs_layer_sorsimple_mbaas")

from obs_layer_sorsimple_mbaas.application.services.event_processor import EventProcessor
from obs_layer_sorsimple_mbaas.application.builders.entity_builder import EntityBuilder
from obs_layer_sorsimple_mbaas.domain.rules.engine import RuleEngine
from obs_layer_sorsimple_mbaas.domain.entities.entity import Entity
from obs_layer_sorsimple_mbaas.common.factories.repository_factory import RepositoryFactory

__all__ = [
    "EventProcessor",
    "EntityBuilder",
    "RuleEngine",
    "Entity",
    "RepositoryFactory"
]
