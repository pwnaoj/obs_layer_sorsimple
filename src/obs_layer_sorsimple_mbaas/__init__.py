# read version from installed package
from importlib.metadata import version
__version__ = version("obs_layer_sorsimple_mbaas")

from obs_layer_sorsimple_mbaas.core.service import EventProcessor

__all__ = [
    "EventProcessor"
]
