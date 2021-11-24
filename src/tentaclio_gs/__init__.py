"""This package implements the tentaclio gs client """
from tentaclio import *  # noqa

from .clients.gs_client import ClientClassName


# Add DB registry
DB_REGISTRY.register("gs", ClientClassName)  # type: ignore
