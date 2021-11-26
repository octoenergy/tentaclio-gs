"""This package implements the tentaclio gs client """
from tentaclio import *  # noqa
from tentaclio.clients import LocalFSClient

from .clients.gs_client import GSClient


STREAM_HANDLER_REGISTRY.register("", StreamURLHandler(LocalFSClient))  # type: ignore
STREAM_HANDLER_REGISTRY.register("file", StreamURLHandler(LocalFSClient))  # type: ignore
STREAM_HANDLER_REGISTRY.register("gs", StreamURLHandler(GSClient))  # type: ignore
STREAM_HANDLER_REGISTRY.register("gcs", StreamURLHandler(GSClient))  # type: ignore
REMOVER_REGISTRY.register("gs", ClientRemover(GSClient))  # type: ignore
REMOVER_REGISTRY.register("gcs", ClientRemover(GSClient))  # type: ignore
