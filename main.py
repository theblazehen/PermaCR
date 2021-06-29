#!/usr/bin/python3G
import os

from starlette.applications import Starlette
from starlette.routing import Route

from resources.arweave_interface import ArweaveBackend, Organization
import resources.docker_api as docker_api

from conf import conf

import logging

logging.basicConfig(level=logging.WARNING)

routes = [
    Route("/v2/{organization}/{image_name}/manifests/{ref}", docker_api.Manifests),
    Route("/v2/{organization}/{image_name}/blobs/uploads/", docker_api.BlobUploadInit),
    Route("/v2/{organization}/{image_name}/blobs/uploads/{upload_uuid}", docker_api.BlobUpload),
    Route("/v2/{organization}/{image_name}/blobs/{ref}", docker_api.Blobs),
]

app = Starlette(routes=routes)
