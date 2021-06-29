from resources.arweave_interface import ArweaveBackend, Organization
import asyncio

from starlette.responses import Response
from starlette.endpoints import HTTPEndpoint
from starlette.requests import Request
from starlette.background import BackgroundTask

from conf import conf
from pprint import pprint
import logging

import hashlib

import uuid
import os

from cachetools import TTLCache

ar = ArweaveBackend()

refcache = TTLCache(maxsize=10000, ttl=300)

class Manifests(HTTPEndpoint):

    # Patch for https://github.com/encode/starlette/issues/1221
    async def dispatch(self) -> None:
        request = Request(self.scope, receive=self.receive)

        if request.method == "HEAD":
            if hasattr(self, "head"):
                handler_name = "head"
            else:
                handler_name = "get"
        else:
            handler_name = request.method.lower()

        handler = getattr(self, handler_name, self.method_not_allowed)
        is_async = asyncio.iscoroutinefunction(handler)
        if is_async:
            response = await handler(request)
        else:
            response = await run_in_threadpool(handler, request)
        await response(self.scope, self.receive, self.send)

    async def head(self, request):
        # HEAD requests sends a 200 with a docker-content-digest header
        # containing sha256:<ref> to indicate the ref referenced by the tag
        organization = request.path_params["organization"]
        image_name = request.path_params["image_name"]
        tag = request.path_params["ref"]

        ref = ar.get_tag_ref(organization, image_name, tag)
        if ref:
            resp = Response(
                "",
                status_code=200,
                headers={"docker-content-digest": ref, "content-length": "0"},
                media_type="application/vnd.docker.distribution.manifest.list.v2+json",
            )
        else:
            resp = Response("", status_code=404)

        return resp

    async def get(self, request):
        organization = request.path_params["organization"]
        image_name = request.path_params["image_name"]
        ref = request.path_params["ref"]

        url = ar.get_content_url(organization, image_name, ref)
        if url:
            resp = Response("", status_code=307, headers={"location": url})
        else:
            resp = Response("", status_code=404)

        return resp

    async def put(self, request):
        organization = request.path_params["organization"]
        image_name = request.path_params["image_name"]
        tag = request.path_params["ref"]

        data = await request.body()
        sha = hashlib.sha256(data).hexdigest()
        ref = f"sha256:{sha}"

        ar.upload_manifest(organization, image_name, ref, data)

        ar.set_tag_ref(organization, image_name, tag, ref)

        resp = Response(
            "",
            status_code=201,
            headers={
                "docker-content-digest": ref,
                "location": f"/v2/{organization}/{image_name}/manifests/{ref}",
            },
        )

        return resp


class Blobs(HTTPEndpoint):

    # Patch for https://github.com/encode/starlette/issues/1221
    async def dispatch(self) -> None:
        request = Request(self.scope, receive=self.receive)

        if request.method == "HEAD":
            if hasattr(self, "head"):
                handler_name = "head"
            else:
                handler_name = "get"
        else:
            handler_name = request.method.lower()

        handler = getattr(self, handler_name, self.method_not_allowed)
        is_async = asyncio.iscoroutinefunction(handler)
        if is_async:
            response = await handler(request)
        else:
            response = await run_in_threadpool(handler, request)
        await response(self.scope, self.receive, self.send)

    async def head(self, request):
        organization = request.path_params["organization"]
        image_name = request.path_params["image_name"]
        ref = request.path_params["ref"]

        # Test if the image exists, used to avoid pushing twice
        url = ar.get_content_url(organization, image_name, ref)
        if ref in refcache:
            # It's still uploading, we can say it's okay.
            # First in if statement list to save a little time and avoid unneeded queries
            resp = Response("", status_code=200, headers={"content-length": "0"})
        elif url != False:
            resp = Response("", status_code=200, headers={"content-length": "0"})
        else:
            resp = Response("", status_code=404, headers={"content-length": "0"})

        return resp

    async def get(self, request):
        organization = request.path_params["organization"]
        image_name = request.path_params["image_name"]
        ref = request.path_params["ref"]

        # Here we send a redirect to our arweave gateway for the manifest file requested
        url = ar.get_content_url(organization, image_name, ref)
        if url:
            resp = Response("", status_code=307, headers={"location": url})
        else:
            resp = Response("", status_code=404)

        return resp


class BlobUploadInit(HTTPEndpoint):
    async def post(self, request):
        organization = request.path_params["organization"]
        image_name = request.path_params["image_name"]

        upload_uuid = uuid.uuid4().hex

        resp = Response(
            "",
            status_code=202,
            headers={"location": f"/v2/{organization}/{image_name}/blobs/uploads/{upload_uuid}"},
        )

        return resp


class BlobUpload(HTTPEndpoint):
    async def patch(self, request):
        organization = request.path_params["organization"]
        image_name = request.path_params["image_name"]
        upload_uuid = request.path_params["upload_uuid"]

        body = await request.body()

        upload_file_path = os.path.join(conf["tmp_upload_dir"], upload_uuid)
        with open(upload_file_path, "ab+") as upload_file:
            upload_file.write(body)

        upload_filesize = os.path.getsize(upload_file_path)

        resp = Response(
            "",
            status_code=202,
            headers={
                "docker-upload-uuid": upload_uuid,
                "location": f"/v2/{organization}/{image_name}/blobs/uploads/{upload_uuid}",
                "range": f"0-{upload_filesize}",
            },
        )

        return resp

    async def put(self, request):
        organization = request.path_params["organization"]
        image_name = request.path_params["image_name"]
        upload_uuid = request.path_params["upload_uuid"]

        body = await request.body()
        ref = request.query_params["digest"]

        upload_file_path = os.path.join(conf["tmp_upload_dir"], upload_uuid)
        with open(upload_file_path, "ab+") as upload_file:
            upload_file.write(body)

        upload =  BackgroundTask(ar.upload_file, organization, image_name, ref, upload_file_path)

        # Cache the ref so the HEAD right after upload sees it as complete
        refcache[ref] = 'uploading'

        resp = Response(
            "",
            status_code=201,
            headers={
                "docker-content-digest": ref,
                "location": f"/v2/{organization}/{image_name}/blobs/{ref}",
            },
            background=upload
        )

        return resp
