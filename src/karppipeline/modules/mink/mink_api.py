import logging
from time import sleep
from typing import Final, cast
from urllib.error import HTTPError, URLError
import urllib.request
import urllib.parse
import uuid
from karppipeline.common import PipelineException
from karppipeline.execution.install import DRY_RUN
import karppipeline.util.json as json

logger = logging.getLogger(__name__)

BASE_URL: Final[str] = "https://ws.spraakbanken.gu.se/ws/mink/v3"


class MinkAPI:
    """
    Helper class to do calls to the Mink backend (Mink documentation at <BASE_URL>)
    """

    def __init__(self, api_key: str):
        self.api_key = api_key
        # Before self.init, we do not know what the resource_id is
        self._resource_id = None

    @property
    def resource_id(self) -> str:
        if not self._resource_id:
            raise RuntimeError("You are using the class wrong")
        return self._resource_id

    def _execute(self, path: str, method: str, headers: dict[str, str] | None = None, data=None, check=True):
        """
        Called by all methods for the actual request to API. Either throws errors on erroneous status codes (check=True) or
        returns the response body (check=False).
        """
        if not headers:
            headers = {}
        headers["X-Api-Key"] = self.api_key

        request = urllib.request.Request(BASE_URL + "/" + path, headers=headers, method=method, data=data)

        try:
            if not DRY_RUN or method == "GET":
                # allow GET requets even though DRY_RUN is set
                with urllib.request.urlopen(request) as resp:
                    body = resp.read().decode("utf-8")
            else:
                body = '{"status":"success","resource_id":"<PLACEHOLDER>"}'
        except HTTPError as e:
            body = e.read().decode("utf-8")
            if check:
                message = ["Error when calling Mink API:"]
                message.append(f"url: {request.full_url}")
                message.append(f"method: {method}")
                headers["X-Api-Key"] = "<HIDDEN>"
                message.append(f"headers: {headers}")
                message.append(f"response body: {body}")
                message.append(f"payload sent: {'yes' if data else 'no'}")
                raise PipelineException("\n".join(message))
        except URLError as e:
            raise PipelineException(f"Mink API not reachable on {request.full_url}") from e
        resp_obj = json.loads(body)
        return resp_obj

    def init(self, resource_id: str) -> None:
        """
        Set the given resource_id as the current one without checking if it exists.
        """
        self._resource_id = resource_id

    def create_lexicon(self, resource_id) -> str:
        """
        Checks if resource_id is already a resource that user has access to in mink. If not, creates
        a new resource and returns that name.
        """
        resp_obj = self._execute("lexicon/list", "GET")
        for available_resource in resp_obj["resources"]:
            if available_resource == resource_id:
                # if resource already exists, update ContextVar and return
                self.init(resource_id)
                break
        else:
            # if resource did not exist, create a new one
            resp_obj = self._execute("lexicon/create", "POST")

            new_resource_id = cast(str, resp_obj["resource_id"])
            self.init(new_resource_id)

            # return the newly created resource_id
            return new_resource_id
        return resource_id

    def upload_data(self, path):
        """
        Deletes existing files first which are not named <resource_id>.jsonl

        resource_id: the resource id of an existing Mink resource which user has access to.
        path: a path to the file to upload.
        """
        resource_id = self.resource_id
        url = f"lexicon/sources/list/{resource_id}"
        # the file name that we want to use in Mink
        source_file_path = resource_id + ".jsonl"
        source_files = self._execute(url, "GET")
        for source_file in source_files["contents"]:
            server_source_path = source_file["path"]
            if path != source_file_path:
                url = f"lexicon/sources/remove/{resource_id}?remove={server_source_path}"
                self._execute(url, "DELETE")
                logger.info(f"Deleted source file: {server_source_path}, from Mink")

        url = f"lexicon/sources/upload/{resource_id}"

        with open(path, "rb") as fp:
            data = fp.read()

        body, boundary = _encode_multipart_file(source_file_path, data)

        headers = {
            "Content-Type": f"multipart/form-data; boundary={boundary}",
        }

        self._execute(url, "PUT", headers=headers, data=body)

    def upload_config(self, config: str):
        """
        Url encode the config and upload it to Mink

        config: plain text representation of yaml file
        """
        url = f"lexicon/config/upload/{self.resource_id}?config={urllib.parse.quote(config)}"
        self._execute(url, "PUT")

    def run_pipeline(self) -> bool:
        """
        Run Karp pipeline on Mink server. Blocks until done or times out.
        returns: True if a request with a "job_done" was found within timeout, else False
        """
        url = f"lexicon/job/run/{self.resource_id}"
        resp_obj = self._execute(url, method="PUT", check=False)

        if resp_obj["status"] != "success":
            logger.error("Failed to run Karp pipeline via Mink")
            return False
        else:
            self._check_status()
            return True

    def install_karp_s(self) -> bool:
        """
        Run Karp-s installation in the instance configured by Mink server. Blocks until done or timeouts.
        returns: True if a request with a "job_done" was found within timeout, else False
        """
        url = f"lexicon/karps/install/{self.resource_id}"
        resp_obj = self._execute(url, method="PUT", check=False)
        if resp_obj["status"] != "success":
            logger.error("Failed to install resource in Karp search via Mink")
            return False
        else:
            self._check_status()
            return True

    def delete_lexicon(self) -> bool:
        """
        Delete lexicon from Mink, which also deletes the resource from auth system and any installations.
        """
        resource_id = self.resource_id
        url = f"lexicon/remove/{resource_id}"
        response = self._execute(url, method="DELETE", check=False)
        if response["status"] == "error":
            if response["return_code"] == "resource_not_found":
                logger.error(
                    f"Resource {resource_id} is unknown or you do not have access. Check that resource_id in config.yaml is correct."
                )
            else:
                logger.error(f"Unknown issue when trying to install resource with id: {self.resource_id}")
            return False
        return True

    def _check_status(self):
        """
        Blocks until status is done or timeouts after 30 seconds.
        """
        timeout = 30
        url = f"resource/status/get/{self.resource_id}"
        # keep track of how long we have waited, so a timeout can be done
        used_time = 0
        while True:
            if used_time > timeout:
                raise PipelineException("Timeout when checking Mink job status")

            resp_obj = self._execute(url, "GET", check=False)

            if resp_obj.get("job_status") == "done":
                break

            # sleep 1 second between checks
            sleep(1)
            used_time += 1


def _encode_multipart_file(file_name: str, data: bytes):
    boundary = uuid.uuid4().hex

    body = (
        f"--{boundary}\r\n"
        f'Content-Disposition: form-data; name="files"; filename="{file_name}"\r\n'
        "Content-Type: application/octet-stream\r\n"
        "\r\n"
    ).encode()

    body += data
    body += f"\r\n--{boundary}--\r\n".encode()

    return body, boundary
