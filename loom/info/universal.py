from enum import Enum
import getpass
import os
from typing import Optional
from bson import ObjectId
from google.cloud import secretmanager
import keyring

from dotenv import load_dotenv

load_dotenv()


LOCAL_MONGODB_CLIENT_URI = "mongodb://localhost:27017/"
ZERO_ID = ObjectId("000000000000000000000000")

class SecretManager(Enum):
    GCS_SECRET_MANAGER = "gcs_secret_manager"
    LOCAL_KEYRING = "keyring"
    ENV = "env_variable"


def access_secret(
    secret_id: str,
    source_id: Optional[str] = None,
    version_id: str = "latest",
    manager: SecretManager = SecretManager.GCS_SECRET_MANAGER,
) -> str:
    """
    Accesses a secret from a specified secret manager.

    Args:
        secret_id (str): The ID of the secret.
        source_id (str, optional): The source identifier. For GCS, this is the
            project ID (defaults to `GOOGLE_CLOUD_PROJECT_NUM` env var). For
            local keyring, this is the username (defaults to `LOCAL_KEYRING_USERNAME`
            env var or the current user).
        version_id (str, optional): The version of the secret (for GCS).
            Defaults to `"latest"`.
        manager (SecretManager, optional): The secret manager to use.
            Defaults to `SecretManager.GCS_SECRET_MANAGER`.

    Returns:
        str: The secret payload.
    """

    match manager:
        case SecretManager.GCS_SECRET_MANAGER:
            project_num = source_id or os.getenv("GOOGLE_CLOUD_PROJECT_NUM")
            if (project_num is None) or (len(project_num) == 0):
                raise Exception("GOOGLE_CLOUD_PROJECT_NUM is not set")

            client = secretmanager.SecretManagerServiceClient()
            name = f"projects/{project_num}/secrets/{secret_id}/versions/{version_id}"
            response = client.access_secret_version(request={"name": name})
            return response.payload.data.decode("utf-8")
        case SecretManager.LOCAL_KEYRING:
            username = (
                source_id
                or os.getenv("LOCAL_KEYRING_USERNAME")
                or getpass.getuser()
            )

            ret = keyring.get_password(secret_id, username)
            if ret is None:
                raise Exception("Secret not found in keyring")
            return ret
        case SecretManager.ENV:
            ret = os.getenv(secret_id)
            if ret is None:
                raise Exception("Secret not found in environment")

    raise Exception("Unknown secret manager")


def set_secret(
    secret_id: str,
    secret_value: str,
    source_id: Optional[str] = None,
    manager: SecretManager = SecretManager.GCS_SECRET_MANAGER,
) -> None:
    """
    Sets a secret in a specified secret manager.

    Args:
        secret_id (str): The ID of the secret.
        secret_value (str): The value of the secret to set.
        source_id (str, optional): The source identifier. For GCS, this is the
            project ID (defaults to `GOOGLE_CLOUD_PROJECT_NUM` env var). For
            local keyring, this is the username (defaults to `LOCAL_KEYRING_USERNAME`
            env var or the current user).
        manager (SecretManager, optional): The secret manager to use.
            Defaults to `SecretManager.GCS_SECRET_MANAGER`.
    """
    match manager:
        case SecretManager.GCS_SECRET_MANAGER:
            project_num = source_id or os.getenv("GOOGLE_CLOUD_PROJECT_NUM")
            if (project_num is None) or (len(project_num) == 0):
                raise Exception("GOOGLE_CLOUD_PROJECT_NUM is not set")

            client = secretmanager.SecretManagerServiceClient()
            parent = f"projects/{project_num}/secrets/{secret_id}"

            # Add the secret version.
            payload = secret_value.encode("UTF-8")
            client.add_secret_version(
                request={"parent": parent, "payload": {"data": payload}}
            )
            return

        case SecretManager.LOCAL_KEYRING:
            username = (
                source_id
                or os.getenv("LOCAL_KEYRING_USERNAME")
                or getpass.getuser()
            )
            keyring.set_password(secret_id, username, secret_value)
            return
        case SecretManager.ENV:
            os.environ[secret_id] = secret_value
            return

    raise Exception("Unknown secret manager")