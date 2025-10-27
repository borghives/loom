from enum import Enum
import getpass
import os
import threading
from typing import Optional
from bson import ObjectId
from google.cloud import secretmanager
import keyring
import pymongo
from pymongo import AsyncMongoClient

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


# TLV Thread Local Variable
thread_data = threading.local()


def get_local_db_client(
    uri: Optional[str] = None, secret_name: Optional[str] = None
) -> pymongo.MongoClient:
    """
    Gets a thread-local MongoDB client for the local database.

    Args:
        uri (str, optional): The URI of the local database. If a secret is used,
            this should be a format string (e.g., "mongodb+srv://<user>:%s@host/").
            Defaults to the value of the `MONGODB_LOCAL_URI` environment
            variable or `"mongodb://localhost:27017/"`.
        secret_name (str, optional): The name of the secret for the local
            database. Defaults to the value of the `MONGODB_LOCAL_SECRET`
            environment variable.

    Returns:
        pymongo.MongoClient: A thread-local MongoDB client.
    """

    mongodb_uri = uri or os.getenv("MONGODB_LOCAL_URI") or LOCAL_MONGODB_CLIENT_URI
    mongodb_uri = mongodb_uri.strip()

    if (mongodb_uri is None) or (len(mongodb_uri) == 0):
        raise Exception("MONGODB_LOCAL_URI is empty or not set")

    secret_id = secret_name or os.getenv("MONGODB_LOCAL_SECRET")
    if (secret_id is not None) and (len(secret_id) > 0):
        full_mongodb_uri = mongodb_uri % access_secret(secret_id)
    else:
        # for local db, allow for non authenticated access
        # this is not allowed in remote db
        full_mongodb_uri = mongodb_uri

    local_db_client = getattr(thread_data, "local_db_client", None)
    if local_db_client is None:
        local_db_client = pymongo.MongoClient(full_mongodb_uri, tz_aware=True)
        setattr(thread_data, "local_db_client", local_db_client)
    return local_db_client


def get_remote_db_client(
    uri: Optional[str] = None,
    secret_name: Optional[str] = None,
    client_name: str = "default",
) -> pymongo.MongoClient:
    """
    Gets a thread-local MongoDB client for the remote database.

    Args:
        uri (str, optional): The URI of the remote database. If a secret is used,
            this should be a format string (e.g., "mongodb+srv://<user>:%s@host/").
            Defaults to the value of the `MONGODB_URI` environment variable.
        secret_name (str, optional): The name of the secret containing the remote
            database password. Defaults to the value of the `MONGODB_SECRET`
            environment variable.
        client_name (str, optional): The name of the client. Defaults to "default".
            Used to manage multiple remote clients.

    Returns:
        pymongo.MongoClient: A thread-local MongoDB client.
    """

    secret_id = secret_name or os.getenv("MONGODB_SECRET")
    if (secret_id is None) or (len(secret_id) == 0):
        raise Exception(
            '''
            MONGODB_SECRET is not set for remote client.  For safety reason there should 
            always be a secret part to the remote database uri.  You might try to by pass 
            this little friction, but I implore you to reconsider.  Extend the access_secret 
            function to support a secret manager of your choice.
            '''
        )

    mongodb_uri = uri or os.getenv("MONGODB_URI")
    if mongodb_uri is None:
        raise Exception("MONGODB_URI not set for remote client")

    mongodb_uri = mongodb_uri.strip()

    if len(mongodb_uri) == 0:
        raise Exception("MONGODB_URI is empty")

    remote_db_client = getattr(thread_data, f"remote_db_client_{client_name}", None)
    if remote_db_client is None:
        full_url = mongodb_uri % access_secret(secret_id)
        remote_db_client = pymongo.MongoClient(full_url, tz_aware=True)
        setattr(thread_data, f"remote_db_client_{client_name}", remote_db_client)

    return remote_db_client

def get_async_local_db_client(
    uri: Optional[str] = None, secret_name: Optional[str] = None
) -> AsyncMongoClient:
    """
    Gets a thread-local MongoDB async client for the local database.
    """
    mongodb_uri = uri or os.getenv("MONGODB_LOCAL_URI") or LOCAL_MONGODB_CLIENT_URI
    mongodb_uri = mongodb_uri.strip()

    if not mongodb_uri:
        raise Exception("MONGODB_LOCAL_URI is empty or not set")

    secret_id = secret_name or os.getenv("MONGODB_LOCAL_SECRET")
    if secret_id:
        full_mongodb_uri = mongodb_uri % access_secret(secret_id)
    else:
        full_mongodb_uri = mongodb_uri

    local_db_client = getattr(thread_data, "async_local_db_client", None)
    if local_db_client is None:
        local_db_client = AsyncMongoClient(full_mongodb_uri, tz_aware=True)
        setattr(thread_data, "async_local_db_client", local_db_client)
    return local_db_client


def get_async_remote_db_client(
    uri: Optional[str] = None,
    secret_name: Optional[str] = None,
    client_name: str = "default",
) -> AsyncMongoClient:
    """
    Gets a thread-local MongoDB async client for the remote database.
    """
    secret_id = secret_name or os.getenv("MONGODB_SECRET")
    if not secret_id:
        raise Exception(
            """
            MONGODB_SECRET is not set for remote client. For safety reasons, there should
            always be a secret part to the remote database URI.
            """
        )

    mongodb_uri = uri or os.getenv("MONGODB_URI")
    if not mongodb_uri:
        raise Exception("MONGODB_URI not set for remote client")

    mongodb_uri = mongodb_uri.strip()
    if not mongodb_uri:
        raise Exception("MONGODB_URI is empty")

    remote_db_client = getattr(thread_data, f"async_remote_db_client_{client_name}", None)
    if remote_db_client is None:
        full_url = mongodb_uri % access_secret(secret_id)
        remote_db_client = AsyncMongoClient(full_url, tz_aware=True)
        setattr(thread_data, f"async_remote_db_client_{client_name}", remote_db_client)

    return remote_db_client