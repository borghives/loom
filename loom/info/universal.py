import os
import threading
from typing import Optional
from bson import ObjectId
from google.cloud import secretmanager
import pymongo

LOCAL_MONGODB_CLIENT_URI = "mongodb://localhost:27017/"
ZERO_ID = ObjectId("000000000000000000000000")


def access_secret(
    secret_id: str, project_id: Optional[str] = None, version_id: str = "latest"
) -> str:
    """
    Accesses a secret from Google Cloud Secret Manager.

    Args:
        project_id (str, optional): The ID of the Google Cloud project. Defaults
            to the value of the `GOOGLE_CLOUD_PROJECT_NUM` environment variable.
        secret_id (str): The ID of the secret.
        version_id (str, optional): The version of the secret. Defaults to `"latest"`.

    Returns:
        str: The secret payload.
    """

    project_num = project_id or os.getenv("GOOGLE_CLOUD_PROJECT_NUM")
    if (project_num is None) or (len(project_num) == 0):
        raise Exception("GOOGLE_CLOUD_PROJECT_NUM is not set")

    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{project_num}/secrets/{secret_id}/versions/{version_id}"
    response = client.access_secret_version(request={"name": name})
    return response.payload.data.decode("utf-8")


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
        local_db_client = pymongo.MongoClient(full_mongodb_uri)
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
            """
            MONGODB_SECRET is not set for remote client.  For safety reason there should 
            always be a secret part to the remote database uri.  You might try to by pass 
            this little friction, but I implore you to reconsider.  Extend the access_secret 
            function to support a secret manager of your choice.
            """
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
        remote_db_client = pymongo.MongoClient(full_url)
        setattr(thread_data, f"remote_db_client_{client_name}", remote_db_client)

    return remote_db_client
