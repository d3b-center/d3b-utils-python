import boto3
import pandas as pd
import re


def fetch_aws_bucket_obj_info(
    bucket_name, search_prefixes=None, drop_folders=False, drop_path=False
):
    """
    List the objects of a bucket and the size of each object. Returns a
    dict.

    :param bucket_name: The name of the bucket.
    :param path_to_files: path to the specific folder in the given
        bucket where the files you want to list are. Automatically drops
        leading '/'. Include a '/' at the end of folder names. Should look
        like 'path/to/files/'. This can also be any iteratable.
    """
    search_prefixes = search_prefixes or ""

    if isinstance(search_prefixes, str):
        search_prefixes = [search_prefixes]

    # Drop leading non-word characters from prefixes
    search_prefixes = [re.sub(r"^/", "", prefix) for prefix in search_prefixes]

    session = boto3.session.Session(profile_name="saml")
    client = session.client("s3")

    paginator = client.get_paginator("list_objects_v2")

    # Aggregate files found under all of the given search prefixes
    if isinstance(search_prefixes, str):
        prefixes = (search_prefixes,)
    else:
        prefixes = search_prefixes

    # Build the bucket contents
    bucket_contents = []
    for key_prefix in prefixes:
        for page in paginator.paginate(Bucket=bucket_name, Prefix=key_prefix):
            bucket_contents.extend(page.get("Contents", []))

    if drop_folders:
        bucket_contents = [
            object for object in bucket_contents if object["Size"] > 0
        ]

    if drop_path:
        for index in range(len(bucket_contents)):
            bucket_contents[index]["Key"] = bucket_contents[index]["Key"].split(
                "/"
            )[-1]

    return bucket_contents
