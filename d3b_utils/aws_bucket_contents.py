import boto3
import pandas as pd
import re


def fetch_aws_bucket_obj_info(bucket_name, search_prefixes=None):
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
    return bucket_contents


def get_bucket_as_df(bucket_name, bucket_path):
    """
    Wrapper around fetch_aws_bucket_obj_info that returns the dict as a
    DataFrame. It also cleans out the folders from the list of files,
    and drops paths from object names.

    :param bucket_name: The name of the bucket.
    :param path_to_files: specific folder in the given bucket where you
        want to return lists of objects from. Can be a tuple of paths,
        to only include files from those specific directories. Exclude
        leading '/' but include a '/' at the end of the path. Should
        look like 'path/to/files/'. This can also be a tuple of paths.

    """
    # Pull the contents of the bucket
    bucket_contents = fetch_aws_bucket_obj_info(bucket_name, bucket_path)
    # convert to a df
    bucket_df = pd.DataFrame(bucket_contents)
    # drop paths from the object names
    bucket_df.object_name = bucket_df.object_name.str.split("/").str[-1]
    # drop the folders out of the df
    bucket_df = bucket_df[(bucket_df.object_name != "")]
    # sort the buckets alphabetically
    bucket_df = bucket_df.sort_values("object_name")
    # reset indices
    bucket_df = bucket_df.reset_index(drop=True)

    return bucket_df
