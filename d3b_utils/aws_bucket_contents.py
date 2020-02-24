import boto3
import pandas as pd


def fetch_aws_bucket_obj_info(bucket_name, path_to_files):
    """
    List the objects of a bucket and the size of each object. Returns a
    dict. Inspiration from
    https://alexwlchan.net/2019/07/listing-s3-keys/

    :param bucket_name: The name of the bucket.
    :param path_to_files: path to the specific folder in the given
        bucket where the files you want to list are. Exclude leading
        '/' but include a '/' at the end of the folder. Should look
        like 'path/to/files/'. This can also be a tuple of paths.
    """

    client = boto3.client(service_name="s3")

    # Create a paginator. This is needed to page through results when the
    # number of objects in a bucket is greater than 1000.

    paginator = client.get_paginator("list_objects_v2")

    kwargs = {"Bucket": bucket_name}

    # We can pass the prefix directly to the S3 API.  If the user has passed
    # a tuple or list of prefixes, we go through them one by one.
    if isinstance(path_to_files, str):
        prefixes = (path_to_files,)
    else:
        prefixes = path_to_files

    # Build the bucket contents dict
    bucket_contents = []

    for key_prefix in prefixes:
        kwargs["Prefix"] = key_prefix

        for page in paginator.paginate(**kwargs):
            try:
                contents = page["Contents"]
            except KeyError:
                return

            for obj in contents:
                bucket_contents.append(
                    {"object_name": obj["Key"], "object_size": obj["Size"]}
                )

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
