import csv
import os
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from itertools import chain

import boto3


def _check_delimiter(output_filename, delim=None):
    """Detect delimiter by filename extension if not set"""
    if output_filename and (delim is None):
        delimiters = {"tsv": "\t", "csv": ","}
        delim = delimiters[output_filename.rsplit(".", 1)[-1].lower()]
        assert delim, "File output delimiter not known. Cannot proceed."

    return delim


def _s3meta_to_file(data, output_filename, delim):
    """Write results to files"""

    def _write(content, output_filename, delim):
        keys = sorted(set(chain(*(d.keys() for d in content))))
        with open(output_filename, "w") as f:
            w = csv.DictWriter(f, restval="", fieldnames=keys, delimiter=delim)
            w.writeheader()
            w.writerows(content)

    if output_filename:
        if isinstance(data, dict):
            for prefix, storage in data.items():
                _write(storage, f"{prefix}-{output_filename}", delim)
        else:
            _write(data, output_filename, delim)


def fetch_obj_list_info(
    pathlist,
    output_filename=None,
    delim=None,
    profile="saml",
    all_versions=False,
):
    """List the metadata for a given set of S3 objects. Like
    fetch_bucket_obj_info but for a list of file paths instead of a bucket.

    :param pathlist: list of s3 file paths
    :type pathlist: list
    :param output_filename: see fetch_bucket_obj_info
    :param delim: see fetch_bucket_obj_info
    :param profile: see fetch_bucket_obj_info
    :param all_versions: see fetch_bucket_obj_info

    :return: see fetch_bucket_obj_info
    """
    delim = _check_delimiter(output_filename, delim)

    # Group files by their buckets
    groups = {}
    for path in pathlist:
        bucket, key = re.sub(r"^s3://", "", path, count=1).split("/", 1)
        groups.setdefault(bucket, set()).add(key)

    def scrape(bucket, prefix):
        return fetch_bucket_obj_info(
            bucket,
            search_prefixes=[prefix],
            profile=profile,
            all_versions=all_versions,
        )

    if all_versions:
        all_results = {"Versions": [], "DeleteMarkers": []}
    else:
        all_results = []

    # List files at the commonpath prefix of our desired files for each
    # bucket with fetch_bucket_obj_info and only keep the ones we want.
    # Few threads avoids throttling.
    with ThreadPoolExecutor(max_workers=5) as tpex:
        futures = {
            tpex.submit(scrape, bucket, os.path.commonpath(keys)): bucket
            for bucket, keys in groups.items()
        }
        for f in as_completed(futures):
            keep = groups[futures[f]]
            if all_versions:
                for content_type, type_storage in all_results.items():
                    type_storage.extend(
                        o for o in f.result()[content_type] if o["Key"] in keep
                    )
            else:
                all_results.extend(o for o in f.result() if o["Key"] in keep)

    # Write to file if desired
    _s3meta_to_file(all_results, output_filename, delim)

    return all_results


def fetch_bucket_obj_info(
    bucket_name,
    search_prefixes=None,
    drop_folders=False,
    output_filename=None,
    delim=None,
    profile="saml",
    all_versions=False,
):
    """
    List the metadata for objects in a given S3 bucket.

    :param bucket_name: The name of the bucket.
    :type bucket_name: str
    :param search_prefixes: prefix(es) in the given bucket where the files
        you want to list are. Automatically drops leading '/'. If you want
        to search for items in a particular folder, include a '/' at the
        end of folder names (e.g. 'path/to/files/'). This can also be an
        iteratable.
    :type search_prefixes: str, list
    :param drop_folders: Drops folders from the list of returned objects.
        This is done by removing objects where `Size = 0`. Default is
        False.
    :type drop_folders: bool, optional
    :param output_filename: If provided, write delimited results to this
        file. If all_versions is False, writes a single file. If
        all_versions is True, writes two files:
        "Versions-{output_filename}" for the file versions and
        "DeleteMarkers-{output_filename}" for the delete markers
    :type output_filename: str, optional
    :param delim: If writing output to a delimited file, use this delimiter
    :type delim: str, optional, default based on output_filename extension
    :param profile: AWS SAML single-sign-on profile to use, defaults to "saml"
    :type profile: str, optional
    :param all_versions: get all object versions (and delete markers) instead
        of only getting current bucket contents, defaults to False
    :type all_versions: bool, optional

    :return: If all_versions is False, list of dicts where each dict has
        information about each object in the bucket (or each object that has
        a path matching the search_prefixes). If all_versions is True, a dict
        with two items, Versions and DeleteMarkers, each containing a list of
        dicts of Versions and DeleteMarkers objects respectively, i.e.:
        {
            Versions: [{version-a}, ... {version-n}],
            DeleteMarkers: [{delete_marker-a}, ... {delete_marker-n}],
        }
    """
    delim = _check_delimiter(output_filename, delim)

    search_prefixes = search_prefixes or [""]
    if isinstance(search_prefixes, str):
        search_prefixes = [search_prefixes]

    # Drop leading slashes from prefixes
    search_prefixes = [re.sub(r"^/+", "", prefix) for prefix in search_prefixes]

    session = boto3.session.Session(profile_name=profile)
    client = session.client("s3")

    def scrape(client, key_prefix):
        if all_versions:
            paginator = client.get_paginator("list_object_versions")
            prefix_results = {"Versions": [], "DeleteMarkers": []}
        else:
            paginator = client.get_paginator("list_objects_v2")
            prefix_results = {"Contents": []}

        for page in paginator.paginate(Bucket=bucket_name, Prefix=key_prefix):
            for content_type, type_storage in prefix_results.items():
                page_entries = [
                    entry
                    for entry in page.get(content_type, [])
                    if (not drop_folders)
                    or ((entry["Key"][-1] != "/") or (entry.get("Size", 1) > 0))
                ]
                type_storage.extend(page_entries)

        return prefix_results

    all_results = {}

    # few workers to reduce likelihood of AWS account throttling
    with ThreadPoolExecutor(max_workers=5) as tpex:
        for f in as_completed(
            [tpex.submit(scrape, client, p) for p in search_prefixes]
        ):
            for k, v in f.result().items():
                all_results.setdefault(k, []).extend(v)

    for type_storage in all_results.values():
        for object in type_storage:
            object["Bucket"] = bucket_name
            if "ETag" in object:
                # ETag comes back with unnecessary quotation marks, so strip them
                object["ETag"] = object["ETag"].strip('"')

    # Convert to list if not versioned (historical)
    all_results = all_results if all_versions else all_results.popitem()[1]

    # Write to file if desired
    _s3meta_to_file(all_results, output_filename, delim)

    return all_results


# backwards compatibility
fetch_aws_bucket_obj_info = fetch_bucket_obj_info
