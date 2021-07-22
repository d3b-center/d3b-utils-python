import csv
import re
from itertools import chain

import boto3


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
    List the objects of a bucket and the size of each object. Returns a
    list of dicts.

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
    :param profile: aws profile
    :type profile: str, optional
    :param all_versions: get all object versions (and delete markers) instead
        of only getting current bucket contents.
    :type all_versions: bool, optional

    :returns: list of dicts. If all_versions is False, each dict has
        information about each object in the bucket (or each object that has
        a path matching the search_prefixes). If all_versions is True, the
        list of dicts has two items, Versions and DeleteMarkers, each
        containing a list of dicts of Versions and DeleteMarkers
        respectively, i.e.:
        {
            Versions: [{version-a}, ... {version-n}],
            DeleteMarkers: [{delete_marker-a}, ... {delete_marker-n}],
        }

    """
    # Check output filename
    if output_filename:
        delimiters = {"tsv": "\t", "csv": ","}
        delim = delim or delimiters[output_filename.rsplit(".", 1)[-1].lower()]
        assert delim, "File output delimiter not known. Cannot proceed."

    search_prefixes = search_prefixes or [""]
    if isinstance(search_prefixes, str):
        search_prefixes = [search_prefixes]

    # Drop leading slashes from prefixes
    search_prefixes = [re.sub(r"^/+", "", prefix) for prefix in search_prefixes]

    session = boto3.session.Session(profile_name=profile)
    if all_versions:
        paginator = session.client("s3").get_paginator("list_object_versions")
        results = {"Versions": [], "DeleteMarkers": []}
    else:
        paginator = session.client("s3").get_paginator("list_objects_v2")
        results = {"Contents": []}

    for key_prefix in search_prefixes:
        for page in paginator.paginate(Bucket=bucket_name, Prefix=key_prefix):
            for content_type, type_storage in results.items():
                page_entries = [
                    entry
                    for entry in page.get(content_type, [])
                    if (not drop_folders)
                    or ((entry["Key"][-1] != "/") or (entry.get("Size", 1) > 0))
                ]
                type_storage.extend(page_entries)

    for type_storage in results.values():
        for object in type_storage:
            object["Bucket"] = bucket_name
            if "ETag" in object:
                # ETag comes back with unnecessary quotation marks, so strip them
                object["ETag"] = object["ETag"].strip('"')

    # Write to file
    if output_filename:
        for content_type, type_storage in results.items():
            prefix = f"{content_type}-" if len(results) > 1 else ""
            keys = set(chain(*(d.keys() for d in type_storage)))
            with open(f"{prefix}{output_filename}", "w") as f:
                dict_writer = csv.DictWriter(
                    f, restval="", fieldnames=keys, delimiter=delim
                )
                dict_writer.writeheader()
                dict_writer.writerows(type_storage)

    return results if len(results) > 1 else results.popitem()[1]


# backwards compatibility
fetch_aws_bucket_obj_info = fetch_bucket_obj_info
