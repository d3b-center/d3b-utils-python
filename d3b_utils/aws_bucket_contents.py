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
    get_versions=False,
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
        file. If get_versions is False, writes a single file. If
        get_versions is True, writes two files:
        "versions-{output_filename}" for the file versions and
        "delete_markers-{output_filename}" for the delete markers
    :type output_filename: str, optional
    :param delim: If writing output to a delimited file, use this delimiter
    :type delim: str, optional, default based on output_filename extension
    :param profile: aws profile
    :type profile: str, optional
    :param get_versions: get object versions (and delete markers) instead of
        only getting current bucket contents.
    :type get_versions: bool, optional

    :returns: list of dicts. If get_versions is False, each dict has
        information about each object in the bucket (or each object that has
        a path matching the search_prefixes). If get_versions is True, the
        list of dicts has two items, versions and delete_markers, each
        containing a list of dicts of versions and delete_markers
        respectively, i.e.:
        {
            versions: [{version-a}, ... {version-n}],
            delete_markers: [{delete_marker-a}, ... {delete_marker-n}],
        }

    """
    search_prefixes = search_prefixes or ""

    if isinstance(search_prefixes, str):
        search_prefixes = [search_prefixes]

    # Drop leading slashes from prefixes
    search_prefixes = [re.sub(r"^/+", "", prefix) for prefix in search_prefixes]

    session = boto3.session.Session(profile_name=profile)
    client = session.client("s3")

    paginator = client.get_paginator("list_objects_v2")

    # Prep file writing
    if output_filename:
        delimiters = {"tsv": "\t", "csv": ","}
        delim = delim or delimiters[output_filename.rsplit(".", 1)[-1].lower()]
        assert delim, "File output delimiter not known. Cannot proceed."

    if get_versions:
        version_paginator = client.get_paginator("list_object_versions")
        bucket_contents = {"versions": [], "delete_markers": []}
        for key_prefix in search_prefixes:
            for page in version_paginator.paginate(
                Bucket=bucket_name, Prefix=key_prefix
            ):
                bucket_contents["versions"].extend(page.get("Versions", []))
                bucket_contents["delete_markers"].extend(
                    page.get("DeleteMarkers", [])
                )
        if drop_folders:
            bucket_contents["versions"] = [
                version
                for version in bucket_contents["versions"]
                if ((version["Key"][-1] != "/") or (version["Size"] > 0))
            ]
            bucket_contents["delete_markers"] = [
                marker
                for marker in bucket_contents["delete_markers"]
                if ((marker["Key"][-1] != "/"))
            ]
        for version in bucket_contents["versions"]:
            version["Bucket"] = bucket_name
            # ETag comes back with unnecessary quotation marks, so strip them
            version["ETag"] = version["ETag"].strip('"')
        for marker in bucket_contents["delete_markers"]:
            marker["Bucket"] = bucket_name
        # Write to file
        if output_filename:
            for part in bucket_contents.keys():
                keys = set(chain(*(d.keys() for d in bucket_contents[part])))
                with open(part + "-" + output_filename, "w") as f:
                    dict_writer = csv.DictWriter(
                        f, restval="", fieldnames=keys, delimiter=delim
                    )
                    dict_writer.writeheader()
                    dict_writer.writerows(bucket_contents[part])
    else:
        paginator = client.get_paginator("list_objects_v2")
        # Build the bucket contents
        bucket_contents = []
        for key_prefix in search_prefixes:
            for page in paginator.paginate(
                Bucket=bucket_name, Prefix=key_prefix
            ):
                bucket_contents.extend(page.get("Contents", []))
        if drop_folders:
            bucket_contents = [
                object
                for object in bucket_contents
                if ((object["Key"][-1] != "/") or (object["Size"] > 0))
            ]

        for object in bucket_contents:
            object["Bucket"] = bucket_name
            # ETag comes back with unnecessary quotation marks, so strip them
            object["ETag"] = object["ETag"].strip('"')

        # Write to file
        if output_filename:
            keys = set(chain(*(d.keys() for d in bucket_contents)))
            with open(output_filename, "w") as f:
                dict_writer = csv.DictWriter(
                    f, restval="", fieldnames=keys, delimiter=delim
                )
                dict_writer.writeheader()
                dict_writer.writerows(bucket_contents)
    return bucket_contents


# backwards compatibility
fetch_aws_bucket_obj_info = fetch_bucket_obj_info
