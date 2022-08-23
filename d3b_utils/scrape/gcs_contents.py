import re

from google.cloud import storage

from .utils import _check_delimiter, _meta_to_file


def fetch_bucket_obj_info(
    bucket_name,
    search_prefixes=None,
    output_filename=None,
    delim=None,
):
    """
    List the metadata for objects in a given S3 bucket. NOTE that this function
    expects an environment variable, `GOOGLE_APPLICATION_CREDENTIALS`, to be
    set. This should point to a credentials json file with access to the
    resources you are trying to access.

    :param bucket_name: The name of the bucket.
    :type bucket_name: str
    :param search_prefixes: prefix(es) in the given bucket where the files
        you want to list are. Automatically drops leading '/'. If you want
        to search for items in a particular folder, include a '/' at the
        end of folder names (e.g. 'path/to/files/'). This can also be an
        iteratable.
    :type search_prefixes: str, list
    :param output_filename: If provided, write delimited results to this
        file. If all_versions is False, writes a single file. If
        all_versions is True, writes two files:
        "Versions-{output_filename}" for the file versions and
        "DeleteMarkers-{output_filename}" for the delete markers
    :type output_filename: str, optional
    :param delim: If writing output to a delimited file, use this delimiter
    :type delim: str, optional, default based on output_filename extension
    :return: List of dicts where each dict has information about each object
        in the bucket (or each object that has a path matching the
        search_prefixes).
    """
    delim = _check_delimiter(output_filename, delim)

    search_prefixes = search_prefixes or [""]
    if isinstance(search_prefixes, str):
        search_prefixes = [search_prefixes]

    # Drop leading slashes from prefixes
    search_prefixes = [re.sub(r"^/+", "", prefix) for prefix in search_prefixes]

    client = storage.Client(project="broad-data-check")

    def scrape(client, search_prefixes):
        blobs = client.list_blobs(bucket_name, prefix=search_prefixes)
        return blobs

    all_results = []
    for blob in scrape(client, search_prefixes):
        object = {
            "key": blob.name,
            "LastModified": blob.updated,
            "ETag": blob.etag,
            "Size": blob.size,
            "StorageClass": blob.storage_class,
            "Bucket": blob.bucket.name,
        }
        all_results.append(object)

    # Write to file if desired
    _meta_to_file(all_results, output_filename, delim)

    return all_results
