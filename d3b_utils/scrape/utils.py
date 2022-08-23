import csv
from itertools import chain


def _check_delimiter(output_filename, delim=None):
    """Detect delimiter by filename extension if not set"""
    if output_filename and (delim is None):
        delimiters = {"tsv": "\t", "csv": ","}
        delim = delimiters[output_filename.rsplit(".", 1)[-1].lower()]
        assert delim, "File output delimiter not known. Cannot proceed."

    return delim


def _meta_to_file(data, output_filename, delim):
    """Write results of a scrape to files"""

    def _write(content, output_filename, delim):
        keys = set(chain(*(d.keys() for d in content)))
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

# backwards compatibility
_s3meta_to_file = _meta_to_file
