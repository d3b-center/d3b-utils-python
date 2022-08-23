# Collection of reusable python utilities

## Requires

Python >= 3.6

## How to install

Using pip

`pip install -U git+https://github.com/d3b-center/d3b-utils-python.git@latest-release`

## Requests with retries

```Python
from d3b_utils.requests_retry import Session

response = Session().get("https://www.foo.com")
```

The python requests library doesn't retry on connection errors unless you add
your own custom transport adapter. Many connection errors are intermittent and
self-correct, so we should definitely retry them.

Don't use the requests library directly. Use this instead.

## S3 contents metadata

### Fetch S3 bucket contents metadata using `fetch_bucket_obj_info`

#### List all the items in a bucket

```python
from d3b_utils.s3_contents import fetch_bucket_obj_info

contents = fetch_bucket_obj_info("my_bucket")
```

#### List the items in selected subpaths of a bucket

```python
from d3b_utils.s3_contents import fetch_bucket_obj_info

contents = fetch_bucket_obj_info(
  "my_bucket",
  search_prefixes=["source/pics/", "source/uploads/"]
)
```

#### Drop folders (keys ending in "/") from the list of returned objects

```python
from d3b_utils.s3_contents import fetch_bucket_obj_info

contents = fetch_bucket_obj_info(
  "my_bucket",
  drop_folders=True,
)
```

#### Write the contents to a delimited file

```python
from d3b_utils.s3_contents import fetch_bucket_obj_info

contents = fetch_bucket_obj_info(
  "my_bucket",
  search_prefixes="source/pics/",
  drop_folders=True,
  output_filename="my_bucket_contents.tsv"
)
```

#### Specify the AWS Profile

```python
from d3b_utils.s3_contents import fetch_bucket_obj_info

contents = fetch_bucket_obj_info(
  "my_bucket",
  profile="user1"
)
```

#### Get Object versions and delete markers

```python
from d3b_utils.s3_contents import fetch_bucket_obj_info

contents = fetch_bucket_obj_info(
  "my_bucket",
  all_versions=True
)
```

---

### Fetch S3 metadata for a list of files using `fetch_obj_list_info`

```python
from d3b_utils.s3_contents import fetch_obj_list_info

contents = fetch_obj_list_info(
  ["s3://bucket1/path1", "bucket2/path2"],
  profile="user1",
  all_versions=False
)
```

## GCS contents metadata

### Fetch GCS bucket contents metadata using `fetch_bucket_obj_info`

#### List all the items in a bucket

To authenticate with gcs, the environment variable
`GOOGLE_APPLICATION_CREDENTIALS` should exist and point to a google credential
json file.  See
[here](https://cloud.google.com/docs/authentication/provide-credentials-adc)
for more information.

```python
from d3b_utils.gcs_contents import fetch_bucket_obj_info

contents = fetch_bucket_obj_info("my_bucket")
```

See the related s3 functions above for options `search_prefixes`,
output_filename`, and`delim`.

n.b. while for s3, there is an option to `drop_folders`, gcp automatically
drops them when returning lists of files. Likewise, file versions are currently
not supported here.
