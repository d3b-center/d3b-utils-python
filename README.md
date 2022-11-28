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

## Test connection to a postgres database

```python
from d3b_utils.database import test_pg_connection

db_url = "postgresql://username:password@localhost:5432/postgres"

if test_pg_connection(db_url):
  print("Able to connect to postgres")
else:
  print("Unable to connect to database")
```

Test if it's possible to connect to a postgres database. Useful if your script needs to do different things depending on if it's possible to connect to the postgres database of interest. Is usefull because psycopg2 will retry connecting for an extended amount of time if there are issues with conection to the database.