# Collection of reusable python utilities

## Requires

Python >= 3.6

## How to install

Using pip

`pip install git+https://github.com/d3b-center/d3b-utils-python.git`

## Included so far

### Requests with retries

```Python
from d3b_utils.requests_retry import Session

response = Session().get("https://www.foo.com")
```

The python requests library doesn't retry on connection errors unless you add
your own custom transport adapter. Many connection errors are intermittent and
self-correct, so we should definitely retry them.

Don't use the requests library directly. Use this instead.

### Fetch AWS Bucket contents

Before using these functions, make sure to authenticate. Use [`chopaws`](https://github.research.chop.edu/devops/aws-auth-cli).

```Python
from d3b_utils.aws_bucket_contents import fetch_bucket_obj_info
```

#### List the items in a bucket

```python
contents = fetch_bucket_obj_info(
  "kf-study-us-east-1-dev-sd-me0wme0w",
  search_prefixes="source/pics/"
)
```

#### List the items in multiple folders in a bucket

```python
contents = fetch_bucket_obj_info(
  "kf-study-us-east-1-dev-sd-me0wme0w",
  search_prefixes=["source/pics/", "source/uploads/"]
)
```

#### Drop folders from the list of returned objects

The list of returned objects includes folders. You can drop the folders by setting `drop_folders=True`

```python
contents = fetch_bucket_obj_info(
  "kf-study-us-east-1-dev-sd-me0wme0w",
  drop_folders=True,
)
```

#### Write the contents to a delimited file

```python
contents = fetch_bucket_obj_info(
  "kf-study-us-east-1-dev-sd-me0wme0w",
  search_prefixes="source/pics/",
  drop_folders=True,
  output_filename="my_bucket_contents.tsv"
)
```

#### Specify the AWS Profile 

Specify the profile name. If not authenticating with `chopaws`, set the profile name. 

```python
contents = fetch_bucket_obj_info(
  "kf-study-us-east-1-dev-sd-me0wme0w",
  profile="user1"
)
```

#### Get Object versions and delete markers

Return object versions and delete markers. 

```python
contents = fetch_bucket_obj_info(
  "kf-study-us-east-1-dev-sd-me0wme0w",
  all_versions=True
)
```
