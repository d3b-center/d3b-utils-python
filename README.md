# Collection of reusable python utilities

## Requires

Python >= 3.6

## How to install

Using pip

`pip install -e git+ssh://git@github.com/d3b-center/d3b-utils-python.git#egg=d3b_utils`

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

#### fetch the items in a bucket

```python
fetch_aws_bucket_obj_info(
  "kf-study-us-east-1-dev-sd-me0wme0w",
  "source/pics/"
)
```

#### fetch the items in multiple folders in a bucket

```python
fetch_aws_bucket_obj_info(
  "kf-study-us-east-1-dev-sd-me0wme0w",
  ["source/pics/", "source/uploads/"]
)
```

#### Drop folders from the list of returned objects

The list of returned objects includes folders. You can drop the folders by setting `drop_folders=True`

```python
fetch_aws_bucket_obj_info(
  "kf-study-us-east-1-dev-sd-me0wme0w",
  "source/pics/",
  drop_folders=True,
)
```

#### Remove Path names from the keys of the returned objects

You can return the name of each object without the path name by setting `drop_path=True`. 

```python
fetch_aws_bucket_obj_info(
  "kf-study-us-east-1-dev-sd-me0wme0w",
  "source/pics/",
  drop_path=True
)
```
