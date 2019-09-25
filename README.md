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
