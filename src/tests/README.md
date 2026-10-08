# Tests for Your Pydantic Models

## How to Run the Tests

```bash
pip install pydantic polars pytest pytest-cov
cd project/                                # execute the next commands at the root of your ptoject
pytest -q                                  # basic tests
pytest -q --cov --cov-report=term-missing  # tests with coverage
```

## Troubleshooting ?

- Assert that your tests script filename respect the 'test*' pattern.
