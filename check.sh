set -e
ruff check .
ruff format .
ty check
mypy --strict .
pytest .
