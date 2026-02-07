.PHONY: test test-all validate validate-quick lint format typecheck

# Fast unit + property tests (default, no DB or network needed)
test:
	uv run pytest

# All tests including slow, network, and DB-dependent
test-all:
	uv run pytest -m ""

# EVALIDator validation tests (compares pyFIA estimates to official USFS values)
validate:
	uv run pytest tests/validation/ -m "" --ignore=tests/validation/test_all_snum.py

# Full validation including all 754 SNUM API tests (slow, hits EVALIDator API)
validate-all:
	uv run pytest tests/validation/ -m ""

# Code quality
lint:
	uv run ruff check --fix

format:
	uv run ruff format

typecheck:
	uv run mypy src/pyfia/
