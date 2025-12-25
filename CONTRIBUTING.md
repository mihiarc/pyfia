# Contributing to pyFIA

Thank you for your interest in contributing to pyFIA! This document provides guidelines for contributing to the project.

## Code of Conduct

By participating in this project, you agree to maintain a respectful and inclusive environment. We expect all contributors to:

- Be respectful and constructive in discussions
- Welcome newcomers and help them get started
- Focus on what is best for the community and the project

## Getting Started

### Development Setup

1. **Clone the repository**
   ```bash
   git clone https://github.com/mihiarc/pyfia.git
   cd pyfia
   ```

2. **Create a virtual environment with uv**
   ```bash
   uv venv
   source .venv/bin/activate  # Linux/macOS
   # or .venv\Scripts\activate on Windows
   ```

3. **Install in development mode**
   ```bash
   uv pip install -e .[dev]
   ```

4. **Verify installation**
   ```bash
   uv run pytest tests/unit -v
   ```

### Running Tests

```bash
# Run all tests
uv run pytest

# Run unit tests only (fast)
uv run pytest tests/unit

# Run with coverage
uv run pytest --cov=pyfia

# Run specific test markers
uv run pytest -m "not slow"  # Skip slow tests
uv run pytest -m "not network"  # Skip network-dependent tests
```

### Code Quality

We use `ruff` for linting/formatting and `mypy` for type checking:

```bash
# Format code
uv run ruff format src/pyfia tests

# Lint and auto-fix
uv run ruff check --fix src/pyfia tests

# Type checking
uv run mypy src/pyfia
```

## How to Contribute

### Reporting Issues

Before opening an issue, please:

1. **Search existing issues** to avoid duplicates
2. **Use the issue templates** if available
3. **Provide context**:
   - pyFIA version (`pip show pyfia`)
   - Python version
   - Operating system
   - Minimal reproducible example
   - Expected vs actual behavior

### Submitting Pull Requests

1. **Open an issue first** to discuss significant changes
2. **Fork the repository** and create a feature branch:
   ```bash
   git checkout -b feature/your-feature-name
   ```
3. **Make your changes** following the code style guidelines below
4. **Add tests** for new functionality
5. **Run the test suite** to ensure nothing is broken
6. **Update documentation** if needed
7. **Submit a pull request** with a clear description

### Code Style Guidelines

- **Simplicity first**: Avoid over-engineering. Direct functions over complex abstractions.
- **Descriptive names**: Use clear, descriptive variable and function names
- **Type hints**: Add type annotations to public functions
- **Docstrings**: Follow NumPy docstring format for public APIs
- **Testing**: Write tests for new functionality using real FIA data where possible

### Statistical Accuracy

pyFIA implements design-based estimation following Bechtold & Patterson (2005). When modifying estimation code:

- **Validate against EVALIDator**: Use the built-in validation tools
- **Document methodology**: Reference specific equations from the literature
- **Preserve accuracy**: Never sacrifice statistical correctness for convenience

## Development Priorities

1. **Statistical validity** - Results must match EVALIDator
2. **User value** - Features should reduce friction for end users
3. **Simplicity** - When in doubt, choose the simpler approach
4. **Performance** - Choose fast implementations over elegant abstractions

## Questions?

- Open a [GitHub Discussion](https://github.com/mihiarc/pyfia/discussions) for questions
- Open an [Issue](https://github.com/mihiarc/pyfia/issues) for bugs or feature requests

## License

By contributing, you agree that your contributions will be licensed under the MIT License.
