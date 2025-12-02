# Contributing to agentexec

Thank you for your interest in contributing to agentexec! This guide will help you get started.

## Code of Conduct

By participating in this project, you agree to abide by our Code of Conduct. Please be respectful and constructive in all interactions.

## Getting Started

### Prerequisites

- Python 3.11 or higher
- Redis 7.0 or higher
- uv (for package management)
- Git

### Development Setup

1. **Fork and clone the repository:**

```bash
git clone https://github.com/YOUR_USERNAME/agentexec.git
cd agentexec
```

2. **Install uv (if not already installed):**

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

3. **Install dependencies:**

```bash
uv sync
```

4. **Start Redis (for tests):**

```bash
# macOS
brew services start redis

# Ubuntu/Debian
sudo systemctl start redis
```

5. **Run tests to verify setup:**

```bash
uv run pytest
```

## Development Workflow

### Creating a Branch

Create a branch for your changes:

```bash
git checkout -b feature/your-feature-name
# or
git checkout -b fix/your-bug-fix
```

### Making Changes

1. Write your code following our style guidelines
2. Add or update tests as needed
3. Update documentation if applicable
4. Run the test suite

### Running Tests

```bash
# Run all tests
uv run pytest

# Run with coverage
uv run pytest --cov=agentexec

# Run specific test file
uv run pytest tests/test_task.py

# Run specific test
uv run pytest tests/test_task.py::test_task_creation
```

### Code Quality

We use ruff for linting and formatting:

```bash
# Check for issues
uv run ruff check .

# Fix auto-fixable issues
uv run ruff check --fix .

# Format code
uv run ruff format .
```

We use mypy for type checking:

```bash
uv run mypy src/agentexec
```

### Pre-commit Hooks

Install pre-commit hooks to automatically check code before commits:

```bash
uv run pre-commit install
```

## Code Style

### Python Style

- Follow [PEP 8](https://pep8.org/) style guidelines
- Use type hints for all function signatures
- Write docstrings for public functions and classes
- Keep functions focused and small

### Example

```python
from uuid import UUID
from typing import Optional

from pydantic import BaseModel

class TaskContext(BaseModel):
    """Context for task execution.

    Attributes:
        name: Name of the task
        priority: Task priority (1-10)
        metadata: Optional additional metadata
    """
    name: str
    priority: int = 5
    metadata: Optional[dict] = None


async def process_task(
    agent_id: UUID,
    context: TaskContext,
    timeout: int = 300,
) -> dict:
    """Process a task with the given context.

    Args:
        agent_id: Unique identifier for the task
        context: Task configuration
        timeout: Maximum execution time in seconds

    Returns:
        Dict containing the task result

    Raises:
        TimeoutError: If task exceeds timeout
        ValueError: If context is invalid
    """
    # Implementation here
    pass
```

### Commit Messages

Use clear, descriptive commit messages:

```
feat: add support for custom queue names

- Allow specifying queue_name in enqueue()
- Update WorkerPool to accept queue_name parameter
- Add tests for custom queue functionality
```

Prefixes:
- `feat:` - New feature
- `fix:` - Bug fix
- `docs:` - Documentation changes
- `test:` - Test additions or changes
- `refactor:` - Code refactoring
- `chore:` - Maintenance tasks

## Testing

### Test Structure

```
tests/
├── test_task.py           # Task-related tests
├── test_activity.py       # Activity tracking tests
├── test_worker_pool.py    # Worker pool tests
├── test_queue.py          # Queue operation tests
├── test_runner.py         # Runner tests
└── conftest.py            # Shared fixtures
```

### Writing Tests

```python
import pytest
from uuid import uuid4
from unittest.mock import AsyncMock, patch

import agentexec as ax

@pytest.fixture
def mock_redis():
    """Fixture for mocked Redis."""
    with patch("agentexec.core.redis_client.get_redis") as mock:
        mock.return_value = AsyncMock()
        yield mock.return_value

@pytest.mark.asyncio
async def test_enqueue_creates_activity(mock_redis):
    """Test that enqueueing a task creates an activity record."""
    # Arrange
    context = MyContext(data="test")

    # Act
    task = await ax.enqueue("test_task", context)

    # Assert
    assert task.agent_id is not None
    assert task.task_name == "test_task"
```

### Test Guidelines

- Test one thing per test function
- Use descriptive test names
- Include docstrings explaining what's being tested
- Use fixtures for common setup
- Mock external dependencies (Redis, databases, APIs)

## Documentation

### Adding Documentation

Documentation is in the `docs/` directory:

```
docs/
├── index.md                    # Main landing page
├── getting-started/            # Getting started guides
├── concepts/                   # Conceptual documentation
├── guides/                     # How-to guides
├── api-reference/              # API documentation
├── deployment/                 # Deployment guides
└── contributing.md             # This file
```

### Documentation Style

- Use clear, concise language
- Include code examples
- Add cross-references to related docs
- Keep examples up to date

### Building Documentation

```bash
# Preview documentation locally (if using MkDocs)
uv run mkdocs serve
```

## Pull Request Process

### Before Submitting

1. Ensure all tests pass
2. Run linting and type checking
3. Update documentation if needed
4. Add entry to CHANGELOG.md

### Creating a Pull Request

1. Push your branch to your fork
2. Create a PR against the `main` branch
3. Fill out the PR template
4. Wait for CI checks to pass
5. Request review from maintainers

### PR Template

```markdown
## Description
Brief description of changes

## Type of Change
- [ ] Bug fix
- [ ] New feature
- [ ] Breaking change
- [ ] Documentation update

## Testing
How was this tested?

## Checklist
- [ ] Tests added/updated
- [ ] Documentation updated
- [ ] CHANGELOG.md updated
- [ ] Code follows style guidelines
```

### Review Process

- Maintainers will review your PR
- Address any feedback promptly
- Once approved, your PR will be merged

## Release Process

Releases are managed by maintainers:

1. Update version in `pyproject.toml`
2. Update CHANGELOG.md
3. Create a release tag
4. CI publishes to PyPI

## Getting Help

- **GitHub Issues**: For bugs and feature requests
- **GitHub Discussions**: For questions and discussions
- **Discord**: Join our community (if available)

## Recognition

Contributors are recognized in:
- CHANGELOG.md for their contributions
- GitHub contributors page

Thank you for contributing to agentexec!
