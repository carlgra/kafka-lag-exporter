# Contributing to Kafka Lag Exporter

Thank you for your interest in contributing! This project welcomes contributions from anyone.

## How to Contribute

### Reporting Issues

- Search [existing issues](https://github.com/seglo/kafka-lag-exporter/issues) before opening a new one
- Include steps to reproduce, expected vs actual behavior, and your environment details
- For security vulnerabilities, please report privately rather than opening a public issue

### Submitting Changes

1. Fork the repository and create a feature branch from `master`
2. Make your changes, following the guidelines below
3. Add or update tests as appropriate
4. Ensure all tests pass: `make test`
5. Open a pull request with a clear description of the change

### Development Setup

```bash
# Prerequisites: Go 1.22+, Docker (for integration tests)

# Build
make build

# Run unit tests
make test

# Run linter
make lint

# Format code
make fmt

# Run integration tests (requires Docker)
make test-integration
```

### Code Guidelines

- Follow standard Go conventions and idioms
- Run `make fmt` and `make lint` before submitting
- Keep pull requests focused on a single change
- Add tests for new functionality
- Maintain backwards compatibility with existing configuration and metrics

### Commit Messages

- Use a concise subject line describing the change
- Reference related issues where applicable (e.g., `Fixes #42`)

## Code of Conduct

Be respectful and constructive in all interactions. We are committed to providing a welcoming and inclusive experience for everyone.

## License

By contributing, you agree that your contributions will be licensed under the [Apache License 2.0](LICENSE).
