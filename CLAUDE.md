# Claude Code Project Instructions

## Pre-push requirements
- NEVER use `--no-verify` when pushing. All hooks must run.
- Before pushing, always run `make check` (which runs fmt, vet, lint, test).
- If a hook or check fails, fix the underlying issue — do not bypass it.

## Code style
- All Go files must pass `gofmt` before commit.
- Run `go vet ./...` to catch issues.
- Follow existing code patterns and conventions in the codebase.

## Testing
- Run `go test -race -count=1 ./internal/... ./cmd/...` before pushing.
- Maintain >80% test coverage on application packages.
- Use table-driven tests where appropriate.
