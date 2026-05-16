# Projects

This is a project to port the EpiProcess R package to Python. The EpiProcess R
package is a tool for working with epidemiological data, particularly in the
context of forecasting and nowcasting. The R package is in
/home/dskel/repos/delphi/epiprocess.

- [PORTING.md](PORTING.md) - Notes on porting EpiProcess from R to Python.

## Development Commands

```bash
# Run all tests
uv run pytest tests/
# Run linting and formatting
uv run ruff check . --fix
uv run ruff format .
```
