# AGENTS.md - Testing Guide

This document describes how to run tests for the parproc project.

## Running Tests

Tests are run using the `scripts/test.sh` script. This script uses `uv` to manage dependencies and runs the Python unittest framework.

### Basic Usage

Run all tests:
```bash
bash scripts/test.sh
```

### Running Specific Test Modules

You can specify which test modules to run by passing them as arguments:

```bash
# Run only simple tests
bash scripts/test.sh tests.simple

# Run only proto tests
bash scripts/test.sh tests.proto

# Run multiple test modules
bash scripts/test.sh tests.simple tests.proto
```

### Default Test Modules

If no arguments are provided, the script runs these test modules by default:
- `tests.simple`
- `tests.proto`
- `tests.errorformat`

### Test Structure

Tests are located in the `tests/` directory:
- `tests/simple.py` - Basic functionality tests
- `tests/proto.py` - Proto and process prototype tests
- `tests/errorformat.py` - Error handling and formatting tests

### Requirements

- `uv` must be installed and available in PATH
- Python 3.x must be available
- All project dependencies must be installed (handled automatically by `uv`)

### Example Output

When tests run successfully, you'll see output showing:
- Process execution logs (DEBUG level)
- Process status indicators (• for running, ✓ for success, ✗ for failure)
- Final test results

### Troubleshooting

If tests fail:
1. Ensure `uv` is installed: `curl -LsSf https://astral.sh/uv/install.sh | sh`
2. Check that you're in the project root directory
3. Verify Python version compatibility
4. Check test output for specific error messages
