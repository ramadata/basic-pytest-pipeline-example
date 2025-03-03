# Data Pipeline Testing with pytest

This repository demonstrates best practices for testing data engineering pipelines using pytest. The example shows how to test each component of an ETL (Extract, Transform, Load) pipeline as well as the integration of the full workflow.

## Project Structure

```
basic_pipeline/
├── pipeline.py         # Main ETL pipeline implementation
├── README.md           # This file
└── tests/
    ├── test_pipeline.py # Test cases for the pipeline
    └── test_data/
        ├── sample_input.csv      # Test input data
        └── expected_output.csv   # Expected test results
```

## Pipeline Overview

The sample pipeline performs these operations:

1. **Extract**: Read data from CSV files
2. **Transform**: Apply business logic transformations:
   - Fill missing values
   - Calculate profit and profit margin
   - Filter out negative profit entries
3. **Load**: Write transformed data to a CSV output

## Testing Approach

The test suite demonstrates several testing strategies for data pipelines:

### Unit Tests

Individual components are tested in isolation:
- `test_extract`: Verifies data loading functionality
- `test_transform`: Validates transformation logic against expected results
- `test_load`: Ensures data is correctly written to the destination

### Integration Tests

- `test_pipeline_integration`: Tests the entire ETL workflow end-to-end

### Fixtures

The tests use pytest fixtures to:
- Create sample data frames
- Manage test file paths
- Handle test setup and teardown

### Data Validation

- Uses pandas testing utilities to compare data frames
- Validates business rules (e.g., no negative profits)
- Checks structural integrity (expected columns, data types)

## Getting Started

### Prerequisites

- Python 3.7+
- pandas
- pytest

### Installation

```bash
# Clone the repository
git clone https://github.com/yourusername/data-pipeline-testing.git
cd data-pipeline-testing

# Create and activate virtual environment (optional)
python -m venv venv
source venv/bin/activate 

# Install dependencies
pip install -r requirements.txt
```

### Running Tests

```bash
# Run all tests
pytest

# Run with verbose output
pytest -v

# Run with coverage report
pytest --cov=data_pipeline

# Run specific test file
pytest tests/test_pipeline.py
```

## Extending the Example

This basic example can be extended in several ways:

1. Add more complex transformations
2. Implement database connections
3. Add cloud storage support (S3, Azure Blob, etc.)
4. Incorporate data validation rules


## License

MIT
