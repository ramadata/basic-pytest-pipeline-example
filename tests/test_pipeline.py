import pytest
import pandas as pd
import os
from pipeline import DataPipeline

# Define fixtures for test data
@pytest.fixture
def sample_data():
    return pd.DataFrame({
        'id': [1, 2, 3, 4, 5],
        'revenue': [100, 200, 150, 300, 250],
        'cost': [50, 150, 100, 100, None]
    })

@pytest.fixture
def expected_transformed_data():
    return pd.DataFrame({
        'id': [1, 2, 3, 4, 5],
        'revenue': [100, 200, 150, 300, 250],
        'cost': [50, 150, 100, 100, 0],
        'profit': [50, 50, 50, 200, 250],
        'profit_margin': [0.50, 0.25, 0.33, 0.67, 1.00]
    })

@pytest.fixture
def test_files():
    # Setup
    test_dir = os.path.dirname(os.path.abspath(__file__))
    input_file = os.path.join(test_dir, 'test_data', 'sample_input.csv')
    output_file = os.path.join(test_dir, 'test_data', 'output.csv')
    expected_file = os.path.join(test_dir, 'test_data', 'expected_output.csv')
    
    # Return file paths
    yield {
        'input': input_file,
        'output': output_file,
        'expected': expected_file
    }
    
    # Teardown - remove output file after test
    if os.path.exists(output_file):
        os.remove(output_file)

# Unit tests for individual components
def test_extract(test_files):
    pipeline = DataPipeline()
    df = pipeline.extract(test_files['input'])
    
    assert isinstance(df, pd.DataFrame)
    assert not df.empty
    assert 'revenue' in df.columns
    assert 'cost' in df.columns

def test_transform(sample_data, expected_transformed_data):
    pipeline = DataPipeline()
    result = pipeline.transform(sample_data)
    
    # Check shape
    assert result.shape[0] <= sample_data.shape[0]  # May have filtered rows
    assert result.shape[1] > sample_data.shape[1]  # Should have new columns
    
    # Check new columns exist
    assert 'profit' in result.columns
    assert 'profit_margin' in result.columns
    
    # Check data integrity
    assert 'id' in result.columns
    assert all(result['profit'] >= 0)  # Validate business rule
    
    # Compare with expected result
    pd.testing.assert_frame_equal(
        result.sort_values('id').reset_index(drop=True), 
        expected_transformed_data.sort_values('id').reset_index(drop=True)
    )

def test_load(sample_data, test_files):
    pipeline = DataPipeline()
    success = pipeline.load(sample_data, test_files['output'])
    
    assert success
    assert os.path.exists(test_files['output'])
    
    # Verify data was written correctly
    loaded_data = pd.read_csv(test_files['output'])
    assert loaded_data.shape == sample_data.shape

# Integration test for the full pipeline
def test_pipeline_integration(test_files):
    pipeline = DataPipeline()
    
    config = {
        'source_path': test_files['input'],
        'destination_path': test_files['output']
    }
    
    success = pipeline.run(config)
    
    # Check pipeline completed successfully
    assert success
    
    # Check output file exists
    assert os.path.exists(test_files['output'])
    
    # Check output matches expected output
    result = pd.read_csv(test_files['output'])
    expected = pd.read_csv(test_files['expected'])
    
    pd.testing.assert_frame_equal(
        result.sort_values('id').reset_index(drop=True), 
        expected.sort_values('id').reset_index(drop=True)
    )

# Test error handling
def test_error_handling():
    pipeline = DataPipeline()
    
    config = {
        'source_path': 'non_existent_file.csv',
        'destination_path': 'output.csv'
    }
    
    success = pipeline.run(config)
    assert not success  # Should handle the error gracefully