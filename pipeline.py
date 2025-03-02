# File structure:
# /basic_pipeline/
#   pipeline.py
#   tests/
#     test_pipeline.py
#     test_data/
#       sample_input.csv
#       expected_output.csv

# pipeline.py
import pandas as pd
from typing import Dict, Any

class DataPipeline:
    def extract(self, source_path: str) -> pd.DataFrame:
        """Extract data from source"""
        return pd.read_csv(source_path)
    
    def transform(self, data: pd.DataFrame) -> pd.DataFrame:
        """Apply transformations to data"""
        # Example transformations
        transformed = data.copy()
        
        # Fill missing values
        transformed.fillna(0, inplace=True)
        
        # Add a calculated column
        if 'revenue' in transformed.columns and 'cost' in transformed.columns:
            transformed['profit'] = transformed['revenue'] - transformed['cost']
            transformed['profit_margin'] = (transformed['profit'] / transformed['revenue']).round(2)
        
        # Filter out rows with negative profit
        if 'profit' in transformed.columns:
            transformed = transformed[transformed['profit'] >= 0]
            
        return transformed
    
    def load(self, data: pd.DataFrame, destination_path: str) -> bool:
        """Load transformed data to destination"""
        data.to_csv(destination_path, index=False)
        return True
    
    def run(self, config: Dict[str, Any]) -> bool:
        """Run the full ETL pipeline"""
        try:
            # Extract
            raw_data = self.extract(config['source_path'])
            
            # Transform
            transformed_data = self.transform(raw_data)
            
            # Load
            success = self.load(transformed_data, config['destination_path'])
            
            return success
        except Exception as e:
            print(f"Pipeline failed: {e}")
            return False


# tests/test_pipeline.py
