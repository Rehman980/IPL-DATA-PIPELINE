import pandas as pd

def validate_data(df, schema):
    """Basic data validation"""
    # Check for nulls in key columns
    if df.isnull().values.any():
        raise ValueError("Data contains null values")
    
    # Check for empty DataFrame
    if df.empty:
        raise ValueError("Empty DataFrame received")
    
    return True