import pandas as pd
import re
from datetime import datetime
from typing import List, Dict, Union
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class TransactionNormalizer:
    """
    A class to normalize transaction data from different sources (bank/ledger) into a unified format.
    """
    
    def __init__(self):
        # Define the expected output schema
        self.output_columns = [
            'id', 'date', 'amount', 'merchant', 'merchant_clean', 
            'reference', 'currency', 'source'
        ]
        
        # Define column mappings for different sources
        self.column_mappings = {
            'bank': {
                'transactionID': 'id',
                'date_of_txn': 'date',
                'txn_amount': 'amount',
                'merchant': 'merchant',
                'referenceID': 'reference',
                'currency_code': 'currency'
            },
            'ledger': {
                'txn_id': 'id',
                'trans_date': 'date',
                'amount': 'amount',
                'merchant_name': 'merchant',
                'ref_no': 'reference',
                'curr': 'currency'
            }
        }
    
    def load_data(self, file_path: str, source: str) -> pd.DataFrame:
        """
        Load transaction data from CSV file.
        
        Args:
            file_path: Path to the CSV file
            source: Source of the data ('bank' or 'ledger')
            
        Returns:
            pd.DataFrame: Loaded and standardized DataFrame
        """
        try:
            # Load CSV with flexible parsing
            df = pd.read_csv(file_path, dtype=str, encoding='utf-8')
            logger.info(f"Successfully loaded {len(df)} records from {file_path}")
            return df
        except Exception as e:
            logger.error(f"Error loading {file_path}: {str(e)}")
            raise
    
    def clean_merchant_name(self, merchant: str) -> str:
        """
        Clean and standardize merchant names.
        
        Args:
            merchant: Raw merchant name
            
        Returns:
            str: Cleaned merchant name
        """
        if pd.isna(merchant):
            return ""
            
        # Convert to string and lowercase
        merchant = str(merchant).lower().strip()
        
        # Remove special characters and extra spaces
        merchant = re.sub(r'[^\w\s]', ' ', merchant)
        merchant = re.sub(r'\s+', ' ', merchant).strip()
        
        # Remove common business suffixes
        suffixes = ['pvt ltd', 'ltd', 'llp', 'inc', 'llc', 'pvt', 'limited']
        for suffix in suffixes:
            merchant = re.sub(fr'\s*{re.escape(suffix)}\s*$', '', merchant)
        
        return merchant
    
    def clean_date(self, date_str: str) -> str:
        """
        Parse and standardize date strings to YYYY-MM-DD format.
        
        Args:
            date_str: Date string in various formats
            
        Returns:
            str: Date in YYYY-MM-DD format or empty string if invalid
        """
        if pd.isna(date_str):
            return ""
            
        date_str = str(date_str).strip()
        date_formats = [
            '%Y-%m-%d', '%d-%m-%Y', '%d/%m/%Y', 
            '%Y/%m/%d', '%d.%m.%Y', '%Y%m%d'
        ]
        
        for fmt in date_formats:
            try:
                date_obj = datetime.strptime(date_str, fmt)
                return date_obj.strftime('%Y-%m-%d')
            except ValueError:
                continue
                
        logger.warning(f"Could not parse date: {date_str}")
        return ""
    
    def clean_amount(self, amount: str) -> float:
        """
        Clean and convert amount to float.
        
        Args:
            amount: Amount as string (may contain currency symbols, commas, etc.)
            
        Returns:
            float: Cleaned amount or 0.0 if invalid
        """
        if pd.isna(amount):
            return 0.0
            
        try:
            # Remove currency symbols, commas, and extra spaces
            cleaned = re.sub(r'[^\d.-]', '', str(amount))
            return float(cleaned) if cleaned else 0.0
        except (ValueError, TypeError):
            logger.warning(f"Could not parse amount: {amount}")
            return 0.0
    
    def normalize_dataframe(self, df: pd.DataFrame, source: str) -> pd.DataFrame:
        """
        Normalize the input DataFrame according to the specified source.
        
        Args:
            df: Input DataFrame
            source: Source of the data ('bank' or 'ledger')
            
        Returns:
            pd.DataFrame: Normalized DataFrame
        """
        if source not in ['bank', 'ledger']:
            raise ValueError("Source must be either 'bank' or 'ledger'")
        
        # Make a copy to avoid modifying the original
        df = df.copy()
        
        # Standardize column names
        column_mapping = self.column_mappings[source]
        df = df.rename(columns={k: v for k, v in column_mapping.items() 
                              if k in df.columns})
        
        # Initialize missing columns with empty strings
        for col in self.output_columns:
            if col not in df.columns:
                df[col] = ""
        
        # Clean and standardize each field
        if 'date' in df.columns:
            df['date'] = df['date'].apply(self.clean_date)
        
        if 'amount' in df.columns:
            df['amount'] = df['amount'].apply(self.clean_amount)
        
        if 'merchant' in df.columns:
            df['merchant_clean'] = df['merchant'].apply(self.clean_merchant_name)
            df['merchant'] = df['merchant'].str.strip()
        
        # Ensure all required columns exist
        for col in self.output_columns:
            if col not in df.columns:
                df[col] = ""
        
        # Add source information
        df['source'] = source
        
        # Select and order columns according to output schema
        return df[self.output_columns]
    
    def normalize_batch(self, file_path: str, source: str) -> List[Dict[str, Union[str, float]]]:
        """
        Main function to normalize a batch of transactions.
        
        Args:
            file_path: Path to the input CSV file
            source: Source of the data ('bank' or 'ledger')
            
        Returns:
            List[Dict]: List of normalized transaction records
        """
        try:
            # Load the data
            df = self.load_data(file_path, source)
            
            # Normalize the data
            normalized_df = self.normalize_dataframe(df, source)
            
            # Convert to list of dictionaries
            records = normalized_df.to_dict('records')
            
            logger.info(f"Successfully normalized {len(records)} records from {source}")
            return records
            
        except Exception as e:
            logger.error(f"Error normalizing {source} data: {str(e)}")
            raise


def main():
    """Example usage"""
    normalizer = TransactionNormalizer()
    
    # Example usage (uncomment and modify paths as needed)
    """
    # Normalize bank data
    bank_records = normalizer.normalize_batch(
        'path/to/bank_transactions.csv', 
        'bank'
    )
    
    # Normalize ledger data
    ledger_records = normalizer.normalize_batch(
        'path/to/ledger_entries.csv', 
        'ledger'
    )
    
    # Combine and save to a single file
    all_records = bank_records + ledger_records
    pd.DataFrame(all_records).to_csv('normalized_transactions.csv', index=False)
    """
    print("Data normalizer module loaded. Import and use the TransactionNormalizer class.")


if __name__ == "__main__":
    main()
