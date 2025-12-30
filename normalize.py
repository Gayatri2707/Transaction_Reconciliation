import pandas as pd
import re
from datetime import datetime
from typing import List, Dict, Optional
import os

def clean_amount(amount_str: str) -> float:
    """
    Clean amount strings: remove ₹, commas, spaces, quotes.
    Convert to float. Handle missing/invalid values.
    """
    if pd.isna(amount_str) or amount_str == '' or amount_str is None:
        return 0.0
    
    # Convert to string and strip
    amount_str = str(amount_str).strip()
    
    # Remove currency symbols, commas, quotes, spaces
    amount_str = re.sub(r'[₹$€£, " \']', '', amount_str)
    
    try:
        return float(amount_str)
    except (ValueError, TypeError):
        return 0.0


def clean_merchant_name(merchant: str) -> str:
    """
    Clean merchant names for NLP/text similarity:
    - Lowercase
    - Remove special characters (keep spaces)
    - Remove extra spaces
    - Optional: remove common stopwords
    """
    if pd.isna(merchant) or merchant == '' or merchant is None:
        return ''
    
    merchant = str(merchant).strip()
    
    # Lowercase
    merchant = merchant.lower()
    
    # Replace common separators with spaces (underscores, dashes, dots, slashes)
    merchant = re.sub(r'[_\-./]', ' ', merchant)
    
    # Remove special characters but keep alphanumeric and spaces
    merchant = re.sub(r'[^a-z0-9\s]', '', merchant)
    
    # Remove extra whitespace
    merchant = re.sub(r'\s+', ' ', merchant).strip()
    
    # Optional: Remove common stopwords
    stopwords = ['pvt', 'ltd', 'limited', 'india', 'inc', 'corp', 'corporation', 
                 'online', 'pos', 'trans', 'transaction']
    words = merchant.split()
    words = [w for w in words if w not in stopwords]
    merchant = ' '.join(words).strip()
    
    return merchant


def parse_date(date_str: str) -> Optional[str]:
    """
    Parse dates in multiple formats into standard YYYY-MM-DD format.
    Handle invalid or missing dates.
    """
    if pd.isna(date_str) or date_str == '' or date_str is None:
        return None
    
    date_str = str(date_str).strip()
    
    # List of date formats to try
    date_formats = [
        '%d-%m-%Y',           # 06-10-2025 (ledger format)
        '%Y.%m.%d',           # 2025.11.22 (bank format)
        '%d-%b-%Y',           # 01-Nov-2025
        '%d/%m/%Y',           # 31/10/2025
        '%m/%d/%y',           # 10/15/25
        '%d/%m/%y',           # 12/11/25
        '%Y-%m-%d',           # 2025-10-06
    ]
    
    for fmt in date_formats:
        try:
            dt = datetime.strptime(date_str, fmt)
            # Convert 2-digit years to 4-digit (assuming 20xx)
            if dt.year < 100:
                dt = dt.replace(year=2000 + dt.year)
            return dt.strftime('%Y-%m-%d')
        except ValueError:
            continue
    
    # If no format matched, return None
    return None


def normalize_batch(path: str, source: str) -> List[Dict]:
    """
    Main normalization function.
    Load CSV, clean & standardize, return list of dicts.
    
    Args:
        path: Path to CSV file
        source: "bank" or "ledger"
    
    Returns:
        List of dictionaries with normalized transaction data
    """
    # 1. Load CSV with error handling
    try:
        df = pd.read_csv(path, encoding='utf-8')
    except UnicodeDecodeError:
        # Try different encodings
        try:
            df = pd.read_csv(path, encoding='latin-1')
        except:
            df = pd.read_csv(path, encoding='iso-8859-1')
    
    if df.empty:
        return []
    
    # 2. Standardize columns based on source
    if source.lower() == 'ledger':
        # Ledger columns: txn_id, entry_date, amt, vendor, ref_code, cur
        column_mapping = {
            'txn_id': 'id',
            'entry_date': 'date',
            'amt': 'amount',
            'vendor': 'merchant',
            'ref_code': 'reference',
            'cur': 'currency'
        }
    elif source.lower() == 'bank':
        # Bank columns: stmt_ref, booking_date, tx_val, description, ext_memo, unit
        column_mapping = {
            'stmt_ref': 'id',
            'booking_date': 'date',
            'tx_val': 'amount',
            'description': 'merchant',
            'ext_memo': 'reference',
            'unit': 'currency'
        }
    else:
        raise ValueError(f"Source must be 'bank' or 'ledger', got: {source}")
    
    # Rename columns (only if they exist)
    df = df.rename(columns={k: v for k, v in column_mapping.items() if k in df.columns})
    
    # 3. Clean dates
    if 'date' in df.columns:
        df['date'] = df['date'].apply(parse_date)
    
    # 4. Clean amounts
    if 'amount' in df.columns:
        df['amount'] = df['amount'].apply(clean_amount)
    
    # 5. Clean merchant names (create merchant_clean)
    if 'merchant' in df.columns:
        df['merchant_clean'] = df['merchant'].apply(clean_merchant_name)
        # Keep original merchant name as well
    else:
        df['merchant'] = ''
        df['merchant_clean'] = ''
    
    # 6. Handle missing reference and currency
    if 'reference' not in df.columns:
        df['reference'] = ''
    else:
        df['reference'] = df['reference'].fillna('').astype(str)
    
    if 'currency' not in df.columns:
        df['currency'] = 'INR'
    else:
        df['currency'] = df['currency'].fillna('INR').astype(str).str.upper()
    
    # 7. Add source field
    df['source'] = source.lower()
    
    # 8. Ensure all required columns exist
    required_columns = ['id', 'date', 'amount', 'merchant', 'merchant_clean', 
                       'reference', 'currency', 'source']
    
    for col in required_columns:
        if col not in df.columns:
            df[col] = '' if col != 'amount' else 0.0
    
    # 9. Convert to list of dictionaries with unified schema
    result = df[required_columns].to_dict('records')
    
    return result


# Example usage and testing
if __name__ == "__main__":
    # Test with both files
    ledger_path = "ledger_5000.csv"
    bank_path = "bank_5000.csv"
    
    print("Normalizing ledger data...")
    ledger_normalized = normalize_batch(ledger_path, "ledger")
    print(f"✅ Normalized {len(ledger_normalized)} ledger transactions")
    print("\nSample ledger transaction:")
    print(ledger_normalized[0] if ledger_normalized else "No data")
    
    print("\n" + "="*50)
    print("Normalizing bank data...")
    bank_normalized = normalize_batch(bank_path, "bank")
    print(f"✅ Normalized {len(bank_normalized)} bank transactions")
    print("\nSample bank transaction:")
    print(bank_normalized[0] if bank_normalized else "No data")
    
    # Optional: Save normalized data
    if ledger_normalized:
        df_ledger = pd.DataFrame(ledger_normalized)
        df_ledger.to_csv("ledger_normalized.csv", index=False)
        print("\n✅ Saved normalized ledger to ledger_normalized.csv")
    
    if bank_normalized:
        df_bank = pd.DataFrame(bank_normalized)
        df_bank.to_csv("bank_normalized.csv", index=False)
        print("✅ Saved normalized bank to bank_normalized.csv")