import pandas as pd
import numpy as np
import random
from datetime import datetime

# ==========================================
# STEP 1: CONFIGURATION
# ==========================================
LEDGER_FILE_PATH = r"C:\Users\SIS\Downloads\ledger_5000.csv" 
BANK_FILE_PATH   = r"C:\Users\SIS\Downloads\bank_5000.csv"

# ==========================================
# STEP 2: DEFINE THE CHAOS
# ==========================================
def make_date_messy(val):
    try:
        d = pd.to_datetime(val)
        formats = ["%m/%d/%y", "%d-%b-%Y", "%d/%m/%Y", "%Y.%m.%d"]
        return d.strftime(random.choice(formats))
    except:
        return str(val)

def make_amount_messy(val):
    try:
        # Convert number to "₹ 1,499.00" string
        return f"₹ {float(val):,.2f}"
    except:
        return str(val)

def make_merchant_messy(val):
    junk = ["-INC", " *0912", "_LTD", " POS-TRANS", "/ONLINE", " CORP"]
    return str(val).upper() + random.choice(junk)

# ==========================================
# STEP 3: EXECUTE IN-PLACE MODIFICATION
# ==========================================
try:
    print("Reading files...")
    l_df = pd.read_csv(LEDGER_FILE_PATH)
    b_df = pd.read_csv(BANK_FILE_PATH)

    # RENAME HEADERS
    l_df.columns = ['txn_id', 'entry_date', 'amt', 'vendor', 'ref_code', 'cur']
    b_df.columns = ['stmt_ref', 'booking_date', 'tx_val', 'description', 'ext_memo', 'unit']

    # FIX DTYPE WARNINGS: Convert columns to 'object' (string) so they accept messy data
    l_df['entry_date'] = l_df['entry_date'].astype(object)
    b_df['booking_date'] = b_df['booking_date'].astype(object)
    b_df['tx_val'] = b_df['tx_val'].astype(object)
    b_df['description'] = b_df['description'].astype(object)
    b_df['ext_memo'] = b_df['ext_memo'].astype(object)

    print("Injecting noise into 70% matching data...")
    # MODIFY MATCHING ROWS (Top 3500 rows)
    for i in range(min(3500, len(l_df))):
        # Ledger Date: DD-MM-YYYY
        l_df.at[i, 'entry_date'] = pd.to_datetime(l_df.at[i, 'entry_date']).strftime('%d-%m-%Y')
        
        # Bank Date: Chaos formats
        b_df.at[i, 'booking_date'] = make_date_messy(b_df.at[i, 'booking_date'])
        
        # Bank Amount: String with symbols
        b_df.at[i, 'tx_val'] = make_amount_messy(b_df.at[i, 'tx_val'])
        
        # Bank Merchant: Uppercase + Junk
        b_df.at[i, 'description'] = make_merchant_messy(b_df.at[i, 'description'])
        
        # Randomly delete references in Bank (10% of matches)
        if random.random() < 0.10:
            b_df.at[i, 'ext_memo'] = ""

    print("Creating 30% outliers (Non-matches)...")
    # BREAK REMAINING 30% (3500 to end)
    for i in range(3500, len(l_df)):
        b_df.at[i, 'tx_val'] = f"₹ {random.randint(90000, 150000):,.2f}"
        b_df.at[i, 'ext_memo'] = f"BANK-ONLY-REF-{random.randint(100,999)}"
        
        l_df.at[i, 'amt'] = random.randint(1, 20)
        l_df.at[i, 'ref_code'] = f"LEDGER-MISSING-{random.randint(100,999)}"

    # SAVE BACK TO ORIGINAL FILES
    print("Saving files... (MAKE SURE EXCEL IS CLOSED!)")
    l_df.to_csv(LEDGER_FILE_PATH, index=False)
    b_df.to_csv(BANK_FILE_PATH, index=False)

    print("-" * 30)
    print("SUCCESS! Files modified in-place.")
    print(f"Path 1: {LEDGER_FILE_PATH}")
    print(f"Path 2: {BANK_FILE_PATH}")
    print("-" * 30)

except PermissionError:
    print("\nERROR: Permission Denied!")
    print("Please CLOSE the CSV files in Excel and try running the script again.")
except Exception as e:
    print(f"ERROR: {e}")