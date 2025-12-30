from normalizer import normalize_batch

bank_txns = [
    {"id": "B102", "amt": "4,250.00", "desc": "SWIGGY*FOOD", "date": "24-10-2025"}
]

ledger_txns = [
    {"txn_id": "L556", "amount": 4250, "narration": "Swiggy Order", "timestamp": "2025/10/24"}
]

bank_norm = normalize_batch(bank_txns, "bank")
ledger_norm = normalize_batch(ledger_txns, "ledger")

print(bank_norm)
print(ledger_norm)
