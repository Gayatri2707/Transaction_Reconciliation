# normalizer.py

import re
from datetime import datetime
from typing import Dict, Any


class TransactionNormalizer:
    def __init__(self, source: str):
        """
        source: 'bank', 'ledger', 'gateway', etc.
        """
        self.source = source.lower()

    def normalize(self, txn: Dict[str, Any]) -> Dict[str, Any]:
        """
        Convert raw transaction dict into unified format
        """
        return {
            "id": self._get_id(txn),
            "source": self.source,
            "amount": self._normalize_amount(txn),
            "date": self._normalize_date(txn),
            "merchant": self._normalize_text(self._get_description(txn)),
            "raw_text": self._get_description(txn),
            "meta": txn  # keep full raw record for traceability
        }

    # ---------- Field Extractors ----------

    def _get_id(self, txn: Dict[str, Any]) -> str:
        return str(
            txn.get("id")
            or txn.get("txn_id")
            or txn.get("transaction_id")
            or ""
        )

    def _get_description(self, txn: Dict[str, Any]) -> str:
        return str(
            txn.get("desc")
            or txn.get("description")
            or txn.get("narration")
            or txn.get("merchant")
            or ""
        )

    # ---------- Normalizers ----------

    def _normalize_amount(self, txn: Dict[str, Any]) -> float:
        amt = txn.get("amount") or txn.get("amt") or 0
        if isinstance(amt, str):
            amt = amt.replace(",", "").strip()
        try:
            return float(amt)
        except:
            return 0.0

    def _normalize_date(self, txn: Dict[str, Any]) -> str:
        raw = txn.get("date") or txn.get("timestamp") or txn.get("time")
        if not raw:
            return ""

        for fmt in ("%Y-%m-%d", "%d-%m-%Y", "%Y/%m/%d", "%d/%m/%Y", "%Y-%m-%d %H:%M:%S"):
            try:
                return datetime.strptime(raw[:19], fmt).date().isoformat()
            except:
                continue
        return ""

    def _normalize_text(self, text: str) -> str:
        text = text.lower()
        text = re.sub(r"[^a-z0-9\s]", " ", text)  # remove symbols
        text = re.sub(r"\s+", " ", text)         # collapse spaces
        return text.strip()


# ---------- Helper for batch ----------

def normalize_batch(txns: list, source: str) -> list:
    normalizer = TransactionNormalizer(source)
    return [normalizer.normalize(t) for t in txns]
