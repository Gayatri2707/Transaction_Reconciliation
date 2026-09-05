# 🔄 Transaction Reconciliation System

A data-driven **Transaction Reconciliation System** that automatically compares **bank transactions with ledger records**, identifies mismatches and anomalies, and provides a clear reconciliation summary through data processing and visualization.

The project is designed to reduce manual reconciliation effort and improve the accuracy and efficiency of financial transaction verification.

---

## 📌 Problem Statement

Financial systems often contain discrepancies between **bank statements** and **internal ledger records** due to:

* Missing transactions
* Duplicate transactions
* Incorrect transaction amounts
* Different transaction descriptions
* Formatting inconsistencies
* Data-entry errors
* Delayed or incomplete records
* Noise and variations in transaction data

Manually identifying these discrepancies becomes difficult when transaction volumes increase.

### 💡 Our Solution

This project automates the reconciliation process by:

**Bank Data + Ledger Data → Data Normalization → Transaction Matching → Mismatch Detection → Analysis → Dashboard**

---

## 🚀 Key Features

### 🔹 1. Data Normalization

Different transaction sources can contain inconsistent formats.

The project normalizes transaction data so that bank and ledger records can be compared reliably.

Normalization includes handling:

* Transaction IDs
* Dates
* Amounts
* Descriptions
* Missing values
* Formatting differences

---

### 🔹 2. Transaction Reconciliation

The system compares bank transactions with ledger transactions and identifies:

* ✅ Matched transactions
* ❌ Missing bank transactions
* ❌ Missing ledger transactions
* ⚠️ Amount mismatches
* ⚠️ Duplicate records
* ⚠️ Suspicious/anomalous records

---

### 🔹 3. Noise & Data Variation Simulation

The project includes scripts for introducing controlled variations into transaction data.

This helps simulate realistic financial data problems such as:

* Modified transaction amounts
* Changed descriptions
* Missing records
* Formatting variations
* Other inconsistencies

This allows the reconciliation logic to be tested against realistic scenarios.

---

### 🔹 4. Data Processing Pipeline

The project follows a structured processing workflow:

```text
Raw Bank Data
      ↓
Data Cleaning
      ↓
Bank Normalization
      ↓
Raw Ledger Data
      ↓
Ledger Normalization
      ↓
Transaction Matching
      ↓
Discrepancy Detection
      ↓
Analysis
      ↓
Dashboard / Report
```

---

### 🔹 5. Data Analysis & ML Exploration

The project also contains a Jupyter Notebook for exploring transaction data and experimenting with machine-learning-based analysis.

This provides a foundation for extending the system toward:

* Anomaly detection
* Suspicious transaction identification
* Pattern recognition
* Automated discrepancy classification

---

## 🏗️ Project Architecture

```text
                 ┌─────────────────────┐
                 │   Bank Transactions │
                 │        CSV          │
                 └──────────┬──────────┘
                            │
                            ▼
                  ┌──────────────────┐
                  │ Data Normalizer  │
                  └────────┬─────────┘
                           │
                           ▼
                  ┌──────────────────┐
                  │ Normalized Bank  │
                  │      Data        │
                  └────────┬─────────┘
                           │
                           │
                           ▼
                  ┌──────────────────┐
                  │ Reconciliation   │
                  │     Engine       │
                  └────────┬─────────┘
                           ▲
                           │
                  ┌────────┴─────────┐
                  │ Normalized Ledger│
                  │      Data        │
                  └────────▲─────────┘
                           │
                  ┌────────┴─────────┐
                  │ Data Normalizer  │
                  └────────▲─────────┘
                           │
                 ┌─────────┴──────────┐
                 │ Ledger Transactions│
                 │        CSV         │
                 └────────────────────┘

                           │
                           ▼
                ┌─────────────────────┐
                │ Discrepancy Analysis│
                └──────────┬──────────┘
                           │
                           ▼
                ┌─────────────────────┐
                │ Dashboard / Reports │
                └─────────────────────┘
```

---

## 📂 Project Structure

```text
Transaction_Reconciliation/
│
├── 📊 bank_5000.csv
├── 📊 bank_5000 (1).csv
├── 📊 bank_normalized.csv
│
├── 📊 ledger_5000.csv
├── 📊 ledger_5000 (1).csv
├── 📊 ledger_normalized.csv
│
├── 🐍 data_normalizer.py
├── 🐍 normalize.py
├── 🐍 normalizer.py
├── 🐍 noise.py
├── 🐍 test.py
│
├── 📓 firstml.ipynb
│
├── 🌐 dashboard.html
│
└── 📄 README.md
```

---

## 🛠️ Technology Stack

| Technology              | Purpose                              |
| ----------------------- | ------------------------------------ |
| **Python**              | Data processing and reconciliation   |
| **Pandas**              | Data manipulation and analysis       |
| **NumPy**               | Numerical operations                 |
| **Scikit-learn**        | Machine learning experimentation     |
| **Jupyter Notebook**    | Data analysis and ML experimentation |
| **HTML/CSS/JavaScript** | Dashboard / visualization            |
| **CSV**                 | Transaction data storage             |

---

## ⚙️ How It Works

### Step 1 — Input Data

The system takes two transaction sources:

```text
Bank Transactions
        +
Ledger Transactions
```

---

### Step 2 — Normalize Data

The input datasets are cleaned and converted into a common format.

```text
Raw Data
   ↓
Cleaning
   ↓
Formatting
   ↓
Standardized Data
```

---

### Step 3 — Match Transactions

Transactions are compared using relevant fields such as:

* Transaction ID
* Amount
* Date
* Description
* Other transaction attributes

---

### Step 4 — Detect Discrepancies

The system identifies transactions that cannot be reliably matched.

Examples:

```text
Bank Transaction       Ledger Transaction
------------------------------------------------
TXN001   ₹500          TXN001   ₹500       ✅ Match

TXN002   ₹800          TXN002   ₹850       ⚠️ Amount mismatch

TXN003   ₹1200         —                  ❌ Missing ledger record
```

---

### Step 5 — Analyze Results

The reconciliation output can then be analyzed to understand:

* Number of matched transactions
* Number of unmatched transactions
* Types of discrepancies
* Transaction patterns
* Potential anomalies

---

### Step 6 — Visualization

The processed results can be displayed through the project dashboard for easier interpretation.

---

## 📊 Dataset

The repository contains transaction datasets with approximately **5,000 records** used for development and testing.

The datasets represent two sides of a financial transaction system:

```text
Bank Dataset
     ↕
Reconciliation
     ↕
Ledger Dataset
```

The project also includes normalized versions of the datasets.

---

## 🧪 Testing

The project includes testing scripts to verify the behavior of the data processing and reconciliation pipeline.

Testing focuses on scenarios such as:

* Matching transactions
* Mismatched amounts
* Missing records
* Duplicate records
* Data inconsistencies
* Normalization correctness

---

## 🔮 Future Enhancements

The current system can be extended into a more complete financial reconciliation platform.

### Planned improvements

* 🤖 Advanced ML-based anomaly detection
* 🔍 Fuzzy transaction matching
* 📈 Interactive analytics dashboard
* ⚡ Real-time transaction reconciliation
* 🗄️ Database integration
* 🔐 User authentication and authorization
* 📧 Automated discrepancy alerts
* 📄 Automated reconciliation reports
* ☁️ Cloud deployment
* 🧾 Audit trail for reconciliation decisions
* 🔄 Scheduled reconciliation jobs

---

## 🎯 Use Cases

This system can be useful for:

* 🏦 Banks
* 💳 FinTech companies
* 💰 Payment processing systems
* 🧾 Accounting departments
* 🏢 Enterprise finance teams
* 📊 Financial data analysis
* 🔍 Transaction monitoring

---

## 💡 Why This Project?

Transaction reconciliation is an important financial data-quality process. Automating it can help organizations:

* Reduce manual effort
* Detect discrepancies faster
* Improve financial accuracy
* Reduce human errors
* Handle large transaction volumes
* Generate actionable insights

---

## 👩‍💻 Author

**Gayatri Aiwale**

Computer Engineering Student
VIT Pune

GitHub: [Gayatri2707](https://github.com/Gayatri2707)

---

## ⭐ Project Status

🚧 **Currently under development**

The current version focuses on transaction data normalization, reconciliation, discrepancy analysis, and visualization. Future versions can introduce more advanced matching algorithms and machine-learning-based anomaly detection.

---

## 📄 License

This project is intended for **educational and research purposes**.
