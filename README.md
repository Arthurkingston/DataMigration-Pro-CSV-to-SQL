# DataMigration Pro: Automated CSV to SQL Schema Engine

**DataMigration Pro** is a robust Python-based desktop application designed to streamline the process of converting flat CSV files into structured, relational SQL environments. It handles data cleaning, automatic encoding detection, and schema generation with a professional GUI.

## 🚀 Key Features
- **Auto-Encoding Detection:** Automatically attempts multiple encodings (UTF-8, Latin1, etc.) to prevent data corruption.
- **Dynamic Schema Mapping:** Uses keyword analysis to automatically group data into logical tables (Clients, Inventory, Transactions).
- **Relational Integrity:** Automatically identifies and maintains "link" columns (IDs/SKUs) across generated tables.
- **Dual Export Options:** Export directly to a **SQLite Database (.db)** or generate a **Standard SQL Script (.sql)** ready for MySQL Workbench.
- **System Logging:** Records all migration events and errors in a local log file for auditing and debugging.

## 🛠️ Tech Stack
- **Language:** Python 3.x
- **UI Framework:** CustomTkinter (Modern Dark Mode Interface)
- **Data Handling:** Pandas
- **Database Engine:** SQLAlchemy

## 📥 Installation & Setup
1. **Clone the repository:**
   ```bash
   git clone https://github.com/Arthurkingston/DataMigration-Pro-CSV-to-SQL.git
   ```
   **Developed by:** Parth Kalaskar (Arthurkingston)
