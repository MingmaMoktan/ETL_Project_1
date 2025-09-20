# 🏦 ETL Pipeline: World's Largest Banks

This project demonstrates a complete **ETL (Extract, Transform, Load)** pipeline in Python.  
It acquires, processes, and stores information about the **world’s largest banks**.  

The project was built as part of a hands-on lab and includes logging, CSV storage, and a database backend.

---

## 📂 Project Structure

├── banks_project.py # Main ETL script
├── data/ # Input and processed data files
│ └── exchange_rate.csv
├── Banks.db # SQLite database (generated after running script)
├── banks.csv # Final transformed CSV (generated after running script)
├── code_log.txt # Log file (auto-generated)
├── docs/ # Auto-generated documentation (via pdoc)
├── .gitignore # Ignored files (db, logs, venv, etc.)
└── README.md # Project description


---

## ⚙️ Features

- **Task 1**: Logging function (`log_progress`)  
- **Task 2**: Data extraction (`extract`)  
- **Task 3**: Data transformation (`transform`)  
- **Task 4**: Load data into CSV (`load_to_csv`)  
- **Task 5**: Load data into SQLite database (`load_to_db`)  
- **Task 6**: Run SQL queries (`run_query`)  
- **Task 7**: Verify log entries  

---

## 🚀 How to Run

1. Clone the repo:
   ```bash
   git clone https://github.com/MingmaMoktan/ETL_Project_1.git
   cd <your repo>

2. Create a virtual environment and install dependencies:
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt

3. Run the ETL script:
python banks_project.py

4. Generated files:
-banks.csv → Transformed CSV file
-Banks.db → SQLite database
-code_log.txt → Log file

📊 Example Queries

The script supports running SQL queries like:

SELECT * FROM Largest_banks;
SELECT AVG(MC_GBP_Billion) FROM Largest_banks;
SELECT Name FROM Largest_banks LIMIT 5;


📖 Documentation
Auto-generated documentation is available using pdoc.
Generate docs with:
pdoc banks_project.py -o docs

If hosted on GitHub Pages, view it here:

# To Do / Improvements
- Make pipeline more modular with separate extract.py, transform.py, load.py
- Add unit tests for each function
- Automate ETL scheduling (e.g., with Airflow or Cron)
- Containerize with Docker


👨‍💻 Author
Mingma Moktan
GitHub: @MingmaMoktan
