# **ETL Unit Test Framework**

A Python-based unit testing framework designed for validating ETL pipelines, specifically for PySpark workflows. This framework automates the testing of **Extraction**, **Transformation**, and **Load** modules with support for complex SQL queries, dynamic datasets, and temporary views.

---

## **Features**
- Validates ETL processes end-to-end (Extraction, Transformation, and Load).
- Supports dynamic datasets and namespace handling.
- Handles CSV data input with delimited sections for multiple tables.
- Logs key actions and errors for better traceability.
- Configurable for various ETL scenarios using JSON-based configuration files.

---

## **Pre-Requisites**
1. **Python**: Ensure Python 3.8 or later is installed.
2. **PySpark**: Install PySpark for distributed data processing.
3. **Git**: Required for identifying file changes in the `mapper.py` functionality.
4. **Dependencies**: Install required libraries using:
   ```bash
   pip install -r requirements.txt
   ```

---

## **Directory Structure**
```
project-root/
│
├── src/
│   ├── transform_data.py     # Transformation logic
│   └── ...
│
├── test/
│   ├── test_cases.py         # Unit test cases for the ETL framework
│   ├── run_test.py           # Script to inject Spark and config for tests
│   ├── commons/
│       ├── mapper.py         # Identify affected tests
│       ├── run_affected_tests.py # Integrated mapper and test runner
│       └── Test_Cases_Mapping.json
│
├── config/
│   ├── queries_config.py     # ETL queries for Extraction, Transformation, Load
│
├── test-data/
│   ├── sampledata.csv        # Input CSV containing multiple tables
│
├── requirements.txt          # Dependencies
└── README.md                 # Framework documentation
```

---

## **How It Works**

### **1. Extraction**
The framework:
- Processes input CSV files containing data for multiple tables, delimited by `[table_name]`.
- Creates temporary views in PySpark for each table.

### **2. Transformation**
- Executes complex transformation queries using SQL.
- Registers intermediate and final results as PySpark views for further validation.

### **3. Load**
- Ensures final dataframes meet expected conditions before persisting them.

---

## **Configuration**

### **Sample Config (queries_config.py)**
```python
queries = {
    "extract": [
        {
            "name": "customer_data",
            "query": "SELECT * FROM customer_table"
        }
    ],
    "transform": [
        {
            "name": "final_table",
            "query": '''
                SELECT 
                    c.customer_id, 
                    o.order_id, 
                    o.order_amount 
                FROM customer_data c
                JOIN orders o ON c.customer_id = o.customer_id
            '''
        }
    ],
    "load": [
        {
            "table_name": "final_table",
            "dataframe_name": "final_table",
            "load_type": "overwrite"
        }
    ]
}
```

### **Input CSV File**
`test-data/sampledata.csv`:
```
[customer_data]
customer_id,customer_name,region
1,Alice,North
2,Bob,East

[orders]
order_id,customer_id,order_amount
101,1,250.50
102,2,100.00
```

---

## **Usage**

### **Step 1: Prepare Configuration**
- Define your **extract**, **transform**, and **load** queries in `config/queries_config.py`.

### **Step 2: Prepare Input Data**
- Create a single CSV file with delimited table sections for all required datasets.

### **Step 3: Run Tests**
1. Run all test cases:
   ```bash
   python test/run_test.py
   ```
   This script:
   - Loads the input data.
   - Executes extraction and transformation queries.
   - Validates the final output.

2. Run only affected tests (after code/config changes):
   ```bash
   python test/commons/run_affected_tests.py --mapping-file test/commons/Test_Cases_Mapping.json
   ```

---

## **Logs**
- All test actions and errors are logged for traceability. Check the logs for:
  - Successfully created temporary views.
  - Errors in SQL syntax or execution.

---

## **Contributing**
1. Fork the repository.
2. Create a feature branch:
   ```bash
   git checkout -b feature/your-feature-name
   ```
3. Commit your changes and push:
   ```bash
   git commit -m "Add your message"
   git push origin feature/your-feature-name
   ```
4. Create a pull request.

---

## **License**
This project is licensed under the MIT License.

---

Feel free to update the documentation as your project evolves!
