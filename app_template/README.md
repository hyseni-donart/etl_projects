# 🧩 ETL Exercise: NYC For-Hire Vehicles (FHV Active)

## 🎯 Objective
Your task is to build a small **ETL pipeline** that processes the **NYC For-Hire Vehicles (FHV Active)** dataset using **Pandas** and loads the cleaned data into a **PostgreSQL** table.

You will complete the first three steps of the ETL process:
1. **Extract** – Read the dataset from a CSV file into Pandas.  
2. **Transform** – Clean, standardize, and reduce the dataset.  
3. **Load** – Create a PostgreSQL table and insert the cleaned data.

---

## 📦 Dataset
**Source:** [NYC Open Data – For-Hire Vehicles (FHV Active)](https://data.cityofnewyork.us/Transportation/For-Hire-Vehicles-FHV-Active/8wbx-tsch/about_data)

This dataset lists active vehicles licensed by the NYC Taxi and Limousine Commission (TLC).

You can download it as a CSV file from the “Export” → “CSV” option on the page.

---

## 🧾 Step 1 — Extract
1. Download the dataset as a **CSV** file.
2. Load it into a **Pandas DataFrame**.
3. Display the shape, column names, and first few rows.

**Deliverables:**
- The CSV is read into Pandas successfully.
- Print output includes the number of rows/columns and column names.

---

## 🧹 Step 2 — Transform
Clean and prepare the dataset for loading into PostgreSQL.

### Required cleaning steps:
- **Standardize column names** → all lowercase, spaces replaced with underscores.  
- **Convert `expiration_date`** to a proper datetime type.  
- **Trim whitespace** from text fields.  
- **Remove duplicates** based on `license_number` and `dmv_license_plate`.  
- **Drop unnecessary columns** (you can choose which ones, but keep at least these):
  - `license_number`
  - `license_type`
  - `dmv_license_plate`
  - `vehicle_vin_number`
  - `expiration_date`
  - `wheelchair_accessible`
  - `active`
- **Drop rows** where any of the key identifiers (`license_number` or `dmv_license_plate`) are missing.

### Enhancements (bonus):
- Add a new column `days_until_expiration` = `expiration_date - current_date`.

**Deliverables:**
- Transformed Pandas DataFrame ready for loading.
---

## 🗄️ Step 3 — Load
1. Create a **PostgreSQL** table named `fhv_active_cleaned`.
2. Define appropriate data types for each column.
3. Insert the cleaned data from your Pandas DataFrame into PostgreSQL.

You can use:
- `sqlalchemy` with `pandas.DataFrame.to_sql()`, or  

**Deliverables:**
- A PostgreSQL table populated with the cleaned dataset.
- Run and show a sample SQL query, e.g.:

