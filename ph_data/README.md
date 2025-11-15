# ETL Project: Product Hierarchy Cleanup (pandas + DuckDB)

Clean and model a single CSV into an analytics-ready table using **pandas** or **DuckDB** behind **one** entry point. You’ll practice robust text parsing, whitespace/control‑character cleanup, type normalization, and reproducible pipelines — aligned to the **actual** source headers and sample data you provided.

---

## 🎯 Objectives (updated)
1. **Single ETL script** that can run with **either** pandas **or** DuckDB based on an **environment variable**. If the variable is **unset or invalid**, the script **defaults to pandas**.
2. **Split** `product (brand)` into two columns: `product` and `brand`.
3. **Normalize whitespace & remove control characters** in relevant text columns, then **capitalize each word** in `product`, `brand`, `category`, `subcategory`, and `type`.
4. **Split** `category || sub_category` into two columns: `category` and `subcategory` (trim around the delimiter, tolerate inconsistent spacing).
5. **Parse dimensions** from `length x depth x width (in cm)` into numeric columns: `length_cm`, `depth_cm`, `width_cm` (floats; accept integers or decimals like `13.5`).
6. Preserve `product_id` as the primary key and carry it through to the final output.

> ✅ The README describes behavior and requirements only. Implementation details (including how the environment variable is read) are kept in the code, with the **default engine = pandas**.

---

## 📦 Input & Output
- **Input file**: `data/producthierarchy.csv`
- **Expected columns in input** (from your sample):
  - `product_id`
  - `product (brand)`
  - `type`
  - `length x depth x width (in cm)`
  - `category || sub_category`

- **Outputs (required)**:
  - `data/curated/producthierarchy_clean.csv`
  - `data/warehouse/producthierarchy_clean.parquet`

---

## 🗂️ Project Layout (single entry point)
```
.
├── data/
│   ├── producthierarchy.csv
│   ├── curated/
│   │   └── producthierarchy_clean.csv
│   └── warehouse/
│       └── producthierarchy_clean.parquet
├── etl.py                    # single script: engine selected by ENV var (default: pandas)
├── requirements.txt
└── README.md
```

`requirements.txt` (minimum)
```
duckdb
pandas
pyarrow
```

---

## 🧠 Assumptions & Edge Cases (specific to your sample)
- **`product (brand)`** may contain:
  - Leading/trailing whitespace, embedded tabs, and multiple spaces (e.g., `"\tserum\t (Livon)"`, `"good \t\t day ..."`).
  - A **final brand in parentheses** (e.g., `(Britannia)`). Brand is assumed to be the last parenthetical group; product is the remaining text.
- **Whitespace & control characters**: Treat ASCII `[\x00-\x1F\x7F]`, tabs (`\t`), and repeated spaces as noise. Collapse any run of whitespace (spaces/tabs/newlines) into a **single space**, then trim.
- **Capitalization**: Apply **Title Case** to `product`, `brand`, `category`, `subcategory`, and `type` after cleaning. (Acronyms may be preserved where they appear fully uppercase in the source; optional.)
- **`category || sub_category`**: Use `||` as the delimiter, tolerating extra spaces around it (e.g., `"A || B"`, `"A||B"`). If `sub_category` is missing, leave `subcategory = NULL`.
- **Dimensions**: `length x depth x width (in cm)` holds values like `5 x 20 x 12` or `13.5 x 22 x 20`. Accept integer or decimal components, optional extra spaces, and either with or without an explicit trailing `cm` text in the cell.
- **Pass-through**: Keep `product_id` unchanged and present in the final dataset.

---

## 🧪 Target Schema (Data Contract)
| column | type | rule |
|---|---|---|
| product_id | STRING | Copied from source; trimmed; unique per row. |
| product | STRING | Extracted from `product (brand)` (text before the final parentheses); whitespace normalized; Title Case. |
| brand | STRING | Extracted from `product (brand)` (inside final parentheses); whitespace normalized; Title Case; NULL if missing. |
| type | STRING | Cleaned from `type`; whitespace normalized; Title Case; NULL if empty. |
| category | STRING | Left side of `category || sub_category`; whitespace normalized; Title Case; NULL if empty. |
| subcategory | STRING | Right side of `category || sub_category`; whitespace normalized; Title Case; NULL if missing/empty. |
| length_cm | DOUBLE | First numeric in dimensions; parsed as float; NULL if unparsable. |
| depth_cm | DOUBLE | Second numeric in dimensions; parsed as float; NULL if unparsable. |
| width_cm | DOUBLE | Third numeric in dimensions; parsed as float; NULL if unparsable. |

> Optional derived metrics (not required): `volume_cm3 = length_cm * depth_cm * width_cm`.

---

## 🔧 Transformation Requirements (step-by-step, engine‑agnostic)
1. **Load & audit**
   - Read `data/producthierarchy.csv` with headers.
   - Verify all expected columns listed above are present.

2. **Normalize whitespace & remove control characters** (apply to `product (brand)`, `type`, `category || sub_category`):
   - Replace all ASCII control characters `[\x00-\x1F\x7F]` with empty string.
   - Replace any sequence of whitespace (spaces, tabs, newlines) with a **single space**.
   - Trim leading and trailing spaces.

3. **Split product & brand**
   - From cleaned `product (brand)`, set `brand` to the content of the **last** parentheses pair `(...)` (if present). If none, `brand = NULL`.
   - Set `product` to the remaining text after removing the trailing parenthetical brand.
   - Apply **Title Case** to both `product` and `brand`.

4. **Split category and subcategory**
   - On the cleaned `category || sub_category` column, split on the `||` delimiter, ignoring surrounding spaces.
   - `category` = left part; `subcategory` = right part (or `NULL` if absent).
   - Apply **Title Case** to both.

5. **Clean and standardize `type`**
   - Apply the same whitespace/control‑char cleanup.
   - Title Case the value; set to `NULL` if empty after cleaning.

6. **Parse dimensions**
   - Use a robust pattern that tolerates spaces and decimals: `^[\s]*([0-9]+(?:\.[0-9]+)?)\s*x\s*([0-9]+(?:\.[0-9]+)?)\s*x\s*([0-9]+(?:\.[0-9]+)?)(?:\s*cm)?[\s]*$`.
   - Cast captures to **FLOAT/DOUBLE** into `length_cm`, `depth_cm`, `width_cm`.
   - If parsing fails, set the corresponding numeric to `NULL`.

7. **Select & order columns**
   - Final column order: `product_id, product, brand, type, category, subcategory, length_cm, depth_cm, width_cm`.

8. **Write outputs**
   - CSV: `data/curated/producthierarchy_clean.csv` (with header, UTF‑8, comma delimiter).
   - Parquet: `data/warehouse/producthierarchy_clean.parquet` (recommended compression: `snappy`).

---

## ⚙️ Engine Selection (single script behavior)
- The script checks an **environment variable** (e.g., `ETL_ENGINE`) at startup to decide which engine to use.
- **Allowed values**: `pandas`, `duckdb` (case‑insensitive recommended).
- **Default**: If the variable is **unset** or holds an **unexpected value**, the script **defaults to `pandas`**.
- The script should **log/print** which engine was selected at runtime for transparency.

> Implementation detail for reading the environment variable is intentionally omitted here. Treat this as a runtime configuration toggle.

---

## ✅ Acceptance Criteria (single entry point)
- [ ] A **single** executable `etl.py` controls the workflow end‑to‑end.
- [ ] Engine is chosen via **environment variable**; **defaults to pandas** when not provided or invalid.
- [ ] `product_id` is preserved and unique per row.
- [ ] `product` and `brand` correctly split where a final parenthetical brand exists; rows without parentheses yield `brand = NULL`.
- [ ] All target text fields have **no control characters**, **no leading/trailing spaces**, and **collapsed internal whitespace**.
- [ ] `product`, `brand`, `type`, `category`, `subcategory` are **Title Cased** after cleaning.
- [ ] `category` / `subcategory` are correctly split on `||` regardless of spaces around the delimiter.
- [ ] `length_cm`, `depth_cm`, `width_cm` are numeric (`DOUBLE`), handling integers or decimals; unparsable values become `NULL`.
- [ ] Outputs exist at the specified paths and contain exactly the target columns in the defined order.
- [ ] Script clearly **logs the chosen engine** and **completes without errors**.

---

## 🧪 QA / Validation Plan (no code)
- **Schema check**: Verify final columns and types match the Target Schema exactly.
- **Nullability spot‑checks**: Sample 10 rows where `brand IS NULL` and confirm these rows lacked parentheses in `product (brand)`.
- **Whitespace audit**: Confirm there are no leading/trailing spaces and no tabs in any of the text columns.
- **Dimensions sanity**: Ensure all parsed numbers are non‑negative and within realistic ranges for packaged goods.
- **Determinism**: Re‑run the pipeline twice and confirm identical outputs (row count, checksums) under the same engine.
- **Engine toggle**: Run once with the env var set to `duckdb`, once with it unset (pandas default), and compare that both outputs meet acceptance criteria.

---

## 🧯 Troubleshooting Hints
- **Unexpected extra parentheses**: If a product name legitimately contains parentheses (not brand), prefer the **last** parentheses group as brand; log ambiguous cases for review.
- **Mixed delimiters**: If some rows use an alternative category delimiter, standardize to `||` in a pre‑cleanup step.
- **Locale issues**: If decimals appear with commas (e.g., `13,5`), normalize to dot (`13.5`) before number casting.
- **Engine‑specific differences**: If minor differences arise (e.g., trimming or regex nuances), document them and ensure they don’t violate acceptance criteria.

---

## 📤 Submission
Submit:
1. Your single `etl.py` (engine selected by environment variable; defaults to pandas).
2. A brief note describing any deviations you needed (e.g., alternative delimiters, unit variations, locale changes).
3. The generated files in `data/curated/` and `data/warehouse/`.

Good luck — and keep it clean and deterministic! 🚀

