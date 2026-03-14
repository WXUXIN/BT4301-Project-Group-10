# Data Cleaning — Home Credit Pipeline
---

## 1. Data quality checks (pre-cleaning)

The pipeline runs the following checks and **logs** results.

| Check | Description |
|-------|-------------|
| **Missing values** | Per-column missing count and %. Columns with ≥50% missing are logged as warnings. |
| **Invalid categories** | Categorical columns are checked against allowed values (e.g. `CODE_GENDER` in M, F, XNA; `STATUS` in C, X, 0–5). Invalid counts are logged. |
| **IQR outliers** | Numeric columns: values outside 1.5×IQR (Q1, Q3) are counted and logged. Not used to alter data; for awareness and downstream capping if needed. |

These checks inform which cleaning steps are applied below.

---

## 2. application_train → app_clean

| Step | What is done |
|------|----------------------|
| **CODE_GENDER** | Missing values (NaN) are set to the category **"XNA"**. No imputation with mode; the column has three levels: **M**, **F**, **XNA**. |
| **DAYS_EMPLOYED sentinel** | The value **365243** (unemployed/pensioner) is replaced with **0**. A new column **FLAG_UNEMPLOYED** is added (1 where original was 365243, else 0) for downstream ML. |
| **Amount capping** | **AMT_INCOME_TOTAL**, **AMT_CREDIT**, **AMT_ANNUITY** are capped at the 99th percentile (upper bound only) to limit outlier impact. |
| **Duplicate IDs** | Rows with duplicate **SK_ID_CURR** are dropped (first occurrence kept). A warning is logged if any were dropped. |
| **Numeric missing** | All numeric columns **except TARGET** that contain any NaN are filled with **0**. |
| **TARGET** | Left unchanged so labels (0/1) remain valid for EDA and modeling. |

Row count may be lower than raw if duplicate SK_ID_CURR existed. The 99th percentile caps for the AMT columns are stored as **AMT_Q99_CAPS** and reused for application_test (no leakage).

---

## 2b. application_test → app_test_clean

The **same** cleaning steps as application_train are applied, using **train-derived** statistics where needed:

| Step | What is done |
|------|----------------------|
| **CODE_GENDER** | Missing set to **"XNA"** (same as train). |
| **DAYS_EMPLOYED sentinel** | **365243** replaced with **0**; **FLAG_UNEMPLOYED** added (1 where original was 365243, else 0). |
| **Amount capping** | **AMT_INCOME_TOTAL**, **AMT_CREDIT**, **AMT_ANNUITY** are capped using the **same 99th percentile values computed from application_train** (AMT_Q99_CAPS) so test is not used to derive bounds. |
| **Duplicate IDs** | Duplicate **SK_ID_CURR** dropped (first kept); warning logged if any dropped. |
| **Numeric missing** | All numeric columns with any NaN are filled with **0**. (application_test has no TARGET column.) |

Result: **app_test_clean** is written to MySQL as `application_test`.

---

## 3. bureau → bureau_clean

| Step | What is done |
|------|----------------------|
| **All missing** | Every NaN in the table is replaced with **0** (numeric and categorical-like codes). |

Shape unchanged.

---

## 4. bureau_balance → bureau_balance_clean

| Step | What is done |
|------|----------------------|
| **STATUS** | Only the following codes are kept: **C**, **X**, **0**, **1**, **2**, **3**, **4**, **5** (C = closed, X = unknown, 0–5 = days past due). Any other value is replaced with **"X"**. |

No other columns are modified; shape unchanged.

---

## 5. Outputs

- **app_clean** — cleaned application training data (loaded to MySQL as `application_train`).
- **app_test_clean** — cleaned application test data (loaded to MySQL as `application_test`).
- **bureau_clean** — cleaned bureau data (loaded as `bureau`).
- **bureau_balance_clean** — cleaned bureau balance data (loaded as `bureau_balance`).

These cleaned DataFrames are written to the `home_credit` MySQL database as the single trusted source for downstream EDA, risk driver analysis, and inference.
