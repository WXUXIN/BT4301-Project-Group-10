# Customer Churn Analytics Report
**Dataset:** `customer_churn` data warehouse — 1,000,000 records, 33 features across 5 tables  
**Analysis source:** `dataops/customer_churn_analysis.ipynb`

---

## 1. Churn Distribution

| Status | Count | Proportion |
|---|---|---|
| Not Churned | 900,773 | **90.08%** |
| Churned | 99,227 | **9.92%** |

The dataset is significantly **class-imbalanced** — roughly 9:1 in favour of non-churners. No missing values were found across any of the 33 columns.

---

## 2. Numerical Feature Distributions (KDE, Histogram, Boxplot, Violin)

Key observations from distributions split by churn status:

| Feature | Observation |
|---|---|
| `customer_satisfaction` | Churned customers visibly skew left — lower satisfaction is a consistent churn signal |
| `num_complaints` | Right-skewed for both groups; churned customers have a heavier upper tail |
| `num_service_calls` | Churned group centres at higher call counts — a strong behavioural signal |
| `late_payments` | Higher frequency of late payments in churned customers |
| `has_tech_support` | Customers without tech support churn at higher rates |
| `tenure` | Churned customers cluster at lower tenure — shorter-term customers are more at risk |
| `total_charges` | Churned group skews lower, driven by lower tenure |
| `monthly_charges` | Distributions largely overlap — not a standalone discriminator |
| `age` | Near-identical distributions — weak predictor |
| `annual_income` | Near-identical distributions — weak predictor |
| `credit_score` | Near-identical distributions — weak predictor |
| `avg_monthly_gb` | Broad overlap — usage volume alone is not a signal |
| `days_since_last_interaction` | Uniform distribution across both groups — not predictive |

---

## 3. Categorical Feature Distributions (Bar Charts)

| Feature | Observation |
|---|---|
| `contract` | **Strongest categorical signal** — month-to-month customers churn at a far higher rate than one-year or two-year holders |
| `payment_method` | Broadly even churn rates across all methods — very weak signal |
| `paperless_billing` | Minimal difference |
| `gender` | No meaningful difference in churn rate |
| `education` | Consistent churn rates across all education levels |
| `marital_status` | Minor variation, no clear pattern |

---

## 4. Correlation Results

### 4a. Pearson Correlation — Top 10 numeric/binary features vs churn

| Feature | Correlation |
|---|---|
| `customer_satisfaction` | 0.084 |
| `num_complaints` | 0.078 |
| `num_service_calls` | 0.077 |
| `late_payments` | 0.048 |
| `has_tech_support` | 0.045 |
| `high_risk_flag` *(engineered)* | 0.036 |
| `has_online_security` | 0.034 |
| `num_services` | 0.033 |
| `totalcharges` | 0.017 |
| `avg_charge_per_month` | 0.016 |

### 4b. Cramér's V — Categorical features vs churn

| Feature | Cramér's V |
|---|---|
| `contract` | **0.140** |
| `marital_status` | 0.002 |
| `education` | 0.002 |
| `paperless_billing` | 0.001 |
| `payment_method` | 0.001 |
| `gender` | 0.000 |

### 4c. Mixed Correlation — All features vs churn (top 15)

| Rank | Feature | Correlation |
|---|---|---|
| 1 | `contract` | 0.136 |
| 2 | `customer_satisfaction` | 0.084 |
| 3 | `num_complaints` | 0.078 |
| 4 | `num_service_calls` | 0.077 |
| 5 | `late_payments` | 0.048 |
| 6 | `has_tech_support` | 0.045 |
| 7 | `high_risk_flag` *(engineered)* | 0.036 |
| 8 | `has_online_security` | 0.034 |
| 9 | `num_services` | 0.033 |
| 10 | `totalcharges` | 0.017 |
| 11 | `avg_charge_per_month` | 0.016 |
| 12 | `has_internet_service` | 0.015 |
| 13 | `monthlycharges` | 0.014 |
| 14 | `tenure` | 0.013 |
| 15 | `avg_monthly_gb` | 0.006 |

**Notable:** `age`, `annual_income`, `credit_score`, and `gender` do not appear in the top 15 — confirming they carry minimal predictive signal for churn.

---

## 5. Key Insights

### 5.1 Class Imbalance — To Address Before Any Modelling

The 90:10 split means a naive model predicting "no churn" every time achieves **90% accuracy** — a completely misleading metric.

**Actions:**
- Apply **SMOTE** or `class_weight='balanced'` in all classifiers
- Evaluate using **F1-score, Precision-Recall AUC, and ROC-AUC** — never raw accuracy
- Tune the **classification threshold** for business cost (a missed churner likely costs more than a false positive)

### 5.2 Correlations Are Individually Weak — Ensemble Models Are the Right Call

The highest single-feature correlation with churn is only **0.14** (`contract`). No feature dominates.

This tells us:
- Linear models will underperform — the signal is distributed and likely non-linear
- **Tree-based ensemble models** (XGBoost, Random Forest, LightGBM) are the natural fit — they capture feature interactions and non-linear thresholds without requiring strong individual correlations
- Feature interactions (e.g. `contract` × `customer_satisfaction`, `late_payments` × `high_risk_flag`) may unlock more signal

### 5.3 Feature Selection — What to Keep and What to Drop

**Keep (top predictors confirmed by correlation analysis):**
- `contract`, `customer_satisfaction`, `num_complaints`, `num_service_calls`
- `late_payments`, `has_tech_support`, `has_online_security`
- `high_risk_flag`, `num_services`, `tenure`

**Drop (no meaningful signal):**
- `age`, `annual_income`, `credit_score`, `gender`
- `days_since_last_interaction`, `avg_monthly_gb`
- All other categorical features except `contract`

### 5.4 The Engineered Features Already Add Value

`high_risk_flag` (engineered in the ETL) ranks **7th overall** in the mixed correlation matrix — above individual features like `has_internet_service`, `monthlycharges`, and `tenure`. This validates the feature engineering step in the pipeline.

**Additional features to engineer for modelling:**
- `tenure_bucket` — bin tenure (0–12, 13–24, 25–48, 48+) to capture non-linearity
- `complaint_rate` — `num_complaints / tenure` to normalise for customer age
- `dissatisfaction_flag` — binary flag for `customer_satisfaction` below a threshold (e.g. < 4)

### 5.5 The Incremental ETL Is Already MLOps-Ready

The Airflow pipeline loads data in monthly batches. This maps directly to an MLOps workflow:

| ETL Event | MLOps Action |
|---|---|
| New monthly batch loaded | Trigger data quality checks |
| Feature distribution shifts from baseline | Alert + schedule retraining |
| Model F1 on holdout drops below threshold | Trigger automated retraining DAG |
| Retraining completes | Run scoring batch, write predictions to DB |

### 5.6 Monitoring Strategy

Define these monitors in the Airflow DAG post-ingestion:
- **Data drift:** compare distribution of top features (`customer_satisfaction`, `num_complaints`, `contract`) each period against a reference window
- **Label drift:** track the churn rate per period — if it deviates significantly from ~10%, investigate
- **Prediction drift:** if the model's predicted churn rate shifts, flag for review even before performance metrics degrade

### 5.7 Serving Design

- **Batch inference** is the right choice — scores are produced once per monthly period, aligned with the ETL cadence
- Output: churn probability score per `customer_id`, written back to the `customer_churn` DB for downstream CRM/retention use
- No need for real-time serving infrastructure

---

## 6. Summary Table for the Meeting

| Area | Finding | MLOps Action |
|---|---|---|
| Class imbalance | 9.92% churn — 9:1 ratio | SMOTE / balanced weights / use F1 not accuracy |
| Top predictor | `contract` (Cramér's V = 0.14) | Must include; encode as ordinal (m2m < 1yr < 2yr) |
| Behavioural signals | Satisfaction, complaints, service calls, late payments | Core feature set for model input |
| Weak predictors | Age, income, credit score, gender | Drop to reduce noise |
| Correlation strength | Max 0.14 — distributed signal | Use ensemble models (XGBoost/LightGBM), not linear |
| Engineered features | `high_risk_flag` ranks 7th | Validated — build more (tenure bucket, complaint rate) |
| ETL cadence | Monthly incremental load | Align retraining and scoring to batch completion |
| Monitoring | Incremental batches enable drift detection | Add distribution checks per period to Airflow |
| Inference | Monthly batch fits the use case | Batch scoring, write predictions back to DB |
