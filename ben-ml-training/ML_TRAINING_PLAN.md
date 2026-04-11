# ML Training Plan — Customer Churn Prediction
**Branch:** `ben-ml-training`  
**Source table:** `customer_churn.train_churn_model`  
**Target:** `churn` (binary: 0 = stayed, 1 = churned)  
**Dataset:** 1,000,000 rows | 16 features + 1 target | ~9.9% churn rate

---

## 1. Correlation Matrix Analysis

### What the matrix tells you

From the encoded feature correlation heatmap:

**`contract_one_year` ↔ `contract_two_year` (strong negative, ~−0.5)**
These are two dummies from the same one-hot encoded `contract` column — they are by definition negatively correlated. This is expected and not a problem; just make sure you always drop one contract dummy as the reference category (i.e., keep `contract_one_year` and `contract_two_year`, drop `contract_month_to_month` which is the reference).

**`tenure_segment_Loyal` ↔ `tenure_segment_Stable`, `tenure_segment_Veteran` (moderate negative)**
Same issue — dummies from `tenure_segment`. Negative correlations between them are structurally unavoidable. No action needed beyond standard dummy encoding.

**`age_group_*` dummies (light blue pattern between groups)**
Mild negative correlations across age group dummies — again structural. Not multicollinearity in the harmful sense.

**`num_complaints` ↔ `num_service_calls` (light positive, ~0.2)**
Low-to-moderate positive correlation. Both measure "contact intensity." Not strong enough to drop either, but this is exactly why `complaint_to_call_ratio` and `service_burden` (from the `dim_behavior` feature engineering) are valuable — they resolve the shared variance into a single clean signal.

**`high_risk_flag` ↔ `num_complaints`, `late_payments` (moderate positive)**
`high_risk_flag` is derived from `num_complaints` and `late_payments`, so this overlap is expected. In a logistic regression, you would drop `high_risk_flag` to avoid partial multicollinearity. In tree-based models, keep it — it encodes a non-linear OR boundary that the model benefits from having pre-computed.

**Everything else is near-zero (grey)**
`annual_income`, all service flags, and most behavioral features are largely uncorrelated with each other. This is a healthy feature set — low redundancy between most features.

### Conclusion from the matrix
The correlation matrix does **not** reveal any harmful multicollinearity requiring feature removal. The non-diagonal coloured cells are all structurally explained by one-hot encoding. Proceed with all features.

---

## 2. Additional Feature Selection Methods to Apply

The correlation matrix only shows **linear, pairwise** relationships. Three more methods should be run before finalising:

### 2a. Mutual Information (MI) Classification
**Why:** Captures non-linear dependencies. A feature like `customer_satisfaction` may have a non-linear relationship with churn (threshold effect at low scores) that Pearson correlation underestimates.  
**How:** `sklearn.feature_selection.mutual_info_classif` on the full encoded feature set.  
**Decision rule:** Drop features with MI score < 0.005 (negligible information).

### 2b. Permutation Importance (from a quick Random Forest)
**Why:** Shows how much each feature contributes to a tree ensemble's predictions. Immune to scale and distribution assumptions.  
**How:** Fit a shallow `RandomForestClassifier` (100 trees, max_depth=6) on a 200k stratified sample. Extract `feature_importances_`.  
**Decision rule:** Flag features consistently in the bottom 3 across both MI and RF importance for potential removal.

### 2c. Variance Inflation Factor (VIF) — for logistic regression only
**Why:** Detects multicollinearity more rigorously than pairwise correlation by checking if one feature is a linear combination of others.  
**How:** `statsmodels.stats.outliers_influence.variance_inflation_factor` on the standardised numeric features.  
**Decision rule:** VIF > 10 = remove or merge. The matrix suggests VIF will be fine for this dataset, but worth confirming before fitting logistic regression.

### 2d. Chi-Square Test for Categoricals
**Why:** Tests whether a categorical feature's distribution is independent of the target.  
**How:** `scipy.stats.chi2_contingency` on each encoded categorical vs `churn`. Report Cramér's V as effect size.  
**Decision rule:** Cramér's V < 0.05 = weak signal, consider dropping.

---

## 3. Feature Type Analysis

Full breakdown of the 16 features in `train_churn_model`:

| Feature | Type | Encoding needed | Notes |
|---|---|---|---|
| `annual_income` | Continuous | StandardScaler (LR/SVM) / none (trees) | Right-skewed; consider log transform for linear models |
| `customer_satisfaction` | Continuous (1–9) | StandardScaler | Strong churn signal; threshold effect at low scores |
| `num_complaints` | Count (0–7) | StandardScaler or as-is | Correlated with `high_risk_flag`; keep both for trees |
| `num_service_calls` | Count (0–12) | StandardScaler or as-is | Weak standalone; useful via ratios |
| `late_payments` | Count (0–5) | StandardScaler or as-is | Financial instability signal |
| `has_phone_service` | Binary (0/1) | None | Already encoded |
| `has_internet_service` | Binary (0/1) | None | Already encoded |
| `has_tech_support` | Binary (0/1) | None | Already encoded |
| `has_streaming_tv` | Binary (0/1) | None | Already encoded |
| `has_streaming_movies` | Binary (0/1) | None | Already encoded |
| `high_risk_flag` | Binary (0/1) | None | Derived; keep for trees, review for LR |
| `is_auto_pay` | Binary (0/1) | None | Already encoded |
| `contract` | Nominal (3 levels) | One-hot (drop `month_to_month` as reference) | Strongest categorical predictor |
| `age_group` | Ordinal (7 levels) | Ordinal int OR one-hot | Ordinal encoding preferred (`0-18`=0, `18-29`=1, ...) |
| `tenure_segment` | Ordinal (4 levels) | Ordinal int OR one-hot | Ordinal encoding preferred (`Infant`=0, `Stable`=1, `Loyal`=2, `Veteran`=3) |

### Preprocessing pipeline summary

```
Numeric:     [annual_income, customer_satisfaction, num_complaints,
              num_service_calls, late_payments]
             → log1p(annual_income) for linear models
             → StandardScaler for LR/SVM, no scaling for trees

Binary:      [has_phone_service, has_internet_service, has_tech_support,
              has_streaming_tv, has_streaming_movies, high_risk_flag, is_auto_pay]
             → Pass through as-is

Ordinal:     [age_group, tenure_segment]
             → OrdinalEncoder with explicit category order

Nominal:     [contract]
             → OneHotEncoder(drop='first') → 2 dummy columns
```

---

## 4. Class Imbalance — Must Address Before Training

The dataset has **~9.9% churn rate** (99,227 churned vs 900,773 not churned) — roughly 1:9 imbalance.

**Impact:** A naïve model that predicts "never churn" for every customer achieves 90.1% accuracy but is completely useless. Standard accuracy is a misleading metric here.

**Strategy:**

1. **Primary metric:** `ROC-AUC` and `F1-score (macro)` — both are imbalance-aware
2. **Secondary metrics:** Precision, Recall, and `PR-AUC` (Precision-Recall AUC) — especially important if the business cost of missing a churner (false negative) is higher than a false positive
3. **Handling the imbalance (pick one per model):**
   - `class_weight='balanced'` — built into sklearn's LR, RF, SVM; re-weights the loss function. Simple, no data modification.
   - `scale_pos_weight` in XGBoost/LightGBM — equivalent to `class_weight` for gradient boosted trees. Set to `900773 / 99227 ≈ 9.08`.
   - SMOTE (Synthetic Minority Oversampling) — generate synthetic churner samples. Apply **only to training set**, never to validation/test.

**Recommended:** Use `class_weight='balanced'` / `scale_pos_weight` as the primary strategy. Only add SMOTE if recall on churners is still poor after tuning.

---

## 5. Model Training Plan

### 5a. Train/Validation/Test Split

```
Total: 1,000,000 rows
Split: 70% train / 15% validation / 15% test
→ train:      700,000 rows
→ validation: 150,000 rows
→ test:       150,000 rows

Method: Stratified split (sklearn.model_selection.train_test_split, stratify=y)
        Ensures ~9.9% churn rate is preserved in all three sets.
```

### 5b. Models to Train and Compare

We train 4 model families, each chosen for a specific reason:

---

#### Model 1 — Logistic Regression (Baseline)
**Why:** Interpretable, fast, establishes a linear baseline. If a complex model barely beats LR, it's not worth the added complexity.

```python
LogisticRegression(
    C=1.0,                    # regularisation strength (tune via CV)
    class_weight='balanced',
    max_iter=1000,
    solver='lbfgs'
)
```

**Preprocessing required:** StandardScaler on all numeric, OrdinalEncoder on ordered categoricals, OneHotEncoder on `contract`.  
**Expected strength:** Handles well-separated linear boundaries. Good for baseline and coefficient interpretability.  
**Weakness:** Cannot capture the interaction effects (e.g., `frustration_index` type multiplicative relationships) without explicit engineered features.  
**Key hyperparameter to tune:** `C` (regularisation) via 5-fold CV.

---

#### Model 2 — Random Forest
**Why:** Handles non-linearity and interactions natively. Robust to outliers and doesn't require feature scaling. Provides reliable permutation importance scores.

```python
RandomForestClassifier(
    n_estimators=300,
    max_depth=None,           # tune
    min_samples_leaf=50,      # prevents overfitting on 1M rows
    class_weight='balanced',
    n_jobs=-1,
    random_state=42
)
```

**Preprocessing required:** OrdinalEncoder / OneHotEncoder for categoricals only. No scaling.  
**Expected strength:** Strong out-of-the-box, interpretable via feature importance.  
**Weakness:** Slower to train at 1M rows; can overfit on noisy features without min_samples constraints.  
**Key hyperparameters to tune:** `n_estimators`, `max_depth`, `min_samples_leaf`.

---

#### Model 3 — XGBoost (Primary candidate)
**Why:** Gradient-boosted trees are the empirical benchmark for tabular churn prediction. Handles imbalance natively via `scale_pos_weight`. Generally the best-performing model class on this type of structured data.

```python
XGBClassifier(
    n_estimators=500,
    learning_rate=0.05,
    max_depth=6,
    subsample=0.8,
    colsample_bytree=0.8,
    scale_pos_weight=9.08,    # 900773 / 99227
    eval_metric='auc',
    early_stopping_rounds=20,
    random_state=42,
    n_jobs=-1
)
```

**Preprocessing required:** OrdinalEncoder / OneHotEncoder for categoricals only. No scaling.  
**Expected strength:** Best generalisation on tabular data; built-in regularisation (L1/L2); early stopping prevents overfitting.  
**Key hyperparameters to tune:** `max_depth`, `learning_rate`, `n_estimators` (via early stopping), `subsample`, `colsample_bytree`.

---

#### Model 4 — LightGBM (Speed-optimised alternative to XGBoost)
**Why:** Faster than XGBoost on large datasets (leaf-wise vs level-wise growth). Often matches or beats XGBoost performance. Native support for categorical features — can pass `age_group`, `tenure_segment`, `contract` without one-hot encoding.

```python
LGBMClassifier(
    n_estimators=500,
    learning_rate=0.05,
    num_leaves=63,
    min_child_samples=100,
    subsample=0.8,
    colsample_bytree=0.8,
    is_unbalance=True,        # equivalent to class_weight='balanced'
    random_state=42,
    n_jobs=-1
)
```

**Preprocessing required:** Can use `categorical_feature` parameter natively for `contract`, `age_group`, `tenure_segment` — no encoding needed for those.  
**Expected strength:** Fastest training at 1M rows; good for rapid iteration and hyperparameter search.  
**Key hyperparameters to tune:** `num_leaves`, `min_child_samples`, `learning_rate`.

---

### 5c. Evaluation Framework

All models are evaluated on the **held-out test set** (150k rows, never seen during training or validation).

| Metric | Why |
|---|---|
| `ROC-AUC` | Primary ranking metric — measures overall discrimination regardless of threshold |
| `F1-score (class 1 = churn)` | Balances precision and recall for the minority class |
| `PR-AUC` | More informative than ROC-AUC under heavy imbalance; focuses on the positive class |
| `Recall (churn)` | Business-critical: what % of churners did we catch? |
| `Precision (churn)` | What % of predicted churners were actually churning? |
| `Accuracy` | Report for completeness; **not** used for model selection |

**Threshold tuning:** After training, find the optimal classification threshold per model by maximising F1 on the validation set (default 0.5 is almost always wrong under imbalance).

---

### 5d. Comparison Table (to fill in after training)

| Model | ROC-AUC | PR-AUC | F1 (churn) | Recall (churn) | Precision (churn) | Train time |
|---|---|---|---|---|---|---|
| Logistic Regression | | | | | | |
| Random Forest | | | | | | |
| XGBoost | | | | | | |
| LightGBM | | | | | | |

---

### 5e. Hyperparameter Tuning Strategy

For the top 2 models from the comparison table:

1. **Optuna** (recommended) — Bayesian optimisation, much more efficient than grid search at 1M rows. Run 50 trials on the validation set.
2. Alternatively: `RandomizedSearchCV` with 20 iterations if Optuna is not available.

**Do not tune all 4 models** — identify the winner first from default settings, then tune only the top 1–2.

---

## 6. Recommended Execution Order

```
Step 1  — Feature finalisation
          Run MI, VIF, Chi-Square on train_churn_model
          Confirm no features need dropping

Step 2  — Preprocessing pipeline
          Build sklearn Pipeline with ColumnTransformer
          (OrdinalEncoder + OneHotEncoder + StandardScaler)

Step 3  — Baseline (LR)
          Fit on 700k train, evaluate on 150k val
          Record metrics

Step 4  — Tree models
          Fit RF, XGBoost, LightGBM on 700k train
          Use validation set for early stopping (XGB/LGBM)
          Record metrics

Step 5  — Compare all 4 on test set
          Pick winner

Step 6  — Tune winner
          Optuna on validation set
          Final evaluation on test set

Step 7  — Explainability
          SHAP values on XGBoost/LightGBM winner
          Identify top 5 features driving churn predictions
```

---

## 7. Files to Create in `ben-ml-training/`

```
ben-ml-training/
├── ML_TRAINING_PLAN.md          ← this file
├── 01_feature_selection.ipynb   ← MI, VIF, Chi-Square, final feature list
├── 02_preprocessing.ipynb       ← ColumnTransformer pipeline, split
├── 03_model_training.ipynb      ← All 4 models, comparison table
└── 04_model_tuning.ipynb        ← Optuna tuning on winner + SHAP
```
