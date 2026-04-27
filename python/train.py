import pyarrow.parquet as pq
import pandas as pd
import numpy as np
import lightgbm as lgb
from sklearn.metrics import roc_auc_score, classification_report
import sys

PATH = sys.argv[1] if len(sys.argv) > 1 else "data/features_20260422_193137.parquet"
HORIZON_S = 10

df = pq.read_table(PATH).to_pandas()
print(f"rows: {len(df)}")

# Drop the flat label=0 rows; predict directional move only.
df = df[df["label"] != 0].copy()
df["y"] = (df["label"] == 1).astype(int)
print(f"after dropping flat: {len(df)}, up rate: {df['y'].mean():.3f}")

FEATURES = [
    "spread", "microprice_diff", "imb_l1", "imb_l5", "imb_l10",
    "ofi_l1", "tfi", "trade_count", "realized_vol",
]
# Note: not using raw mid/microprice as features — those are levels, not signal.

# Time-ordered split: 70% train, 15% val, 15% test.
df = df.sort_values("event_time_ms").reset_index(drop=True)
n = len(df)
i_train = int(n * 0.70)
i_val = int(n * 0.85)
train = df.iloc[:i_train]
val = df.iloc[i_train:i_val]
test = df.iloc[i_val:]
print(f"train={len(train)} val={len(val)} test={len(test)}")

X_train, y_train = train[FEATURES], train["y"]
X_val, y_val = val[FEATURES], val["y"]
X_test, y_test = test[FEATURES], test["y"]

model = lgb.LGBMClassifier(
    n_estimators=500,
    learning_rate=0.03,
    num_leaves=31,
    min_child_samples=50,
    reg_lambda=1.0,
    random_state=42,
)
model.fit(
    X_train, y_train,
    eval_set=[(X_val, y_val)],
    callbacks=[lgb.early_stopping(30), lgb.log_evaluation(50)],
)

p_val = model.predict_proba(X_val)[:, 1]
p_test = model.predict_proba(X_test)[:, 1]
print(f"\nval AUC:  {roc_auc_score(y_val, p_val):.4f}")
print(f"test AUC: {roc_auc_score(y_test, p_test):.4f}")

# Naive Sharpe: take signal direction, hold next bar.
test = test.copy()
test["pred"] = p_test
test["signal"] = np.where(test["pred"] > 0.6, 1, np.where(test["pred"] < 0.4, -1, 0))
test["ret"] = (test["future_mid"] - test["mid_at_t"]) / test["mid_at_t"]
test["pnl"] = test["signal"] * test["ret"]
nz = test[test["signal"] != 0]
if len(nz) > 0:
    sharpe = nz["pnl"].mean() / (nz["pnl"].std() + 1e-12) * np.sqrt(252 * 6.5 * 3600 / HORIZON_S)
    print(f"trades: {len(nz)}, hit rate: {(nz['pnl']>0).mean():.3f}, annualized Sharpe: {sharpe:.2f}")
else:
    print("no trades triggered")

print("\nfeature importance:")
imp = pd.DataFrame({"feature": FEATURES, "gain": model.booster_.feature_importance("gain")})
print(imp.sort_values("gain", ascending=False))

model.booster_.save_model("models/lgbm_v1.txt")
print("\nsaved models/lgbm_v1.txt")