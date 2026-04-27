import pyarrow.parquet as pq

t = pq.read_table("data/features_20260422_193137.parquet").to_pandas()
print(t.head())
print("rows:", len(t))
print("label distribution:")
print(t["label"].value_counts(normalize=True))
print("\nfeature stats:")
print(t.describe().T[["mean", "std", "min", "max"]])