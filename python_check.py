import pyarrow.parquet as pq
t = pq.read_table("data/book_20260422_193137.parquet").to_pandas()
print(t.tail())
print("rows:", len(t))
print("update_id monotonic?", t["last_update_id"].is_monotonic_increasing)
print("spread stats:")
print((t["best_ask"] - t["best_bid"]).describe())