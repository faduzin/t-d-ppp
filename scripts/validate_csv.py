import sys, glob, pandas as pd

files = sorted(glob.glob("/data/*.csv"))
if not files:
    print("No CSVs found at /data/*.csv")
    sys.exit(1)

print(f"Found {len(files)} files:")
for f in files[:5]:
    print(" -", f)

sample = pd.read_csv(
    files[0],
    nrows=1000,
    sep=",",
    encoding="utf-8",
    on_bad_lines="skip"
)

print("\nColumns:", list(sample.columns))
missing = { "tpep_pickup_datetime", "tpep_dropoff_datetime", "tip_amount" } - set(sample.columns)
if missing:
    print("ERROR - missing required columns:", missing); sys.exit(2)

for c in ["tpep_pickup_datetime", "tpep_dropoff_datetime"]:
    sample[c] = pd.to_datetime(sample[c], errors="coerce", infer_datetime_format=True, utc=False)

sample["tip_amount"] = pd.to_numeric(
    sample["tip_amount"]
        .astype(str)
        .str.replace(r"[\$,]", "", regex=True)
        .str.strip(),
    errors="coerce"
)

print("\nDtypes:")
print(sample.dtypes)

print("\nNulls in required columns (sample of 1000 rows):")
print(sample[["tpep_pickup_datetime","tpep_dropoff_datetime","tip_amount"]].isna().sum())

ok_rows = (
    sample["tip_amount"].gt(0) &
    (sample["tpep_dropoff_datetime"] > sample["tpep_pickup_datetime"])
).sum()
print(f"\nRows with tip>0 and dropoff>pickup (sample): {ok_rows} / {len(sample)}")

print("\nIf this looks reasonable, the main scripts should run.")
