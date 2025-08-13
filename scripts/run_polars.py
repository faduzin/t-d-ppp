import time
import polars as pl
from runner_utils import MemoryMonitor, insert_result_csv, get_output_size_mb, parse_common_args

def debug(s): 
    print(s, flush=True)

def build_scan(files, pickup_col, tip_col):
    scans = []
    for f in files:
        lf = pl.scan_csv(
            f,
            has_header=True,
            infer_schema_length=0,
            ignore_errors=True,
            try_parse_dates=False,
        )
        scans.append(lf)
    lf_all = pl.concat(scans, how="vertical")

    # Parse robusto: pickup como Datetime; tip como Float64 limpando "$" e ","
    lf_all = lf_all.with_columns([
        pl.col(pickup_col)
          .cast(pl.Utf8)
          .str.replace_all(r"^\s+|\s+$", "")
          .str.strptime(pl.Datetime, strict=False)
          .alias(pickup_col),
        pl.col(tip_col)
          .cast(pl.Utf8)
          .str.replace_all(r"[\$,]", "")
          .str.replace_all(r"\s+", "")
          .cast(pl.Float64, strict=False)
          .alias(tip_col),
    ])

    return lf_all

def build_aggregation(lf, pickup_col, tip_col):
    # 1) filtro tip>0
    lf = lf.filter(pl.col(tip_col).is_not_null() & (pl.col(tip_col) > 0))
    # 2) hour do pickup (0–23)
    lf = lf.with_columns(pl.col(pickup_col).dt.hour().alias("hour"))
    # 3) média de tip por hora
    lf = (
        lf.group_by("hour")
          .agg(pl.col(tip_col).mean().alias("avg_tip_amount"))
          .sort("hour")
    )
    return lf

if __name__ == "__main__":
    # saída: parquet pequeno com (hour, avg_tip_amount)
    args, files = parse_common_args("/results/polars_tips_by_hour.parquet")
    tool = "polars"
    debug(f"[{tool}] files ({len(files)}): {files}")

    t_import = t_transform = t_save = 0.0

    with MemoryMonitor() as mm:
        # 1) Import + concat (força uma passada de leitura)
        t0 = time.perf_counter()
        lf_raw = build_scan(files, args.pickup_col, args.tip_col)
        _ = lf_raw.select(pl.len()).collect(engine="streaming")
        t_import = time.perf_counter() - t0
        debug(f"[{tool}] import+concat done: t_import={t_import:.3f}s")

        # 2) Transformações -> agregação (coleta para imprimir)
        t1 = time.perf_counter()
        lf_result = build_aggregation(lf_raw, args.pickup_col, args.tip_col)
        result_df = lf_result.collect(engine="streaming")   # ~24 linhas
        t_transform = time.perf_counter() - t1
        debug(f"[{tool}] transforms done: t_transform={t_transform:.3f}s")

        # Print no console (0..23)
        print(result_df, flush=True)

        # 3) Salvar (o agregado pequeno em parquet)
        t2 = time.perf_counter()
        result_df.write_parquet(args.output)
        t_save = time.perf_counter() - t2
        debug(f"[{tool}] save done: {args.output}, t_save={t_save:.3f}s")

    out_size = get_output_size_mb(args.output)
    insert_result_csv((tool, args.exp, t_import, t_transform, t_save, mm.avg_mb, mm.peak_mb, out_size))
    print(
        f"[OK] {tool} exp={args.exp} -> import={t_import:.3f}s, transform={t_transform:.3f}s, save={t_save:.3f}s, "
        f"mem_avg={mm.avg_mb:.1f}MB, mem_peak={mm.peak_mb:.1f}MB, out={out_size:.2f}MB",
        flush=True
    )
