import time, os
import numpy as np
import pandas as pd
from runner_utils import MemoryMonitor, insert_result_csv, get_output_size_mb, parse_common_args

def debug(s): print(s, flush=True)

def coerce_chunk(chunk, pickup_col, tip_col):
    # normaliza nomes
    chunk.columns = [c.strip() for c in chunk.columns]

    # parse datetime e numeric (robusto a $ , e espaços)
    chunk[pickup_col] = pd.to_datetime(chunk[pickup_col], errors="coerce")
    chunk[tip_col] = (
        chunk[tip_col].astype(str)
        .str.replace(r"[\$,]", "", regex=True)
        .str.strip()
    )
    chunk[tip_col] = pd.to_numeric(chunk[tip_col], errors="coerce")
    return chunk

if __name__ == "__main__":
    # saída: um único parquet pequeno com (hour, avg_tip_amount)
    args, files = parse_common_args("/results/pandas_tips_by_hour.parquet")
    tool = "pandas"
    chunk_size = int(os.environ.get("CHUNK_SIZE", "1000000"))  # 1M linhas por chunk

    debug(f"[{tool}] files: {files}")
    debug(f"[{tool}] chunk_size={chunk_size}")

    # acumuladores para média por hora (0..23): soma e contagem
    sum_tips = np.zeros(24, dtype="float64")
    cnt_tips = np.zeros(24, dtype="int64")

    t_read = t_transform = t_write = 0.0

    usecols = [args.pickup_col, args.tip_col]  # só o necessário

    with MemoryMonitor() as mm:
        for f in files:
            debug(f"[{tool}] reading file: {f}")
            reader = pd.read_csv(
                f,
                dtype=str,
                on_bad_lines="skip",
                sep=",",
                encoding="utf-8",
                chunksize=chunk_size,
                usecols=usecols
            )

            for i, chunk in enumerate(reader, 1):
                # 1) leitura+coerção
                c0 = time.perf_counter()
                chunk = coerce_chunk(chunk, args.pickup_col, args.tip_col)
                t_read += time.perf_counter() - c0

                # 2) transformações (filtro + hora + agregação parcial)
                c1 = time.perf_counter()
                # descarta NAs
                chunk = chunk.dropna(subset=[args.pickup_col, args.tip_col])
                # tip > 0
                chunk = chunk[chunk[args.tip_col] > 0]
                if not chunk.empty:
                    hours = chunk[args.pickup_col].dt.hour
                    # agregação parcial: soma e contagem por hora no chunk
                    grp = chunk.groupby(hours)[args.tip_col].agg(["sum", "count"])
                    # atualiza acumuladores globais
                    for h, row in grp.iterrows():
                        if 0 <= h <= 23:
                            sum_tips[h] += float(row["sum"])
                            cnt_tips[h] += int(row["count"])
                t_transform += time.perf_counter() - c1

                debug(f"[{tool}] chunk {i}: read={t_read:.2f}s transform={t_transform:.2f}s "
                      f"| mem_avg={mm.avg_mb:.0f}MB peak={mm.peak_mb:.0f}MB")

        # 3) monta resultado final (24 linhas)
        hours = np.arange(24, dtype=int)
        with np.errstate(divide='ignore', invalid='ignore'):
            avg = np.where(cnt_tips > 0, sum_tips / cnt_tips, np.nan)
        result = pd.DataFrame({"hour": hours, "avg_tip_amount": avg})

        # printa no console
        print(result.to_string(index=False), flush=True)

        # salvar parquet
        c2 = time.perf_counter()
        result.to_parquet(args.output, engine="pyarrow", index=False)
        t_write += time.perf_counter() - c2
        debug(f"[{tool}] save done: {args.output}, t_save={t_write:.3f}s")

    out_size = get_output_size_mb(args.output)
    insert_result_csv((tool, args.exp, t_read, t_transform, t_write, mm.avg_mb, mm.peak_mb, out_size))
    print(
        f"[OK] {tool} exp={args.exp} -> import(read)={t_read:.3f}s, transform={t_transform:.3f}s, "
        f"save(write)={t_write:.3f}s, mem_avg={mm.avg_mb:.1f}MB, mem_peak={mm.peak_mb:.1f}MB, "
        f"out_size={out_size:.2f}MB",
        flush=True
    )
