import time, duckdb
from runner_utils import MemoryMonitor, insert_result_csv, get_output_size_mb, parse_common_args

if __name__ == "__main__":
    # saída: parquet pequeno com (hour, avg_tip_amount)
    args, files = parse_common_args("/results/duckdb_tips_by_hour.parquet")
    tool = "duckdb"

    with MemoryMonitor() as mm:
        con = duckdb.connect(database=':memory:')
        con.execute("PRAGMA threads=4")

        # 1) importar + concatenar (leitura; all_varchar para schema estável)
        t0 = time.perf_counter()
        # t_raw: tudo como texto; evita inferência inconsistente entre arquivos
        con.execute("""
            CREATE TEMP TABLE t_raw AS
            SELECT * FROM read_csv_auto(?, HEADER=TRUE, ALL_VARCHAR=TRUE)
        """, [files])
        # materializa a leitura
        con.execute("SELECT COUNT(*) FROM t_raw").fetchone()
        t_import = time.perf_counter() - t0

        # 2) transformações (clean + hour + group by)
        t1 = time.perf_counter()
        # limpar tip_amount e fazer cast
        con.execute(f"""
            CREATE TEMP TABLE t_clean AS
            SELECT
              -- pickup timestamp
              TRY_CAST({args.pickup_col} AS TIMESTAMP) AS {args.pickup_col},
              -- tip como double após remover símbolos $ e ,
              TRY_CAST(REGEXP_REPLACE(CAST({args.tip_col} AS VARCHAR), '[\\$,]', '') AS DOUBLE) AS {args.tip_col}
            FROM t_raw
        """)

        # agrega média por hora (0..23) somente para tip > 0
        con.execute(f"""
            CREATE TEMP TABLE tips_by_hour AS
            SELECT
              EXTRACT(hour FROM {args.pickup_col})::INT AS hour,
              AVG({args.tip_col}) AS avg_tip_amount
            FROM t_clean
            WHERE {args.tip_col} > 0 AND {args.pickup_col} IS NOT NULL
            GROUP BY hour
            ORDER BY hour
        """)

        # materializa resultado
        con.execute("SELECT COUNT(*) FROM tips_by_hour").fetchone()
        t_transform = time.perf_counter() - t1

        # Print no console (0..23)
        rows = con.execute("SELECT hour, avg_tip_amount FROM tips_by_hour ORDER BY hour").fetchall()
        print("hour\tavg_tip_amount")
        for h, avg_tip in rows:
            print(f"{h}\t{avg_tip}")

        # 3) salvar em Parquet (arquivo único)
        # 3) salvar em Parquet (arquivo único)
        t2 = time.perf_counter()
        out_path = args.output.replace("'", "''")  # escapa aspas simples para literal SQL
        con.execute(f"COPY (SELECT * FROM tips_by_hour ORDER BY hour) TO '{out_path}' (FORMAT PARQUET)")
        t_save = time.perf_counter() - t2


        con.close()

    out_size = get_output_size_mb(args.output)
    insert_result_csv((tool, args.exp, t_import, t_transform, t_save, mm.avg_mb, mm.peak_mb, out_size))
    print(
        f"[OK] {tool} exp={args.exp} -> import={t_import:.3f}s, transform={t_transform:.3f}s, "
        f"save={t_save:.3f}s, mem_avg={mm.avg_mb:.1f}MB, mem_peak={mm.peak_mb:.1f}MB, out={out_size:.2f}MB",
        flush=True
    )
