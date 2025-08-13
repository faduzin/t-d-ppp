import time
from pyspark.sql import SparkSession
import pyspark.sql.functions as f
from runner_utils import MemoryMonitor, insert_result_csv, get_output_size_mb, parse_common_args

if __name__ == "__main__":
    args, files = parse_common_args("/results/pyspark_output")  # diretório parquet de saída
    tool = "pyspark"

    spark = (
        SparkSession.builder
        .appName("experiment-pyspark")
        .config("spark.sql.session.timeZone", "UTC")
        # .master("local[4]")  # opcional: fixe nº de threads
        .getOrCreate()
    )

    with MemoryMonitor() as mm:
        # 1) importar + concatenar (materializa para medir)
        t0 = time.perf_counter()
        df = (
            spark.read
            .option("header", True)
            .option("inferSchema", True)   # para seu dataset costuma funcionar bem
            .csv(files)
        )
        df_cached = df.cache()
        _ = df_cached.count()
        t_import = time.perf_counter() - t0

        # 2) transformações (materializa para medir)
        t1 = time.perf_counter()

        # Cast robusto: remove $ e , se houver, depois converte para double
        tip_col_clean = f.regexp_replace(f.col(args.tip_col).cast("string"), r"[\$,]", "")
        df2 = (
            df_cached
            .withColumn(args.pickup_col, f.to_timestamp(f.col(args.pickup_col)))
            .filter(tip_col_clean.isNotNull())
            .withColumn(args.tip_col, tip_col_clean.cast("double"))
            .filter(f.col(args.tip_col) > 0)
        )

        # Cria coluna da HORA (0–23) do pickup
        df2 = df2.withColumn("hour", f.hour(f.col(args.pickup_col)))

        # Agrupamento: média de tip_amount por hour
        result = (
            df2.groupBy("hour")
               .agg(f.avg(f.col(args.tip_col)).alias("avg_tip_amount"))
               .orderBy("hour")
        )

        # materializa o resultado (conta 24 linhas, via count)
        result_cached = result.cache()
        _ = result_cached.count()
        t_transform = time.perf_counter() - t1

        # Print no console (24 linhas esperadas: 0..23)
        result_cached.show(24, truncate=False)

        # 3) salvar (parquet em diretório)
        t2 = time.perf_counter()
        result_cached.write.mode("overwrite").parquet(args.output)
        t_save = time.perf_counter() - t2

    spark.stop()

    out_size = get_output_size_mb(args.output)  # soma dos arquivos do diretório parquet
    insert_result_csv((tool, args.exp, t_import, t_transform, t_save, mm.avg_mb, mm.peak_mb, out_size))
    print(
        f"[OK] {tool} exp={args.exp} -> import={t_import:.3f}s, transform={t_transform:.3f}s, "
        f"save={t_save:.3f}s, mem_avg={mm.avg_mb:.1f}MB, mem_peak={mm.peak_mb:.1f}MB, out={out_size:.2f}MB",
        flush=True
    )
