import argparse, os, time, glob, threading, psutil, pathlib, csv

def _sum_rss_mb(proc: psutil.Process):
    try:
        rss = proc.memory_info().rss
    except psutil.Error:
        rss = 0
    total = rss
    for ch in proc.children(recursive=True):
        try:
            total += ch.memory_info().rss
        except psutil.Error:
            pass
    return total / (1024 * 1024)

class MemoryMonitor:
    def __init__(self, interval_sec=0.1):
        self.interval = interval_sec
        self.samples = []
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._proc = psutil.Process(os.getpid())

    def _run(self):
        while not self._stop.is_set():
            self.samples.append(_sum_rss_mb(self._proc))
            time.sleep(self.interval)

    def __enter__(self):
        self._thread.start()
        return self

    def __exit__(self, exc_type, exc, tb):
        self._stop.set()
        self._thread.join()

    @property
    def avg_mb(self):
        return float(sum(self.samples)/len(self.samples)) if self.samples else 0.0

    @property
    def peak_mb(self):
        return float(max(self.samples)) if self.samples else 0.0

def get_output_size_mb(path: str) -> float:
    p = pathlib.Path(path)
    if p.is_file():
        return p.stat().st_size / (1024*1024)
    total = 0
    for f in p.rglob("*"):
        if f.is_file():
            total += f.stat().st_size
    return total / (1024*1024)

def insert_result_csv(row, csv_path="/results/metrics.csv"):
    """
    row = (tool, exp_no, t_import, t_transform, t_save, mem_avg, mem_peak, out_size)
    """
    header = ["tool", "exp_no", "t_import_concat_s", "t_transform_s", "t_save_s",
              "mem_avg_mb", "mem_peak_mb", "output_size_mb", "created_at"]
    os.makedirs(os.path.dirname(csv_path), exist_ok=True)
    write_header = not os.path.exists(csv_path)
    with open(csv_path, "a", newline="") as f:
        writer = csv.writer(f)
        if write_header:
            writer.writerow(header)
        writer.writerow(list(row) + [time.strftime("%Y-%m-%d %H:%M:%S")])

def parse_common_args(default_output):
    ap = argparse.ArgumentParser()
    ap.add_argument("--exp", type=int, required=True, help="Número do experimento (inteiro)")
    ap.add_argument("--glob", type=str, default="", help="Padrão de arquivos CSV, ex: /data/*.csv")
    ap.add_argument("--inputs", nargs="*", default=[], help="Lista de CSVs (alternativa ao --glob)")
    ap.add_argument("--pickup-col", type=str, default="tpep_pickup_datetime")
    ap.add_argument("--dropoff-col", type=str, default="tpep_dropoff_datetime")
    ap.add_argument("--tip-col", type=str, default="tip_amount")
    ap.add_argument("--output", type=str, default=default_output)
    args = ap.parse_args()

    files = args.inputs
    if args.glob:
        files = sorted(glob.glob(args.glob))
    if len(files) == 0:
        raise SystemExit("Nenhum CSV encontrado. Use --glob '/data/*.csv' ou --inputs file1 file2 ...")
    return args, files
