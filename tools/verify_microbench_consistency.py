#!/usr/bin/env python3
import csv
import re
import sys
from pathlib import Path


def read_csv(path):
    with open(path, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def extract_table_rows(path):
    rows = {}
    text = Path(path).read_text(encoding="utf-8")
    for line in text.splitlines():
        if "&" not in line or "\\\\" not in line:
            continue
        if not line.strip().startswith("$"):
            continue
        parts = [p.strip().replace("\\\\", "").strip() for p in line.split("&")]
        m = re.sub(r"[^0-9]", "", parts[0])
        if not m:
            continue
        rows[int(m)] = parts[1:]
    return rows


def main():
    glox_trials = read_csv("results/glox_sweep/summary_trials.csv")
    glox_summary = read_csv("results/glox_sweep/summary.csv")
    lox_summary = read_csv("results/lox_baseline/summary_stats.csv")

    det_fields = [
        "k_state",
        "sent_per_iter_B",
        "recv_per_iter_B",
        "s0_gc_bytes",
        "s1_gc_bytes",
        "gc_bytes_sum",
        "gc_bytes_max",
    ]

    by_m = {}
    for row in glox_trials:
        by_m.setdefault(int(row["M"]), []).append(row)
    for m, rows in by_m.items():
        for f in det_fields:
            vals = {row[f] for row in rows}
            if len(vals) != 1:
                raise AssertionError(f"Deterministic field mismatch: M={m}, field={f}, values={sorted(vals)}")

    gc_rows = extract_table_rows("results/glox_sweep/glox_microbenchmark_gc_table.tex")
    mem_rows = extract_table_rows("results/glox_sweep/glox_microbenchmark_memory_table.tex")
    combo_rows = extract_table_rows("results/glox_sweep/glox_lox_combined_table.tex")
    lox_by_m = {int(r["M"]): r for r in lox_summary}

    for s in glox_summary:
        m = int(s["M"])
        gc = gc_rows[m]
        if gc[0] != str(int(float(s["s0_gc_bytes"]))) or gc[1] != str(int(float(s["s1_gc_bytes"]))):
            raise AssertionError(f"GC byte mismatch at M={m}")
        if gc[2] != f"{float(s['s0_gc_ms_mean']):.3f}" or gc[3] != f"{float(s['s0_gc_ms_std']):.3f}":
            raise AssertionError(f"S0 GC ms mismatch at M={m}")
        if gc[4] != f"{float(s['s1_gc_ms_mean']):.3f}" or gc[5] != f"{float(s['s1_gc_ms_std']):.3f}":
            raise AssertionError(f"S1 GC ms mismatch at M={m}")

        mem = mem_rows[m]
        if mem[0] != f"{float(s['client_hwm_mb_mean']):.3f}" or mem[1] != f"{float(s['client_hwm_mb_std']):.3f}":
            raise AssertionError(f"Client peak-memory mismatch at M={m}")
        if mem[2] != f"{float(s['state_max_hwm_mb_mean']):.3f}" or mem[3] != f"{float(s['state_max_hwm_mb_std']):.3f}":
            raise AssertionError(f"State peak-memory mismatch at M={m}")
        if mem[4] != f"{float(s['dir_max_hwm_mb_mean']):.3f}" or mem[5] != f"{float(s['dir_max_hwm_mb_std']):.3f}":
            raise AssertionError(f"Directory peak-memory mismatch at M={m}")

        c = combo_rows[m]
        l = lox_by_m[m]
        g_sent = float(s["sent_per_iter_B"]) / 1024.0
        g_recv = float(s["recv_per_iter_B"]) / 1024.0
        g_total = (float(s["sent_per_iter_B"]) + float(s["recv_per_iter_B"])) / 1024.0
        if c[0] != f"{g_sent:.3f}" or c[1] != f"{g_recv:.3f}" or c[2] != f"{g_total:.3f}":
            raise AssertionError(f"G-Lox communication mismatch in combined table at M={m}")
        if c[3] != f"{float(s['mean_iter_ms_mean']):.3f}" or c[4] != f"{float(s['mean_iter_ms_std']):.3f}":
            raise AssertionError(f"G-Lox runtime mismatch in combined table at M={m}")
        if c[5] != f"{float(l['sent_per_iter_KiB']):.3f}" or c[6] != f"{float(l['recv_per_iter_KiB']):.3f}":
            raise AssertionError(f"Lox communication mismatch in combined table at M={m}")
        if c[7] != f"{float(l['total_per_iter_KiB']):.3f}":
            raise AssertionError(f"Lox total communication mismatch in combined table at M={m}")
        if c[8] != f"{float(l['mean_iter_ms_mean']):.3f}" or c[9] != f"{float(l['mean_iter_ms_std']):.3f}":
            raise AssertionError(f"Lox runtime mismatch in combined table at M={m}")

    print("PASS: deterministic field checks and table/CSV consistency checks completed.")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except AssertionError as exc:
        print(f"FAIL: {exc}")
        raise SystemExit(1)
