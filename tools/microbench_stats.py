#!/usr/bin/env python3
import argparse
import csv
import math
import os
from collections import defaultdict
from pathlib import Path


def read_csv(path):
    with open(path, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def write_csv(path, fieldnames, rows):
    out_path = Path(path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for row in rows:
            w.writerow(row)


def mean_std(values):
    n = len(values)
    if n == 0:
        return 0.0, 0.0
    mu = sum(values) / n
    if n == 1:
        return mu, 0.0
    var = sum((x - mu) ** 2 for x in values) / (n - 1)
    return mu, math.sqrt(var)


def is_constant(values, tol=1e-9):
    if not values:
        return True
    first = values[0]
    return all(abs(v - first) <= tol for v in values[1:])


def latex_m(v):
    return "$" + f"{int(v):,}".replace(",", "{,}") + "$"


def fmt_num(v, digits=3):
    return f"{v:.{digits}f}"


def fmt_int(v):
    return str(int(round(v)))


def generate_glox_gc_table(path, rows, iters, repeats):
    lines = []
    lines.append("\\begin{table}[t]")
    lines.append("\\centering")
    lines.append("\\small")
    lines.append("\\setlength{\\tabcolsep}{4pt}")
    lines.append("\\begin{tabular}{r r r r r r r}")
    lines.append("\\toprule")
    lines.append("$M$ & $S_0$ GC bytes & $S_1$ GC bytes & $S_0$ GC ms (mean) & $S_0$ $\\sigma$ & $S_1$ GC ms (mean) & $S_1$ $\\sigma$ \\\\")
    lines.append("\\midrule")
    for r in rows:
        lines.append(
            f"{latex_m(r['M'])} & {fmt_int(r['s0_gc_bytes'])} & {fmt_int(r['s1_gc_bytes'])} & "
            f"{fmt_num(r['s0_gc_ms_mean'])} & {fmt_num(r['s0_gc_ms_std'])} & "
            f"{fmt_num(r['s1_gc_ms_mean'])} & {fmt_num(r['s1_gc_ms_std'])} \\\\"
        )
    lines.append("\\bottomrule")
    lines.append("\\end{tabular}")
    lines.append(
        f"\\caption{{Measured inter-server EMP cost for the G-Lox microbenchmark over {repeats} repeated runs per $M$ "
        f"(each run uses \\texttt{{iters}}={iters} with \\texttt{{ops=gb,rb\\_best,dir}}). "
        "GC-byte fields are deterministic and shown as fixed values; runtime variability is shown only for GC ms.}"
    )
    lines.append("\\label{tab:glox-micro-gc}")
    lines.append("\\end{table}")
    Path(path).write_text("\n".join(lines) + "\n", encoding="utf-8")


def generate_glox_client_table(path, rows, iters, repeats, ops):
    lines = []
    lines.append("\\begin{table}[t]")
    lines.append("\\centering")
    lines.append("\\small")
    lines.append("\\setlength{\\tabcolsep}{4pt}")
    lines.append("\\begin{tabular}{r r r r r r}")
    lines.append("\\toprule")
    lines.append("$M$ & $|k_{\\mathsf{state}}|$ (B) & Sent/iter (B) & Recv/iter (B) & Mean ms/iter & $\\sigma$ \\\\")
    lines.append("\\midrule")
    for r in rows:
        lines.append(
            f"{latex_m(r['M'])} & {fmt_int(r['k_state'])} & {fmt_int(r['sent_per_iter_B'])} & "
            f"{fmt_int(r['recv_per_iter_B'])} & {fmt_num(r['mean_iter_ms_mean'])} & {fmt_num(r['mean_iter_ms_std'])} \\\\"
        )
    lines.append("\\bottomrule")
    lines.append("\\end{tabular}")
    lines.append(
        f"\\caption{{Measured client-visible cost for the G-Lox microbenchmark over {repeats} repeated runs per $M$ "
        f"(\\texttt{{iters}}={iters}, \\texttt{{ops={ops}}}). Communication-size fields are deterministic and shown as fixed values.}}"
    )
    lines.append("\\label{tab:glox-micro-client}")
    lines.append("\\end{table}")
    Path(path).write_text("\n".join(lines) + "\n", encoding="utf-8")


def generate_glox_memory_table(path, rows, iters, repeats):
    lines = []
    lines.append("\\begin{table}[t]")
    lines.append("\\centering")
    lines.append("\\small")
    lines.append("\\setlength{\\tabcolsep}{4pt}")
    lines.append("\\begin{tabular}{r r r r r r r}")
    lines.append("\\toprule")
    lines.append("$M$ & Client peak MB (mean) & $\\sigma$ & State max peak MB (mean) & $\\sigma$ & Dir max peak MB (mean) & $\\sigma$ \\\\")
    lines.append("\\midrule")
    for r in rows:
        lines.append(
            f"{latex_m(r['M'])} & {fmt_num(r['client_hwm_mb_mean'])} & {fmt_num(r['client_hwm_mb_std'])} & "
            f"{fmt_num(r['state_max_hwm_mb_mean'])} & {fmt_num(r['state_max_hwm_mb_std'])} & "
            f"{fmt_num(r['dir_max_hwm_mb_mean'])} & {fmt_num(r['dir_max_hwm_mb_std'])} \\\\"
        )
    lines.append("\\bottomrule")
    lines.append("\\end{tabular}")
    lines.append(
        f"\\caption{{Peak memory (VmHWM) for the G-Lox microbenchmark over {repeats} repeated runs per $M$ "
        f"(\\texttt{{iters}}={iters}). State and directory columns take the maximum across their two servers in each run; "
        "the table reports mean and standard deviation across runs.}"
    )
    lines.append("\\label{tab:glox-micro-memory}")
    lines.append("\\end{table}")
    Path(path).write_text("\n".join(lines) + "\n", encoding="utf-8")


def generate_combined_table(path, glox_rows, lox_rows, iters, ops, glox_repeats, lox_repeats):
    lox_by_m = {int(r["M"]): r for r in lox_rows}
    lines = []
    lines.append("\\begin{table*}[t]")
    lines.append("\\centering")
    lines.append("\\small")
    lines.append("\\setlength{\\tabcolsep}{4pt}")
    lines.append("\\begin{tabular}{r ccccc ccccc}")
    lines.append("\\toprule")
    lines.append("& \\multicolumn{5}{c}{\\textbf{G-Lox}} & \\multicolumn{5}{c}{\\textbf{Open-source Lox}} \\\\")
    lines.append("\\cmidrule(lr){2-6} \\cmidrule(lr){7-11}")
    lines.append("$M$ & Sent/iter (KiB) & Recv/iter (KiB) & Total/iter (KiB) & Mean ms/iter & $\\sigma$ & Sent/iter (KiB) & Recv/iter (KiB) & Total/iter (KiB) & Mean ms/iter & $\\sigma$ \\\\")
    lines.append("\\midrule")
    for g in glox_rows:
        m = int(g["M"])
        if m not in lox_by_m:
            continue
        l = lox_by_m[m]
        g_sent_kib = float(g["sent_per_iter_B"]) / 1024.0
        g_recv_kib = float(g["recv_per_iter_B"]) / 1024.0
        g_tot_kib = (float(g["sent_per_iter_B"]) + float(g["recv_per_iter_B"])) / 1024.0
        lines.append(
            f"{latex_m(m)} & {fmt_num(g_sent_kib)} & {fmt_num(g_recv_kib)} & {fmt_num(g_tot_kib)} & "
            f"{fmt_num(float(g['mean_iter_ms_mean']))} & {fmt_num(float(g['mean_iter_ms_std']))} & "
            f"{fmt_num(float(l['sent_per_iter_KiB']))} & {fmt_num(float(l['recv_per_iter_KiB']))} & "
            f"{fmt_num(float(l['total_per_iter_KiB']))} & {fmt_num(float(l['mean_iter_ms_mean']))} & "
            f"{fmt_num(float(l['mean_iter_ms_std']))} \\\\"
        )
    lines.append("\\bottomrule")
    lines.append("\\end{tabular}")
    lines.append(
        "\\caption{Per-iteration client-visible cost for G-Lox and open-source Lox. "
        f"G-Lox communication fields are deterministic per $M$ and shown as fixed values; runtime is reported as mean/standard deviation over {glox_repeats} runs. "
        f"Lox communication fields are deterministic and runtime is reported as mean/standard deviation over {lox_repeats} runs. "
        f"Each run uses \\texttt{{iters}}={iters}; G-Lox uses \\texttt{{ops={ops}}}.}}"
    )
    lines.append("\\label{tab:glox-lox-comparison}")
    lines.append("\\end{table*}")
    Path(path).write_text("\n".join(lines) + "\n", encoding="utf-8")


def build_presentation_summary(
    path,
    glox_repeats,
    lox_repeats,
    glox_det_fields,
    lox_det_fields,
    glox_trials,
    glox_summary,
    lox_trials,
    lox_summary,
):
    lines = []
    lines.append("# Microbenchmark Presentation Summary")
    lines.append("")
    lines.append("## Repetition Counts")
    lines.append(f"- G-Lox runtime statistics: `{glox_repeats}` runs per `M`.")
    lines.append(f"- Lox runtime statistics: `{lox_repeats}` runs per `M`.")
    lines.append("")
    lines.append("## Deterministic vs Runtime-Variable")
    lines.append("- Deterministic (shown as plain values in tables):")
    lines.append(f"  - G-Lox: `{', '.join(glox_det_fields)}`")
    lines.append(f"  - Lox: `{', '.join(lox_det_fields)}`")
    lines.append("- Runtime-variable (shown with standard deviation where informative):")
    lines.append("  - G-Lox: `mean_iter_ms`, `s0_gc_ms`, `s1_gc_ms`, and peak-memory (VmHWM) columns.")
    lines.append("  - Lox: `mean_iter_ms`.")
    lines.append("")
    lines.append("## Table Choices")
    lines.append("- Inter-server EMP table: keep GC-byte columns plain; show mean/std only for GC milliseconds.")
    lines.append("- Peak-memory table: show mean/std for peak VmHWM (client, state-max, directory-max).")
    lines.append("- G-Lox vs Lox comparison: keep communication-size fields plain; show runtime mean/std in separate columns.")
    lines.append("")
    lines.append("## Generated Artifacts")
    lines.append(f"- Raw G-Lox trials: `{glox_trials}`")
    lines.append(f"- Aggregated G-Lox summary: `{glox_summary}`")
    lines.append(f"- Raw Lox trials: `{lox_trials}`")
    lines.append(f"- Aggregated Lox summary: `{lox_summary}`")
    Path(path).write_text("\n".join(lines) + "\n", encoding="utf-8")


def run_glox(args):
    rows_in = read_csv(args.trials)
    if not rows_in:
        raise RuntimeError("No rows found in G-Lox trial CSV.")

    parsed = []
    for r in rows_in:
        parsed.append(
            {
                "trial": int(r["trial"]),
                "M": int(r["M"]),
                "k_state": float(r["k_state"]),
                "sent_per_iter_B": float(r["sent_per_iter_B"]),
                "recv_per_iter_B": float(r["recv_per_iter_B"]),
                "mean_iter_ms": float(r["mean_iter_ms"]),
                "client_rss_mb": float(r["client_rss_mb"]),
                "client_hwm_mb": float(r["client_hwm_mb"]),
                "s0_gc_bytes": float(r["s0_gc_bytes"]),
                "s1_gc_bytes": float(r["s1_gc_bytes"]),
                "gc_bytes_sum": float(r["gc_bytes_sum"]),
                "gc_bytes_max": float(r["gc_bytes_max"]),
                "s0_gc_ms": float(r["s0_gc_ms"]),
                "s1_gc_ms": float(r["s1_gc_ms"]),
                "s0_allow_count": float(r["s0_allow_count"]),
                "s1_allow_count": float(r["s1_allow_count"]),
                "s0_rss_mb": float(r["s0_rss_mb"]),
                "s1_rss_mb": float(r["s1_rss_mb"]),
                "dir_s0_rss_mb": float(r["dir_s0_rss_mb"]),
                "dir_s1_rss_mb": float(r["dir_s1_rss_mb"]),
                "s0_hwm_mb": float(r["s0_hwm_mb"]),
                "s1_hwm_mb": float(r["s1_hwm_mb"]),
                "dir_s0_hwm_mb": float(r["dir_s0_hwm_mb"]),
                "dir_s1_hwm_mb": float(r["dir_s1_hwm_mb"]),
            }
        )

    by_m = defaultdict(list)
    for r in parsed:
        r["state_max_rss_mb"] = max(r["s0_rss_mb"], r["s1_rss_mb"])
        r["state_max_hwm_mb"] = max(r["s0_hwm_mb"], r["s1_hwm_mb"])
        r["dir_max_rss_mb"] = max(r["dir_s0_rss_mb"], r["dir_s1_rss_mb"])
        r["dir_max_hwm_mb"] = max(r["dir_s0_hwm_mb"], r["dir_s1_hwm_mb"])
        by_m[r["M"]].append(r)

    deterministic_candidates = [
        "k_state",
        "sent_per_iter_B",
        "recv_per_iter_B",
        "s0_gc_bytes",
        "s1_gc_bytes",
        "gc_bytes_sum",
        "gc_bytes_max",
    ]
    glox_det_fields = []
    for f in deterministic_candidates:
        stable = True
        for m_rows in by_m.values():
            if not is_constant([x[f] for x in m_rows]):
                stable = False
                break
        if stable:
            glox_det_fields.append(f)

    out_rows = []
    for m in sorted(by_m.keys()):
        m_rows = by_m[m]
        n = len(m_rows)

        def collect(field):
            return [x[field] for x in m_rows]

        mean_iter_mu, mean_iter_sd = mean_std(collect("mean_iter_ms"))
        s0_gc_mu, s0_gc_sd = mean_std(collect("s0_gc_ms"))
        s1_gc_mu, s1_gc_sd = mean_std(collect("s1_gc_ms"))
        client_hwm_mu, client_hwm_sd = mean_std(collect("client_hwm_mb"))
        state_hwm_mu, state_hwm_sd = mean_std(collect("state_max_hwm_mb"))
        dir_hwm_mu, dir_hwm_sd = mean_std(collect("dir_max_hwm_mb"))
        client_rss_mu, client_rss_sd = mean_std(collect("client_rss_mb"))
        state_rss_mu, state_rss_sd = mean_std(collect("state_max_rss_mb"))
        dir_rss_mu, dir_rss_sd = mean_std(collect("dir_max_rss_mb"))

        row0 = m_rows[0]
        sent = row0["sent_per_iter_B"]
        recv = row0["recv_per_iter_B"]
        out_rows.append(
            {
                "M": m,
                "n_runs": n,
                "k_state": row0["k_state"],
                "sent_per_iter_B": sent,
                "recv_per_iter_B": recv,
                "total_per_iter_B": sent + recv,
                "s0_gc_bytes": row0["s0_gc_bytes"],
                "s1_gc_bytes": row0["s1_gc_bytes"],
                "gc_bytes_sum": row0["gc_bytes_sum"],
                "gc_bytes_max": row0["gc_bytes_max"],
                "mean_iter_ms_mean": mean_iter_mu,
                "mean_iter_ms_std": mean_iter_sd,
                "s0_gc_ms_mean": s0_gc_mu,
                "s0_gc_ms_std": s0_gc_sd,
                "s1_gc_ms_mean": s1_gc_mu,
                "s1_gc_ms_std": s1_gc_sd,
                "client_hwm_mb_mean": client_hwm_mu,
                "client_hwm_mb_std": client_hwm_sd,
                "state_max_hwm_mb_mean": state_hwm_mu,
                "state_max_hwm_mb_std": state_hwm_sd,
                "dir_max_hwm_mb_mean": dir_hwm_mu,
                "dir_max_hwm_mb_std": dir_hwm_sd,
                "client_rss_mb_mean": client_rss_mu,
                "client_rss_mb_std": client_rss_sd,
                "state_max_rss_mb_mean": state_rss_mu,
                "state_max_rss_mb_std": state_rss_sd,
                "dir_max_rss_mb_mean": dir_rss_mu,
                "dir_max_rss_mb_std": dir_rss_sd,
            }
        )

    summary_fields = [
        "M",
        "n_runs",
        "k_state",
        "sent_per_iter_B",
        "recv_per_iter_B",
        "total_per_iter_B",
        "s0_gc_bytes",
        "s1_gc_bytes",
        "gc_bytes_sum",
        "gc_bytes_max",
        "mean_iter_ms_mean",
        "mean_iter_ms_std",
        "s0_gc_ms_mean",
        "s0_gc_ms_std",
        "s1_gc_ms_mean",
        "s1_gc_ms_std",
        "client_hwm_mb_mean",
        "client_hwm_mb_std",
        "state_max_hwm_mb_mean",
        "state_max_hwm_mb_std",
        "dir_max_hwm_mb_mean",
        "dir_max_hwm_mb_std",
        "client_rss_mb_mean",
        "client_rss_mb_std",
        "state_max_rss_mb_mean",
        "state_max_rss_mb_std",
        "dir_max_rss_mb_mean",
        "dir_max_rss_mb_std",
    ]
    write_csv(args.summary, summary_fields, out_rows)

    generate_glox_client_table(args.client_table, out_rows, args.iters, args.repeats, args.ops)
    generate_glox_gc_table(args.gc_table, out_rows, args.iters, args.repeats)
    generate_glox_memory_table(args.memory_table, out_rows, args.iters, args.repeats)

    lox_rows = []
    lox_det_fields = ["sent_per_iter_B", "recv_per_iter_B", "total_per_iter_B"]
    lox_repeats = 0
    if args.lox_summary and os.path.exists(args.lox_summary):
        lox_rows = read_csv(args.lox_summary)
        if lox_rows:
            lox_repeats = int(float(lox_rows[0]["n_runs"]))
            generate_combined_table(
                args.combined_table,
                out_rows,
                lox_rows,
                args.iters,
                args.ops,
                args.repeats,
                lox_repeats,
            )
    else:
        Path(args.combined_table).write_text(
            "% Lox summary not found; combined table not generated.\n", encoding="utf-8"
        )

    lox_trials_path = ""
    if args.lox_summary:
        maybe_trials = str(Path(args.lox_summary).with_name("summary_trials.csv"))
        if os.path.exists(maybe_trials):
            lox_trials_path = maybe_trials

    build_presentation_summary(
        args.presentation_summary,
        args.repeats,
        lox_repeats,
        glox_det_fields,
        lox_det_fields,
        args.trials,
        args.summary,
        lox_trials_path,
        args.lox_summary if args.lox_summary else "",
    )


def generate_lox_table(path, rows, iters, repeats):
    lines = []
    lines.append("\\begin{table}[t]")
    lines.append("\\centering")
    lines.append("\\small")
    lines.append("\\setlength{\\tabcolsep}{5pt}")
    lines.append("\\begin{tabular}{r r r r r r}")
    lines.append("\\toprule")
    lines.append("$M_{\\mathrm{eq}}$ & Sent/iter (KiB) & Recv/iter (KiB) & Total/iter (KiB) & Mean ms/iter & $\\sigma$ \\\\")
    lines.append("\\midrule")
    for r in rows:
        lines.append(
            f"{latex_m(r['M'])} & {fmt_num(float(r['sent_per_iter_KiB']))} & {fmt_num(float(r['recv_per_iter_KiB']))} & "
            f"{fmt_num(float(r['total_per_iter_KiB']))} & {fmt_num(float(r['mean_iter_ms_mean']))} & {fmt_num(float(r['mean_iter_ms_std']))} \\\\"
        )
    lines.append("\\bottomrule")
    lines.append("\\end{tabular}")
    lines.append(
        f"\\caption{{Measured open-source Lox baseline over \\texttt{{iters}}={iters} iterations. "
        f"Communication-size fields are deterministic and shown as fixed values; runtime is reported as mean/standard deviation over {repeats} runs.}}"
    )
    lines.append("\\label{tab:lox-baseline-sweep}")
    lines.append("\\end{table}")
    Path(path).write_text("\n".join(lines) + "\n", encoding="utf-8")


def run_lox(args):
    rows_in = read_csv(args.op_trials)
    if not rows_in:
        raise RuntimeError("No rows found in Lox op-trials CSV.")

    by_trial_m = defaultdict(dict)
    for r in rows_in:
        trial = int(r["trial"])
        m = int(r["M"])
        op = r["op"]
        by_trial_m[(trial, m)][op] = {
            "iters": int(r["iters"]),
            "mean_req_bytes": float(r["mean_req_bytes"]),
            "mean_resp_bytes": float(r["mean_resp_bytes"]),
            "mean_total_bytes": float(r["mean_total_bytes"]),
            "mean_total_ms": float(r["mean_total_ms"]),
        }

    bundle_trials = []
    for (trial, m), ops in sorted(by_trial_m.items()):
        if "bundle_total" not in ops:
            continue
        b = ops["bundle_total"]
        bundle_trials.append(
            {
                "trial": trial,
                "M": m,
                "iters": b["iters"],
                "sent_per_iter_B": b["mean_req_bytes"],
                "recv_per_iter_B": b["mean_resp_bytes"],
                "total_per_iter_B": b["mean_total_bytes"],
                "mean_iter_ms": b["mean_total_ms"],
                "getbridge_ms": ops.get("getbridge_like", {}).get("mean_total_ms", float("nan")),
                "redeem_ms": ops.get("redeem_like", {}).get("mean_total_ms", float("nan")),
                "reportblocked_ms": ops.get("reportblocked_like", {}).get("mean_total_ms", float("nan")),
            }
        )

    write_csv(
        args.bundle_trials,
        [
            "trial",
            "M",
            "iters",
            "sent_per_iter_B",
            "recv_per_iter_B",
            "total_per_iter_B",
            "mean_iter_ms",
            "getbridge_ms",
            "redeem_ms",
            "reportblocked_ms",
        ],
        bundle_trials,
    )

    by_m = defaultdict(list)
    for r in bundle_trials:
        by_m[int(r["M"])].append(r)

    out_rows = []
    for m in sorted(by_m.keys()):
        m_rows = by_m[m]
        n = len(m_rows)

        def collect(field):
            return [float(x[field]) for x in m_rows]

        sent = collect("sent_per_iter_B")[0]
        recv = collect("recv_per_iter_B")[0]
        total = collect("total_per_iter_B")[0]
        mean_mu, mean_sd = mean_std(collect("mean_iter_ms"))
        gb_mu, gb_sd = mean_std(collect("getbridge_ms"))
        rd_mu, rd_sd = mean_std(collect("redeem_ms"))
        rb_mu, rb_sd = mean_std(collect("reportblocked_ms"))

        out_rows.append(
            {
                "M": m,
                "n_runs": n,
                "iters": int(m_rows[0]["iters"]),
                "sent_per_iter_B": sent,
                "recv_per_iter_B": recv,
                "total_per_iter_B": total,
                "sent_per_iter_KiB": sent / 1024.0,
                "recv_per_iter_KiB": recv / 1024.0,
                "total_per_iter_KiB": total / 1024.0,
                "mean_iter_ms_mean": mean_mu,
                "mean_iter_ms_std": mean_sd,
                "getbridge_ms_mean": gb_mu,
                "getbridge_ms_std": gb_sd,
                "redeem_ms_mean": rd_mu,
                "redeem_ms_std": rd_sd,
                "reportblocked_ms_mean": rb_mu,
                "reportblocked_ms_std": rb_sd,
            }
        )

    summary_fields = [
        "M",
        "n_runs",
        "iters",
        "sent_per_iter_B",
        "recv_per_iter_B",
        "total_per_iter_B",
        "sent_per_iter_KiB",
        "recv_per_iter_KiB",
        "total_per_iter_KiB",
        "mean_iter_ms_mean",
        "mean_iter_ms_std",
        "getbridge_ms_mean",
        "getbridge_ms_std",
        "redeem_ms_mean",
        "redeem_ms_std",
        "reportblocked_ms_mean",
        "reportblocked_ms_std",
    ]
    write_csv(args.summary, summary_fields, out_rows)

    legacy_rows = []
    for r in out_rows:
        legacy_rows.append(
            {
                "M": r["M"],
                "sent_per_iter_B": r["sent_per_iter_B"],
                "recv_per_iter_B": r["recv_per_iter_B"],
                "total_per_iter_B": r["total_per_iter_B"],
                "sent_per_iter_KiB": r["sent_per_iter_KiB"],
                "recv_per_iter_KiB": r["recv_per_iter_KiB"],
                "total_per_iter_KiB": r["total_per_iter_KiB"],
                "mean_iter_ms": r["mean_iter_ms_mean"],
                "getbridge_ms": r["getbridge_ms_mean"],
                "redeem_ms": r["redeem_ms_mean"],
                "reportblocked_ms": r["reportblocked_ms_mean"],
            }
        )
    write_csv(
        args.legacy_summary,
        [
            "M",
            "sent_per_iter_B",
            "recv_per_iter_B",
            "total_per_iter_B",
            "sent_per_iter_KiB",
            "recv_per_iter_KiB",
            "total_per_iter_KiB",
            "mean_iter_ms",
            "getbridge_ms",
            "redeem_ms",
            "reportblocked_ms",
        ],
        legacy_rows,
    )

    iters = out_rows[0]["iters"] if out_rows else args.iters
    generate_lox_table(args.table, out_rows, iters, args.repeats)


def main():
    parser = argparse.ArgumentParser(description="Aggregate microbenchmark trial outputs.")
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_glox = sub.add_parser("glox", help="Aggregate G-Lox sweep trials and generate tables.")
    p_glox.add_argument("--trials", required=True)
    p_glox.add_argument("--summary", required=True)
    p_glox.add_argument("--client-table", required=True)
    p_glox.add_argument("--gc-table", required=True)
    p_glox.add_argument("--memory-table", required=True)
    p_glox.add_argument("--combined-table", required=True)
    p_glox.add_argument("--lox-summary", default="")
    p_glox.add_argument("--presentation-summary", required=True)
    p_glox.add_argument("--iters", type=int, required=True)
    p_glox.add_argument("--repeats", type=int, required=True)
    p_glox.add_argument("--ops", required=True)

    p_lox = sub.add_parser("lox", help="Aggregate repeated Lox baseline runs.")
    p_lox.add_argument("--op-trials", required=True)
    p_lox.add_argument("--bundle-trials", required=True)
    p_lox.add_argument("--summary", required=True)
    p_lox.add_argument("--legacy-summary", required=True)
    p_lox.add_argument("--table", required=True)
    p_lox.add_argument("--repeats", type=int, required=True)
    p_lox.add_argument("--iters", type=int, default=10)

    args = parser.parse_args()
    if args.cmd == "glox":
        run_glox(args)
    elif args.cmd == "lox":
        run_lox(args)


if __name__ == "__main__":
    main()
