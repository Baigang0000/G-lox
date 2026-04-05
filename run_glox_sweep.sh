#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")" && pwd)"
BIN="$ROOT_DIR/build/glox_real"
OUT_DIR="${1:-$ROOT_DIR/results/glox_sweep}"

ITERS="${ITERS:-10}"
REPEATS="${REPEATS:-5}"
B="${B:-128}"
N="${N:-65536}"
D="${D:-256}"
OPS="${OPS:-gb,rb_best,dir}"
LOX_SUMMARY_CSV="${LOX_SUMMARY_CSV:-$ROOT_DIR/results/lox_baseline/summary_stats.csv}"
M_VALUES=(1024 2048 4096 8192 16384 32768 65536)

mkdir -p "$OUT_DIR"
TRIALS_CSV="$OUT_DIR/summary_trials.csv"
SUMMARY_CSV="$OUT_DIR/summary.csv"
CLIENT_TABLE_TEX="$OUT_DIR/glox_microbenchmark_client_table.tex"
GC_TABLE_TEX="$OUT_DIR/glox_microbenchmark_gc_table.tex"
MEMORY_TABLE_TEX="$OUT_DIR/glox_microbenchmark_memory_table.tex"
COMBINED_TABLE_TEX="$OUT_DIR/glox_lox_combined_table.tex"
PRESENTATION_SUMMARY_MD="$OUT_DIR/microbenchmark_presentation_summary.md"

PIDS=()

cleanup() {
  if ((${#PIDS[@]})); then
    kill "${PIDS[@]}" 2>/dev/null || true
    wait "${PIDS[@]}" 2>/dev/null || true
    PIDS=()
  fi
}

trap cleanup EXIT

wait_for_files() {
  local tries=0
  while ((tries < 40)); do
    local ready=1
    for f in "$@"; do
      if [[ ! -s "$f" ]]; then
        ready=0
        break
      fi
    done
    if ((ready)); then
      return 0
    fi
    sleep 0.5
    tries=$((tries + 1))
  done
  return 1
}

extract_key_value() {
  local key="$1"
  local file="$2"
  awk -v key="$key" '
    {
      for (i = 1; i <= NF; i++) {
        if ($i ~ ("^" key "=")) {
          split($i, a, "=")
          print a[2]
          exit
        }
      }
    }
  ' "$file"
}

per_iter_value() {
  local total="$1"
  local iters="$2"
  awk -v t="$total" -v n="$iters" 'BEGIN { printf "%.6f", t / n }'
}

write_trials_header() {
  cat > "$TRIALS_CSV" <<'EOF'
trial,M,k_state,sent_per_iter_B,recv_per_iter_B,mean_iter_ms,client_rss_mb,client_hwm_mb,s0_gc_bytes,s1_gc_bytes,gc_bytes_sum,gc_bytes_max,s0_gc_ms,s1_gc_ms,s0_allow_count,s1_allow_count,s0_rss_mb,s1_rss_mb,dir_s0_rss_mb,dir_s1_rss_mb,s0_hwm_mb,s1_hwm_mb,dir_s0_hwm_mb,dir_s1_hwm_mb
EOF
}

append_trial_row() {
  local trial="$1"
  local M="$2"
  local point_dir="$3"

  local client_log="$point_dir/client.log"
  local s0_csv="$point_dir/server_s0.csv"
  local s1_csv="$point_dir/server_s1.csv"
  local d0_csv="$point_dir/dir_server_s0.csv"
  local d1_csv="$point_dir/dir_server_s1.csv"

  local k_state bytes_sent bytes_recv mean_iter_ms client_rss client_hwm
  k_state="$(extract_key_value dpf_key_bytes_state "$client_log")"
  bytes_sent="$(extract_key_value bytes_sent "$client_log")"
  bytes_recv="$(extract_key_value bytes_recv "$client_log")"
  mean_iter_ms="$(extract_key_value mean_iter_ms "$client_log")"
  client_rss="$(extract_key_value client_rss_mb "$client_log")"
  client_hwm="$(extract_key_value client_hwm_mb "$client_log")"

  local s0_gc_ms s0_gc_bytes s0_allow s0_rss s0_hwm
  local s1_gc_ms s1_gc_bytes s1_allow s1_rss s1_hwm
  local d0_rss d0_hwm d1_rss d1_hwm
  IFS=, read -r _ _ _ s0_gc_ms s0_gc_bytes s0_allow _ _ _ _ _ _ _ _ _ _ _ _ _ s0_rss s0_hwm < <(tail -n 1 "$s0_csv")
  IFS=, read -r _ _ _ s1_gc_ms s1_gc_bytes s1_allow _ _ _ _ _ _ _ _ _ _ _ _ _ s1_rss s1_hwm < <(tail -n 1 "$s1_csv")
  IFS=, read -r _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ d0_rss d0_hwm < <(tail -n 1 "$d0_csv")
  IFS=, read -r _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ d1_rss d1_hwm < <(tail -n 1 "$d1_csv")

  local sent_per_iter recv_per_iter gc_bytes_sum gc_bytes_max
  sent_per_iter="$(per_iter_value "$bytes_sent" "$ITERS")"
  recv_per_iter="$(per_iter_value "$bytes_recv" "$ITERS")"
  gc_bytes_sum=$((s0_gc_bytes + s1_gc_bytes))
  gc_bytes_max=$(( s0_gc_bytes > s1_gc_bytes ? s0_gc_bytes : s1_gc_bytes ))

  printf '%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n' \
    "$trial" "$M" "$k_state" "$sent_per_iter" "$recv_per_iter" "$mean_iter_ms" "$client_rss" "$client_hwm" \
    "$s0_gc_bytes" "$s1_gc_bytes" "$gc_bytes_sum" "$gc_bytes_max" "$s0_gc_ms" "$s1_gc_ms" "$s0_allow" "$s1_allow" \
    "$s0_rss" "$s1_rss" "$d0_rss" "$d1_rss" "$s0_hwm" "$s1_hwm" "$d0_hwm" "$d1_hwm" \
    >> "$TRIALS_CSV"
}

run_point() {
  local M="$1"
  local trial="$2"
  local point_dir="$OUT_DIR/M_$M/trial_$trial"
  mkdir -p "$point_dir"
  rm -f "$point_dir"/*

  cleanup

  "$BIN" server --party=1 --port_client=9001 --peer_host=127.0.0.1 --port_peer=9901 \
    --M="$M" --B="$B" --N="$N" --server_csv="$point_dir/server_s0.csv" > "$point_dir/state_s0.log" 2>&1 &
  PIDS+=($!)
  "$BIN" server --party=2 --port_client=9002 --peer_host=127.0.0.1 --port_peer=9901 \
    --M="$M" --B="$B" --N="$N" --server_csv="$point_dir/server_s1.csv" > "$point_dir/state_s1.log" 2>&1 &
  PIDS+=($!)
  "$BIN" server --party=1 --port_client=9101 --peer_host=127.0.0.1 --port_peer=9902 \
    --dir_server --N="$N" --D="$D" --server_csv="$point_dir/dir_server_s0.csv" > "$point_dir/dir_s0.log" 2>&1 &
  PIDS+=($!)
  "$BIN" server --party=2 --port_client=9102 --peer_host=127.0.0.1 --port_peer=9902 \
    --dir_server --N="$N" --D="$D" --server_csv="$point_dir/dir_server_s1.csv" > "$point_dir/dir_s1.log" 2>&1 &
  PIDS+=($!)

  sleep 2

  "$BIN" client --iters="$ITERS" --cM="$M" --cB="$B" \
    --st0=127.0.0.1:9001 --st1=127.0.0.1:9002 \
    --dirpir --cN="$N" --cD="$D" --dir0=127.0.0.1:9101 --dir1=127.0.0.1:9102 \
    --ops="$OPS" --csv="$point_dir/client.csv" > "$point_dir/client.log" 2>&1

  sleep 1
  wait_for_files \
    "$point_dir/server_s0.csv" \
    "$point_dir/server_s1.csv" \
    "$point_dir/dir_server_s0.csv" \
    "$point_dir/dir_server_s1.csv"

  cleanup
  append_trial_row "$trial" "$M" "$point_dir"
}

write_trials_header
for M in "${M_VALUES[@]}"; do
  for trial in $(seq 1 "$REPEATS"); do
    echo "[run_glox_sweep] M=$M trial=$trial/$REPEATS"
    run_point "$M" "$trial"
  done
done

python3 "$ROOT_DIR/tools/microbench_stats.py" glox \
  --trials "$TRIALS_CSV" \
  --summary "$SUMMARY_CSV" \
  --client-table "$CLIENT_TABLE_TEX" \
  --gc-table "$GC_TABLE_TEX" \
  --memory-table "$MEMORY_TABLE_TEX" \
  --combined-table "$COMBINED_TABLE_TEX" \
  --lox-summary "$LOX_SUMMARY_CSV" \
  --presentation-summary "$PRESENTATION_SUMMARY_MD" \
  --iters "$ITERS" \
  --repeats "$REPEATS" \
  --ops "$OPS"

echo "[run_glox_sweep] wrote:"
echo "  $TRIALS_CSV"
echo "  $SUMMARY_CSV"
echo "  $CLIENT_TABLE_TEX"
echo "  $GC_TABLE_TEX"
echo "  $MEMORY_TABLE_TEX"
if [[ -s "$COMBINED_TABLE_TEX" ]]; then
  echo "  $COMBINED_TABLE_TEX"
fi
echo "  $PRESENTATION_SUMMARY_MD"
