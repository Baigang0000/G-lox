#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
OUT_DIR="${1:-$ROOT_DIR/results/lox_baseline}"
REPEATS="${REPEATS:-5}"
TEST_NAME="${TEST_NAME:-stats_test_glox_microbench_baseline}"

mkdir -p "$OUT_DIR/trials"

OP_TRIALS_CSV="$OUT_DIR/op_trials.csv"
BUNDLE_TRIALS_CSV="$OUT_DIR/summary_trials.csv"
SUMMARY_STATS_CSV="$OUT_DIR/summary_stats.csv"
SUMMARY_LEGACY_CSV="$OUT_DIR/summary.csv"
TABLE_TEX="$OUT_DIR/lox_baseline_bundle_table.tex"

cat > "$OP_TRIALS_CSV" <<'EOF'
trial,M,iters,op,mean_req_bytes,mean_resp_bytes,mean_total_bytes,mean_total_ms
EOF

for trial in $(seq 1 "$REPEATS"); do
  LOG_FILE="$OUT_DIR/trials/trial_${trial}.log"
  echo "[run_lox_baseline_repeats] trial=$trial/$REPEATS"
  (
    cd "$ROOT_DIR/lox_baseline"
    cargo test --release -- --nocapture "$TEST_NAME"
  ) > "$LOG_FILE" 2>&1

  awk -F, -v trial="$trial" '
    BEGIN { OFS="," }
    /^lox_baseline,/ {
      print trial, $2, $3, $4, $5, $6, $7, $8
    }
  ' "$LOG_FILE" >> "$OP_TRIALS_CSV"
done

python3 "$ROOT_DIR/tools/microbench_stats.py" lox \
  --op-trials "$OP_TRIALS_CSV" \
  --bundle-trials "$BUNDLE_TRIALS_CSV" \
  --summary "$SUMMARY_STATS_CSV" \
  --legacy-summary "$SUMMARY_LEGACY_CSV" \
  --table "$TABLE_TEX" \
  --repeats "$REPEATS"

echo "[run_lox_baseline_repeats] wrote:"
echo "  $OP_TRIALS_CSV"
echo "  $BUNDLE_TRIALS_CSV"
echo "  $SUMMARY_STATS_CSV"
echo "  $SUMMARY_LEGACY_CSV"
echo "  $TABLE_TEX"
