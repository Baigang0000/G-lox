# G-Lox

This repository contains:
- A C++ implementation of the G-Lox prototype in `src/glox_real.cpp`
- A microbenchmark sweep pipeline in `run_glox_sweep.sh`
- Data-processing and consistency scripts in `tools/`

## Repository Layout

- `src/glox_real.cpp`: main server/client binary (`glox_real`)
- `run_glox_sweep.sh`: repeated microbenchmark sweep + summary/table generation
- `tools/microbench_stats.py`: aggregates trial CSVs and emits summary tables
- `tools/verify_microbench_consistency.py`: checks table/CSV consistency
- `results/`: generated benchmark outputs

## Prerequisites

- CMake >= 3.16
- C++17 compiler (GCC/Clang/MSVC)
- OpenSSL development package
- Python 3 (for `tools/microbench_stats.py` and verification)
- Bash (for `run_glox_sweep.sh`)

Notes:
- `CMakeLists.txt` fetches EMP dependencies (`emp-tool`, `emp-ot`, `emp-sh2pc`) from GitHub.
- Internet access is required at configure time to fetch those dependencies.

## Build

```bash
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build -j
```

Binary output:
- `build/glox_real`

## Run Microbenchmark Sweep

Default sweep (M in `1024..65536`, repeats = 5, iters = 10):

```bash
bash run_glox_sweep.sh
```

Common overrides:

```bash
REPEATS=5 ITERS=10 OPS=gb,rb_best,dir bash run_glox_sweep.sh
```

Primary outputs:
- `results/glox_sweep/summary_trials.csv`
- `results/glox_sweep/summary.csv`
- `results/glox_sweep/glox_microbenchmark_client_table.tex`
- `results/glox_sweep/glox_microbenchmark_gc_table.tex`
- `results/glox_sweep/glox_microbenchmark_memory_table.tex`
- `results/glox_sweep/glox_lox_combined_table.tex`

## Verify Microbenchmark Consistency

```bash
python3 tools/verify_microbench_consistency.py
```

Expected success message:
- `PASS: deterministic field checks and table/CSV consistency checks completed.`

## Direct Binary Usage (`glox_real`)

From the built binary:

```text
server --party=1|2 --port_client=PORT --peer_host=IP --port_peer=PORT [--dir_server]
       --M=262144 --B=128 (state) or --N=65536 --D=256 (dir) [--server_csv=PATH]
client --iters=1 --cM=262144 --cB=128 --st0=IP:PORT --st1=IP:PORT
       [--dirpir --cN=65536 --cD=256 --dir0=IP:PORT --dir1=IP:PORT]
       [--ops=gb,rb_best,rb_worst,dir]
```

## Recovery Note

After recovery cleanup, `lox_baseline/` currently only contains:
- `lox_baseline/tests/tests.rs` (placeholder path)

If you want to rerun Lox baseline generation via `tools/run_lox_baseline_repeats.sh`, restore the full Lox baseline project into `lox_baseline/` first.
