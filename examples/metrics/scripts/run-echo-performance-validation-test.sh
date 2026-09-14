#!/usr/bin/env bash
# Echo validation runner 的 fixture contract tests；不啟動 Go process、Redis 或壓測。
set -Eeuo pipefail

SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
source "$SCRIPT_DIR/run-echo-performance-validation.sh"

fail() { echo "FAIL: $*" >&2; exit 1; }

assert_success() {
	if ! "$@"; then fail "expected success: $*"; fi
}

assert_failure() {
	if "$@" >/dev/null 2>&1; then fail "expected failure: $*"; fi
}

fixture=$(mktemp -d "/tmp/echo-performance-validation-test.XXXXXX")
trap 'rm -rf "$fixture"' EXIT

write_metrics_fixture() {
	local directory=$1 value=$2
	mkdir -p "$directory/metrics"
	cat >"$directory/metrics/load-baseline.prom" <<'EOF'
go_sched_gomaxprocs_threads 2
gaming_core_example_load_echo_round_trips_in_flight 0
gaming_core_example_load_echo_round_trips_total{result="success"} 0
gaming_core_example_load_echo_round_trips_total{result="error"} 0
gaming_core_example_load_echo_round_trips_total{result="cancelled"} 0
EOF
	cat >"$directory/metrics/gate-baseline.prom" <<'EOF'
go_sched_gomaxprocs_threads 2
gaming_core_gate_websocket_connection_closes_total{reason="client_closed"} 0
EOF
	cat >"$directory/metrics/game-baseline.prom" <<'EOF'
go_sched_gomaxprocs_threads 2
EOF
	cat >"$directory/metrics/load-final.prom" <<EOF
go_sched_gomaxprocs_threads 2
gaming_core_example_load_echo_round_trips_in_flight 0
gaming_core_example_load_echo_round_trips_total{result="success"} $value
gaming_core_example_load_echo_round_trips_total{result="error"} 0
gaming_core_example_load_echo_round_trips_total{result="cancelled"} 0
gaming_core_example_load_echo_round_trip_duration_seconds_count{result="success"} $value
EOF
	cat >"$directory/metrics/gate-final.prom" <<EOF
go_sched_gomaxprocs_threads 2
gaming_core_gate_websocket_connections 0
gaming_core_gate_websocket_commands_in_flight 0
gaming_core_gate_game_grpc_in_flight 0
gaming_core_gate_websocket_writes_in_flight 0
gaming_core_gate_websocket_write_queue_messages 0
gaming_core_gate_websocket_connection_closes_total{reason="client_closed"} $value
gaming_core_gate_websocket_commands_total{command="forward",result="success",route="game"} $value
gaming_core_gate_websocket_command_duration_seconds_count{command="forward",result="success",route="game"} $value
gaming_core_gate_game_grpc_requests_total{code="OK"} $value
gaming_core_gate_game_grpc_duration_seconds_count{code="OK"} $value
gaming_core_gate_server_send_requests_total{result="queued",target="connection"} $value
gaming_core_gate_server_send_delivery_duration_seconds_count{result="success",target="connection"} $value
gaming_core_gate_websocket_writes_total{result="success",source="server_send"} $value
gaming_core_gate_websocket_write_duration_seconds_count{result="success",source="server_send"} $value
EOF
	cat >"$directory/metrics/game-final.prom" <<EOF
go_sched_gomaxprocs_threads 2
gaming_core_game_gate_commands_in_flight 0
gaming_core_game_server_send_in_flight{operation="request_player"} 0
gaming_core_game_gate_commands_total{command="4043309073",result="success"} $value
gaming_core_game_gate_command_duration_seconds_count{command="4043309073",result="success"} $value
gaming_core_game_server_send_requests_total{operation="request_player",result="success"} $value
gaming_core_game_server_send_duration_seconds_count{operation="request_player",result="success"} $value
EOF
	for service in game gate load; do
		printf 'timestamp\tstatus\tpath\n' >"$directory/metrics/$service-samples.tsv"
		for sample in 1 2 3 4 5; do
			path="$directory/metrics/$service-$sample.prom"
			printf '# sample %s\n' "$sample" >"$path"
			printf '2026-01-01T00:00:0%sZ\t200\t%s\n' "$sample" "$path" >>"$directory/metrics/$service-samples.tsv"
		done
	done
}

valid="$fixture/valid"
write_metrics_fixture "$valid" 2
mkdir -p "$fixture/summary/connections-0001/attempt-01"
printf '{"valid":true,"reason":"","rps":100,"p95_seconds":0.1}\n' >"$fixture/summary/connections-0001/attempt-01/run-status.json"
ARTIFACT_DIR="$fixture/summary"
append_summary 1 1
grep -q $'1\t01\ttrue\t100\t0.1' "$fixture/summary/summary.tsv" || fail "summary append failed"
assert_success validate_baseline "$valid"
assert_success validate_counter_contract "$valid" 2
assert_success validate_error_contract "$valid" 2
assert_failure validate_error_contract "$valid" 3
assert_success validate_periodic_metrics "$valid"
assert_success validate_terminal_gauges "$valid/metrics/gate-final.prom" "$valid/metrics/game-final.prom" "$valid/metrics/load-final.prom"
assert_success validate_close_contract "$valid/metrics/gate-baseline.prom" "$valid/metrics/gate-final.prom" 2
sed 's/reason="client_closed"} 2/reason="read_error"} 2/' "$valid/metrics/gate-final.prom" >"$valid/metrics/gate-close-mismatch.prom"
assert_failure validate_close_contract "$valid/metrics/gate-baseline.prom" "$valid/metrics/gate-close-mismatch.prom" 2

if [[ "$(calculate_rps 1000 10)" != "100" ]]; then
	fail "RPS used a duration other than admission duration"
fi
assert_failure calculate_rps 1000 0
assert_failure calculate_rps 1000 -1
assert_failure calculate_rps 1000 invalid

assert_failure bash "$SCRIPT_DIR/run-echo-performance-validation.sh" --duration invalid

cat >"$fixture/echo-measured.json" <<'EOF'
{"run_id":"echo-test","timestamp":"2026-01-01T00:00:00Z","measurement_start":"2026-01-01T00:00:00Z","admission_end":"2026-01-01T00:00:10Z","measurement_end":"2026-01-01T00:00:12Z","admission_duration_seconds":10,"measured_duration_seconds":12,"terminal_drain_duration_seconds":2,"successful_echo_requests":1000,"connection_failures":0}
EOF
assert_success validate_measurement_marker "$fixture/echo-measured.json"
sed 's/"connection_failures":0/"connection_failures":1/' "$fixture/echo-measured.json" >"$fixture/echo-measured-failure.json"
assert_success validate_measurement_marker "$fixture/echo-measured-failure.json"
assert_failure validate_connection_failures_zero "$fixture/echo-measured-failure.json"
sed 's/"terminal_drain_duration_seconds":2/"terminal_drain_duration_seconds":e/' "$fixture/echo-measured.json" >"$fixture/echo-measured-invalid-number.json"
assert_failure validate_measurement_marker "$fixture/echo-measured-invalid-number.json"
sed 's/"successful_echo_requests":1000/"successful_echo_requests":1.5/' "$fixture/echo-measured.json" >"$fixture/echo-measured-invalid-count.json"
assert_failure validate_measurement_marker "$fixture/echo-measured-invalid-count.json"

profile_cpu="$fixture/profile-cpu"
mkdir -p "$profile_cpu/profiles"
printf 'event\ttarget\tstart\tend\tstatus\tpid\n' >"$profile_cpu/profiles/status.tsv"
for target in game gate load; do
	printf 'profile\n' >"$profile_cpu/profiles/$target-cpu-20s.pb.gz"
	printf 'goroutine\n' >"$profile_cpu/profiles/$target-goroutine.txt"
	printf 'cpu\t%s\t2026-01-01T00:00:00Z\t2026-01-01T00:00:20Z\tok\t1\n' "$target" >>"$profile_cpu/profiles/status.tsv"
	printf 'goroutine\t%s\t2026-01-01T00:00:00Z\t2026-01-01T00:00:20Z\tok\t1\n' "$target" >>"$profile_cpu/profiles/status.tsv"
done
assert_success validate_profile_artifacts "$profile_cpu" cpu
: >"$profile_cpu/profiles/game-goroutine.txt"
assert_failure validate_profile_artifacts "$profile_cpu" cpu

profile_trace="$fixture/profile-trace"
mkdir -p "$profile_trace/profiles"
printf 'event\ttarget\tstart\tend\tstatus\tpid\n' >"$profile_trace/profiles/status.tsv"
for target in game gate load; do
	printf 'trace\n' >"$profile_trace/profiles/$target-trace-5s.out"
	printf 'trace\t%s\t2026-01-01T00:00:00Z\t2026-01-01T00:00:05Z\tok\t1\n' "$target" >>"$profile_trace/profiles/status.tsv"
done
assert_success validate_profile_artifacts "$profile_trace" trace
awk -F '\t' 'BEGIN { OFS="\t" } NR == 2 {$5="error"} {print}' "$profile_trace/profiles/status.tsv" >"$profile_trace/profiles/status-error.tsv"
mv "$profile_trace/profiles/status-error.tsv" "$profile_trace/profiles/status.tsv"
assert_failure validate_profile_artifacts "$profile_trace" trace

periodic_error="$fixture/periodic-error"
cp -R "$valid" "$periodic_error"
awk -F '\t' 'BEGIN { OFS="\t" } NR == 2 {$2="error"} {print}' "$periodic_error/metrics/game-samples.tsv" >"$periodic_error/metrics/game-samples-error.tsv"
mv "$periodic_error/metrics/game-samples-error.tsv" "$periodic_error/metrics/game-samples.tsv"
assert_failure validate_periodic_metrics "$periodic_error"
periodic_missing="$fixture/periodic-missing"
cp -R "$valid" "$periodic_missing"
awk -F '\t' 'BEGIN { OFS="\t" } NR == 2 {$3=$3 ".missing"} {print}' "$periodic_missing/metrics/game-samples.tsv" >"$periodic_missing/metrics/game-samples-missing.tsv"
mv "$periodic_missing/metrics/game-samples-missing.tsv" "$periodic_missing/metrics/game-samples.tsv"
assert_failure validate_periodic_metrics "$periodic_missing"

cat >"$fixture/histogram-before.prom" <<'EOF'
gaming_core_example_load_echo_round_trip_duration_seconds_bucket{result="success",le="0.1"} 0
gaming_core_example_load_echo_round_trip_duration_seconds_bucket{result="success",le="0.5"} 0
gaming_core_example_load_echo_round_trip_duration_seconds_bucket{result="success",le="+Inf"} 0
EOF
cat >"$fixture/histogram-after.prom" <<'EOF'
gaming_core_example_load_echo_round_trip_duration_seconds_bucket{result="success",le="0.1"} 2
gaming_core_example_load_echo_round_trip_duration_seconds_bucket{result="success",le="0.5"} 2
gaming_core_example_load_echo_round_trip_duration_seconds_bucket{result="success",le="+Inf"} 2
EOF
if [[ "$(histogram_quantile_delta "$fixture/histogram-before.prom" "$fixture/histogram-after.prom" gaming_core_example_load_echo_round_trip_duration_seconds_bucket 'result="success"' 0.95)" != 0.1 ]]; then
	fail "histogram quantile did not use cumulative bucket deltas"
fi
assert_failure histogram_quantile_delta "$fixture/histogram-before.prom" "$fixture/histogram-before.prom" gaming_core_example_load_echo_round_trip_duration_seconds_bucket 'result="success"' 0.95
assert_failure histogram_quantile_delta "$fixture/histogram-before.prom" "$fixture/missing.prom" gaming_core_example_load_echo_round_trip_duration_seconds_bucket 'result="success"' 0.95
cat >"$fixture/histogram-infinite.prom" <<'EOF'
gaming_core_example_load_echo_round_trip_duration_seconds_bucket{result="success",le="0.1"} 0
gaming_core_example_load_echo_round_trip_duration_seconds_bucket{result="success",le="+Inf"} 2
EOF
assert_failure histogram_quantile_delta "$fixture/histogram-before.prom" "$fixture/histogram-infinite.prom" gaming_core_example_load_echo_round_trip_duration_seconds_bucket 'result="success"' 0.95

sed 's/result="success"} 2/result="success"} 1/' "$valid/metrics/load-final.prom" >"$valid/metrics/load-mismatch.prom"
mv "$valid/metrics/load-mismatch.prom" "$valid/metrics/load-final.prom"
assert_failure validate_counter_contract "$valid" 2

PLATEAU_ALLOWANCE=0
mkdir -p "$fixture/plateau/connections-0100" "$fixture/plateau/connections-0200"
for level in 0100 0200; do
	for attempt in 01 02 03; do
		if [[ "$level" == 0100 ]]; then rps=100; p95=0.1; else rps=102; p95=0.11; fi
		mkdir -p "$fixture/plateau/connections-$level/attempt-$attempt"
		printf '{"valid":true,"rps":%s,"p95_seconds":%s}\n' "$rps" "$p95" >"$fixture/plateau/connections-$level/attempt-$attempt/run-status.json"
	done
done
ARTIFACT_DIR="$fixture/plateau"
assert_success should_stop_after_level 100 200

PLATEAU_ALLOWANCE=0
for level in 0100 0200; do
	for attempt in 01 02 03; do
		if [[ "$level" == 0100 ]]; then rps=100; else rps=101; fi
		mkdir -p "$fixture/stable/connections-$level/attempt-$attempt"
		printf '{"valid":true,"rps":%s,"p95_seconds":0.1}\n' "$rps" >"$fixture/stable/connections-$level/attempt-$attempt/run-status.json"
	done
done
ARTIFACT_DIR="$fixture/stable"
assert_failure should_stop_after_level 100 200
assert_success should_stop_after_level 100 200

mkdir -p "$fixture/saturated/connections-0200/attempt-01/host"
printf 'timestamp\tservice\tpid\tpcpu\trss_kb\tthreads\n' >"$fixture/saturated/connections-0200/attempt-01/host/processes.tsv"
for second in 1 2 3 4 5; do printf '2026-01-01T00:00:0%sZ\tgame\t1\t190\t100\t4\n' "$second" >>"$fixture/saturated/connections-0200/attempt-01/host/processes.tsv"; done
ARTIFACT_DIR="$fixture/saturated"
assert_success sustained_cpu_saturation 200

host_dir="$fixture/host"
mkdir -p "$host_dir"
printf 'top sample\n' >"$host_dir/top.txt"
printf 'nettop sample\n' >"$host_dir/nettop.csv"
printf 'timestamp\tlocal_port\tremote_port\tsocket\n2026-01-01T00:00:00Z\t18080\t1234\tESTABLISHED\n' >"$host_dir/socket-queues.tsv"
printf 'timestamp\tservice\tpid\tpcpu\tmemory\tthreads\tcontext_switches\tsysbsd\tsysmach\n' >"$host_dir/processes.tsv"
for service in game gate load; do
	for second in 1 2 3 4 5; do
		printf '2026-01-01T00:00:0%sZ\t%s\t1\t10.0\t10M\t2\t3\t4\t5\n' "$second" "$service" >>"$host_dir/processes.tsv"
	done
done
assert_success validate_host_samples "$fixture"

awk -F '\t' 'BEGIN {OFS="\t"} NR == 2 {$4=$5=$6=$7=$8=$9="NA"} {print}' "$host_dir/processes.tsv" >"$host_dir/processes-invalid.tsv"
mv "$host_dir/processes-invalid.tsv" "$host_dir/processes.tsv"
assert_failure validate_host_samples "$fixture"

status_dir="$fixture/status"
mkdir -p "$status_dir"
write_status "$status_dir" false counter_mismatch 3
grep -q '"valid":false' "$status_dir/run-status.json" || fail "invalid status missing valid=false"
grep -q 'counter_mismatch' "$status_dir/run-status.json" || fail "invalid status missing bounded reason"

printf 'echo validation script contracts: PASS\n'
