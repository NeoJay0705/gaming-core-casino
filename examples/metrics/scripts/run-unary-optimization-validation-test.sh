#!/usr/bin/env bash
# run-unary-optimization-validation.sh 的 deterministic contract regression；
# 不啟動 Game、grpcload 或 Redis，也不執行任何 Git staging 操作。
set -Eeuo pipefail

SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
source "$SCRIPT_DIR/run-unary-optimization-validation.sh"

fail() {
	echo "FAIL: $*" >&2
	exit 1
}

assert_success() {
	if ! "$@"; then
		fail "expected success: $*"
	fi
}

assert_failure() {
	if "$@" >/dev/null 2>&1; then
		fail "expected failure: $*"
	fi
}

assert_success validate_positive_integer attempts 3
assert_failure validate_positive_integer attempts 0
assert_success validate_nonnegative_integer payload-bytes 0
assert_failure validate_nonnegative_integer payload-bytes -1
assert_success validate_duration 30s
assert_failure validate_duration 30
assert_success validate_worker_spec 0,4,8
assert_failure validate_worker_spec 0,3,8
assert_success calculate_rps 200 2 >/dev/null
[[ "$(calculate_rps 200 2)" == 100 ]] || fail "calculate_rps returned an unexpected value"

fixture=$(mktemp -d "/tmp/unary-optimization-validation-test.XXXXXX")
trap 'rm -rf "$fixture"' EXIT
ARTIFACT_DIR="$fixture/campaign"
mkdir -p "$ARTIFACT_DIR"
write_summary_header

# Marker 寫入採 temporary file + rename，讀取者不應看到半份 JSON。
marker_dir="$fixture/markers"
mkdir -p "$marker_dir"
assert_success write_marker "$marker_dir" grpc-ready.json run-1
[[ "$(json_string "$marker_dir/grpc-ready.json" run_id)" == run-1 ]] || fail "marker run ID mismatch"
[[ "$(json_string "$marker_dir/grpc-ready.json" timestamp)" =~ T ]] || fail "marker timestamp is missing"
assert_success validate_identity_marker "$marker_dir/grpc-ready.json" run-1
assert_failure validate_identity_marker "$marker_dir/grpc-ready.json" run-2
printf '%s\n' '{"run_id":"run-1"}' >"$fixture/missing-timestamp.json"
assert_failure validate_identity_marker "$fixture/missing-timestamp.json" run-1
printf '%s\n' '{not-json' >"$fixture/malformed-marker.json"
assert_failure validate_identity_marker "$fixture/malformed-marker.json" run-1

before="$fixture/before.prom"
after="$fixture/after.prom"
printf '%s\n' \
	'gaming_core_example_grpc_load_round_trips_total{result="success"} 10' \
	'gaming_core_example_grpc_load_round_trip_duration_seconds_count{result="success"} 10' \
	'gaming_core_example_grpc_load_round_trip_duration_seconds_sum{result="success"} 0.10' \
	'gaming_core_example_grpc_load_round_trip_duration_seconds_bucket{result="success",le="0.001"} 5' \
	'gaming_core_example_grpc_load_round_trip_duration_seconds_bucket{result="success",le="0.010"} 10' \
	'gaming_core_example_grpc_load_round_trip_duration_seconds_bucket{result="success",le="+Inf"} 10' >"$before"
printf '%s\n' \
	'gaming_core_example_grpc_load_round_trips_total{result="success"} 12' \
	'gaming_core_example_grpc_load_round_trip_duration_seconds_count{result="success"} 12' \
	'gaming_core_example_grpc_load_round_trip_duration_seconds_sum{result="success"} 0.14' \
	'gaming_core_example_grpc_load_round_trip_duration_seconds_bucket{result="success",le="0.001"} 6' \
	'gaming_core_example_grpc_load_round_trip_duration_seconds_bucket{result="success",le="0.010"} 12' \
	'gaming_core_example_grpc_load_round_trip_duration_seconds_bucket{result="success",le="+Inf"} 12' >"$after"
[[ "$(metric_delta "$before" "$after" gaming_core_example_grpc_load_round_trips_total 'result="success"')" == 2 ]] || fail "counter delta mismatch"
mean=$(histogram_mean_delta "$before" "$after" gaming_core_example_grpc_load_round_trip_duration_seconds 'result="success"')
awk -v got="$mean" 'BEGIN { exit((got > 0.019999 && got < 0.020001) ? 0 : 1) }' || fail "histogram mean mismatch: $mean"
if [[ -n "$(histogram_quantile_delta "$before" "$after" unknown_family_bucket 'result="success"' 0.95 2>/dev/null || true)" ]]; then
	fail "unknown histogram family unexpectedly succeeded"
fi
[[ "$(histogram_quantile_delta "$before" "$after" gaming_core_example_grpc_load_round_trip_duration_seconds_bucket 'result="success"' 0.95)" == 0.010 ]] || fail "histogram quantile mismatch"

attempt="$fixture/attempt"
mkdir -p "$attempt/orchestration" "$attempt/metrics" "$attempt/host"
printf '%s\n' '{"run_id":"run-1","timestamp":"2026-01-01T00:00:00.000000000Z"}' >"$attempt/orchestration/grpc-ready.json"
cp "$before" "$attempt/metrics/game-baseline.prom"
cp "$after" "$attempt/metrics/game-final.prom"
printf '%s\n' \
	'go_sched_gomaxprocs_threads 4' \
	'gaming_core_game_gate_commands_in_flight 0' \
	'gaming_core_game_gate_commands_total{command="4043309073",result="success"} 10' \
	'gaming_core_game_gate_command_duration_seconds_count{command="4043309073",result="success"} 10' \
	'gaming_core_game_server_send_requests_total{operation="request_player",result="success"} 10' \
	'gaming_core_game_server_send_duration_seconds_count{operation="request_player",result="success"} 10' >"$attempt/metrics/game-baseline.prom"
printf '%s\n' \
	'go_sched_gomaxprocs_threads 4' \
	'gaming_core_game_gate_commands_in_flight 0' \
	'gaming_core_game_gate_commands_total{command="4043309073",result="success"} 12' \
	'gaming_core_game_gate_command_duration_seconds_count{command="4043309073",result="success"} 12' \
	'gaming_core_game_server_send_requests_total{operation="request_player",result="success"} 12' \
	'gaming_core_game_server_send_duration_seconds_count{operation="request_player",result="success"} 12' >"$attempt/metrics/game-final.prom"
{
	cat "$before"
	printf '%s\n' \
		'go_sched_gomaxprocs_threads 4' \
		'gaming_core_example_grpc_load_round_trips_in_flight 0'
} >"$attempt/metrics/grpcload-baseline.prom"
{
	cat "$after"
	printf '%s\n' \
		'go_sched_gomaxprocs_threads 4' \
		'gaming_core_example_grpc_load_round_trips_in_flight 0'
} >"$attempt/metrics/grpcload-final.prom"
printf '%s\n' '{"run_id":"run-1","timestamp":"2026-01-01T00:00:00.000000000Z","measurement_start":"2026-01-01T00:00:00.000000000Z","admission_end":"2026-01-01T00:00:01.000000000Z","measurement_end":"2026-01-01T00:00:01.100000000Z","admission_duration_seconds":1,"measured_duration_seconds":1.1,"terminal_drain_duration_seconds":0.1,"successful_requests":2,"failed_workers":0}' >"$attempt/orchestration/grpc-measured.json"
printf '%s\n' '{"run_id":"run-1","timestamp":"2026-01-01T00:00:00.000000000Z"}' >"$attempt/orchestration/grpc-start.json"
printf '%s\n' '{"run_id":"run-1","timestamp":"2026-01-01T00:00:01.100000000Z"}' >"$attempt/orchestration/grpc-final-scraped.json"
assert_success validate_measured_marker "$attempt/orchestration/grpc-measured.json" run-1
assert_success validate_marker_set "$attempt" run-1
printf '%s\n' '{"run_id":"run-2","timestamp":"2026-01-01T00:00:00.000000000Z","measurement_start":"2026-01-01T00:00:00.000000000Z","admission_end":"2026-01-01T00:00:01.000000000Z","measurement_end":"2026-01-01T00:00:01.100000000Z","admission_duration_seconds":1,"measured_duration_seconds":1.1,"terminal_drain_duration_seconds":0.1,"successful_requests":2,"failed_workers":0}' >"$fixture/wrong-run-id.json"
assert_failure validate_measured_marker "$fixture/wrong-run-id.json" run-1
printf '%s\n' '{"run_id":"run-1","timestamp":"2026-01-01T00:00:00.000000000Z","measurement_start":"2026-01-01T00:00:00.000000000Z","admission_end":"2026-01-01T00:00:01.000000000Z","measurement_end":"2026-01-01T00:00:01.100000000Z","admission_duration_seconds":0,"measured_duration_seconds":1.1,"terminal_drain_duration_seconds":0.1,"successful_requests":2,"failed_workers":0}' >"$fixture/zero-admission.json"
assert_failure validate_measured_marker "$fixture/zero-admission.json" run-1
printf '%s\n' '{"run_id":"run-1","timestamp":"2026-01-01T00:00:00.000000000Z","measurement_start":"2026-01-01T00:00:00.000000000Z","admission_end":"2026-01-01T00:00:01.000000000Z","measurement_end":"2026-01-01T00:00:01.100000000Z","admission_duration_seconds":1,"measured_duration_seconds":1.1,"terminal_drain_duration_seconds":0.1,"successful_requests":0,"failed_workers":0}' >"$fixture/zero-success.json"
assert_failure validate_measured_marker "$fixture/zero-success.json" run-1
printf '%s\n' '{"run_id":"run-1","timestamp":"2026-01-01T00:00:00.000000000Z","measurement_start":"2026-01-01T00:00:00.000000000Z","admission_end":"2026-01-01T00:00:01.000000000Z","measurement_end":"2026-01-01T00:00:01.100000000Z","admission_duration_seconds":1,"measured_duration_seconds":1.1,"terminal_drain_duration_seconds":0.1,"successful_requests":2,"failed_workers":1}' >"$fixture/failed-workers.json"
assert_success validate_measured_marker "$fixture/failed-workers.json" run-1
[[ "$(marker_workload_reason "$fixture/failed-workers.json")" == request_error ]] || fail "failed worker outcome was not classified as request_error"
assert_success validate_counter_contract "$attempt" 2
assert_success validate_terminal_gauges "$attempt/metrics/game-final.prom" "$attempt/metrics/grpcload-final.prom"
write_metadata "$attempt" 0 1 30s 400 2026-01-01T00:00:00.000000000Z 2026-01-01T00:00:30.000000000Z
grep -q '"run_id":"run-1"' "$attempt/metadata.json" || fail "metadata did not include orchestration run ID"
grep -q '"gomaxprocs_game":4' "$attempt/metadata.json" || fail "metadata did not include fixed GOMAXPROCS"

# Periodic sample 與 host/process sample 必須被視為 artifact contract 的一部分。
printf 'timestamp\tstatus\tpath\n' >"$attempt/metrics/game-samples.tsv"
printf 'timestamp\tstatus\tpath\n' >"$attempt/metrics/grpcload-samples.tsv"
for service in game grpcload; do
	for n in 1 2 3 4 5; do
		sample="$attempt/metrics/$service-$n.prom"
		printf 'go_goroutines %s\n' "$n" >"$sample"
		printf '2026-01-01T00:00:0%dZ\t200\t%s\n' "$n" "$sample" >>"$attempt/metrics/$service-samples.tsv"
	done
done
printf 'CPU usage: 10%% user, 5%% sys, 85%% idle\n' >"$attempt/host/top.txt"
printf 'timestamp\tservice\tpid\tpcpu\tthreads\tmemory\tcontext_switches\tsysbsd\tsysmach\n2026-01-01T00:00:00Z\tgame\t123\t10.0\t4\t1M\t2\t3\t4\n' >"$attempt/host/processes.tsv"
assert_success validate_periodic_metrics "$attempt"
assert_success validate_host_samples "$attempt"
[[ "$(max_sample_metric "$attempt/metrics/game-samples.tsv" go_goroutines)" == 5 ]] || fail "max sampled goroutines mismatch"
[[ "$(min_host_idle "$attempt/host/top.txt")" == 85 ]] || fail "minimum host idle mismatch"

# Invalid rows retain the same number of columns as summary.tsv header.
append_summary "$fixture/missing" 0 1 false startup_failed 0
header_fields=$(awk -F '\t' 'NR == 1 { print NF }' "$ARTIFACT_DIR/summary.tsv")
row_fields=$(awk -F '\t' 'NR == 2 { print NF }' "$ARTIFACT_DIR/summary.tsv")
[[ "$header_fields" == "$row_fields" ]] || fail "invalid summary row has $row_fields fields, want $header_fields"

# Valid row 也必須保留完整欄位，且把 attempt identity 與 measured window 帶入摘要。
append_summary "$attempt" 0 1 true "" 2
valid_row_fields=$(awk -F '\t' 'NR == 3 { print NF }' "$ARTIFACT_DIR/summary.tsv")
[[ "$header_fields" == "$valid_row_fields" ]] || fail "valid summary row has $valid_row_fields fields, want $header_fields"
awk -F '\t' 'NR == 3 { exit !($1 == "0" && $2 == "1" && $3 == "true" && $4 == "" && $5 == "2" && $6 == "1" && $7 == "2" && $24 == "5" && $25 == "5" && $26 == "85") }' "$ARTIFACT_DIR/summary.tsv" || fail "valid summary row values are incomplete"

if grep -Eq '(^|[[:space:]])(git[[:space:]]+(add|reset|checkout|restore|commit)|pkill)([[:space:]]|$)' "$SCRIPT_DIR/run-unary-optimization-validation.sh"; then
	fail "validation script contains a staging-changing or broad process command"
fi
[[ "$GOMAXPROCS_VALUE" == 4 && "$CONCURRENCY" == 400 && "$CLIENT_CONNECTIONS" == 1 && "$PAYLOAD_BYTES" == 32 ]] || fail "fixed experiment parameters changed"

printf 'unary optimization validation script contracts: PASS\n'
