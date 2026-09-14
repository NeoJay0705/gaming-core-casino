#!/usr/bin/env bash
# run-server-send-validation.sh 的純驗證 contract regression；不啟動服務或 Redis。
set -Eeuo pipefail

SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
source "$SCRIPT_DIR/run-server-send-validation.sh"

fail() { echo "FAIL: $*" >&2; exit 1; }

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

fixture=$(mktemp -d "/tmp/server-send-validation-test.XXXXXX")
trap 'rm -rf "$fixture"' EXIT

redis_stub_dir="$fixture/redis-bin"
redis_fixture_dir="$fixture/redis-crlf"
mkdir -p "$redis_stub_dir" "$redis_fixture_dir/os" "$redis_fixture_dir/control"
printf '%s\n' \
	'#!/usr/bin/env bash' \
	'printf '\''# Stats\r\nused_cpu_sys:1.25\r\nused_cpu_user:2.5\r\ntotal_commands_processed:3\r\ninstantaneous_ops_per_sec:4\r\nconnected_clients:5\r\nblocked_clients:0\r\nrejected_connections:0\r\n'\''' >"$redis_stub_dir/redis-cli"
chmod 755 "$redis_stub_dir/redis-cli"
(
	PATH="$redis_stub_dir:$PATH"
	LOAD_PID=$$
	start_redis_collector "$redis_fixture_dir"
	sleep 2
	: >"$redis_fixture_dir/control/collectors-stop"
	wait "$REDIS_PID"
)
if ! awk -F '\t' 'NR > 1 && $9 == "ok" { found=1 } END { exit(found ? 0 : 1) }' "$redis_fixture_dir/os/redis.tsv"; then
	fail "Redis INFO CRLF output was not normalized"
fi

valid_result='{"timestamp":"2026-01-01T00:00:00.000000000Z","run_id":"run-1","planned":4,"attempted":4,"success":4,"partial":0,"error":0,"missed":0}'
assert_success validate_warmup_result_line "$valid_result"
assert_failure validate_warmup_result_line '{"timestamp":"2026-01-01T00:00:00.000000000Z","run_id":"run-1","planned":4,"attempted":3,"success":3,"partial":0,"error":0,"missed":0}'
assert_failure validate_warmup_result_line '{"timestamp":"2026-01-01T00:00:00.000000000Z","run_id":"run-1","planned":4,"attempted":4,"success":3,"partial":0,"error":0,"missed":0}'
assert_failure validate_warmup_result_line '{"timestamp":"2026-01-01T00:00:00.000000000Z","run_id":"run-1","planned":0,"attempted":0,"success":0,"partial":0,"error":0,"missed":0}'

valid_drained='{"timestamp":"2026-01-01T00:00:00.000000000Z","run_id":"run-1","planned":4,"attempted":4,"success":4,"partial":0,"error":0,"missed":0,"received":4,"duplicate":0,"sequence_gap":0,"invalid":0,"missing":0,"reader_failures":0}'
assert_success validate_warmup_drained "$valid_drained"
assert_failure validate_warmup_drained '{"timestamp":"2026-01-01T00:00:00.000000000Z","run_id":"run-1","planned":4,"attempted":4,"success":4,"partial":0,"error":0,"missed":0,"received":4,"duplicate":1,"sequence_gap":0,"invalid":0,"missing":0,"reader_failures":0}'
assert_failure validate_warmup_drained '{"timestamp":"2026-01-01T00:00:00.000000000Z","run_id":"run-1","planned":4,"attempted":4,"success":4,"partial":0,"error":0,"missed":0,"received":4,"duplicate":0,"sequence_gap":0,"invalid":0,"missing":0,"reader_failures":0'

# planned 與 attempted 的差額只代表 cadence；實際 emitted ticks 全部送達時，
# 即使 planned missing 非零，也不能判定為 delivery failure。
assert_success client_delivery_matches_emitted 1870000 1870 1000 0 0 0 0
assert_failure client_delivery_matches_emitted 1869999 1870 1000 0 0 0 0

bin_dir="$fixture/bin"
mkdir -p "$bin_dir"
for name in game gate load; do
	printf '%s\n' "$name" >"$bin_dir/$name"
done
checksum_file="$fixture/binary-checksums.txt"
shasum -a 256 "$bin_dir/game" "$bin_dir/gate" "$bin_dir/load" >"$checksum_file"
assert_success verify_binary_checksums "$checksum_file" "$bin_dir"
printf 'changed\n' >"$bin_dir/gate"
assert_failure verify_binary_checksums "$checksum_file" "$bin_dir"

baseline_dir="$fixture/baseline"
mkdir -p "$baseline_dir/metrics" "$baseline_dir/control"
WORKLOAD=broadcast
assert_failure validate_baseline_before_measured "$baseline_dir"

baseline_flow_dir="$fixture/baseline-flow"
mkdir -p "$baseline_flow_dir/metrics" "$baseline_flow_dir/control" "$baseline_flow_dir/os"
printf '%s\n' '{"timestamp":"2026-01-01T00:00:00.000000000Z","message":"example push workload completed","phase":"warmup","run_id":"run-1","planned_ticks":4,"attempted":4,"success":4,"partial":0,"error":0,"missed":0}' >"$baseline_flow_dir/game.log"
printf '%s\n' '{}' >"$baseline_flow_dir/control/warmup-drained.json"
if (
	WORKLOAD=broadcast
	LOAD_PID=1
	GAME_PID=2
	GATE_PID=3
	WARMUP_SECONDS=1
	DURATION_SECONDS=1
	DRAIN_SECONDS=1
	process_alive() { return 0; }
	record_event() { :; }
	wait_for_run_marker() { return 0; }
	validate_warmup_drained() { return 0; }
	wait_for_gate_queue_empty() { return 0; }
	capture_metrics_snapshot() {
		local dir=$1
		printf '%s\n' \
			'gaming_core_game_gate_commands_in_flight 0' \
			'gaming_core_game_server_send_in_flight 0' \
			'gaming_core_game_server_send_queue_messages{operation="broadcast"} 0' \
			'gaming_core_game_server_send_queue_bytes{operation="broadcast"} 0' \
			'gaming_core_game_server_send_queue_messages{operation="player"} 0' \
			'gaming_core_game_server_send_queue_bytes{operation="player"} 0' >"$dir/metrics/baseline-game.prom"
		printf '%s\n' \
			'gaming_core_gate_websocket_commands_in_flight 0' \
			'gaming_core_gate_game_grpc_in_flight 0' \
			'gaming_core_gate_websocket_writes_in_flight 0' \
			'gaming_core_gate_websocket_write_queue_messages 0' \
			'gaming_core_gate_server_send_outbound_queue_messages{operation="broadcast"} 0' \
			'gaming_core_gate_server_send_outbound_queue_bytes{operation="broadcast"} 0' >"$dir/metrics/baseline-gate.prom"
		printf 'load\n' >"$dir/metrics/baseline-load.prom"
	}
	capture_baseline_when_warmup_completes "$baseline_flow_dir"
); then
	fail "invalid baseline unexpectedly released measured phase"
fi
[[ -s "$baseline_flow_dir/control/baseline-metrics-invalid" ]] || fail "baseline invalid marker is missing"
[[ ! -e "$baseline_flow_dir/control/baseline-ready.json" ]] || fail "invalid baseline created baseline-ready marker"

printf '%s\n' \
	'gaming_core_game_server_send_requests_total{operation="broadcast",result="success"} 1' \
	'gaming_core_game_server_send_duration_seconds_count{operation="broadcast",result="success"} 1' \
	'gaming_core_game_gate_commands_in_flight 0' \
	'gaming_core_game_server_send_in_flight 0' \
	'gaming_core_game_server_send_queue_messages{operation="broadcast"} 0' \
	'gaming_core_game_server_send_queue_bytes{operation="broadcast"} 0' \
	'gaming_core_game_server_send_queue_messages{operation="player"} 0' \
	'gaming_core_game_server_send_queue_bytes{operation="player"} 0' >"$baseline_dir/metrics/baseline-game.prom"
printf '%s\n' \
	'gaming_core_gate_server_send_delivery_duration_seconds_count{target="room",result="success"} 1' \
	'gaming_core_gate_websocket_writes_total{source="server_send",result="success"} 1' \
	'gaming_core_gate_websocket_commands_in_flight 0' \
	'gaming_core_gate_game_grpc_in_flight 0' \
	'gaming_core_gate_websocket_writes_in_flight 0' \
	'gaming_core_gate_websocket_write_queue_messages 0' \
	'gaming_core_gate_server_send_outbound_queue_messages{operation="broadcast"} 0' \
	'gaming_core_gate_server_send_outbound_queue_bytes{operation="broadcast"} 0' >"$baseline_dir/metrics/baseline-gate.prom"
printf 'load\n' >"$baseline_dir/metrics/baseline-load.prom"
assert_success validate_baseline_before_measured "$baseline_dir"

counter_before="$fixture/counters-before.prom"
counter_after="$fixture/counters-after.prom"
printf '%s\n' \
	'gaming_core_game_server_send_queue_rejected_total{operation="broadcast",reason="full"} 0' \
	'gaming_core_game_server_send_worker_duration_seconds_count{operation="broadcast",result="error"} 0' >"$counter_before"
cp "$counter_before" "$counter_after"
assert_success validate_async_terminal_counters "$counter_before" "$counter_after" "$counter_before" "$counter_after" broadcast
printf '%s\n' \
	'gaming_core_game_server_send_queue_rejected_total{operation="broadcast",reason="full"} 1' \
	'gaming_core_game_server_send_worker_duration_seconds_count{operation="broadcast",result="error"} 0' >"$counter_after"
assert_failure validate_async_terminal_counters "$counter_before" "$counter_after" "$counter_before" "$counter_after" broadcast

CASE_ARTIFACT_ROOT="$fixture/summary"
mkdir -p "$CASE_ARTIFACT_ROOT/run-1" "$CASE_ARTIFACT_ROOT/run-2"
printf '{"evidence_status":"environment_invalid","first_failure":"collector/coverage"}\n' >"$CASE_ARTIFACT_ROOT/run-1/run-status.json"
printf '{"evidence_status":"evidence_valid","first_failure":""}\n' >"$CASE_ARTIFACT_ROOT/run-2/run-status.json"
summary=$(print_invalid_attempts 2>&1)
if ! printf '%s\n' "$summary" | grep -q 'run-1/run-status.json'; then
	fail "invalid summary omitted environment-invalid attempt"
fi
if printf '%s\n' "$summary" | grep -q 'run-2/run-status.json'; then
	fail "invalid summary included evidence-valid attempt"
fi

correctness_attempt_log="$fixture/correctness-attempts"
if correctness_output=$(
	(
		ARTIFACT_ROOT="$fixture/correctness"
		run_attempt() {
			printf '%s\n' "$1" >>"$correctness_attempt_log"
			return 1
		}
		write_evidence_manifest() { :; }
		run_correctness_attempt broadcast
	) 2>&1
); then
	fail "correctness unexpectedly succeeded after an invalid run"
fi
[[ -s "$correctness_attempt_log" ]] || fail "correctness attempt was not executed"
[[ "$(wc -l <"$correctness_attempt_log")" -eq 1 ]] || fail "correctness retried after an invalid run: $correctness_output"
[[ "$(sed -n '1p' "$correctness_attempt_log")" == 1 ]] || fail "correctness did not use run-1: $correctness_output"

if retry_output=$(
	(
		run_attempt() {
			local attempt=$1
			mkdir -p "$CASE_ARTIFACT_ROOT/run-$attempt"
			printf '{"evidence_status":"environment_invalid","first_failure":"test/retry"}\n' >"$CASE_ARTIFACT_ROOT/run-$attempt/run-status.json"
			return 1
		}
		write_evidence_manifest() { :; }
		ARTIFACT_ROOT="$fixture/retry"
		run_case single broadcast 33ms 1 1s 1s 3
	) 2>&1
); then
	fail "run_case unexpectedly succeeded after environment-invalid attempts"
fi
if ! printf '%s\n' "$retry_output" | grep -q 'stopped after 5/5 attempts'; then
	fail "retry did not stop at requested+2 attempts: $retry_output"
fi
[[ -s "$fixture/retry/single/broadcast-33/run-5/run-status.json" ]] || fail "retry attempt 5 is missing"
[[ ! -e "$fixture/retry/single/broadcast-33/run-6/run-status.json" ]] || fail "retry exceeded requested+2 attempts"

printf 'server-send validation script contracts: PASS\n'
