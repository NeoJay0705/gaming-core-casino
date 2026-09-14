#!/usr/bin/env bash
# Client → Gate → Game Echo validation 的唯一入口。
# 腳本只管理本次啟動並保存 PID 的 process；不使用 pkill，也不修改 Git staging。
set -Eeuo pipefail
export LC_ALL=C

SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
ROOT_DIR=$(cd "$SCRIPT_DIR/../../.." && pwd)
CONFIG_DIR="$ROOT_DIR/examples/metrics/configs"

ARTIFACT_DIR=""
CONNECTIONS_SPEC=""
ATTEMPTS=3
DURATION=30s
PAYLOAD_BYTES=32
GATE_CLIENT_WRITE_BUFFER=0
GAME_SERVER_WRITE_BUFFER=0
PROFILE=0
PROFILE_KIND=""
ATTEMPT_DIR_SUFFIX=""

# 設計固定值；不提供 CLI override，避免產生不可比較的 campaign。
SETUP_TIMEOUT_SECONDS=120
REQUEST_TIMEOUT_SECONDS=10
SETUP_CONCURRENCY=32
WARMUP_REQUESTS=1
GOMAXPROCS_VALUE=2
GATE_HTTP_PORT=18081
GATE_WS_PORT=18080
GATE_GRPC_PORT=19091
GAME_HTTP_PORT=19080
GAME_GRPC_PORT=19090
LOAD_METRICS_PORT=22081
GATE_PPROF_PORT=18182
GAME_PPROF_PORT=19182
LOAD_PPROF_PORT=22083
REDIS_HOST=127.0.0.1
REDIS_PORT=6379
ECHO_MEASURED_MARKER=echo-measured.json
PLATEAU_ALLOWANCE=0

GAME_PID=""
GATE_PID=""
LOAD_PID=""
TOP_PID=""
NETTOP_PID=""
PROCESS_SAMPLER_PID=""
METRICS_SAMPLER_PID=""
SOCKET_SAMPLER_PID=""
PROFILE_PID=""
CURRENT_ATTEMPT_DIR=""

usage() {
	cat <<'EOF'
Usage: run-echo-performance-validation.sh [options]

  --artifact-dir DIR
  --connections N                 one level only; default 100,200,400,800,1600,3200
  --attempts N                    default 3
  --duration D                    default 30s
  --payload-bytes N               default 32
  --gate-client-write-buffer-bytes N  default 0 (grpc-go default)
  --game-server-write-buffer-bytes N  default 0 (grpc-go default)
  --profile                       independent CPU and trace profile-only runs; requires --connections
  -h, --help

The script is intended for the macOS local example topology. Redis/OrbStack is
kept running. Every normal attempt starts fresh Gate, Game, and load processes.
EOF
}

die() {
	echo "error: $*" >&2
	exit 2
}

parse_args() {
	while (($#)); do
		case "$1" in
		--artifact-dir)
			(($# >= 2)) || die "$1 requires a value"
			ARTIFACT_DIR=$2
			shift 2
			;;
		--connections)
			(($# >= 2)) || die "$1 requires a value"
			CONNECTIONS_SPEC=$2
			shift 2
			;;
		--attempts)
			(($# >= 2)) || die "$1 requires a value"
			ATTEMPTS=$2
			shift 2
			;;
		--duration)
			(($# >= 2)) || die "$1 requires a value"
			DURATION=$2
			shift 2
			;;
		--payload-bytes)
			(($# >= 2)) || die "$1 requires a value"
			PAYLOAD_BYTES=$2
			shift 2
			;;
		--gate-client-write-buffer-bytes)
			(($# >= 2)) || die "$1 requires a value"
			GATE_CLIENT_WRITE_BUFFER=$2
			shift 2
			;;
		--game-server-write-buffer-bytes)
			(($# >= 2)) || die "$1 requires a value"
			GAME_SERVER_WRITE_BUFFER=$2
			shift 2
			;;
		--profile)
			PROFILE=1
			shift
			;;
		-h|--help)
			usage
			exit 0
			;;
		*)
			die "unknown option: $1"
			;;
		esac
	done
}

require_command() {
	command -v "$1" >/dev/null 2>&1 || die "required command not found: $1"
}

validate_positive_integer() {
	local name=$1 value=$2
	[[ "$value" =~ ^[1-9][0-9]*$ ]] || die "$name must be a positive integer"
}

validate_nonnegative_integer() {
	local name=$1 value=$2
	[[ "$value" =~ ^[0-9]+$ ]] || die "$name must be a non-negative integer"
}

validate_duration() {
	local value=$1
	[[ "$value" =~ ^[1-9][0-9]*(ns|us|µs|ms|s|m|h)$ ]] || die "invalid duration: $value"
}

# macOS BSD date 沒有 GNU %N；marker 與 sample 仍須使用合法 UTC timestamp。
timestamp_now() {
	if command -v python3 >/dev/null 2>&1; then
		python3 -c 'import datetime,time; n=time.time_ns(); dt=datetime.datetime.fromtimestamp(n//1000000000, datetime.timezone.utc); print(dt.strftime("%Y-%m-%dT%H:%M:%S") + ".%09dZ" % (n%1000000000))'
		return
	fi
	if command -v perl >/dev/null 2>&1; then
		perl -MTime::HiRes=time -MPOSIX=strftime -e '$n=int(time()*1000000000); print strftime("%Y-%m-%dT%H:%M:%S", gmtime(int($n/1000000000))), sprintf(".%09dZ", $n%1000000000), "\n"'
		return
	fi
	printf '%s.000000000Z\n' "$(date -u +%Y-%m-%dT%H:%M:%S)"
}

json_string() {
	local file=$1 key=$2
	sed -n "s/.*\"${key}\":\"\([^\"]*\)\".*/\1/p" "$file" | head -n 1
}

json_number() {
	local file=$1 key=$2
	sed -n "s/.*\"${key}\":\([-+0-9.eE]*\).*/\1/p" "$file" | head -n 1
}

is_nonnegative_number() {
	local value=$1
	[[ "$value" =~ ^[0-9]+([.][0-9]+)?([eE][+-]?[0-9]+)?$ ]] || return 1
	awk -v value="$value" 'BEGIN {
		rendered = sprintf("%.17g", value + 0)
		exit((value + 0 >= 0 && rendered !~ /[iI][nN][fF]|[nN][aA][nN]/) ? 0 : 1)
	}'
}

is_positive_number() {
	local value=$1
	is_nonnegative_number "$value" || return 1
	awk -v value="$value" 'BEGIN { exit(value + 0 > 0 ? 0 : 1) }'
}

write_atomic() {
	local path=$1 data=$2 directory temporary
	directory=$(dirname "$path")
	mkdir -p "$directory"
	temporary=$(mktemp "$directory/.tmp.XXXXXX")
	if ! printf '%s\n' "$data" >"$temporary"; then
		rm -f "$temporary"
		return 1
	fi
	chmod 0640 "$temporary"
	if ! mv -f "$temporary" "$path"; then
		rm -f "$temporary"
		return 1
	fi
}

write_marker() {
	local directory=$1 name=$2 run_id=$3
	write_atomic "$directory/$name" "{\"run_id\":\"$run_id\",\"timestamp\":\"$(timestamp_now)\"}"
}

wait_for_file() {
	local path=$1 timeout=$2 started now
	started=$(date +%s)
	while [[ ! -s "$path" ]]; do
		now=$(date +%s)
		((now - started < timeout)) || return 1
		sleep 0.02
	done
}

wait_http() {
	local url=$1 timeout=$2 started now
	started=$(date +%s)
	while :; do
		if curl -fsS --max-time 2 "$url" >/dev/null 2>&1; then
			return 0
		fi
		now=$(date +%s)
		((now - started < timeout)) || return 1
		sleep 0.2
	done
}

port_is_free() {
	local port=$1
	! lsof -nP -iTCP:"$port" -sTCP:LISTEN >/dev/null 2>&1
}

wait_ports_free() {
	local timeout=$1 started now port
	started=$(date +%s)
	while :; do
		for port in "$GATE_WS_PORT" "$GATE_HTTP_PORT" "$GATE_GRPC_PORT" "$GAME_HTTP_PORT" "$GAME_GRPC_PORT" "$LOAD_METRICS_PORT"; do
			port_is_free "$port" || break
		done
		if ((port == LOAD_METRICS_PORT)) && port_is_free "$port"; then return 0; fi
		now=$(date +%s)
		((now - started < timeout)) || return 1
		sleep 0.1
	done
}

metric_sum_or_zero() {
	local file=$1 name=$2 labels=${3:-}
	awk -v name="$name" -v labels="$labels" '
		function matches(token) {
			return (token == name || index(token, name "{") == 1) && (labels == "" || index(token, labels) > 0)
		}
		$1 !~ /^#/ && matches($1) { sum += $2; found = 1 }
		END { printf "%.17g\n", found ? sum : 0 }
	' "$file"
}

metric_present() {
	local file=$1 name=$2 labels=${3:-}
	awk -v name="$name" -v labels="$labels" '
		function matches(token) {
			return (token == name || index(token, name "{") == 1) && (labels == "" || index(token, labels) > 0)
		}
		$1 !~ /^#/ && matches($1) { found = 1 }
		END { exit(found ? 0 : 1) }
	' "$file"
}

metric_delta() {
	local before=$1 after=$2 name=$3 labels=${4:-} left right
	left=$(metric_sum_or_zero "$before" "$name" "$labels")
	right=$(metric_sum_or_zero "$after" "$name" "$labels")
	awk -v left="$left" -v right="$right" 'BEGIN { printf "%.17g\n", right-left }'
}

metric_delta_equals() {
	local before=$1 after=$2 name=$3 labels=$4 expected=$5 delta
	delta=$(metric_delta "$before" "$after" "$name" "$labels")
	awk -v got="$delta" -v want="$expected" 'BEGIN { exit((got == want) ? 0 : 1) }'
}

metric_delta_zero() {
	local before=$1 after=$2 name=$3 labels=${4:-} delta
	delta=$(metric_delta "$before" "$after" "$name" "$labels")
	awk -v got="$delta" 'BEGIN { exit((got == 0) ? 0 : 1) }'
}

calculate_rps() {
	local successful_requests=$1 admission_seconds=$2
	is_nonnegative_number "$successful_requests" || return 1
	is_positive_number "$admission_seconds" || return 1
	awk -v count="$successful_requests" -v seconds="$admission_seconds" \
		'BEGIN { printf "%.9g\n", count/seconds }'
}

validate_measurement_marker() {
	local marker=$1 field value admission measured drain successful failures
	for field in admission_duration_seconds measured_duration_seconds terminal_drain_duration_seconds successful_echo_requests connection_failures; do
		value=$(json_number "$marker" "$field")
		[[ -n "$value" ]] || return 1
	done
	admission=$(json_number "$marker" admission_duration_seconds)
	measured=$(json_number "$marker" measured_duration_seconds)
	drain=$(json_number "$marker" terminal_drain_duration_seconds)
	successful=$(json_number "$marker" successful_echo_requests)
	failures=$(json_number "$marker" connection_failures)
	is_positive_number "$admission" || return 1
	is_positive_number "$measured" || return 1
	is_nonnegative_number "$drain" || return 1
	[[ "$successful" =~ ^[0-9]+$ ]] || return 1
	[[ "$failures" =~ ^[0-9]+$ ]] || return 1
}

validate_connection_failures_zero() {
	local marker=$1 failures
	failures=$(json_number "$marker" connection_failures)
	[[ -n "$failures" ]] || return 1
	awk -v failures="$failures" 'BEGIN { exit(failures == 0 ? 0 : 1) }'
}

validate_gomaxprocs() {
	local file=$1 value
	metric_present "$file" go_sched_gomaxprocs_threads || return 1
	value=$(metric_sum_or_zero "$file" go_sched_gomaxprocs_threads)
	awk -v got="$value" -v want="$GOMAXPROCS_VALUE" 'BEGIN { exit((got == want) ? 0 : 1) }'
}

validate_baseline() {
	local directory=$1 service file
	for service in load gate game; do
		file="$directory/metrics/${service}-baseline.prom"
		[[ -s "$file" ]] || return 1
		validate_gomaxprocs "$file" || return 2
	done
	file="$directory/metrics/load-baseline.prom"
	for result in success error cancelled; do
	awk -v name=gaming_core_example_load_echo_round_trips_total -v labels="result=\"$result\"" '
		$1 !~ /^#/ && ($1 == name || index($1, name "{") == 1) && index($1, labels) > 0 { value += $2 }
		END { exit(value == 0 ? 0 : 1) }
	' "$file" || return 1
	done
}

validate_terminal_gauges() {
	local gate_file=$1 game_file=$2 load_file=$3 name
	for name in \
		gaming_core_gate_websocket_connections \
		gaming_core_gate_websocket_commands_in_flight \
		gaming_core_gate_game_grpc_in_flight \
		gaming_core_gate_websocket_writes_in_flight \
		gaming_core_gate_websocket_write_queue_messages; do
		metric_present "$gate_file" "$name" || return 1
		awk -v value="$(metric_sum_or_zero "$gate_file" "$name")" 'BEGIN { exit(value == 0 ? 0 : 1) }' || return 1
	done
	for name in gaming_core_game_gate_commands_in_flight gaming_core_game_server_send_in_flight; do
		metric_present "$game_file" "$name" || return 1
		awk -v value="$(metric_sum_or_zero "$game_file" "$name")" 'BEGIN { exit(value == 0 ? 0 : 1) }' || return 1
	done
	metric_present "$load_file" gaming_core_example_load_echo_round_trips_in_flight || return 1
	awk -v value="$(metric_sum_or_zero "$load_file" gaming_core_example_load_echo_round_trips_in_flight)" 'BEGIN { exit(value == 0 ? 0 : 1) }'
}

validate_counter_contract() {
	local directory=$1 expected=$2 load_before gate_before game_before load_after gate_after game_after
	load_before="$directory/metrics/load-baseline.prom"
	gate_before="$directory/metrics/gate-baseline.prom"
	game_before="$directory/metrics/game-baseline.prom"
	load_after="$directory/metrics/load-final.prom"
	gate_after="$directory/metrics/gate-final.prom"
	game_after="$directory/metrics/game-final.prom"

	metric_delta_equals "$load_before" "$load_after" gaming_core_example_load_echo_round_trips_total 'result="success"' "$expected" || return 1
	metric_delta_equals "$gate_before" "$gate_after" gaming_core_gate_websocket_commands_total 'command="forward",result="success",route="game"' "$expected" || return 1
	metric_delta_equals "$gate_before" "$gate_after" gaming_core_gate_game_grpc_requests_total 'code="OK"' "$expected" || return 1
	metric_delta_equals "$game_before" "$game_after" gaming_core_game_gate_commands_total 'command="4043309073",result="success"' "$expected" || return 1
	metric_delta_equals "$game_before" "$game_after" gaming_core_game_server_send_requests_total 'operation="request_player",result="success"' "$expected" || return 1
	metric_delta_equals "$gate_before" "$gate_after" gaming_core_gate_server_send_requests_total 'result="queued",target="connection"' "$expected" || return 1
	metric_delta_equals "$gate_before" "$gate_after" gaming_core_gate_websocket_writes_total 'result="success",source="server_send"' "$expected" || return 1

	metric_delta_equals "$load_before" "$load_after" gaming_core_example_load_echo_round_trip_duration_seconds_count 'result="success"' "$expected" || return 1
	metric_delta_equals "$gate_before" "$gate_after" gaming_core_gate_websocket_command_duration_seconds_count 'command="forward",result="success",route="game"' "$expected" || return 1
	metric_delta_equals "$gate_before" "$gate_after" gaming_core_gate_game_grpc_duration_seconds_count 'code="OK"' "$expected" || return 1
	metric_delta_equals "$game_before" "$game_after" gaming_core_game_gate_command_duration_seconds_count 'command="4043309073",result="success"' "$expected" || return 1
	metric_delta_equals "$game_before" "$game_after" gaming_core_game_server_send_duration_seconds_count 'operation="request_player",result="success"' "$expected" || return 1
	metric_delta_equals "$gate_before" "$gate_after" gaming_core_gate_server_send_delivery_duration_seconds_count 'result="success",target="connection"' "$expected" || return 1
	metric_delta_equals "$gate_before" "$gate_after" gaming_core_gate_websocket_write_duration_seconds_count 'result="success",source="server_send"' "$expected" || return 1
}

validate_close_contract() {
	local before=$1 after=$2 expected=$3 total client_closed
	total=$(metric_delta "$before" "$after" gaming_core_gate_websocket_connection_closes_total "")
	client_closed=$(metric_delta "$before" "$after" gaming_core_gate_websocket_connection_closes_total 'reason="client_closed"')
	awk -v total="$total" -v client_closed="$client_closed" -v expected="$expected" \
		'BEGIN { exit((total == expected && client_closed == expected) ? 0 : 1) }'
}

validate_error_contract() {
	local directory=$1 expected=$2 load_before gate_before game_before load_after gate_after game_after
	load_before="$directory/metrics/load-baseline.prom"
	gate_before="$directory/metrics/gate-baseline.prom"
	game_before="$directory/metrics/game-baseline.prom"
	load_after="$directory/metrics/load-final.prom"
	gate_after="$directory/metrics/gate-final.prom"
	game_after="$directory/metrics/game-final.prom"
	metric_delta_zero "$load_before" "$load_after" gaming_core_example_load_echo_round_trips_total 'result="error"' || return 1
	metric_delta_zero "$load_before" "$load_after" gaming_core_example_load_echo_round_trips_total 'result="cancelled"' || return 1
	metric_delta_zero "$gate_before" "$gate_after" gaming_core_gate_websocket_commands_total 'result="error"' || return 1
	metric_delta_non_ok_zero "$gate_before" "$gate_after" gaming_core_gate_game_grpc_requests_total || return 1
	metric_delta_zero "$game_before" "$game_after" gaming_core_game_gate_commands_total 'result="error"' || return 1
	metric_delta_zero "$game_before" "$game_after" gaming_core_game_server_send_requests_total 'operation="request_player",result="error"' || return 1
	metric_delta_zero "$gate_before" "$gate_after" gaming_core_gate_server_send_requests_total 'result="error",target="connection"' || return 1
	metric_delta_zero "$gate_before" "$gate_after" gaming_core_gate_websocket_writes_total 'result="error",source="server_send"' || return 1
	metric_delta_zero "$gate_before" "$gate_after" gaming_core_gate_websocket_write_queue_full_total || return 1
	validate_close_contract "$gate_before" "$gate_after" "$expected" || return 1
}

metric_delta_non_ok_zero() {
	local before=$1 after=$2 name=$3 total ok
	total=$(metric_delta "$before" "$after" "$name" "")
	ok=$(metric_delta "$before" "$after" "$name" 'code="OK"')
	awk -v total="$total" -v ok="$ok" 'BEGIN { exit((total == ok) ? 0 : 1) }'
}

# 只在有 profile 的獨立 run 使用；CPU 與 trace 由兩個 fresh run 分開收集，
# 不混入容量統計。
capture_profiles() {
	local directory=$1
	local -a targets=("game|$GAME_PPROF_PORT|$GAME_PID" "gate|$GATE_PPROF_PORT|$GATE_PID" "load|$LOAD_PPROF_PORT|$LOAD_PID")
	local item target port pid endpoint started ended output status profile_pid
	mkdir -p "$directory/profiles"
	printf 'event\ttarget\tstart\tend\tstatus\tpid\n' >"$directory/profiles/status.tsv"
	local -a pending=()
	if [[ "$PROFILE_KIND" == trace ]]; then
		for item in "${targets[@]}"; do
			IFS='|' read -r target port pid <<<"$item"
			endpoint="http://127.0.0.1:$port"
			started=$(timestamp_now)
			output="$directory/profiles/$target-trace-5s.out"
			(
				status=error
				if curl -fsS --max-time 10 "$endpoint/debug/pprof/trace?seconds=5" -o "$output"; then status=ok; fi
				ended=$(timestamp_now)
				printf 'trace\t%s\t%s\t%s\t%s\t%s\n' "$target" "$started" "$ended" "$status" "$pid" >>"$directory/profiles/status.tsv"
			) &
			pending+=("$!")
		done
		for profile_pid in "${pending[@]}"; do wait "$profile_pid" 2>/dev/null || true; done
		validate_profile_artifacts "$directory" "$PROFILE_KIND"
		return $?
	fi
	for item in "${targets[@]}"; do
		IFS='|' read -r target port pid <<<"$item"
		endpoint="http://127.0.0.1:$port"
		started=$(timestamp_now)
		output="$directory/profiles/$target-cpu-20s.pb.gz"
		(
			status=error
			if curl -fsS --max-time 25 "$endpoint/debug/pprof/profile?seconds=20" -o "$output"; then status=ok; fi
			ended=$(timestamp_now)
			printf 'cpu\t%s\t%s\t%s\t%s\t%s\n' "$target" "$started" "$ended" "$status" "$pid" >>"$directory/profiles/status.tsv"
		) &
		pending+=("$!")
	done
	for profile_pid in "${pending[@]}"; do wait "$profile_pid" 2>/dev/null || true; done
	pending=()
	for item in "${targets[@]}"; do
		IFS='|' read -r target port pid <<<"$item"
		endpoint="http://127.0.0.1:$port"
		started=$(timestamp_now)
		output="$directory/profiles/$target-goroutine.txt"
		status=error
		if curl -fsS --max-time 10 "$endpoint/debug/pprof/goroutine?debug=1" -o "$output"; then status=ok; fi
		ended=$(timestamp_now)
		printf 'goroutine\t%s\t%s\t%s\t%s\t%s\n' "$target" "$started" "$ended" "$status" "$pid" >>"$directory/profiles/status.tsv"
	done
	validate_profile_artifacts "$directory" "$PROFILE_KIND"
}

validate_profile_artifacts() {
	local directory=$1 kind=$2 event file target
	local -a files
	case "$kind" in
	cpu)
		event=cpu
		files=(game-cpu-20s.pb.gz gate-cpu-20s.pb.gz load-cpu-20s.pb.gz game-goroutine.txt gate-goroutine.txt load-goroutine.txt)
		;;
	trace)
		event=trace
		files=(game-trace-5s.out gate-trace-5s.out load-trace-5s.out)
		;;
	*)
		return 1
		;;
	esac
	[[ -s "$directory/profiles/status.tsv" ]] || return 1
	for file in "${files[@]}"; do
		[[ -s "$directory/profiles/$file" ]] || return 1
	done
	for target in game gate load; do
		awk -F '\t' -v event="$event" -v target="$target" \
			'$1 == event && $2 == target && $5 == "ok" { found=1 } END { exit(found ? 0 : 1) }' \
			"$directory/profiles/status.tsv" || return 1
		if [[ "$event" == cpu ]]; then
			awk -F '\t' -v target="$target" \
				'$1 == "goroutine" && $2 == target && $5 == "ok" { found=1 } END { exit(found ? 0 : 1) }' \
				"$directory/profiles/status.tsv" || return 1
		fi
	done
}

capture_metrics() {
	local directory=$1 phase=$2 target_directory=$directory/metrics
	if [[ "$phase" == terminal ]]; then target_directory="$directory/control"; fi
	mkdir -p "$target_directory"
	curl -fsS --max-time 5 "http://127.0.0.1:$GAME_HTTP_PORT/metrics" >"$target_directory/game-$phase.prom" || return 1
	curl -fsS --max-time 5 "http://127.0.0.1:$GATE_HTTP_PORT/metrics" >"$target_directory/gate-$phase.prom" || return 1
	curl -fsS --max-time 5 "http://127.0.0.1:$LOAD_METRICS_PORT/metrics" >"$target_directory/load-$phase.prom" || return 1
}

metrics_sampler() {
	local directory=$1 sequence=0 stamp service endpoint output status
	for service in game gate load; do printf 'timestamp\tstatus\tpath\n' >"$directory/metrics/$service-samples.tsv"; done
	while kill -0 "$LOAD_PID" >/dev/null 2>&1 || [[ ! -s "$directory/orchestration/$ECHO_MEASURED_MARKER" ]]; do
		stamp=$(timestamp_now)
		for service in game gate load; do
			case "$service" in
			game) endpoint="http://127.0.0.1:$GAME_HTTP_PORT/metrics" ;;
			gate) endpoint="http://127.0.0.1:$GATE_HTTP_PORT/metrics" ;;
			load) endpoint="http://127.0.0.1:$LOAD_METRICS_PORT/metrics" ;;
			esac
			output="$directory/metrics/$service-$sequence.prom"
			status=error
			if curl -fsS --max-time 5 "$endpoint" >"$output"; then status=200; else : >"$output"; fi
			printf '%s\t%s\t%s\n' "$stamp" "$status" "$output" >>"$directory/metrics/$service-samples.tsv"
		done
		sequence=$((sequence + 1))
		[[ -s "$directory/orchestration/$ECHO_MEASURED_MARKER" ]] && break
		sleep 1
	done
}

process_sampler() {
	local directory=$1 stamp service pid snapshot
	printf 'timestamp\tservice\tpid\tpcpu\tmemory\tthreads\tcontext_switches\tsysbsd\tsysmach\n' >"$directory/host/processes.tsv"
	while kill -0 "$LOAD_PID" >/dev/null 2>&1 || [[ ! -s "$directory/orchestration/$ECHO_MEASURED_MARKER" ]]; do
		stamp=$(timestamp_now)
		snapshot=$(top -l 2 -s 1 -n 10 -pid "$GAME_PID" -pid "$GATE_PID" -pid "$LOAD_PID" -stats pid,command,cpu,threads,mem,csw,sysbsd,sysmach 2>/dev/null || true)
		for service in game gate load; do
			case "$service" in
			game) pid=$GAME_PID ;;
			gate) pid=$GATE_PID ;;
			load) pid=$LOAD_PID ;;
			esac
			if ! printf '%s\n' "$snapshot" | awk -v wanted="$pid" -v stamp="$stamp" -v service="$service" '$1 == "PID" {sample++} sample == 2 && $1 == wanted {threads=$4; sub(/\/.*/, "", threads); csw=$6; sub(/[+]$/, "", csw); sysbsd=$7; sub(/[+]$/, "", sysbsd); sysmach=$8; sub(/[+]$/, "", sysmach); print stamp "\t" service "\t" $1 "\t" $3 "\t" $5 "\t" threads "\t" csw "\t" sysbsd "\t" sysmach; found=1} END {exit(found ? 0 : 1)}' >>"$directory/host/processes.tsv"; then
				# process 可能在單次 snapshot 前已結束；保留 fallback row，
				# 最終 validity 仍會要求每個 process 有完整 top 欄位。
				printf '%s\t%s\t%s\tNA\tNA\tNA\tNA\tNA\tNA\n' \
					"$stamp" "$service" "$pid" >>"$directory/host/processes.tsv"
			fi
		done
	done
}

socket_sampler() {
	local directory=$1 stamp
	printf 'timestamp\tlocal_port\tremote_port\tsocket\n' >"$directory/host/socket-queues.tsv"
	while kill -0 "$LOAD_PID" >/dev/null 2>&1 || [[ ! -s "$directory/orchestration/$ECHO_MEASURED_MARKER" ]]; do
		stamp=$(timestamp_now)
		netstat -anv -p tcp 2>/dev/null | awk -v stamp="$stamp" '$0 ~ /(^|[.:])(18080|18081|19080|19090|19091|22081)([[:space:]]|$)/ {print stamp "\t" $0}' >>"$directory/host/socket-queues.tsv" || true
		sleep 1
	done
}

start_collectors() {
	local directory=$1
	top -l 0 -s 1 -n 10 -pid "$GAME_PID" -pid "$GATE_PID" -pid "$LOAD_PID" -stats pid,command,cpu,threads,mem,csw,sysbsd,sysmach >"$directory/host/top.txt" 2>&1 & TOP_PID=$!
	nettop -m tcp -n -L 0 -s 1 >"$directory/host/nettop.csv" 2>&1 & NETTOP_PID=$!
	process_sampler "$directory" & PROCESS_SAMPLER_PID=$!
	socket_sampler "$directory" & SOCKET_SAMPLER_PID=$!
	metrics_sampler "$directory" & METRICS_SAMPLER_PID=$!
}

stop_collectors() {
	local pid
	for pid in "$TOP_PID" "$NETTOP_PID" "$PROCESS_SAMPLER_PID" "$SOCKET_SAMPLER_PID" "$METRICS_SAMPLER_PID"; do
		[[ -n "$pid" ]] || continue
		if kill -0 "$pid" >/dev/null 2>&1; then kill "$pid" 2>/dev/null || true; fi
		wait "$pid" 2>/dev/null || true
	done
	TOP_PID=""; NETTOP_PID=""; PROCESS_SAMPLER_PID=""; SOCKET_SAMPLER_PID=""; METRICS_SAMPLER_PID=""
}

stop_process() {
	local pid=$1 name=$2 index
	[[ -n "$pid" ]] || return 0
	if kill -0 "$pid" >/dev/null 2>&1; then
		kill -TERM "$pid" 2>/dev/null || true
		for index in $(seq 1 50); do
			kill -0 "$pid" >/dev/null 2>&1 || break
			sleep 0.1
		done
		if kill -0 "$pid" >/dev/null 2>&1; then
			echo "warning: force stopping $name pid=$pid" >&2
			kill -KILL "$pid" 2>/dev/null || true
		fi
	fi
	wait "$pid" 2>/dev/null || true
}

cleanup_processes() {
	stop_collectors
	stop_process "$PROFILE_PID" profile
	stop_process "$LOAD_PID" load
	stop_process "$GATE_PID" gate
	stop_process "$GAME_PID" game
	PROFILE_PID=""; LOAD_PID=""; GATE_PID=""; GAME_PID=""
}

write_metadata() {
	local directory=$1 level=$2 attempt=$3 duration=$4 run_started=$5 run_finished=${6:-}
	local game_checksum gate_checksum load_checksum
	game_checksum=$(awk '$2 ~ /\/game$/ { print $1; exit }' "$ARTIFACT_DIR/binaries.sha256" 2>/dev/null || true)
	gate_checksum=$(awk '$2 ~ /\/gate$/ { print $1; exit }' "$ARTIFACT_DIR/binaries.sha256" 2>/dev/null || true)
	load_checksum=$(awk '$2 ~ /\/load$/ { print $1; exit }' "$ARTIFACT_DIR/binaries.sha256" 2>/dev/null || true)
	cat >"$directory/metadata.json" <<EOF
{"run_id":"$RUN_ID","connection_level":$level,"attempt":$attempt,"route":"game","payload_bytes":$PAYLOAD_BYTES,"duration":"$duration","warmup_requests":$WARMUP_REQUESTS,"setup_timeout":"${SETUP_TIMEOUT_SECONDS}s","setup_concurrency":$SETUP_CONCURRENCY,"request_timeout":"${REQUEST_TIMEOUT_SECONDS}s","gomaxprocs":{"load":$GOMAXPROCS_VALUE,"gate":$GOMAXPROCS_VALUE,"game":$GOMAXPROCS_VALUE},"gate_connections_per_host":1,"max_concurrent_streams":0,"gate_client_write_buffer_bytes":$GATE_CLIENT_WRITE_BUFFER,"game_server_write_buffer_bytes":$GAME_SERVER_WRITE_BUFFER,"binary_checksums":{"game":"$game_checksum","gate":"$gate_checksum","load":"$load_checksum"},"pids":{"game":${GAME_PID:-0},"gate":${GATE_PID:-0},"load":${LOAD_PID:-0}},"attempt_started_at":"$run_started","attempt_finished_at":"$run_finished"}
EOF
	chmod 0640 "$directory/metadata.json"
}

write_status() {
	local directory=$1 valid=$2 reason=$3 expected=${4:-0} rps=${5:-0} p95=${6:-0}
	printf '{"valid":%s,"reason":"%s","expected_success":%s,"rps":%s,"p95_seconds":%s,"artifacts":{"metadata":"metadata.json","orchestration":"orchestration/","metrics":"metrics/","host":"host/","logs":"logs/","profiles":"profiles/"}}\n' \
		"$valid" "$reason" "$expected" "$rps" "$p95" >"$directory/run-status.json"
	chmod 0640 "$directory/run-status.json"
}

write_attempt_status() {
	local directory=$1 level=$2 attempt=$3 duration=$4 run_started=$5 valid=$6 reason=$7 expected=${8:-0} rps=${9:-0} p95=${10:-0}
	write_metadata "$directory" "$level" "$attempt" "$duration" "$run_started" "$(timestamp_now)"
	write_status "$directory" "$valid" "$reason" "$expected" "$rps" "$p95"
}

start_services() {
	local directory=$1
	local -a game_command gate_command load_command
	game_command=(env GOMAXPROCS="$GOMAXPROCS_VALUE" "CORE_CASINO_METRICS_GAME__GRPC__SERVER__WRITE_BUFFER_SIZE_BYTES=$GAME_SERVER_WRITE_BUFFER" "$ARTIFACT_DIR/bin/game" -config "$CONFIG_DIR/game.yaml" -env-prefix CORE_CASINO_METRICS_GAME__)
	gate_command=(env GOMAXPROCS="$GOMAXPROCS_VALUE" "CORE_CASINO_METRICS_GATE__GRPC__CLIENTS__GAME__WRITE_BUFFER_SIZE_BYTES=$GATE_CLIENT_WRITE_BUFFER" "$ARTIFACT_DIR/bin/gate" -config "$CONFIG_DIR/gate.yaml" -env-prefix CORE_CASINO_METRICS_GATE__)
	load_command=(env GOMAXPROCS="$GOMAXPROCS_VALUE" "$ARTIFACT_DIR/bin/load" -gate-url ws://127.0.0.1:$GATE_WS_PORT/ws -workload echo -echo-route game -connections "$LEVEL" -duration "$ATTEMPT_DURATION" -payload-bytes "$PAYLOAD_BYTES" -setup-concurrency "$SETUP_CONCURRENCY" -setup-timeout "${SETUP_TIMEOUT_SECONDS}s" -warmup-requests "$WARMUP_REQUESTS" -request-timeout "${REQUEST_TIMEOUT_SECONDS}s" -metrics-addr "127.0.0.1:$LOAD_METRICS_PORT" -orchestration-dir "$directory/orchestration")
	if ((PROFILE)); then
		game_command+=( -pprof-addr "127.0.0.1:$GAME_PPROF_PORT" )
		gate_command+=( -pprof-addr "127.0.0.1:$GATE_PPROF_PORT" )
		load_command+=( -pprof-addr "127.0.0.1:$LOAD_PPROF_PORT" )
	fi
	printf 'runner_started_at=%s\n' "$(timestamp_now)" >"$directory/logs/game.log"
	"${game_command[@]}" >>"$directory/logs/game.log" 2>&1 & GAME_PID=$!
	if ! wait_http "http://127.0.0.1:$GAME_HTTP_PORT/ready" "$SETUP_TIMEOUT_SECONDS"; then return 1; fi
	printf 'runner_started_at=%s\n' "$(timestamp_now)" >"$directory/logs/gate.log"
	"${gate_command[@]}" >>"$directory/logs/gate.log" 2>&1 & GATE_PID=$!
	if ! wait_http "http://127.0.0.1:$GATE_HTTP_PORT/ready" "$SETUP_TIMEOUT_SECONDS"; then return 1; fi
	printf 'runner_started_at=%s\n' "$(timestamp_now)" >"$directory/logs/load.log"
	"${load_command[@]}" >>"$directory/logs/load.log" 2>&1 & LOAD_PID=$!
	wait_http "http://127.0.0.1:$LOAD_METRICS_PORT/metrics" "$SETUP_TIMEOUT_SECONDS"
}

run_attempt() {
	local level=$1 attempt=$2 duration=$3
	local attempt_label="$(printf '%02d' "$attempt")$ATTEMPT_DIR_SUFFIX"
	local directory="$ARTIFACT_DIR/connections-$(printf '%04d' "$level")/attempt-$attempt_label"
	local run_id expected admission_duration connection_failures rps p95 reason="" drain_deadline now run_started baseline_status final_marker_written=0
	local profile_directory
	LEVEL=$level
	ATTEMPT_DURATION=$duration
	CURRENT_ATTEMPT_DIR=$directory
	RUN_ID="echo-$level-$attempt-$(date +%s)-$$"
	run_started=$(timestamp_now)
	mkdir -p "$directory/orchestration" "$directory/logs" "$directory/metrics" "$directory/host"
	write_metadata "$directory" "$level" "$attempt" "$duration" "$run_started"
	if ! port_is_free "$GATE_WS_PORT" || ! port_is_free "$GATE_HTTP_PORT" || ! port_is_free "$GATE_GRPC_PORT" || ! port_is_free "$GAME_HTTP_PORT" || ! port_is_free "$GAME_GRPC_PORT" || ! port_is_free "$LOAD_METRICS_PORT"; then
		write_attempt_status "$directory" "$level" "$attempt" "$duration" "$run_started" false startup_failed
		return 1
	fi
	if ! start_services "$directory"; then
		cleanup_processes
		write_attempt_status "$directory" "$level" "$attempt" "$duration" "$run_started" false readiness_failed
		return 1
	fi
	write_metadata "$directory" "$level" "$attempt" "$duration" "$run_started"
	if ! wait_for_file "$directory/orchestration/echo-ready.json" "$SETUP_TIMEOUT_SECONDS"; then
		cleanup_processes
		write_attempt_status "$directory" "$level" "$attempt" "$duration" "$run_started" false orchestration_failed
		return 1
	fi
	run_id=$(json_string "$directory/orchestration/echo-ready.json" run_id)
	[[ -n "$run_id" ]] || { cleanup_processes; write_attempt_status "$directory" "$level" "$attempt" "$duration" "$run_started" false orchestration_failed; return 1; }
	# 以 load 產生的 orchestration run ID 作為 attempt metadata 的唯一關聯鍵。
	RUN_ID="$run_id"
	write_metadata "$directory" "$level" "$attempt" "$duration" "$run_started"
	if ! capture_metrics "$directory" baseline; then
		cleanup_processes
		write_attempt_status "$directory" "$level" "$attempt" "$duration" "$run_started" false metrics_missing
		return 1
	fi
	if validate_baseline "$directory"; then
		:
	else
		baseline_status=$?
		cleanup_processes
		if ((baseline_status == 2)); then
			write_attempt_status "$directory" "$level" "$attempt" "$duration" "$run_started" false gomaxprocs_mismatch
		else
			write_attempt_status "$directory" "$level" "$attempt" "$duration" "$run_started" false orchestration_failed
		fi
		return 1
	fi
	if ! write_marker "$directory/orchestration" echo-start.json "$run_id"; then
		cleanup_processes
		write_attempt_status "$directory" "$level" "$attempt" "$duration" "$run_started" false orchestration_failed
		return 1
	fi
	start_collectors "$directory"
	if ((PROFILE)); then
		profile_directory="$ARTIFACT_DIR/profiles/connection-$(printf '%04d' "$level")/${PROFILE_KIND:-cpu}/attempt-$(printf '%02d' "$attempt")"
		capture_profiles "$profile_directory" & PROFILE_PID=$!
	fi
	if ! wait_for_file "$directory/orchestration/$ECHO_MEASURED_MARKER" "$((SETUP_TIMEOUT_SECONDS + 60))"; then
		[[ -n "${PROFILE_PID:-}" ]] && wait "$PROFILE_PID" 2>/dev/null || true
		cleanup_processes
		write_attempt_status "$directory" "$level" "$attempt" "$duration" "$run_started" false orchestration_failed
		return 1
	fi
	stop_collectors
	if ! validate_measurement_marker "$directory/orchestration/echo-measured.json"; then
		reason=orchestration_failed
	fi
	expected=$(json_number "$directory/orchestration/echo-measured.json" successful_echo_requests)
	admission_duration=$(json_number "$directory/orchestration/echo-measured.json" admission_duration_seconds)
	connection_failures=$(json_number "$directory/orchestration/echo-measured.json" connection_failures)
	if [[ -z "$expected" || -z "$admission_duration" || -z "$connection_failures" ]]; then
		reason=${reason:-orchestration_failed}
	elif ! validate_connection_failures_zero "$directory/orchestration/echo-measured.json"; then
		reason=${reason:-request_error}
	fi
	drain_deadline=$(($(date +%s) + SETUP_TIMEOUT_SECONDS))
	while [[ -z "$reason" ]]; do
		capture_metrics "$directory" terminal 2>/dev/null || true
		if validate_terminal_gauges "$directory/control/gate-terminal.prom" "$directory/control/game-terminal.prom" "$directory/control/load-terminal.prom"; then break; fi
		now=$(date +%s)
		if ((now >= drain_deadline)); then reason=terminal_gauge_nonzero; break; fi
		sleep 1
	done
	if ! capture_metrics "$directory" final; then reason=${reason:-metrics_missing}; fi
	if write_marker "$directory/orchestration" echo-final-scraped.json "$run_id"; then
		final_marker_written=1
	else
		reason=${reason:-orchestration_failed}
	fi
	if [[ -n "${PROFILE_PID:-}" ]]; then
		if ! wait "$PROFILE_PID" 2>/dev/null; then reason=${reason:-metrics_missing}; fi
		PROFILE_PID=""
	fi
	if ((final_marker_written)); then
		if ! wait_for_file "$directory/orchestration/echo-final-scraped.json" "$SETUP_TIMEOUT_SECONDS"; then reason=${reason:-orchestration_failed}; fi
	else
		# marker 無法發布時 load 不可能自行離開；停止已保存 PID，避免 invalid attempt 卡住。
		stop_process "$LOAD_PID" load
		LOAD_PID=""
	fi
	if [[ -n "$LOAD_PID" ]]; then wait "$LOAD_PID" 2>/dev/null || reason=${reason:-load_exit_failed}; LOAD_PID=""; fi
	stop_process "$GATE_PID" gate; GATE_PID=""
	stop_process "$GAME_PID" game; GAME_PID=""
	if ! wait_ports_free 10; then reason=${reason:-load_exit_failed}; fi
	if [[ -z "$reason" ]]; then
		if ! validate_counter_contract "$directory" "$expected"; then reason=counter_mismatch; fi
		if [[ -z "$reason" ]] && ! validate_error_contract "$directory" "$level"; then reason=request_error; fi
		if [[ -z "$reason" ]] && ! validate_terminal_gauges "$directory/metrics/gate-final.prom" "$directory/metrics/game-final.prom" "$directory/metrics/load-final.prom"; then reason=terminal_gauge_nonzero; fi
		if [[ -z "$reason" ]] && ! validate_attempt_artifacts "$directory" "$run_id"; then reason=metrics_missing; fi
		if [[ -z "$reason" ]] && ! validate_host_samples "$directory"; then reason=host_sample_missing; fi
	fi
	if [[ -z "$reason" ]]; then
		rps=$(calculate_rps "$expected" "$admission_duration") || reason=orchestration_failed
		if [[ -z "$reason" ]]; then
			if ! p95=$(histogram_quantile_delta "$directory/metrics/load-baseline.prom" "$directory/metrics/load-final.prom" gaming_core_example_load_echo_round_trip_duration_seconds_bucket 'result="success"' 0.95); then
				reason=metrics_missing
			fi
		fi
		if [[ -z "$reason" ]]; then
			write_attempt_status "$directory" "$level" "$attempt" "$duration" "$run_started" true "" "$expected" "$rps" "$p95"
			return 0
		fi
	fi
	write_attempt_status "$directory" "$level" "$attempt" "$duration" "$run_started" false "$reason" "$expected"
	return 1
}

median_status_value() {
	local level=$1 key=$2 directory value_file status
	directory="$ARTIFACT_DIR/connections-$(printf '%04d' "$level")"
	value_file=$(mktemp "/tmp/echo-median.XXXXXX")
	for status in "$directory"/attempt-*/run-status.json; do
		[[ -s "$status" ]] || continue
		json_number "$status" "$key" >>"$value_file" || true
	done
	sort -n "$value_file" | awk 'NF { values[++count]=$1 } END { if (count == 0) exit 1; if (count % 2) print values[(count+1)/2]; else print (values[count/2]+values[count/2+1])/2 }'
	rm -f "$value_file"
}

sustained_cpu_saturation() {
	local level=$1 directory file
	directory="$ARTIFACT_DIR/connections-$(printf '%04d' "$level")"
	for file in "$directory"/attempt-*/host/processes.tsv; do
		[[ -s "$file" ]] || continue
		if awk -F '\t' '
			NR == 1 { next }
			{ key=$2; cpu=$4+0; if (cpu >= 180) streak[key]++; else streak[key]=0; if (streak[key] >= 5) found=1 }
			END { exit(found ? 0 : 1) }
		' "$file"; then return 0; fi
	done
	return 1
}

validate_host_samples() {
	local directory=$1
	[[ -s "$directory/host/top.txt" ]] || return 1
	[[ -s "$directory/host/nettop.csv" ]] || return 1
	awk 'NR > 1 {found=1} END {exit(found ? 0 : 1)}' "$directory/host/socket-queues.tsv" || return 1
	awk -F '\t' '
		NR == 1 { next }
		{
			service = $2
			if (service != "game" && service != "gate" && service != "load") { invalid = 1; next }
			if (NF < 9 || $3 !~ /^[0-9]+$/ || $4 !~ /^[0-9]+([.][0-9]+)?$/ || $5 == "" || $6 !~ /^[0-9]+$/ || $7 !~ /^[0-9]+$/ || $8 !~ /^[0-9]+$/ || $9 !~ /^[0-9]+$/) { invalid = 1; next }
			count[service]++
		}
		END { exit(invalid || count["game"] < 5 || count["gate"] < 5 || count["load"] < 5 ? 1 : 0) }
	' "$directory/host/processes.tsv"
}

validate_periodic_metrics() {
	local directory=$1 service file stamp status path rows
	for service in game gate load; do
		file="$directory/metrics/${service}-samples.tsv"
		[[ -s "$file" ]] || return 1
		rows=0
		while IFS=$'\t' read -r stamp status path; do
			[[ "$stamp" == timestamp ]] && continue
			[[ -n "$stamp" && "$status" == 200 && -n "$path" && -s "$path" ]] || return 1
			rows=$((rows + 1))
		done <"$file"
		if ((rows < 5)); then return 1; fi
	done
}

validate_attempt_artifacts() {
	local directory=$1 expected_run_id=$2 marker
	for marker in echo-ready.json echo-start.json echo-measured.json echo-final-scraped.json; do
		[[ -s "$directory/orchestration/$marker" ]] || return 1
		[[ "$(json_string "$directory/orchestration/$marker" run_id)" == "$expected_run_id" ]] || return 1
	done
	for file in "$directory/logs/game.log" "$directory/logs/gate.log" "$directory/logs/load.log" \
		"$directory/metrics/game-baseline.prom" "$directory/metrics/gate-baseline.prom" "$directory/metrics/load-baseline.prom" \
		"$directory/metrics/game-final.prom" "$directory/metrics/gate-final.prom" "$directory/metrics/load-final.prom" \
		"$directory/metrics/game-samples.tsv" "$directory/metrics/gate-samples.tsv" "$directory/metrics/load-samples.tsv"; do
		[[ -s "$file" ]] || return 1
	done
	validate_periodic_metrics "$directory" || return 1
}

# 回傳 0 表示依設計停止進入下一個 connection level；回傳 1 表示繼續。
# 若 RPS 平台但所有 latency/resource 訊號穩定，第一次只消耗一次 allowance，
# 允許再驗證一個 level，第二次仍無成長才停止。
should_stop_after_level() {
	local previous_level=$1 current_level=$2 previous_rps current_rps previous_p95 current_p95 growth p95_growth
	previous_rps=$(median_status_value "$previous_level" rps) || return 1
	current_rps=$(median_status_value "$current_level" rps) || return 1
	previous_p95=$(median_status_value "$previous_level" p95_seconds) || return 1
	current_p95=$(median_status_value "$current_level" p95_seconds) || return 1
	growth=$(awk -v current="$current_rps" -v previous="$previous_rps" 'BEGIN { if (previous <= 0) print 100; else print (current-previous)*100/previous }')
	p95_growth=$(awk -v current="$current_p95" -v previous="$previous_p95" 'BEGIN { if (previous <= 0) print 0; else print (current-previous)*100/previous }')
	if awk -v growth="$growth" -v p95_growth="$p95_growth" 'BEGIN { exit(growth <= 5 && p95_growth >= 10 ? 0 : 1) }'; then return 0; fi
	if awk -v growth="$growth" 'BEGIN { exit(growth <= 5 ? 0 : 1) }'; then
		if sustained_cpu_saturation "$current_level"; then return 0; fi
		if ((PLATEAU_ALLOWANCE)); then return 0; fi
		PLATEAU_ALLOWANCE=1
		return 1
	fi
	PLATEAU_ALLOWANCE=0
	return 1
}

histogram_quantile_delta() {
	local before=$1 after=$2 family=$3 labels=$4 quantile=$5 temporary total target result
	temporary=$(mktemp "/tmp/echo-histogram.XXXXXX")
	awk -v family="$family" -v labels="$labels" '
		function matches(token) { return index(token, family "{") == 1 && (labels == "" || index(token, labels) > 0) }
		function upper(token, value) { value=token; sub(/^.*le="/, "", value); sub(/".*$/, "", value); return value }
		FNR==NR { if ($1 !~ /^#/ && matches($1)) before[upper($1)] += $2; next }
		$1 !~ /^#/ && matches($1) { key=upper($1); delta=$2-before[key]; if (delta < 0) delta=0; print key "\t" delta }
	' "$before" "$after" | sort -t $'\t' -k1,1g >"$temporary"
	# Histogram bucket values are cumulative.  The +Inf bucket (or the last
	# finite bucket when +Inf is absent) is the total, not a sum of buckets.
	total=$(awk -F '\t' '$1 == "+Inf" { infinite=$2 } { last=$2 } END { print (infinite == "" ? last : infinite) + 0 }' "$temporary")
	if ! awk -v total="$total" 'BEGIN { exit(total > 0 ? 0 : 1) }'; then rm -f "$temporary"; return 1; fi
	target=$(awk -v total="$total" -v quantile="$quantile" 'BEGIN {print total*quantile}')
	result=$(awk -F '\t' -v target="$target" '{if ($2 >= target) {print $1; exit}}' "$temporary") || { rm -f "$temporary"; return 1; }
	rm -f "$temporary"
	[[ "$result" =~ ^[0-9]+([.][0-9]+)?([eE][+-]?[0-9]+)?$ ]] || return 1
	awk -v value="$result" 'BEGIN { exit(value > 0 ? 0 : 1) }' || return 1
	printf '%s\n' "$result"
}

build_binaries() {
	mkdir -p "$ARTIFACT_DIR/bin"
	(cd "$ROOT_DIR" && go build -o "$ARTIFACT_DIR/bin/game" ./examples/metrics/game && go build -o "$ARTIFACT_DIR/bin/gate" ./examples/metrics/gate && go build -o "$ARTIFACT_DIR/bin/load" ./examples/metrics/load) || return 1
	shasum -a 256 "$ARTIFACT_DIR/bin/game" "$ARTIFACT_DIR/bin/gate" "$ARTIFACT_DIR/bin/load" >"$ARTIFACT_DIR/binaries.sha256"
	chmod 0640 "$ARTIFACT_DIR/binaries.sha256"
}

write_manifest() {
	local dirty=clean
	if ! (cd "$ROOT_DIR" && git diff --quiet && git diff --cached --quiet); then dirty=dirty; fi
	{
		printf 'campaign_started_at=%s\n' "$(timestamp_now)"
		printf 'git_head=%s\n' "$(cd "$ROOT_DIR" && git rev-parse HEAD 2>/dev/null || printf unknown)"
		printf 'git_dirty_state=%s\n' "$dirty"
		printf 'go_version=%s\n' "$(go version)"
		printf 'grpc_go_version=%s\n' "$(cd "$ROOT_DIR" && go list -m -f '{{.Version}}' google.golang.org/grpc)"
		printf 'os=%s\n' "$(uname -a)"
		printf 'logical_cpu=%s\n' "$(sysctl -n hw.logicalcpu 2>/dev/null || printf unknown)"
		printf 'gomaxprocs=%s\n' "$GOMAXPROCS_VALUE"
		printf 'connections_per_host=1\nmax_concurrent_streams=0\n'
		printf 'gate_client_write_buffer_bytes=%s\ngame_server_write_buffer_bytes=%s\n' "$GATE_CLIENT_WRITE_BUFFER" "$GAME_SERVER_WRITE_BUFFER"
	} >"$ARTIFACT_DIR/manifest.txt"
	chmod 0640 "$ARTIFACT_DIR/manifest.txt"
}

preflight() {
	local port
	for command in curl go git shasum awk sed mktemp lsof ps top nettop netstat sysctl redis-cli grep seq sleep date uname; do require_command "$command"; done
	redis-cli -h "$REDIS_HOST" -p "$REDIS_PORT" ping | grep -qx PONG || die "Redis is not reachable at $REDIS_HOST:$REDIS_PORT"
	for port in "$GATE_WS_PORT" "$GATE_HTTP_PORT" "$GATE_GRPC_PORT" "$GAME_HTTP_PORT" "$GAME_GRPC_PORT" "$LOAD_METRICS_PORT"; do
		port_is_free "$port" || die "port $port already has a listener"
	done
	if ((PROFILE)); then
		for port in "$GATE_PPROF_PORT" "$GAME_PPROF_PORT" "$LOAD_PPROF_PORT"; do
			port_is_free "$port" || die "pprof port $port already has a listener"
		done
	fi
	validate_nonnegative_integer gate-client-write-buffer-bytes "$GATE_CLIENT_WRITE_BUFFER"
	validate_nonnegative_integer game-server-write-buffer-bytes "$GAME_SERVER_WRITE_BUFFER"
	mkdir -p "$ARTIFACT_DIR/host"
	{
		printf 'captured_at=%s\n' "$(timestamp_now)"
		sysctl -n vm.loadavg 2>/dev/null || true
		top -l 1 -n 0 -s 1
	} >"$ARTIFACT_DIR/host/preflight-top.txt" 2>&1 || die "failed to capture preflight host sample"
}

append_summary() {
	local level=$1 attempt=$2 attempt_label directory status valid reason rps p95
	attempt_label="$(printf '%02d' "$attempt")$ATTEMPT_DIR_SUFFIX"
	directory="$ARTIFACT_DIR/connections-$(printf '%04d' "$level")/attempt-$attempt_label"
	status="$directory/run-status.json"
	valid=$(sed -En 's/.*"valid":(true|false).*/\1/p' "$status" | head -n 1)
	reason=$(json_string "$status" reason)
	rps=$(json_number "$status" rps); p95=$(json_number "$status" p95_seconds)
	printf '%s\t%s\t%s\t%s\t%s\t%s\n' "$level" "$attempt_label" "${valid:-false}" "${rps:-0}" "${p95:-0}" "${reason:-}" >>"$ARTIFACT_DIR/summary.tsv"
}

run_campaign() {
	local levels level attempt valid_count=0 previous_level="" profile_status=0
	if [[ -n "$CONNECTIONS_SPEC" ]]; then levels=("$CONNECTIONS_SPEC"); else levels=(100 200 400 800 1600 3200); fi
	printf 'connections\tattempt\tvalid\trps\tp95_seconds\treason\n' >"$ARTIFACT_DIR/summary.tsv"
	if ((PROFILE)); then
		((ATTEMPTS=1))
		# CPU profile 與 trace 必須使用不同的 fresh process run，避免前一種
		# profiler 的 observer effect 污染另一種證據。
		for PROFILE_KIND in cpu trace; do
			if [[ "$PROFILE_KIND" == trace ]]; then ATTEMPT_DIR_SUFFIX="-trace"; else ATTEMPT_DIR_SUFFIX=""; fi
			for level in "${levels[@]}"; do
				run_attempt "$level" 1 "$DURATION" || profile_status=1
				append_summary "$level" 1
			done
		done
		PROFILE_KIND=""
		ATTEMPT_DIR_SUFFIX=""
		return "$profile_status"
	fi
	# correctness gate：15s、1 connection，不能進入容量中位數。
	if ! run_attempt 1 1 15s; then
		append_summary 1 1
		return 1
	fi
	append_summary 1 1
	for level in "${levels[@]}"; do
		valid_count=0
		for attempt in $(seq 1 "$ATTEMPTS"); do
			run_attempt "$level" "$attempt" "$DURATION" || true
			append_summary "$level" "$attempt"
			if [[ "$(sed -En 's/.*"valid":(true|false).*/\1/p' "$ARTIFACT_DIR/connections-$(printf '%04d' "$level")/attempt-$(printf '%02d' "$attempt")/run-status.json")" == true ]]; then valid_count=$((valid_count + 1)); fi
		done
		((valid_count == ATTEMPTS)) || return 1
		if [[ -n "$previous_level" ]] && should_stop_after_level "$previous_level" "$level"; then break; fi
		previous_level=$level
	done
	return 0
}

main() {
	parse_args "$@"
	validate_positive_integer attempts "$ATTEMPTS"
	if [[ -n "$CONNECTIONS_SPEC" ]]; then
		validate_positive_integer connections "$CONNECTIONS_SPEC"
	fi
	validate_nonnegative_integer payload-bytes "$PAYLOAD_BYTES"
	validate_duration "$DURATION"
	if [[ -z "$ARTIFACT_DIR" ]]; then ARTIFACT_DIR="$ROOT_DIR/artifacts/echo-performance/$(date -u +%Y%m%dT%H%M%SZ)"; fi
	if ((PROFILE)) && [[ -z "$CONNECTIONS_SPEC" ]]; then die "--profile requires --connections"; fi
	mkdir -p "$ARTIFACT_DIR"
	ARTIFACT_DIR=$(cd "$ARTIFACT_DIR" && pwd)
	preflight
	write_manifest
	if ! build_binaries; then
		printf '{"valid":false,"reason":"build_failed"}\n' >"$ARTIFACT_DIR/campaign-status.json"
		return 1
	fi
	if run_campaign; then
		printf '{"valid":true,"summary":"summary.tsv"}\n' >"$ARTIFACT_DIR/campaign-status.json"
		return 0
	fi
	printf '{"valid":false,"summary":"summary.tsv"}\n' >"$ARTIFACT_DIR/campaign-status.json"
	return 1
}

if [[ "${BASH_SOURCE[0]:-}" == "$0" ]]; then
	trap cleanup_processes EXIT INT TERM
	main "$@"
fi
