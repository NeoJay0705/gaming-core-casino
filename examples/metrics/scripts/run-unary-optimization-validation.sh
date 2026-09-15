#!/usr/bin/env bash
# direct grpcload → Game GateLink Unary 的可重現驗證入口。
# 腳本只管理本次啟動並保存 PID 的 process；不使用 pkill，也不修改 Git staging。
set -Eeuo pipefail
export LC_ALL=C

SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
ROOT_DIR=$(cd "$SCRIPT_DIR/../../.." && pwd)
CONFIG_DIR="$ROOT_DIR/examples/metrics/configs"

ARTIFACT_DIR=""
ATTEMPTS=3
DURATION=30s
PROFILE_WORKER=""
METADATA_BENCHMARK=0

# 本實驗固定條件；不提供 CLI override，避免把不同 workload 混入同一份
# summary。stream workers 是唯一變化因子。
GOMAXPROCS_VALUE=4
CONCURRENCY=400
CLIENT_CONNECTIONS=1
PAYLOAD_BYTES=32
WARMUP_REQUESTS=1
REQUEST_TIMEOUT=10s
SETUP_TIMEOUT_SECONDS=120
ORCHESTRATION_TIMEOUT_SECONDS=120
GAME_HTTP_PORT=19080
GAME_GRPC_PORT=19090
LOAD_METRICS_PORT=22082
GAME_PPROF_PORT=19182
LOAD_PPROF_PORT=22083
REDIS_HOST=127.0.0.1
REDIS_PORT=6379
GRPC_READY_MARKER=grpc-ready.json
GRPC_START_MARKER=grpc-start.json
GRPC_MEASURED_MARKER=grpc-measured.json
GRPC_FINAL_MARKER=grpc-final-scraped.json
ECHO_COMMAND=4043309073
RFC3339_UTC_PATTERN='^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(\.[0-9]{1,9})?Z$'

GAME_PID=""
LOAD_PID=""
TOP_PID=""
METRICS_SAMPLER_PID=""
PROCESS_SAMPLER_PID=""
PROFILE_PID=""
CURRENT_ATTEMPT_DIR=""

usage() {
	cat <<'EOF'
Usage: run-unary-optimization-validation.sh [options]

  --artifact-dir DIR       campaign output directory (default: artifacts/unary-optimization/<UTC>)
  --profile-worker N       run one independent profile-only attempt for N (0,4,8)
  --metadata-benchmark     run the five-count metadata benchmark into the campaign artifact
  -h, --help

The normal campaign first runs the 10s, concurrency=1 correctness gate and then
the fixed 400-worker A/B matrix. GOMAXPROCS is 4 for both processes, one
ClientConn is used, and Redis/OrbStack is left running.
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
		--profile-worker)
			(($# >= 2)) || die "$1 requires a value"
			PROFILE_WORKER=$2
			shift 2
			;;
		--metadata-benchmark)
			METADATA_BENCHMARK=1
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
	[[ "$value" =~ ^[1-9][0-9]*$ ]] || return 1
}

validate_nonnegative_integer() {
	local name=$1 value=$2
	[[ "$value" =~ ^[0-9]+$ ]] || return 1
}

validate_duration() {
	local value=$1
	[[ "$value" =~ ^[1-9][0-9]*(ns|us|µs|ms|s|m|h)$ ]]
}

validate_worker_value() {
	[[ "$1" =~ ^(0|4|8)$ ]]
}

validate_worker_spec() {
	local spec=$1 value count=0
	local -a values
	[[ -n "$spec" ]] || return 1
	IFS=',' read -r -a values <<<"$spec"
	for value in "${values[@]}"; do
		validate_worker_value "$value" || return 1
		count=$((count + 1))
	done
	((count > 0))
}

# macOS BSD date 沒有 GNU %N；marker 與 sample 仍使用合法 UTC timestamp。
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
	jq -er --arg key "$key" '.[$key] | select(type == "string")' "$file"
}

json_number() {
	local file=$1 key=$2
	jq -er --arg key "$key" '.[$key] | select(type == "number")' "$file"
}

is_nonnegative_number() {
	local value=$1
	[[ "$value" =~ ^[0-9]+([.][0-9]+)?([eE][+-]?[0-9]+)?$ ]] || return 1
	awk -v value="$value" 'BEGIN { exit(value + 0 >= 0 ? 0 : 1) }'
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
		for port in "$GAME_HTTP_PORT" "$GAME_GRPC_PORT" "$LOAD_METRICS_PORT"; do
			port_is_free "$port" || break
		done
		if [[ "$port" == "$LOAD_METRICS_PORT" ]] && port_is_free "$port"; then
			return 0
		fi
		now=$(date +%s)
		((now - started < timeout)) || return 1
		sleep 0.1
	done
}

# Prometheus text format 的 sample token 會保留完整 metric name 與 labels；
# labels 以 substring 比對，避免一次 validation 依賴 exposition 的 label 順序。
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
	awk -v count="$successful_requests" -v seconds="$admission_seconds" 'BEGIN { printf "%.9g\n", count/seconds }'
}

# Histogram bucket values are cumulative. Return the upper bound containing the
# requested quantile after subtracting baseline buckets.
histogram_quantile_delta() {
	local before=$1 after=$2 family=$3 labels=$4 quantile=$5 temporary total target result
	temporary=$(mktemp "/tmp/unary-histogram.XXXXXX")
	awk -v family="$family" -v labels="$labels" '
		function matches(token) { return index(token, family "{") == 1 && (labels == "" || index(token, labels) > 0) }
		function upper(token, value) { value=token; sub(/^.*le="/, "", value); sub(/".*$/, "", value); return value }
		FNR==NR { if ($1 !~ /^#/ && matches($1)) before[upper($1)] += $2; next }
		$1 !~ /^#/ && matches($1) { key=upper($1); delta=$2-before[key]; if (delta < 0) delta=0; print key "\t" delta }
	' "$before" "$after" | sort -t $'\t' -k1,1g >"$temporary"
	total=$(awk -F '\t' '$1 == "+Inf" { infinite=$2 } { last=$2 } END { print (infinite == "" ? last : infinite) + 0 }' "$temporary")
	if ! awk -v total="$total" 'BEGIN { exit(total > 0 ? 0 : 1) }'; then
		rm -f "$temporary"
		return 1
	fi
	target=$(awk -v total="$total" -v quantile="$quantile" 'BEGIN { print total*quantile }')
	result=$(awk -F '\t' -v target="$target" '{ if ($2 >= target) { print $1; exit } }' "$temporary")
	rm -f "$temporary"
	[[ "$result" =~ ^[0-9]+([.][0-9]+)?([eE][+-]?[0-9]+)?$ ]] || return 1
	printf '%s\n' "$result"
}

histogram_mean_delta() {
	local before=$1 after=$2 family=$3 labels=$4 sum count
	sum=$(metric_delta "$before" "$after" "${family}_sum" "$labels")
	count=$(metric_delta "$before" "$after" "${family}_count" "$labels")
	awk -v sum="$sum" -v count="$count" 'BEGIN { if (count > 0) printf "%.17g\n", sum/count; else exit 1 }'
}

capture_metrics() {
	local directory=$1 phase=$2
	mkdir -p "$directory/metrics"
	curl -fsS --max-time 5 "http://127.0.0.1:$GAME_HTTP_PORT/metrics" >"$directory/metrics/game-$phase.prom" || return 1
	curl -fsS --max-time 5 "http://127.0.0.1:$LOAD_METRICS_PORT/metrics" >"$directory/metrics/grpcload-$phase.prom" || return 1
}

metrics_sampler() {
	local directory=$1 sequence=0 stamp endpoint service output status
	for service in game grpcload; do
		printf 'timestamp\tstatus\tpath\n' >"$directory/metrics/$service-samples.tsv"
	done
	while kill -0 "$LOAD_PID" >/dev/null 2>&1 || [[ ! -s "$directory/orchestration/$GRPC_MEASURED_MARKER" ]]; do
		stamp=$(timestamp_now)
		for service in game grpcload; do
			if [[ "$service" == game ]]; then
				endpoint="http://127.0.0.1:$GAME_HTTP_PORT/metrics"
			else
				endpoint="http://127.0.0.1:$LOAD_METRICS_PORT/metrics"
			fi
			output="$directory/metrics/$service-$sequence.prom"
			status=error
			if curl -fsS --max-time 5 "$endpoint" >"$output"; then status=200; else : >"$output"; fi
			printf '%s\t%s\t%s\n' "$stamp" "$status" "$output" >>"$directory/metrics/$service-samples.tsv"
		done
		sequence=$((sequence + 1))
		[[ -s "$directory/orchestration/$GRPC_MEASURED_MARKER" ]] && break
		sleep 1
	done
}

process_snapshot() {
	local stamp=$1 service=$2 pid=$3 line
	line=$(top -l 1 -n 10 -pid "$pid" -stats pid,cpu,threads,mem,csw,sysbsd,sysmach 2>/dev/null | awk -v wanted="$pid" '$1 == wanted { print; exit }' || true)
	if [[ -n "$line" && "$(wc -w <<<"$line")" -ge 7 ]]; then
		# The example binary names contain no whitespace, so the requested top
		# columns occupy stable fields on macOS.
		set -- $line
		printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' "$stamp" "$service" "$1" "$2" "$3" "$4" "$5" "$6" "$7"
		return
	fi
	printf '%s\t%s\t%s\tNA\tNA\tNA\tNA\tNA\tNA\n' "$stamp" "$service" "$pid"
}

process_sampler() {
	local directory=$1 stamp service pid
	printf 'timestamp\tservice\tpid\tpcpu\tthreads\tmemory\tcontext_switches\tsysbsd\tsysmach\n' >"$directory/host/processes.tsv"
	while kill -0 "$LOAD_PID" >/dev/null 2>&1 || [[ ! -s "$directory/orchestration/$GRPC_MEASURED_MARKER" ]]; do
		stamp=$(timestamp_now)
		for service in game grpcload; do
			if [[ "$service" == game ]]; then pid=$GAME_PID; else pid=$LOAD_PID; fi
			process_snapshot "$stamp" "$service" "$pid" >>"$directory/host/processes.tsv"
		done
		sleep 1
	done
}

start_collectors() {
	local directory=$1
	mkdir -p "$directory/host"
	top -l 0 -s 1 -n 10 -pid "$GAME_PID" -pid "$LOAD_PID" -stats pid,command,cpu,threads,mem,csw,sysbsd,sysmach >"$directory/host/top.txt" 2>&1 & TOP_PID=$!
	metrics_sampler "$directory" & METRICS_SAMPLER_PID=$!
	process_sampler "$directory" & PROCESS_SAMPLER_PID=$!
}

stop_collectors() {
	local pid
	for pid in "$METRICS_SAMPLER_PID" "$PROCESS_SAMPLER_PID" "$TOP_PID"; do
		if [[ -n "$pid" ]]; then
			kill "$pid" >/dev/null 2>&1 || true
			wait "$pid" >/dev/null 2>&1 || true
		fi
	done
	METRICS_SAMPLER_PID=""
	PROCESS_SAMPLER_PID=""
	TOP_PID=""
}

stop_process() {
	local pid=$1 name=$2
	[[ -n "$pid" ]] || return 0
	if kill -0 "$pid" >/dev/null 2>&1; then
		kill -TERM "$pid" >/dev/null 2>&1 || true
		for _ in $(seq 1 50); do
			kill -0 "$pid" >/dev/null 2>&1 || return 0
			sleep 0.1
		done
		kill -KILL "$pid" >/dev/null 2>&1 || true
	fi
	wait "$pid" >/dev/null 2>&1 || true
}

cleanup_processes() {
	stop_collectors
	if [[ -n "$LOAD_PID" ]]; then stop_process "$LOAD_PID" grpcload; LOAD_PID=""; fi
	if [[ -n "$GAME_PID" ]]; then stop_process "$GAME_PID" game; GAME_PID=""; fi
}

capture_profiles() {
	local directory=$1 endpoint output status target profile_pid binary
	local -a profile_pids=()
	mkdir -p "$directory/profiles"
	printf 'event\ttarget\tstatus\tpath\n' >"$directory/profiles/status.tsv"
	# 兩個 process 的 20 秒 CPU profile 同時進行，避免把不同 runtime
	# window 誤當成同一個 steady state。
	for target in game grpcload; do
		if [[ "$target" == game ]]; then endpoint="http://127.0.0.1:$GAME_PPROF_PORT"; else endpoint="http://127.0.0.1:$LOAD_PPROF_PORT"; fi
		output="$directory/profiles/$target-cpu-20s.pb.gz"
		(
			status=missing
			if curl -fsS --max-time 25 "$endpoint/debug/pprof/profile?seconds=20" -o "$output"; then status=ok; else : >"$output"; fi
			printf 'cpu\t%s\t%s\t%s\n' "$target" "$status" "$output" >>"$directory/profiles/status.tsv"
		) & profile_pids+=("$!")
	done
	for profile_pid in "${profile_pids[@]}"; do
		wait "$profile_pid" >/dev/null 2>&1 || true
	done
	for target in game grpcload; do
		if [[ "$target" == game ]]; then endpoint="http://127.0.0.1:$GAME_PPROF_PORT"; else endpoint="http://127.0.0.1:$LOAD_PPROF_PORT"; fi
		output="$directory/profiles/$target-allocs.pb.gz"
		status=missing
		if curl -fsS --max-time 10 "$endpoint/debug/pprof/allocs" -o "$output"; then status=ok; else : >"$output"; fi
		printf 'allocs\t%s\t%s\t%s\n' "$target" "$status" "$output" >>"$directory/profiles/status.tsv"
		output="$directory/profiles/$target-goroutine.txt"
		status=missing
		if curl -fsS --max-time 10 "$endpoint/debug/pprof/goroutine?debug=2" -o "$output"; then status=ok; else : >"$output"; fi
		printf 'goroutine\t%s\t%s\t%s\n' "$target" "$status" "$output" >>"$directory/profiles/status.tsv"
		binary="$ARTIFACT_DIR/bin/$target"
		if [[ -s "$directory/profiles/$target-cpu-20s.pb.gz" ]]; then
			go tool pprof -top -cum "$binary" "$directory/profiles/$target-cpu-20s.pb.gz" >"$directory/profiles/$target-cpu-top-cum.txt" 2>&1 || return 1
		fi
		if [[ -s "$directory/profiles/$target-allocs.pb.gz" ]]; then
			go tool pprof -top -sample_index=alloc_space "$binary" "$directory/profiles/$target-allocs.pb.gz" >"$directory/profiles/$target-alloc-space-top.txt" 2>&1 || return 1
			go tool pprof -top -sample_index=alloc_objects "$binary" "$directory/profiles/$target-allocs.pb.gz" >"$directory/profiles/$target-alloc-objects-top.txt" 2>&1 || return 1
		fi
	done
	awk -F '\t' 'NR > 1 && $3 != "ok" { failed = 1 } END { exit(failed ? 1 : 0) }' "$directory/profiles/status.tsv"
}

write_metadata() {
	local directory=$1 variant=$2 attempt=$3 duration=$4 concurrency=$5 started=$6 finished=${7:-}
	local run_id git_status grpc_version game_checksum load_checksum
	run_id=$(json_string "$directory/orchestration/$GRPC_READY_MARKER" run_id 2>/dev/null || true)
	git_status=$(cd "$ROOT_DIR" && git status --short | tr '\n' ' ' || printf unknown)
	git_status=$(printf '%s' "$git_status" | sed 's/\\/\\\\/g; s/"/\\"/g')
	grpc_version=$(cd "$ROOT_DIR" && go list -m -f '{{.Version}}' google.golang.org/grpc 2>/dev/null || printf unknown)
	game_checksum=$(awk '$2 ~ /\/game$/ { print $1; exit }' "$ARTIFACT_DIR/binaries.sha256" 2>/dev/null || printf unknown)
	load_checksum=$(awk '$2 ~ /\/grpcload$/ { print $1; exit }' "$ARTIFACT_DIR/binaries.sha256" 2>/dev/null || printf unknown)
	write_atomic "$directory/metadata.json" "{\"run_id\":\"$run_id\",\"variant\":\"$variant\",\"attempt\":$attempt,\"gomaxprocs_game\":$GOMAXPROCS_VALUE,\"gomaxprocs_grpcload\":$GOMAXPROCS_VALUE,\"concurrency\":$concurrency,\"client_connections\":$CLIENT_CONNECTIONS,\"payload_bytes\":$PAYLOAD_BYTES,\"warmup_requests\":$WARMUP_REQUESTS,\"request_timeout\":\"$REQUEST_TIMEOUT\",\"duration\":\"$duration\",\"max_concurrent_streams\":0,\"write_buffer_size_bytes\":0,\"started_at\":\"$started\",\"finished_at\":\"$finished\",\"git_head\":\"$(cd "$ROOT_DIR" && git rev-parse HEAD 2>/dev/null || printf unknown)\",\"git_status\":\"$git_status\",\"go_version\":\"$(go version)\",\"grpc_go_version\":\"$grpc_version\",\"os\":\"$(uname -a)\",\"logical_cpu\":\"$(sysctl -n hw.logicalcpu 2>/dev/null || printf unknown)\",\"binary_checksums\":{\"game\":\"$game_checksum\",\"grpcload\":\"$load_checksum\"},\"binary_checksums_file\":\"binaries.sha256\"}"
}

write_status() {
	local directory=$1 variant=$2 attempt=$3 valid=$4 reason=$5 successful=$6 rps=$7
	write_atomic "$directory/run-status.json" "{\"variant\":\"$variant\",\"attempt\":$attempt,\"valid\":$valid,\"reason\":\"$reason\",\"successful_requests\":$successful,\"rps\":$rps}"
}

validate_gomaxprocs() {
	local file=$1 value
	metric_present "$file" go_sched_gomaxprocs_threads || return 1
	value=$(metric_sum_or_zero "$file" go_sched_gomaxprocs_threads)
	awk -v got="$value" -v want="$GOMAXPROCS_VALUE" 'BEGIN { exit(got == want ? 0 : 1) }'
}

validate_baseline() {
	local directory=$1
	validate_gomaxprocs "$directory/metrics/game-baseline.prom" || return 1
	validate_gomaxprocs "$directory/metrics/grpcload-baseline.prom" || return 1
	metric_present "$directory/metrics/grpcload-baseline.prom" gaming_core_example_grpc_load_round_trips_in_flight || return 1
}

validate_identity_marker() {
	local marker=$1 expected_run_id=${2:-}
	jq -e --arg expected "$expected_run_id" --arg timestamp "$RFC3339_UTC_PATTERN" '
		type == "object" and
		(.run_id | type == "string" and length > 0) and
		($expected == "" or .run_id == $expected) and
		(.timestamp | type == "string" and test($timestamp))
	' "$marker" >/dev/null
}

validate_measured_marker() {
	local marker=$1 expected_run_id=$2
	validate_identity_marker "$marker" "$expected_run_id" || return 1
	jq -e --arg timestamp "$RFC3339_UTC_PATTERN" '
		([.measurement_start, .admission_end, .measurement_end] |
			all(.[]; type == "string" and test($timestamp))) and
		(.admission_duration_seconds |
			type == "number" and isfinite and . > 0) and
		(.measured_duration_seconds |
			type == "number" and isfinite and . >= 0) and
		(.terminal_drain_duration_seconds |
			type == "number" and isfinite and . >= 0) and
		(.successful_requests |
			type == "number" and isfinite and . > 0 and floor == .) and
		(.failed_workers |
			type == "number" and isfinite and . >= 0 and floor == .)
	' "$marker" >/dev/null
}

marker_workload_reason() {
	local marker=$1 failed_workers
	failed_workers=$(json_number "$marker" failed_workers) || return 1
	if ! awk -v value="$failed_workers" 'BEGIN { exit(value == 0 ? 0 : 1) }'; then
		printf 'request_error\n'
	fi
}

validate_marker_set() {
	local directory=$1 expected_run_id=$2 marker
	for marker in "$GRPC_READY_MARKER" "$GRPC_START_MARKER" "$GRPC_MEASURED_MARKER" "$GRPC_FINAL_MARKER"; do
		validate_identity_marker "$directory/orchestration/$marker" "$expected_run_id" || return 1
	done
}

validate_terminal_gauges() {
	local game=$1 load=$2 name
	for name in gaming_core_game_gate_commands_in_flight; do
		metric_present "$game" "$name" || return 1
		awk -v value="$(metric_sum_or_zero "$game" "$name")" 'BEGIN { exit(value == 0 ? 0 : 1) }' || return 1
	done
	metric_present "$load" gaming_core_example_grpc_load_round_trips_in_flight || return 1
	awk -v value="$(metric_sum_or_zero "$load" gaming_core_example_grpc_load_round_trips_in_flight)" 'BEGIN { exit(value == 0 ? 0 : 1) }'
}

validate_counter_contract() {
	local directory=$1 expected=$2 game_before load_before game_after load_after
	game_before="$directory/metrics/game-baseline.prom"
	load_before="$directory/metrics/grpcload-baseline.prom"
	game_after="$directory/metrics/game-final.prom"
	load_after="$directory/metrics/grpcload-final.prom"
	metric_delta_equals "$load_before" "$load_after" gaming_core_example_grpc_load_round_trips_total 'result="success"' "$expected" || return 1
	metric_delta_equals "$load_before" "$load_after" gaming_core_example_grpc_load_round_trip_duration_seconds_count 'result="success"' "$expected" || return 1
	metric_delta_equals "$game_before" "$game_after" gaming_core_game_gate_commands_total "command=\"$ECHO_COMMAND\",result=\"success\"" "$expected" || return 1
	metric_delta_equals "$game_before" "$game_after" gaming_core_game_gate_command_duration_seconds_count "command=\"$ECHO_COMMAND\",result=\"success\"" "$expected" || return 1
	metric_delta_equals "$game_before" "$game_after" gaming_core_game_server_send_requests_total 'operation="request_player",result="success"' "$expected" || return 1
	metric_delta_equals "$game_before" "$game_after" gaming_core_game_server_send_duration_seconds_count 'operation="request_player",result="success"' "$expected" || return 1
	metric_delta_zero "$load_before" "$load_after" gaming_core_example_grpc_load_round_trips_total 'result="error"' || return 1
	metric_delta_zero "$load_before" "$load_after" gaming_core_example_grpc_load_round_trips_total 'result="cancelled"' || return 1
	metric_delta_zero "$game_before" "$game_after" gaming_core_game_gate_commands_total 'result="error"' || return 1
	metric_delta_zero "$game_before" "$game_after" gaming_core_game_server_send_requests_total 'result="error"' || return 1
	metric_delta_zero "$game_before" "$game_after" gaming_core_game_server_send_requests_total 'result="partial"' || return 1
}

validate_periodic_metrics() {
	local directory=$1 service file stamp status path rows
	for service in game grpcload; do
		file="$directory/metrics/$service-samples.tsv"
		[[ -s "$file" ]] || return 1
		rows=0
		while IFS=$'\t' read -r stamp status path; do
			[[ "$stamp" == timestamp ]] && continue
			[[ -n "$stamp" && "$status" == 200 && -n "$path" && -s "$path" ]] || return 1
			rows=$((rows + 1))
		done <"$file"
		((rows >= 5)) || return 1
	done
}

validate_host_samples() {
	local directory=$1
	[[ -s "$directory/host/top.txt" ]] || return 1
	awk -F '\t' 'NR > 1 && $3 ~ /^[0-9]+$/ {found=1} END {exit(found ? 0 : 1)}' "$directory/host/processes.tsv"
}

max_sample_metric() {
	local sample_file=$1 metric=$2 maximum
	[[ -s "$sample_file" ]] || { printf 'NA\n'; return 0; }
	maximum=$(awk -F '\t' 'NR > 1 && $3 != "" {print $3}' "$sample_file" | while read -r path; do
		if [[ -s "$path" ]]; then metric_sum_or_zero "$path" "$metric"; else printf '0\n'; fi
	done | sort -n | tail -n 1)
	printf '%s\n' "${maximum:-NA}"
}

min_host_idle() {
	local file=$1 value
	value=$(awk '/CPU usage:/ { for (i=1; i<=NF; i++) if ($i == "idle") { gsub(/%/, "", $(i-1)); print $(i-1) } }' "$file" | sort -n | head -n 1)
	printf '%s\n' "${value:-NA}"
}

append_summary() {
	local directory=$1 variant=$2 attempt=$3 valid=$4 reason=$5 expected=$6
	local admission rps mean p50 p95 p99 game_cpu load_cpu game_alloc load_alloc game_malloc load_malloc game_gc load_gc game_sched load_sched game_goroutines load_goroutines host_idle
	if [[ ! -s "$directory/orchestration/$GRPC_MEASURED_MARKER" || \
		! -s "$directory/metrics/game-baseline.prom" || ! -s "$directory/metrics/game-final.prom" || \
		! -s "$directory/metrics/grpcload-baseline.prom" || ! -s "$directory/metrics/grpcload-final.prom" ]]; then
		printf '%s\t%s\t%s\t%s\t%s\tNA\tNA\tNA\tNA\tNA\tNA\tNA\tNA\tNA\tNA\tNA\tNA\tNA\tNA\tNA\tNA\tNA\tNA\tNA\tNA\tNA\n' "$variant" "$attempt" "$valid" "$reason" "$expected" >>"$ARTIFACT_DIR/summary.tsv"
		return
	fi
	admission=$(json_number "$directory/orchestration/$GRPC_MEASURED_MARKER" admission_duration_seconds)
	rps=$(calculate_rps "$expected" "$admission" 2>/dev/null || printf NA)
	mean=$(histogram_mean_delta "$directory/metrics/grpcload-baseline.prom" "$directory/metrics/grpcload-final.prom" gaming_core_example_grpc_load_round_trip_duration_seconds 'result="success"' 2>/dev/null || printf NA)
	p50=$(histogram_quantile_delta "$directory/metrics/grpcload-baseline.prom" "$directory/metrics/grpcload-final.prom" gaming_core_example_grpc_load_round_trip_duration_seconds_bucket 'result="success"' 0.50 2>/dev/null || printf NA)
	p95=$(histogram_quantile_delta "$directory/metrics/grpcload-baseline.prom" "$directory/metrics/grpcload-final.prom" gaming_core_example_grpc_load_round_trip_duration_seconds_bucket 'result="success"' 0.95 2>/dev/null || printf NA)
	p99=$(histogram_quantile_delta "$directory/metrics/grpcload-baseline.prom" "$directory/metrics/grpcload-final.prom" gaming_core_example_grpc_load_round_trip_duration_seconds_bucket 'result="success"' 0.99 2>/dev/null || printf NA)
	game_cpu=$(metric_delta "$directory/metrics/game-baseline.prom" "$directory/metrics/game-final.prom" process_cpu_seconds_total)
	load_cpu=$(metric_delta "$directory/metrics/grpcload-baseline.prom" "$directory/metrics/grpcload-final.prom" process_cpu_seconds_total)
	game_alloc=$(metric_delta "$directory/metrics/game-baseline.prom" "$directory/metrics/game-final.prom" go_memstats_alloc_bytes_total)
	load_alloc=$(metric_delta "$directory/metrics/grpcload-baseline.prom" "$directory/metrics/grpcload-final.prom" go_memstats_alloc_bytes_total)
	game_malloc=$(metric_delta "$directory/metrics/game-baseline.prom" "$directory/metrics/game-final.prom" go_memstats_mallocs_total)
	load_malloc=$(metric_delta "$directory/metrics/grpcload-baseline.prom" "$directory/metrics/grpcload-final.prom" go_memstats_mallocs_total)
	game_gc=$(metric_delta "$directory/metrics/game-baseline.prom" "$directory/metrics/game-final.prom" go_gc_duration_seconds_sum)
	load_gc=$(metric_delta "$directory/metrics/grpcload-baseline.prom" "$directory/metrics/grpcload-final.prom" go_gc_duration_seconds_sum)
	game_sched=$(histogram_quantile_delta "$directory/metrics/game-baseline.prom" "$directory/metrics/game-final.prom" go_sched_latencies_seconds_bucket '' 0.99 2>/dev/null || printf NA)
	load_sched=$(histogram_quantile_delta "$directory/metrics/grpcload-baseline.prom" "$directory/metrics/grpcload-final.prom" go_sched_latencies_seconds_bucket '' 0.99 2>/dev/null || printf NA)
	game_goroutines=$(max_sample_metric "$directory/metrics/game-samples.tsv" go_goroutines)
	load_goroutines=$(max_sample_metric "$directory/metrics/grpcload-samples.tsv" go_goroutines)
	host_idle=$(min_host_idle "$directory/host/top.txt")
	awk -v variant="$variant" -v attempt="$attempt" -v valid="$valid" -v reason="$reason" -v expected="$expected" -v admission="$admission" -v rps="$rps" -v mean="$mean" -v p50="$p50" -v p95="$p95" -v p99="$p99" \
		-v game_cpu="$game_cpu" -v load_cpu="$load_cpu" -v game_alloc="$game_alloc" -v load_alloc="$load_alloc" -v game_malloc="$game_malloc" -v load_malloc="$load_malloc" -v game_gc="$game_gc" -v load_gc="$load_gc" \
		-v game_sched="$game_sched" -v load_sched="$load_sched" -v game_goroutines="$game_goroutines" -v load_goroutines="$load_goroutines" -v host_idle="$host_idle" 'BEGIN {
		client_cpu_per_request=load_cpu
		game_cpu_per_request=game_cpu
		client_alloc_per_request=load_alloc
		game_alloc_per_request=game_alloc
		client_malloc_per_request=load_malloc
		game_malloc_per_request=game_malloc
		client_gc_per_request=load_gc
		game_gc_per_request=game_gc
		if (expected > 0) {
			client_cpu_per_request=load_cpu/expected; game_cpu_per_request=game_cpu/expected
			client_alloc_per_request=load_alloc/expected; game_alloc_per_request=game_alloc/expected
			client_malloc_per_request=load_malloc/expected; game_malloc_per_request=game_malloc/expected
			client_gc_per_request=load_gc/expected; game_gc_per_request=game_gc/expected
		}
		printf "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n", variant, attempt, valid, reason, expected, admission, rps, mean, p50, p95, p99, load_cpu, game_cpu, client_cpu_per_request, game_cpu_per_request, client_alloc_per_request, game_alloc_per_request, client_malloc_per_request, game_malloc_per_request, client_gc_per_request, game_gc_per_request, load_sched, game_sched, load_goroutines, game_goroutines, host_idle
	}' >>"$ARTIFACT_DIR/summary.tsv"
}

start_services() {
	local directory=$1 worker=$2 concurrency=$3 duration=$4 game_command load_command
	game_command=(env GOMAXPROCS="$GOMAXPROCS_VALUE" CORE_CASINO_METRICS_GAME__GRPC__SERVER__STREAM_WORKERS="$worker" "$ARTIFACT_DIR/bin/game" -config "$CONFIG_DIR/game.yaml" -env-prefix CORE_CASINO_METRICS_GAME__)
	load_command=(env GOMAXPROCS="$GOMAXPROCS_VALUE" "$ARTIFACT_DIR/bin/grpcload" -game-target "127.0.0.1:$GAME_GRPC_PORT" -client-connections "$CLIENT_CONNECTIONS" -concurrency "$concurrency" -duration "$duration" -payload-bytes "$PAYLOAD_BYTES" -warmup-requests "$WARMUP_REQUESTS" -request-timeout "$REQUEST_TIMEOUT" -metrics-addr "127.0.0.1:$LOAD_METRICS_PORT" -orchestration-dir "$directory/orchestration")
	if [[ -n "$PROFILE_WORKER" ]]; then
		game_command+=( -pprof-addr "127.0.0.1:$GAME_PPROF_PORT" )
		load_command+=( -pprof-addr "127.0.0.1:$LOAD_PPROF_PORT" )
	fi
	printf 'started_at=%s\n' "$(timestamp_now)" >"$directory/logs/game.log"
	"${game_command[@]}" >>"$directory/logs/game.log" 2>&1 & GAME_PID=$!
	wait_http "http://127.0.0.1:$GAME_HTTP_PORT/ready" "$SETUP_TIMEOUT_SECONDS" || return 1
	printf 'started_at=%s\n' "$(timestamp_now)" >"$directory/logs/grpcload.log"
	"${load_command[@]}" >>"$directory/logs/grpcload.log" 2>&1 & LOAD_PID=$!
	wait_http "http://127.0.0.1:$LOAD_METRICS_PORT/metrics" "$SETUP_TIMEOUT_SECONDS" || return 1
}

run_attempt() {
	local variant=$1 attempt=$2 duration=$3 concurrency=$4 category=$5
	local directory="$ARTIFACT_DIR/$category/attempt-$(printf '%02d' "$attempt")"
	local started run_id expected=0 reason="" valid=false rps=0 profile_status=0 final_marker_written=0 workload_reason=""
	CURRENT_ATTEMPT_DIR=$directory
	started=$(timestamp_now)
	mkdir -p "$directory/orchestration" "$directory/logs" "$directory/metrics" "$directory/host"
	write_metadata "$directory" "$variant" "$attempt" "$duration" "$concurrency" "$started"
	if ! port_is_free "$GAME_HTTP_PORT" || ! port_is_free "$GAME_GRPC_PORT" || ! port_is_free "$LOAD_METRICS_PORT"; then
		write_metadata "$directory" "$variant" "$attempt" "$duration" "$concurrency" "$started" "$(timestamp_now)"
		write_status "$directory" "$variant" "$attempt" false startup_failed 0 0
		append_summary "$directory" "$variant" "$attempt" false startup_failed 0
		return 1
	fi
	if ! start_services "$directory" "$variant" "$concurrency" "$duration"; then
		cleanup_processes
		write_metadata "$directory" "$variant" "$attempt" "$duration" "$concurrency" "$started" "$(timestamp_now)"
		write_status "$directory" "$variant" "$attempt" false readiness_failed 0 0
		append_summary "$directory" "$variant" "$attempt" false readiness_failed 0
		return 1
	fi
	if ! wait_for_file "$directory/orchestration/$GRPC_READY_MARKER" "$ORCHESTRATION_TIMEOUT_SECONDS"; then
		reason=orchestration_failed
	else
		if ! validate_identity_marker "$directory/orchestration/$GRPC_READY_MARKER"; then
			reason=orchestration_failed
		else
			run_id=$(json_string "$directory/orchestration/$GRPC_READY_MARKER" run_id)
		fi
		if [[ -z "$reason" ]] && ! capture_metrics "$directory" baseline; then
			reason=metrics_missing
		elif [[ -z "$reason" ]] && ! validate_baseline "$directory"; then
			reason=metrics_missing
		elif [[ -z "$reason" ]]; then
			write_metadata "$directory" "$variant" "$attempt" "$duration" "$concurrency" "$started"
			if ! write_marker "$directory/orchestration" "$GRPC_START_MARKER" "$run_id"; then
				reason=orchestration_failed
			else
				start_collectors "$directory"
				if [[ -n "$PROFILE_WORKER" ]]; then
					capture_profiles "$directory" & PROFILE_PID=$!
				fi
				if ! wait_for_file "$directory/orchestration/$GRPC_MEASURED_MARKER" "$((ORCHESTRATION_TIMEOUT_SECONDS + 60))"; then
					reason=orchestration_failed
				fi
				stop_collectors
				if [[ -z "$reason" ]] && ! validate_measured_marker "$directory/orchestration/$GRPC_MEASURED_MARKER" "$run_id"; then
					reason=orchestration_failed
				elif [[ -z "$reason" ]]; then
					workload_reason=$(marker_workload_reason "$directory/orchestration/$GRPC_MEASURED_MARKER") || reason=orchestration_failed
					if [[ -z "$reason" && -n "$workload_reason" ]]; then reason=$workload_reason; fi
				fi
				expected=$(json_number "$directory/orchestration/$GRPC_MEASURED_MARKER" successful_requests 2>/dev/null || printf 0)
				if [[ -z "$expected" ]]; then expected=0; fi
				# grpcload remains alive until final scrape; capture final before
				# releasing the barrier so all measured counters have one fixed end.
				if ! capture_metrics "$directory" final; then reason=${reason:-metrics_missing}; fi
				if [[ -z "$reason" ]] && ! validate_terminal_gauges "$directory/metrics/game-final.prom" "$directory/metrics/grpcload-final.prom"; then
					reason=terminal_gauge_nonzero
				fi
				if [[ -n "$PROFILE_WORKER" && -n "$PROFILE_PID" ]]; then
					wait "$PROFILE_PID" >/dev/null 2>&1 || profile_status=1
					PROFILE_PID=""
				fi
				if write_marker "$directory/orchestration" "$GRPC_FINAL_MARKER" "$run_id"; then
					final_marker_written=1
				else
					reason=${reason:-orchestration_failed}
				fi
				if ((final_marker_written)) && ! validate_marker_set "$directory" "$run_id"; then
					reason=orchestration_failed
				fi
			fi
		fi
	fi
	if [[ -n "$LOAD_PID" ]]; then
		if ((final_marker_written)); then
			if ! wait "$LOAD_PID" >/dev/null 2>&1; then reason=${reason:-load_exit_failed}; fi
		else
			stop_process "$LOAD_PID" grpcload
			reason=${reason:-orchestration_failed}
		fi
		LOAD_PID=""
	fi
	if [[ -n "$GAME_PID" ]]; then stop_process "$GAME_PID" game; GAME_PID=""; fi
	if ! wait_ports_free 10; then reason=${reason:-load_exit_failed}; fi
	if [[ -z "$reason" ]]; then
		if ! validate_counter_contract "$directory" "$expected"; then reason=counter_mismatch; fi
		if [[ -z "$reason" ]] && ! validate_periodic_metrics "$directory"; then reason=metrics_missing; fi
		if [[ -z "$reason" ]] && ! validate_host_samples "$directory"; then reason=host_sample_missing; fi
	fi
	if [[ -z "$reason" ]]; then
		valid=true
		rps=$(calculate_rps "$expected" "$(json_number "$directory/orchestration/$GRPC_MEASURED_MARKER" admission_duration_seconds)" 2>/dev/null || printf 0)
	fi
	write_metadata "$directory" "$variant" "$attempt" "$duration" "$concurrency" "$started" "$(timestamp_now)"
	if ((profile_status)); then reason=${reason:-profile_missing}; valid=false; fi
	write_status "$directory" "$variant" "$attempt" "$valid" "${reason:-}" "$expected" "$rps"
	append_summary "$directory" "$variant" "$attempt" "$valid" "${reason:-}" "$expected"
	[[ "$valid" == true ]]
}

build_binaries() {
	mkdir -p "$ARTIFACT_DIR/bin"
	(cd "$ROOT_DIR" && go build -o "$ARTIFACT_DIR/bin/game" ./examples/metrics/game && go build -o "$ARTIFACT_DIR/bin/grpcload" ./examples/metrics/grpcload) || return 1
	shasum -a 256 "$ARTIFACT_DIR/bin/game" "$ARTIFACT_DIR/bin/grpcload" >"$ARTIFACT_DIR/binaries.sha256"
	chmod 0640 "$ARTIFACT_DIR/binaries.sha256"
}

write_manifest() {
	{
		printf 'campaign_started_at=%s\n' "$(timestamp_now)"
		printf 'git_head=%s\n' "$(cd "$ROOT_DIR" && git rev-parse HEAD 2>/dev/null || printf unknown)"
		printf 'git_status=%s\n' "$(cd "$ROOT_DIR" && git status --short | tr '\n' ' ' || printf unknown)"
		printf 'go_version=%s\n' "$(go version)"
		printf 'grpc_go_version=%s\n' "$(cd "$ROOT_DIR" && go list -m -f '{{.Version}}' google.golang.org/grpc 2>/dev/null || printf unknown)"
		printf 'os=%s\nlogical_cpu=%s\n' "$(uname -a)" "$(sysctl -n hw.logicalcpu 2>/dev/null || printf unknown)"
		printf 'gomaxprocs=%s\nconcurrency=%s\nclient_connections=%s\npayload_bytes=%s\n' "$GOMAXPROCS_VALUE" "$CONCURRENCY" "$CLIENT_CONNECTIONS" "$PAYLOAD_BYTES"
		printf 'warmup_requests=%s\nrequest_timeout=%s\nmax_concurrent_streams=0\nwrite_buffer_size_bytes=0\n' "$WARMUP_REQUESTS" "$REQUEST_TIMEOUT"
	} >"$ARTIFACT_DIR/manifest.txt"
	chmod 0640 "$ARTIFACT_DIR/manifest.txt"
}

write_summary_header() {
	printf 'variant\tattempt\tvalid\tinvalid_reason\tsuccessful_requests\tadmission_seconds\trps\tlatency_mean_seconds\tlatency_p50_upper_seconds\tlatency_p95_upper_seconds\tlatency_p99_upper_seconds\tclient_cpu_seconds\tgame_cpu_seconds\tclient_cpu_seconds_per_request\tgame_cpu_seconds_per_request\tclient_alloc_bytes_per_request\tgame_alloc_bytes_per_request\tclient_allocs_per_request\tgame_allocs_per_request\tclient_gc_seconds\tgame_gc_seconds\tclient_sched_p99_seconds\tgame_sched_p99_seconds\tclient_max_goroutines\tgame_max_goroutines\thost_idle_min_percent\n' >"$ARTIFACT_DIR/summary.tsv"
}

preflight() {
	local command port
	for command in curl go git jq shasum awk sed mktemp lsof top sysctl redis-cli grep seq sleep date uname; do require_command "$command"; done
	redis-cli -h "$REDIS_HOST" -p "$REDIS_PORT" ping | grep -qx PONG || die "Redis is not reachable at $REDIS_HOST:$REDIS_PORT"
	for port in "$GAME_HTTP_PORT" "$GAME_GRPC_PORT" "$LOAD_METRICS_PORT"; do
		port_is_free "$port" || die "port $port already has a listener"
	done
	if [[ -n "$PROFILE_WORKER" ]]; then
		for port in "$GAME_PPROF_PORT" "$LOAD_PPROF_PORT"; do
			port_is_free "$port" || die "pprof port $port already has a listener"
		done
	fi
	validate_positive_integer attempts "$ATTEMPTS" || die "attempts must be a positive integer"
	validate_duration "$DURATION" || die "invalid duration: $DURATION"
	validate_worker_spec "0,4,8" || die "internal worker matrix is invalid"
	if [[ -n "$PROFILE_WORKER" ]]; then
		validate_worker_value "$PROFILE_WORKER" || die "profile-worker must be 0, 4, or 8"
	fi
}

run_correctness() {
	local saved_worker=$PROFILE_WORKER result=0
	# correctness 使用 worker=0、single worker、10s，且不進 A/B median。
	PROFILE_WORKER=""
	run_attempt 0 1 10s 1 correctness || result=1
	PROFILE_WORKER=$saved_worker
	return "$result"
}

run_campaign() {
	local -a order
	local round attempt worker result=0
	for round in $(seq 1 "$ATTEMPTS"); do
		case $(( (round - 1) % 3 )) in
		0) order=(0 4 8) ;;
		1) order=(4 8 0) ;;
		2) order=(8 0 4) ;;
		esac
		for worker in "${order[@]}"; do
			run_attempt "$worker" "$round" "$DURATION" "$CONCURRENCY" "workers-$(printf '%03d' "$worker")" || result=1
		done
	done
	return "$result"
}

run_profile_only() {
	local saved_profile=$PROFILE_WORKER result=0
	PROFILE_WORKER=$saved_profile
	run_attempt "$PROFILE_WORKER" 1 "$DURATION" "$CONCURRENCY" "profiles/workers-$(printf '%03d' "$PROFILE_WORKER")" || result=1
	return "$result"
}

run_metadata_benchmark() {
	(
		cd "$ROOT_DIR"
		go test ./pkg/logging ./pkg/gatelink -run '^$' -bench 'IncomingMetadata' -benchmem -count 5
	) >"$ARTIFACT_DIR/metadata-benchmark.txt" 2>&1
}

main() {
	parse_args "$@"
	if [[ -z "$ARTIFACT_DIR" ]]; then ARTIFACT_DIR="$ROOT_DIR/artifacts/unary-optimization/$(date -u +%Y%m%dT%H%M%SZ)"; fi
	mkdir -p "$ARTIFACT_DIR"
	ARTIFACT_DIR=$(cd "$ARTIFACT_DIR" && pwd)
	preflight
	write_manifest
	write_summary_header
	if ! build_binaries; then
		write_atomic "$ARTIFACT_DIR/campaign-status.json" '{"valid":false,"reason":"build_failed"}'
		return 1
	fi
	local result=0
	if [[ -n "$PROFILE_WORKER" ]]; then
		run_profile_only || result=1
	else
		if ! run_correctness; then
			result=1
		else
			run_campaign || result=1
		fi
	fi
	if ((METADATA_BENCHMARK)); then
		run_metadata_benchmark || result=1
	fi
	if ((result == 0)); then
		write_atomic "$ARTIFACT_DIR/campaign-status.json" '{"valid":true,"summary":"summary.tsv"}'
	else
		write_atomic "$ARTIFACT_DIR/campaign-status.json" '{"valid":false,"summary":"summary.tsv"}'
	fi
	return "$result"
}

if [[ "${BASH_SOURCE[0]:-}" == "$0" ]]; then
	trap cleanup_processes EXIT INT TERM
	main "$@"
fi
