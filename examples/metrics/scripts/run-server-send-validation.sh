#!/usr/bin/env bash
# Server-send validation 唯一入口；只管理本 script 建立的 process 與 artifacts。
set -Eeuo pipefail
export LC_ALL=C

ROOT_DIR=$(cd "$(dirname "$0")/../../.." && pwd)
CONFIG_DIR="$ROOT_DIR/examples/metrics/configs"
ARTIFACT_ROOT="${ARTIFACT_ROOT:-$ROOT_DIR/artifacts/server-send/$(date -u +%Y%m%dT%H%M%SZ)}"
WORKLOAD=broadcast
INTERVAL=33ms
CONNECTIONS=1000
ATTEMPTS=3
DURATION=30s
WARMUP=5s
DRAIN=10s
PROFILE_TARGET=""
VALIDATE_ONLY=""
TRACE=0
FULL_CAMPAIGN=0
CAMPAIGN_PHASE=single
CASE_ARTIFACT_ROOT=""

usage() {
	cat <<'EOF'
Usage: run-server-send-validation.sh [options]
  --workload broadcast|player --interval 33ms|16ms
  --connections N --attempts N --duration D --warmup D --drain D
  --artifact-root DIR --profile-target game|gate|load
  --trace                         collect the optional 5s Go trace in profile mode
  --full-campaign                 run correctness gate and interleaved 1000-connection matrix
  --validate-only ATTEMPT_DIR
EOF
}

die() { echo "error: $*" >&2; exit 2; }

parse_args() {
	while (($#)); do
		case "$1" in
		--workload) [[ $# -ge 2 ]] || die "$1 requires a value"; WORKLOAD=$2; shift 2 ;;
		--interval) [[ $# -ge 2 ]] || die "$1 requires a value"; INTERVAL=$2; shift 2 ;;
		--connections) [[ $# -ge 2 ]] || die "$1 requires a value"; CONNECTIONS=$2; shift 2 ;;
		--attempts) [[ $# -ge 2 ]] || die "$1 requires a value"; ATTEMPTS=$2; shift 2 ;;
		--duration) [[ $# -ge 2 ]] || die "$1 requires a value"; DURATION=$2; shift 2 ;;
		--warmup) [[ $# -ge 2 ]] || die "$1 requires a value"; WARMUP=$2; shift 2 ;;
		--drain) [[ $# -ge 2 ]] || die "$1 requires a value"; DRAIN=$2; shift 2 ;;
		--artifact-root) [[ $# -ge 2 ]] || die "$1 requires a value"; ARTIFACT_ROOT=$2; shift 2 ;;
		--profile-target) [[ $# -ge 2 ]] || die "$1 requires a value"; PROFILE_TARGET=$2; shift 2 ;;
		--trace) TRACE=1; shift ;;
		--full-campaign) FULL_CAMPAIGN=1; shift ;;
		--validate-only) [[ $# -ge 2 ]] || die "$1 requires a value"; VALIDATE_ONLY=$2; shift 2 ;;
		-h|--help) usage; exit 0 ;;
		*) die "unknown option: $1" ;;
		esac
	done
}

require_command() { command -v "$1" >/dev/null 2>&1 || die "required command not found: $1"; }

# macOS 的 BSD date 不支援 GNU `%N`；統一輸出合法 RFC3339Nano，避免把
# literal `%N` 寫進 phase／metrics／pprof evidence。優先使用 python3 或
# perl 取得 nanoseconds；兩者皆不可用時才退回合法但零 nanos 的 timestamp。
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

validate_host_top() {
	local file=$1
	awk '
		/^CPU usage:/ {
			user=$3; sub(/%/, "", user)
			sys=$5; sub(/%/, "", sys)
			idle=$7; sub(/%/, "", idle)
			if (user ~ /^[0-9]+([.][0-9]+)?$/ && sys ~ /^[0-9]+([.][0-9]+)?$/ && idle ~ /^[0-9]+([.][0-9]+)?$/) {
				sum=user + sys + idle
				if (sum >= 99 && sum <= 101) valid=1
			}
		}
		END { exit(valid ? 0 : 1) }
	' "$file"
}

validate_process_top() {
	local file=$1 pid
	shift
	for pid in "$@"; do
		awk -v expected="$pid" '
			$1 == expected && $3 ~ /^[0-9]+([.][0-9]+)?$/ && $4 != "" && $5 ~ /^[0-9]+$/ { found=1 }
			END { exit(found ? 0 : 1) }
		' "$file" || return 1
	done
}

validate_vm_stat() {
	local file=$1
	awk '
		/^timestamp=/ {
			stamp=$1
			sub(/^timestamp=/, "", stamp)
			if (stamp == "" || (previous != "" && stamp <= previous)) invalid=1
			previous=stamp
			count++
		}
		END { exit(count >= 2 && !invalid ? 0 : 1) }
	' "$file"
}

validate_redis_tsv() {
	local file=$1 coverage_start=${2:-} coverage_end=${3:-}
	local timestamp used_cpu_sys used_cpu_user total_commands instantaneous_ops connected_clients blocked_clients rejected_connections status
	local previous= count=0 before=0 inside=0 after=0 current start_nano= end_nano=
	[[ -s "$file" ]] || return 1
	if [[ -n "$coverage_start" && -n "$coverage_end" ]]; then
		start_nano=$(timestamp_to_unix_nano "$coverage_start") || return 1
		end_nano=$(timestamp_to_unix_nano "$coverage_end") || return 1
		((end_nano >= start_nano)) || return 1
	fi
	IFS=$'\t' read -r timestamp used_cpu_sys used_cpu_user total_commands instantaneous_ops connected_clients blocked_clients rejected_connections status <"$file"
	[[ "$timestamp" == timestamp && "$used_cpu_sys" == used_cpu_sys && "$used_cpu_user" == used_cpu_user &&
		"$total_commands" == total_commands_processed && "$instantaneous_ops" == instantaneous_ops_per_sec &&
		"$connected_clients" == connected_clients && "$blocked_clients" == blocked_clients &&
		"$rejected_connections" == rejected_connections && "$status" == status ]] || return 1
	while IFS=$'\t' read -r timestamp used_cpu_sys used_cpu_user total_commands instantaneous_ops connected_clients blocked_clients rejected_connections status; do
		[[ -n "$timestamp" ]] || continue
		[[ "$status" == ok ]] || return 1
		[[ "$used_cpu_sys" =~ ^[0-9]+([.][0-9]+)?$ && "$used_cpu_user" =~ ^[0-9]+([.][0-9]+)?$ &&
			"$total_commands" =~ ^[0-9]+$ && "$instantaneous_ops" =~ ^[0-9]+$ &&
			"$connected_clients" =~ ^[0-9]+$ && "$blocked_clients" =~ ^[0-9]+$ && "$rejected_connections" =~ ^[0-9]+$ ]] || return 1
		current=$(timestamp_to_unix_nano "$timestamp") || return 1
		[[ -z "$previous" || "$current" -gt "$previous" ]] || return 1
		previous=$current
		count=$((count + 1))
		if [[ -n "$coverage_start" && -n "$coverage_end" ]]; then
			if ((current < start_nano)); then before=$((before + 1));
			elif ((current <= end_nano)); then inside=$((inside + 1));
			else after=$((after + 1)); fi
		fi
	done < <(tail -n +2 "$file")
	((count >= 4)) || return 1
	if [[ -n "$coverage_start" && -n "$coverage_end" ]]; then
		((before >= 1 && inside >= 2 && after >= 1)) || return 1
	fi
}

validate_nettop() {
	local file=$1 pid
	shift
	for pid in "$@"; do
		grep -qE "\\.${pid}," "$file" || return 1
	done
}

validate_profile_evidence() {
	local dir=$1 measured_start=$2 measured_end=$3 status_file target expected_pid checksum
	local event start end status pid sha start_nano end_nano window_start window_end
	status_file="$dir/pprof/status.tsv"
	[[ -s "$status_file" ]] || return 1
	target=$(sed -n 's/^pprof_target=//p' "$dir/metadata.txt")
	case "$target" in game) expected_pid=$(sed -n 's/^game_pid=//p' "$dir/metadata.txt") ;; gate) expected_pid=$(sed -n 's/^gate_pid=//p' "$dir/metadata.txt") ;; load) expected_pid=$(sed -n 's/^load_pid=//p' "$dir/metadata.txt") ;; *) return 1 ;; esac
	checksum=$(sed -n 's/^binary_sha256=//p' "$dir/pprof/window.tsv")
	[[ -n "$expected_pid" && -n "$checksum" ]] || return 1
	window_start=$(timestamp_to_unix_nano "$measured_start") || return 1
	window_end=$(timestamp_to_unix_nano "$measured_end") || return 1
	local -a seen=()
	while IFS=$'\t' read -r event start end status pid sha; do
		[[ "$event" == event ]] && continue
		[[ "$event" == cpu || "$event" == heap || "$event" == goroutine || "$event" == trace ]] || return 1
		[[ "$status" == ok && "$pid" == "$expected_pid" && "$sha" == "$checksum" ]] || return 1
		start_nano=$(timestamp_to_unix_nano "$start") || return 1
		end_nano=$(timestamp_to_unix_nano "$end") || return 1
		((start_nano <= end_nano && start_nano >= window_start && end_nano <= window_end)) || return 1
		seen+=("$event")
	done <"$status_file"
	for event in cpu heap goroutine; do
		printf '%s\n' "${seen[@]}" | grep -qx "$event" || return 1
	done
	if [[ "$(sed -n 's/^trace=//p' "$dir/metadata.txt")" == 1 ]]; then
		printf '%s\n' "${seen[@]}" | grep -qx trace || return 1
	fi
	go tool pprof -top "$dir/pprof/$target-cpu-20s.pb.gz" >/dev/null 2>&1 || return 1
	go tool pprof -top "$dir/pprof/$target-heap.pb.gz" >/dev/null 2>&1 || return 1
	if [[ "$(sed -n 's/^trace=//p' "$dir/metadata.txt")" == 1 ]]; then
		go tool trace -d=parsed "$dir/pprof/$target-trace-5s.out" >/dev/null 2>&1 || return 1
	fi
}

timestamp_to_unix_nano() {
	local stamp=$1 whole fraction seconds
	[[ "$stamp" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]{1,9}Z$ ]] || return 1
	whole=${stamp%%.*}
	fraction=${stamp#*.}
	fraction=${fraction%Z}
	while ((${#fraction} < 9)); do fraction="${fraction}0"; done
	fraction=${fraction:0:9}
	if seconds=$(date -j -u -f '%Y-%m-%dT%H:%M:%S' "$whole" +%s 2>/dev/null); then
		:
	elif seconds=$(date -u -d "$whole" +%s 2>/dev/null); then
		:
	else
		return 1
	fi
	printf '%s\n' "$((seconds * 1000000000 + 10#$fraction))"
}

validate_metrics_index() {
	local file=$1 coverage_start=${2:-} coverage_end=${3:-}
	local stamp status endpoint path previous current first_stamp last_stamp count=0
	[[ -s "$file" ]] || return 1
	while IFS=$'\t' read -r stamp status endpoint path; do
		[[ "$status" == 200 && -n "$endpoint" && -s "$path" ]] || return 1
		current=$(timestamp_to_unix_nano "$stamp") || return 1
		[[ -n "$first_stamp" ]] || first_stamp=$stamp
		last_stamp=$stamp
		if [[ -n "$previous" ]]; then
			(( current > previous && current - previous <= 2500000000 )) || return 1
		fi
		previous=$current
		count=$((count + 1))
	done < <(LC_ALL=C sort -t $'\t' -k1,1 "$file")
	((count >= 2)) || return 1
	if [[ -n "$coverage_start" || -n "$coverage_end" ]]; then
		[[ -n "$coverage_start" && -n "$coverage_end" ]] || return 1
		local first current_start current_end
		first=$(timestamp_to_unix_nano "$first_stamp") || return 1
		current=$(timestamp_to_unix_nano "$last_stamp") || return 1
		current_start=$(timestamp_to_unix_nano "$coverage_start") || return 1
		current_end=$(timestamp_to_unix_nano "$coverage_end") || return 1
		(( first <= current_start && current >= current_end )) || return 1
	fi
}

validate_operation_metrics() {
	local dir=$1 target operation
	if [[ "$WORKLOAD" == player ]]; then
		target=player
		operation=player
	else
		target=room
		operation=broadcast
	fi
	grep -q "^gaming_core_game_server_send_requests_total{.*operation=\"$operation\"" "$dir/metrics/baseline-game.prom" || return 1
	grep -q "^gaming_core_game_server_send_duration_seconds_count{.*operation=\"$operation\"" "$dir/metrics/baseline-game.prom" || return 1
	grep -q "^gaming_core_gate_server_send_delivery_duration_seconds_count{.*target=\"$target\"" "$dir/metrics/baseline-gate.prom" || return 1
	grep -q '^gaming_core_gate_websocket_writes_total{.*source="server_send"' "$dir/metrics/baseline-gate.prom" || return 1
}

# 只用實際成功送出的 tick 判定 client delivery；planned 與 attempted 的差額
# 屬於 producer cadence，不應再次被當成 delivery loss。
client_delivery_matches_emitted() {
	local received=$1 sender_success=$2 connections=$3 duplicate=$4 gap=$5 invalid=$6 reader_failures=$7 expected
	[[ "$received" =~ ^[0-9]+$ && "$sender_success" =~ ^[0-9]+$ && "$connections" =~ ^[0-9]+$ &&
		"$duplicate" =~ ^[0-9]+$ && "$gap" =~ ^[0-9]+$ && "$invalid" =~ ^[0-9]+$ &&
		"$reader_failures" =~ ^[0-9]+$ ]] || return 1
	expected=$((sender_success * connections))
	[[ "$received" -eq "$expected" && "$duplicate" -eq 0 && "$gap" -eq 0 &&
		"$invalid" -eq 0 && "$reader_failures" -eq 0 ]]
}

phase_timestamp() {
	local file=$1 phase=$2 event=$3
	sed -n "s/.*\"timestamp\":\"\([^\"]*\)\".*\"phase\":\"$phase\".*\"event\":\"$event\".*/\1/p" "$file" | tail -n 1
}

duration_seconds() {
	local value=$1 number
	case "$value" in
		([0-9]*ms) number=${value%ms}; [[ "$number" =~ ^[0-9]+$ ]] || return 1; printf '%d\n' "$(((number + 999) / 1000))" ;;
		([0-9]*s) number=${value%s}; [[ "$number" =~ ^[0-9]+$ ]] || return 1; printf '%s\n' "$number" ;;
		([0-9]*m) number=${value%m}; [[ "$number" =~ ^[0-9]+$ ]] || return 1; printf '%s\n' "$((number * 60))" ;;
		([0-9]*h) number=${value%h}; [[ "$number" =~ ^[0-9]+$ ]] || return 1; printf '%s\n' "$((number * 3600))" ;;
		(*) return 1 ;;
	esac
}

verify_binary_checksums() {
	local checksum_file=$1 bin_dir=$2 name expected actual
	for name in game gate load; do
		expected=$(awk -v name="$name" '{path=$2; sub(/^.*\//, "", path); if (path == name) {print $1; exit}}' "$checksum_file")
		actual=$(shasum -a 256 "$bin_dir/$name" | awk '{print $1}')
		[[ -n "$expected" && "$actual" == "$expected" ]] || return 1
	done
}

validate_attempt() {
	local dir=$1 file required found=0 game_pid gate_pid load_pid expected_connections warmup_run_id result_run_id drained_run_id baseline_run_id measured_run_id final_run_id campaign_root campaign_phase campaign_checksum metadata_checksum name
	[[ -d "$dir" ]] || return 1
	dir=$(cd "$dir" 2>/dev/null && pwd) || return 1
	for required in metadata.txt phase-events.jsonl run-status.json commands.txt game.log gate.log load.log; do
		file="$dir/$required"
		[[ -s "$file" ]] || { echo "environment_invalid: missing $file" >&2; return 1; }
	done
	campaign_root=$(sed -n 's/^campaign_root=//p' "$dir/metadata.txt")
	campaign_phase=$(sed -n 's/^campaign_phase=//p' "$dir/metadata.txt")
	[[ -n "$campaign_root" && -n "$campaign_phase" && -d "$campaign_root" ]] || {
		echo "environment_invalid: campaign provenance metadata is missing" >&2
		return 1
	}
	campaign_root=$(cd "$campaign_root" 2>/dev/null && pwd) || return 1
	case "$campaign_phase" in correctness|matrix|profile|single) ;; *) echo "environment_invalid: campaign phase is invalid" >&2; return 1 ;; esac
	case "$dir/" in "$campaign_root"/*) ;; *) echo "environment_invalid: attempt is outside campaign root" >&2; return 1 ;; esac
	if [[ "$campaign_phase" == matrix ]]; then
		[[ -s "$campaign_root/correctness-gate.json" ]] || {
			echo "environment_invalid: correctness gate is missing" >&2
			return 1
		}
	fi
	[[ -s "$campaign_root/binary-checksums.txt" && -s "$campaign_root/source-state.txt" && -s "$dir/checksums.txt" && -s "$dir/source-state.txt" ]] || {
		echo "environment_invalid: binary/source provenance is missing" >&2
		return 1
	}
	verify_binary_checksums "$campaign_root/binary-checksums.txt" "$dir/bin" || {
		echo "environment_invalid: attempt binary checksums differ from campaign snapshot" >&2
		return 1
	}
	verify_binary_checksums "$dir/checksums.txt" "$dir/bin" || {
		echo "environment_invalid: attempt checksum manifest does not match binaries" >&2
		return 1
	}
	for name in game gate load; do
		campaign_checksum=$(awk -v name="$name" '{path=$2; sub(/^.*\//, "", path); if (path == name) {print $1; exit}}' "$campaign_root/binary-checksums.txt")
		metadata_checksum=$(sed -n "s/^binary_sha256_${name}=//p" "$dir/metadata.txt")
		[[ -n "$campaign_checksum" && "$metadata_checksum" == "$campaign_checksum" ]] || {
			echo "environment_invalid: metadata checksum for $name differs from campaign snapshot" >&2
			return 1
		}
	done
	cmp -s "$campaign_root/source-state.txt" "$dir/source-state.txt" || {
		echo "environment_invalid: attempt source provenance differs from campaign snapshot" >&2
		return 1
	}
	for file in "$dir"/metrics/*.prom; do [[ -s "$file" ]] && found=1; done
	[[ "$found" -eq 1 ]] || { echo "environment_invalid: no metrics snapshots" >&2; return 1; }
	for required in game.tsv gate.tsv load.tsv; do
		[[ -s "$dir/metrics/$required" ]] || { echo "environment_invalid: missing metrics index $required" >&2; return 1; }
		validate_metrics_index "$dir/metrics/$required" || { echo "environment_invalid: invalid metrics index $required" >&2; return 1; }
	done
	[[ -s "$dir/control/clients-ready.json" ]] || { echo "environment_invalid: missing clients-ready marker" >&2; return 1; }
	expected_connections=$(sed -n 's/^connections=\([0-9][0-9]*\)$/\1/p' "$dir/metadata.txt")
	[[ -n "$expected_connections" ]] || { echo "environment_invalid: metadata connections is missing" >&2; return 1; }
	local ready_connections
	ready_connections=$(sed -n 's/.*"connections":\([0-9][0-9]*\).*/\1/p' "$dir/control/clients-ready.json")
	[[ "$ready_connections" == "$expected_connections" ]] || {
		echo "environment_invalid: clients-ready connections=$ready_connections, want $expected_connections" >&2
		return 1
	}
	for required in final-game.prom final-gate.prom final-load.prom; do
		[[ -s "$dir/metrics/$required" ]] || { echo "environment_invalid: missing final metrics snapshot $required" >&2; return 1; }
	done
	for required in host-top.txt process-top.txt vm-stat.txt nettop.txt netstat-before.txt netstat-after.txt redis.tsv; do
		[[ -s "$dir/os/$required" ]] || { echo "environment_invalid: missing OS evidence $required" >&2; return 1; }
	done
	validate_redis_tsv "$dir/os/redis.tsv" || { echo "environment_invalid: Redis INFO collector is missing or invalid" >&2; return 1; }
	game_pid=$(sed -n 's/^game_pid=//p' "$dir/metadata.txt")
	gate_pid=$(sed -n 's/^gate_pid=//p' "$dir/metadata.txt")
	load_pid=$(sed -n 's/^load_pid=//p' "$dir/metadata.txt")
	validate_host_top "$dir/os/host-top.txt" || { echo "environment_invalid: host CPU sample is not parseable" >&2; return 1; }
	validate_process_top "$dir/os/process-top.txt" "$game_pid" "$gate_pid" "$load_pid" || {
		echo "environment_invalid: process CPU/RSS/thread sample is not parseable" >&2
		return 1
	}
	validate_vm_stat "$dir/os/vm-stat.txt" || { echo "environment_invalid: vm_stat timestamps are not increasing" >&2; return 1; }
	validate_nettop "$dir/os/nettop.txt" "$game_pid" "$gate_pid" "$load_pid" || {
		echo "environment_invalid: nettop process rows are not parseable" >&2
		return 1
	}
	[[ ! -e "$dir/os/network_process_unavailable" ]] || {
		echo "environment_invalid: nettop did not expose a process row" >&2
		return 1
	}
	for required in redis-before.txt redis-after.txt; do
		[[ -s "$dir/$required" ]] || { echo "environment_invalid: missing Redis evidence $required" >&2; return 1; }
	done
	for pid in "$game_pid" "$gate_pid" "$load_pid"; do
		[[ -n "$pid" && -s "$dir/os/lsof-collectors-ready-$pid.txt" ]] || {
			echo "environment_invalid: missing lsof snapshot for pid $pid" >&2
			return 1
		}
	done
	grep -q '"evidence_status":"evidence_valid"' "$dir/run-status.json" || {
		echo "environment_invalid: run-status does not contain an evidence-valid attempt" >&2
		return 1
	}
	grep -q '"workload_status"' "$dir/run-status.json" || return 1
	grep -q '"terminal_gauges_zero":' "$dir/run-status.json" || return 1
	for field in warmup_cadence_failed warmup_sender_failed warmup_delivery_failed; do
		grep -q "\"$field\":" "$dir/run-status.json" || return 1
	done
	for phase in build dependency_ready services_ready load_started clients_ready collectors_ready; do
		grep -q "\"phase\":\"$phase\".*\"event\":\"start\"" "$dir/phase-events.jsonl" || return 1
		grep -q "\"phase\":\"$phase\".*\"event\":\"end\"" "$dir/phase-events.jsonl" || return 1
	done
	if grep -q '"warmup_started":true' "$dir/run-status.json"; then
		grep -q '"phase":"warmup_running".*"event":"start"' "$dir/phase-events.jsonl" || return 1
		grep -q '"phase":"warmup_running".*"event":"end"' "$dir/phase-events.jsonl" || return 1
		for required in warmup-result.json warmup-drained.json; do
			[[ -s "$dir/control/$required" ]] || { echo "environment_invalid: missing warm-up marker $required" >&2; return 1; }
		done
		warmup_run_id=$(sed -n 's/.*"run_id":"\([^"]*\)".*/\1/p' "$dir/control/warmup-started.json")
		result_run_id=$(sed -n 's/.*"run_id":"\([^"]*\)".*/\1/p' "$dir/control/warmup-result.json")
		drained_run_id=$(sed -n 's/.*"run_id":"\([^"]*\)".*/\1/p' "$dir/control/warmup-drained.json")
		[[ -n "$warmup_run_id" && "$warmup_run_id" == "$result_run_id" && "$warmup_run_id" == "$drained_run_id" ]] || {
			echo "environment_invalid: warm-up marker run IDs do not match" >&2
			return 1
		}
		validate_warmup_result_line "$(cat "$dir/control/warmup-result.json")" || {
			echo "environment_invalid: warm-up result marker is invalid" >&2
			return 1
		}
		validate_warmup_drained "$(cat "$dir/control/warmup-drained.json")" || {
			echo "environment_invalid: warm-up drained marker is invalid" >&2
			return 1
		}
	fi
	if grep -q '"measured_started":true' "$dir/run-status.json"; then
		grep -q '"phase":"measured_running".*"event":"start"' "$dir/phase-events.jsonl" || return 1
		grep -q '"phase":"measured_running".*"event":"end"' "$dir/phase-events.jsonl" || return 1
		grep -q '"phase":"measured_draining".*"event":"start"' "$dir/phase-events.jsonl" || return 1
		grep -q '"phase":"measured_draining".*"event":"end"' "$dir/phase-events.jsonl" || return 1
	fi
	grep -q '"phase":"final_snapshot".*"event":"start"' "$dir/phase-events.jsonl" || return 1
	grep -q '"phase":"final_snapshot".*"event":"end"' "$dir/phase-events.jsonl" || return 1
	if grep -q '"measured_started":true' "$dir/run-status.json"; then
		[[ -s "$dir/control/baseline-ready.json" ]] || {
			echo "environment_invalid: missing baseline-ready barrier" >&2
			return 1
		}
		for required in baseline-game.prom baseline-gate.prom baseline-load.prom; do
			[[ -s "$dir/metrics/$required" ]] || { echo "environment_invalid: missing baseline metrics snapshot $required" >&2; return 1; }
		done
		validate_operation_metrics "$dir" || { echo "environment_invalid: baseline operation metric family is missing" >&2; return 1; }
		validate_game_terminal_gauges "$dir/metrics/baseline-game.prom" || { echo "environment_invalid: Game baseline terminal gauges are not zero" >&2; return 1; }
		validate_gate_terminal_gauges "$dir/metrics/baseline-gate.prom" || { echo "environment_invalid: Gate baseline terminal gauges are not zero" >&2; return 1; }
		baseline_run_id=$(sed -n 's/.*"run_id":"\([^\"]*\)".*/\1/p' "$dir/control/baseline-ready.json")
		warmup_run_id=$(sed -n 's/.*"run_id":"\([^\"]*\)".*/\1/p' "$dir/control/warmup-started.json")
		[[ -n "$baseline_run_id" && "$baseline_run_id" == "$warmup_run_id" ]] || {
			echo "environment_invalid: baseline marker run ID does not match warm-up" >&2
			return 1
		}
	fi
	if grep -q '"measured_started":true' "$dir/run-status.json"; then
		local measured_start measured_end
		measured_start=$(phase_timestamp "$dir/phase-events.jsonl" measured_running start)
		measured_end=$(phase_timestamp "$dir/phase-events.jsonl" measured_running end)
		[[ -n "$measured_start" && -n "$measured_end" ]] || {
			echo "environment_invalid: measured phase timestamps are missing" >&2
			return 1
		}
		for required in game.tsv gate.tsv load.tsv; do
			validate_metrics_index "$dir/metrics/$required" "$measured_start" "$measured_end" || {
				echo "environment_invalid: metrics index $required does not cover measured phase" >&2
				return 1
			}
			done
		validate_redis_tsv "$dir/os/redis.tsv" "$measured_start" "$measured_end" || {
			echo "environment_invalid: Redis INFO collector does not cover measured phase" >&2
			return 1
		}
		for required in final-ready.json final-scrape.json; do
			[[ -s "$dir/control/$required" ]] || { echo "environment_invalid: missing final scrape barrier $required" >&2; return 1; }
		done
		measured_run_id=$(sed -n 's/.*"run_id":"\([^\"]*\)".*/\1/p' "$dir/control/measured-started.json")
		final_run_id=$(sed -n 's/.*"run_id":"\([^\"]*\)".*/\1/p' "$dir/control/final-ready.json")
		[[ -n "$measured_run_id" && "$measured_run_id" == "$final_run_id" ]] || {
			echo "environment_invalid: final-ready marker run ID does not match measured" >&2
			return 1
		}
		final_run_id=$(sed -n 's/.*"run_id":"\([^\"]*\)".*/\1/p' "$dir/control/final-scrape.json")
		[[ -n "$measured_run_id" && "$measured_run_id" == "$final_run_id" ]] || {
			echo "environment_invalid: final-scrape marker run ID does not match measured" >&2
			return 1
		}
	fi
	if [[ -d "$dir/pprof" ]]; then
		[[ -n "${measured_start:-}" && -n "${measured_end:-}" ]] || return 1
		validate_profile_evidence "$dir" "$measured_start" "$measured_end" || {
			echo "environment_invalid: pprof evidence is missing, invalid, or out of measured window" >&2
			return 1
		}
	fi
}

main_setup() {
	parse_args "$@"
	if [[ "$FULL_CAMPAIGN" -eq 1 ]]; then
		[[ -z "$VALIDATE_ONLY" && -z "$PROFILE_TARGET" && "$TRACE" -eq 0 ]] || die "--full-campaign cannot be combined with validate/profile options"
		WORKLOAD=broadcast
		INTERVAL=33ms
		CONNECTIONS=1000
		ATTEMPTS=3
		DURATION=30s
		WARMUP=5s
		DRAIN=10s
	fi

	if [[ -n "$VALIDATE_ONLY" ]]; then
	require_command grep; require_command shasum; require_command cmp; require_command stat
	if [[ -d "$VALIDATE_ONLY/pprof" ]]; then require_command go; fi
	[[ -s "$VALIDATE_ONLY/metadata.txt" ]] || die "validate-only metadata is missing"
	WORKLOAD=$(sed -n 's/^workload=//p' "$VALIDATE_ONLY/metadata.txt")
	INTERVAL=$(sed -n 's/^interval=//p' "$VALIDATE_ONLY/metadata.txt")
	CONNECTIONS=$(sed -n 's/^connections=//p' "$VALIDATE_ONLY/metadata.txt")
	CASE_NAME=$(sed -n 's/^case=//p' "$VALIDATE_ONLY/metadata.txt")
	CURRENT_ATTEMPT=$(sed -n 's/^attempt=//p' "$VALIDATE_ONLY/metadata.txt")
	[[ "$WORKLOAD" == broadcast || "$WORKLOAD" == player ]] || die "validate-only metadata workload is invalid"
	validate_attempt "$VALIDATE_ONLY"
	return $?
	fi

require_command go; require_command curl; require_command redis-cli; require_command shasum
require_command top; require_command vm_stat; require_command nettop; require_command netstat; require_command lsof
case "$WORKLOAD" in broadcast|player) ;; *) die "--workload must be broadcast or player" ;; esac
case "$INTERVAL" in 16ms|33ms) ;; *) die "--interval must be 16ms or 33ms" ;; esac
[[ "$CONNECTIONS" =~ ^[0-9]+$ && "$CONNECTIONS" -gt 0 ]] || die "--connections must be positive"
[[ "$ATTEMPTS" =~ ^[0-9]+$ && "$ATTEMPTS" -gt 0 ]] || die "--attempts must be positive"
WARMUP_SECONDS=$(duration_seconds "$WARMUP") || die "--warmup must be an integer duration (for example 5s)"
DURATION_SECONDS=$(duration_seconds "$DURATION") || die "--duration must be an integer duration (for example 30s)"
DRAIN_SECONDS=$(duration_seconds "$DRAIN") || die "--drain must be an integer duration (for example 10s)"
[[ "$WARMUP_SECONDS" -gt 0 && "$DURATION_SECONDS" -gt 0 && "$DRAIN_SECONDS" -gt 0 ]] || die "duration values must be positive"
COLLECTOR_SAMPLES=$((WARMUP_SECONDS + DURATION_SECONDS + DRAIN_SECONDS + 10))
[[ -z "$PROFILE_TARGET" || "$PROFILE_TARGET" == game || "$PROFILE_TARGET" == gate || "$PROFILE_TARGET" == load ]] || die "invalid profile target"
[[ -z "$PROFILE_TARGET" || "$ATTEMPTS" -eq 1 ]] || die "--profile-target is for a single profile-only attempt"
[[ -z "$PROFILE_TARGET" || "$DURATION_SECONDS" -eq 30 ]] || die "--profile-target requires --duration 30s for the fixed profile window"
[[ "$TRACE" -eq 0 || -n "$PROFILE_TARGET" ]] || die "--trace requires --profile-target"
CASE_SUFFIX=$(printf '%s' "$INTERVAL" | sed 's/ms$//')
CASE_NAME="$WORKLOAD-$CASE_SUFFIX"
mkdir -p "$ARTIFACT_ROOT"
ARTIFACT_ROOT=$(cd "$ARTIFACT_ROOT" && pwd)
CAMPAIGN_BIN_DIR="$ARTIFACT_ROOT/bin"
CAMPAIGN_CHECKSUMS="$ARTIFACT_ROOT/binary-checksums.txt"

	GAME_PID=""; GATE_PID=""; LOAD_PID=""; REDIS_PID=""
	HOST_PID=""; PROCESS_PID=""; VM_PID=""; NET_PID=""; SNAPSHOT_PID=""
	SCRAPE_GAME_PID=""; SCRAPE_GATE_PID=""; SCRAPE_LOAD_PID=""; PROFILE_PID=""
	CASE_ARTIFACT_ROOT="$ARTIFACT_ROOT/$CASE_NAME"
}

stop_pid() {
	local pid=$1
	[[ -n "$pid" ]] || return 0
	if kill -0 "$pid" 2>/dev/null; then
		kill -TERM "$pid" 2>/dev/null || true
		for _ in {1..50}; do kill -0 "$pid" 2>/dev/null || break; sleep 0.1; done
		# TERM 等待後再次確認；process 已結束時不對可能被重用的 PID 發 SIGKILL。
		if kill -0 "$pid" 2>/dev/null; then
			kill -KILL "$pid" 2>/dev/null || true
		fi
	fi
	wait "$pid" 2>/dev/null || true
}

process_alive() {
	local pid=$1 state
	[[ -n "$pid" ]] || return 1
	kill -0 "$pid" 2>/dev/null || return 1
	state=$(ps -o state= -p "$pid" 2>/dev/null | tr -d '[:space:]' || true)
	[[ -z "$state" || "$state" != Z* ]]
}

cleanup() {
	# 先通知 scraper 停止，避免 cleanup 關閉 listener 後又追加 error row，
	# 讓同一份 metrics index 在事後 --validate-only 時保持可重驗。
	if [[ -d "${ATTEMPT_DIR:-}/control" ]]; then
		: >"$ATTEMPT_DIR/control/collectors-stop"
	fi
	stop_pid "$PROFILE_PID"; stop_pid "$SCRAPE_GAME_PID"; stop_pid "$SCRAPE_GATE_PID"; stop_pid "$SCRAPE_LOAD_PID"
	stop_pid "$SNAPSHOT_PID"; stop_pid "$HOST_PID"; stop_pid "$PROCESS_PID"; stop_pid "$VM_PID"; stop_pid "$NET_PID"; stop_pid "$REDIS_PID"
	stop_pid "$LOAD_PID"; stop_pid "$GATE_PID"; stop_pid "$GAME_PID"
}
wait_http() {
	local url=$1 deadline=$((SECONDS + 120))
	while ((SECONDS < deadline)); do
		curl -fsS --max-time 1 "$url" >/dev/null 2>&1 && return 0
		sleep 0.2
	done
	return 1
}

record_event() {
	local file=$1 phase=$2 event=$3 run_id=$4 stamp=$5
	[[ -n "$stamp" ]] || stamp=$(timestamp_now)
	printf '{"timestamp":"%s","case":"%s","attempt":%s,"run_id":"%s","phase":"%s","event":"%s"}\n' "$stamp" "$CASE_NAME" "$CURRENT_ATTEMPT" "$run_id" "$phase" "$event" >>"$file"
}

scrape_loop() {
	local role=$1 url=$2 dir=$3 n=0 stamp output
	local role_pid
	case "$role" in
		game) role_pid=$GAME_PID ;;
		gate) role_pid=$GATE_PID ;;
		load) role_pid=$LOAD_PID ;;
		*) return 1 ;;
	esac
	# Load 結束後仍需保留最後一段 drain／final snapshot；由 cleanup
	# 統一停止 scraper，而不是因 load process 先結束就截斷 load metrics。
	while [[ ! -e "$dir/control/collectors-stop" ]] && process_alive "$role_pid"; do
		stamp=$(timestamp_now); output="$dir/metrics/$role-$n.prom"
		if curl -fsS --max-time 2 "$url" >"$output.tmp" 2>/dev/null; then
			if [[ -e "$dir/control/collectors-stop" ]]; then
				rm -f "$output.tmp"; break
			fi
			mv "$output.tmp" "$output"; printf '%s\t200\t%s\t%s\n' "$stamp" "$url" "$output" >>"$dir/metrics/$role.tsv"
		else
			rm -f "$output.tmp"
			if [[ -e "$dir/control/collectors-stop" ]]; then break; fi
			printf '%s\terror\t%s\t-\n' "$stamp" "$url" >>"$dir/metrics/$role.tsv"
		fi
		n=$((n + 1)); sleep 1
	done
}

capture_metrics_snapshot() {
	local dir=$1 phase=$2 role endpoint output stamp role_endpoint
	for role_endpoint in \
		game:http://127.0.0.1:19080/metrics \
		gate:http://127.0.0.1:18081/metrics \
		load:http://127.0.0.1:22081/metrics; do
		role=${role_endpoint%%:*}; endpoint=${role_endpoint#*:}
		stamp=$(timestamp_now); output="$dir/metrics/$phase-$role.prom"
		if ! curl -fsS --max-time 2 "$endpoint" >"$output.tmp"; then
			rm -f "$output.tmp"
			return 1
		fi
		mv "$output.tmp" "$output"
		printf '%s\t200\t%s\t%s\n' "$stamp" "$endpoint" "$output" >>"$dir/metrics/$role.tsv"
	done
}

capture_final_metrics() {
	local dir=$1 role latest
	if capture_metrics_snapshot "$dir" final; then
		return 0
	fi
	# 若某 listener 在 summary 後先關閉，使用 active scrape loop 的最後
	# 成功快照；若三個 role 均無快照，呼叫端會將 attempt 判為 invalid。
	for role in game gate load; do
		[[ -s "$dir/metrics/final-$role.prom" ]] && continue
		latest=$(ls -1t "$dir/metrics/$role"-*.prom 2>/dev/null | head -n 1 || true)
		[[ -n "$latest" ]] && cp "$latest" "$dir/metrics/final-$role.prom"
	done
	for role in game gate load; do
		[[ -s "$dir/metrics/final-$role.prom" ]] || return 1
	done
}

start_collectors() {
	local dir=$1 samples=$2
	top -l "$samples" -s 1 -n 0 >"$dir/os/host-top.txt" 2>&1 & HOST_PID=$!
	top -l "$samples" -s 1 -pid "$GAME_PID" -pid "$GATE_PID" -pid "$LOAD_PID" -stats pid,command,cpu,mem,threads,csw,state >"$dir/os/process-top.txt" 2>&1 & PROCESS_PID=$!
	(
		for ((sample = 1; sample <= samples; sample++)); do
			printf 'timestamp=%s sample=%d\n' "$(timestamp_now)" "$sample"
			vm_stat -c 1 2>&1
			sleep 1
		done
	) >"$dir/os/vm-stat.txt" & VM_PID=$!
	# 不限制欄位，保留 macOS nettop 的 process summary；在某些版本中
	# `-J bytes_in,bytes_out` 對 idle／短連線會只輸出 header，遺失 PID row。
	nettop -L "$samples" -s 1 -n -P -p "$GAME_PID" -p "$GATE_PID" -p "$LOAD_PID" >"$dir/os/nettop.txt" 2>&1 & NET_PID=$!
	start_redis_collector "$dir"
}

start_redis_collector() {
	local dir=$1
	printf 'timestamp\tused_cpu_sys\tused_cpu_user\ttotal_commands_processed\tinstantaneous_ops_per_sec\tconnected_clients\tblocked_clients\trejected_connections\tstatus\n' >"$dir/os/redis.tsv"
	(
		while [[ ! -e "$dir/control/collectors-stop" ]] && process_alive "$LOAD_PID"; do
			local_stamp=$(timestamp_now)
			if info=$(redis-cli -h 127.0.0.1 -p 6379 INFO stats cpu clients 2>/dev/null); then
				info=${info//$'\r'/}
				used_cpu_sys=$(printf '%s\n' "$info" | sed -n 's/^used_cpu_sys:\(.*\)$/\1/p' | head -n 1)
				used_cpu_user=$(printf '%s\n' "$info" | sed -n 's/^used_cpu_user:\(.*\)$/\1/p' | head -n 1)
				total_commands=$(printf '%s\n' "$info" | sed -n 's/^total_commands_processed:\(.*\)$/\1/p' | head -n 1)
				instantaneous_ops=$(printf '%s\n' "$info" | sed -n 's/^instantaneous_ops_per_sec:\(.*\)$/\1/p' | head -n 1)
				connected_clients=$(printf '%s\n' "$info" | sed -n 's/^connected_clients:\(.*\)$/\1/p' | head -n 1)
				blocked_clients=$(printf '%s\n' "$info" | sed -n 's/^blocked_clients:\(.*\)$/\1/p' | head -n 1)
				rejected_connections=$(printf '%s\n' "$info" | sed -n 's/^rejected_connections:\(.*\)$/\1/p' | head -n 1)
				if [[ "$used_cpu_sys" =~ ^[0-9]+([.][0-9]+)?$ && "$used_cpu_user" =~ ^[0-9]+([.][0-9]+)?$ &&
					"$total_commands" =~ ^[0-9]+$ && "$instantaneous_ops" =~ ^[0-9]+$ &&
					"$connected_clients" =~ ^[0-9]+$ && "$blocked_clients" =~ ^[0-9]+$ && "$rejected_connections" =~ ^[0-9]+$ ]]; then
					printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\tok\n' "$local_stamp" "$used_cpu_sys" "$used_cpu_user" "$total_commands" "$instantaneous_ops" "$connected_clients" "$blocked_clients" "$rejected_connections" >>"$dir/os/redis.tsv"
				else
					printf '%s\t-\t-\t-\t-\t-\t-\t-\tmissing_field\n' "$local_stamp" >>"$dir/os/redis.tsv"
				fi
			else
				printf '%s\t-\t-\t-\t-\t-\t-\t-\tcommand_failed\n' "$local_stamp" >>"$dir/os/redis.tsv"
			fi
			sleep 1
		done
	) & REDIS_PID=$!
}

capture_process_snapshot() {
	local dir=$1 suffix=$2 pid
	for pid in "$GAME_PID" "$GATE_PID" "$LOAD_PID"; do
		[[ -n "$pid" ]] || continue
		lsof -nP -a -p "$pid" >"$dir/os/lsof-$suffix-$pid.txt" 2>&1 || true
	done
}

wait_for_gate_queue_empty() {
	local dir=$1 value deadline=$((SECONDS + DRAIN_SECONDS))
	while ((SECONDS < deadline)); do
		if curl -fsS --max-time 2 http://127.0.0.1:18081/metrics >"$dir/control/gate-queue.prom.tmp" 2>/dev/null &&
			curl -fsS --max-time 2 http://127.0.0.1:19080/metrics >"$dir/control/game-queue.prom.tmp" 2>/dev/null; then
			mv "$dir/control/gate-queue.prom.tmp" "$dir/control/gate-queue.prom"
			mv "$dir/control/game-queue.prom.tmp" "$dir/control/game-queue.prom"
			value=$(awk '$1 == "gaming_core_gate_websocket_write_queue_messages" { print $2; exit }' "$dir/control/gate-queue.prom" || true)
			if [[ "$value" == "0" || "$value" == "0.0" ]] &&
				validate_gate_terminal_gauges "$dir/control/gate-queue.prom" &&
				validate_game_terminal_gauges "$dir/control/game-queue.prom"; then
				return 0
			fi
		fi
		sleep 0.2
	done
	return 1
}

validate_gate_terminal_gauges() {
	local file=$1 gauge value
	for gauge in gaming_core_gate_websocket_commands_in_flight gaming_core_gate_game_grpc_in_flight gaming_core_gate_websocket_writes_in_flight gaming_core_gate_websocket_write_queue_messages; do
		value=$(awk -v gauge="$gauge" '$1 == gauge { print $2; exit }' "$file" || true)
		[[ "$value" == "0" || "$value" == "0.0" ]] || return 1
	done
	if [[ "${WORKLOAD:-}" == broadcast ]]; then
		metric_values_zero_if_present "$file" gaming_core_gate_server_send_outbound_queue_messages || return 1
		metric_values_zero_if_present "$file" gaming_core_gate_server_send_outbound_queue_bytes || return 1
	fi
}

validate_game_terminal_gauges() {
	local file=$1 gauge operation=${WORKLOAD:-}
	for gauge in gaming_core_game_gate_commands_in_flight gaming_core_game_server_send_in_flight; do
		metric_values_zero "$file" "$gauge" || return 1
	done
	[[ "$operation" == player || "$operation" == broadcast ]] || return 1
	metric_values_zero_for_operation "$file" gaming_core_game_server_send_queue_messages "$operation" || return 1
	metric_values_zero_for_operation "$file" gaming_core_game_server_send_queue_bytes "$operation" || return 1
}

capture_baseline_when_warmup_completes() {
	local dir=$1 found=0 n=0 warmup_line warmup_stamp run_id drained_line
	local planned attempted success partial error missed
	while process_alive "$LOAD_PID"; do
		warmup_line=$(grep '"message":"example push workload completed"' "$dir/game.log" 2>/dev/null | grep warmup | tail -n 1 || true)
		if ((found == 0)) && [[ -n "$warmup_line" ]]; then
			run_id=$(printf '%s\n' "$warmup_line" | sed -n 's/.*"run_id":"\([^\"]*\)".*/\1/p')
			warmup_stamp=$(printf '%s\n' "$warmup_line" | sed -n 's/.*"timestamp":"\([^\"]*\)".*/\1/p')
			record_event "$dir/phase-events.jsonl" warmup_running end "$run_id" "$warmup_stamp"
			record_event "$dir/phase-events.jsonl" warmup_draining start "$run_id" "$warmup_stamp"
			planned=$(json_field "$warmup_line" planned_ticks)
			attempted=$(json_field "$warmup_line" attempted)
			success=$(json_field "$warmup_line" success)
			partial=$(json_field "$warmup_line" partial)
			error=$(json_field "$warmup_line" error)
			missed=$(json_field "$warmup_line" missed)
			if ! write_warmup_result "$dir" "$run_id" "$planned" "$attempted" "$success" "$partial" "$error" "$missed"; then
				printf 'warmup_result_invalid\n' >"$dir/control/warmup-workload-invalid"
				return 1
			fi
			# missed、partial、error 與 client 缺額是 workload evidence；只要
			# completion marker 有效且 lifecycle／protocol 沒失敗，仍必須進入
			# quiet drain，讓 measured window 能暴露真正的 cadence boundary。
			if ! wait_for_run_marker warmup-drained.json "$dir" "$run_id" "$DRAIN_SECONDS"; then
				printf 'warmup_drained_marker_timeout\n' >"$dir/control/warmup-drained-timeout"
				return 1
			fi
			drained_line=$(cat "$dir/control/warmup-drained.json")
			if ! validate_warmup_drained "$drained_line"; then
				printf 'warmup_drained_invalid\n' >"$dir/control/warmup-drained-invalid"
				return 1
			fi
			if ! process_alive "$GAME_PID" || ! process_alive "$GATE_PID" || ! process_alive "$LOAD_PID"; then
				printf 'warmup_process_exited\n' >"$dir/control/warmup-process-exited"
				return 1
			fi
			if ! wait_for_gate_queue_empty "$dir"; then
				printf 'warmup_gate_queue_not_empty\n' >"$dir/control/baseline-queue-timeout"
				return 1
			fi
			if ! capture_metrics_snapshot "$dir" baseline; then
				printf 'baseline_metrics_scrape_failed\n' >"$dir/control/baseline-scrape-failed"
				return 1
			fi
			if ! validate_baseline_before_measured "$dir"; then
				printf 'baseline_metrics_invalid\n' >"$dir/control/baseline-metrics-invalid"
				return 1
			fi
			redis-cli -h 127.0.0.1 -p 6379 INFO stats cpu memory clients commandstats >"$dir/redis-before-measured.txt" 2>&1 || true
			capture_process_snapshot "$dir" measured-baseline
			record_event "$dir/phase-events.jsonl" warmup_draining end "$run_id" ""
			record_event "$dir/phase-events.jsonl" measured_baseline start "$run_id" ""
			record_event "$dir/phase-events.jsonl" measured_baseline end "$run_id" ""
			write_baseline_marker "$dir" "$run_id"
			found=1
		fi
		n=$((n + 1)); sleep 0.2
		((n < (WARMUP_SECONDS + DURATION_SECONDS + DRAIN_SECONDS + 120) * 5)) || break
	done
}

profile_endpoint() {
	case "$PROFILE_TARGET" in
	game) echo 127.0.0.1:19082 ;;
	gate) echo 127.0.0.1:18082 ;;
	load) echo 127.0.0.1:22083 ;;
	esac
}

profile_pid() {
	case "$PROFILE_TARGET" in
	game) printf '%s\n' "$GAME_PID" ;;
	gate) printf '%s\n' "$GATE_PID" ;;
	load) printf '%s\n' "$LOAD_PID" ;;
	*) printf '\n' ;;
	esac
}

profile_loop() {
	local dir=$1 control=$2 endpoint cpu_pid target_pid binary checksum cpu_start cpu_end
	local heap_start heap_end goroutine_start goroutine_end trace_start trace_end status_file
	endpoint=$(profile_endpoint)
	target_pid=$(profile_pid)
	binary="$dir/bin/$PROFILE_TARGET"
	checksum=$(shasum -a 256 "$binary" 2>/dev/null | awk '{print $1}' || true)
	mkdir -p "$dir/pprof"
	while [[ ! -s "$control/measured-started.json" ]]; do
		process_alive "$LOAD_PID" || return 0
		sleep 0.02
	done
	status_file="$dir/pprof/status.tsv"
	printf 'event\tstart\tend\tstatus\ttarget_pid\tbinary_sha256\n' >"$status_file"
	printf 'target_pid=%s\nbinary=%s\nbinary_sha256=%s\nmeasured_marker=%s\n' \
		"$target_pid" "$binary" "$checksum" "$(cat "$control/measured-started.json")" >"$dir/pprof/window.tsv"
	# 留出 HTTP handler／timestamp overhead，讓 20s profile 能在 measured
	# window 內完成，trace 也能保留完整 5s duration。
	sleep 4
	cpu_start=$(timestamp_now)
	printf 'cpu_start\t%s\n' "$cpu_start" >>"$dir/pprof/window.tsv"
	curl -fsS --max-time 25 "http://$endpoint/debug/pprof/profile?seconds=20" -o "$dir/pprof/$PROFILE_TARGET-cpu-20s.pb.gz" & cpu_pid=$!
	sleep 10
	heap_start=$(timestamp_now)
	printf 'heap_goroutine_at\t%s\n' "$heap_start" >>"$dir/pprof/window.tsv"
	if curl -fsS --max-time 5 "http://$endpoint/debug/pprof/heap" -o "$dir/pprof/$PROFILE_TARGET-heap.pb.gz"; then
		status=ok
	else
		status=error
	fi
	heap_end=$(timestamp_now)
	printf 'heap\t%s\t%s\t%s\t%s\t%s\n' "$heap_start" "$heap_end" "$status" "$target_pid" "$checksum" >>"$status_file"
	goroutine_start=$(timestamp_now)
	if curl -fsS --max-time 5 "http://$endpoint/debug/pprof/goroutine?debug=1" -o "$dir/pprof/$PROFILE_TARGET-goroutine.txt"; then
		status=ok
	else
		status=error
	fi
	goroutine_end=$(timestamp_now)
	printf 'goroutine\t%s\t%s\t%s\t%s\t%s\n' "$goroutine_start" "$goroutine_end" "$status" "$target_pid" "$checksum" >>"$status_file"
	if wait "$cpu_pid"; then
		status=ok
	else
		status=error
	fi
	cpu_end=$(timestamp_now)
	printf 'cpu\t%s\t%s\t%s\t%s\t%s\n' "$cpu_start" "$cpu_end" "$status" "$target_pid" "$checksum" >>"$status_file"
	if [[ "$TRACE" -eq 1 ]]; then
		trace_start=$(timestamp_now)
		printf 'trace_start\t%s\n' "$trace_start" >>"$dir/pprof/window.tsv"
		if curl -fsS --max-time 10 "http://$endpoint/debug/pprof/trace?seconds=5" -o "$dir/pprof/$PROFILE_TARGET-trace-5s.out"; then
			status=ok
		else
			status=error
		fi
		trace_end=$(timestamp_now)
		printf 'trace_end\t%s\n' "$trace_end" >>"$dir/pprof/window.tsv"
		printf 'trace\t%s\t%s\t%s\t%s\t%s\n' "$trace_start" "$trace_end" "$status" "$target_pid" "$checksum" >>"$status_file"
	fi
}

json_field() {
	local line=$1 key=$2 value
	value=$(printf '%s\n' "$line" | sed -n "s/.*\"$key\":\([0-9][0-9]*\).*/\1/p")
	if [[ -n "$value" ]]; then printf '%s' "$value"; else printf 'null'; fi
}

validate_warmup_result_line() {
	local line=$1 planned attempted success partial error missed
	printf '%s\n' "$line" | grep -Eq '^\{"timestamp":"[^"]+","run_id":"[^"]+","planned":[0-9]+,"attempted":[0-9]+,"success":[0-9]+,"partial":[0-9]+,"error":[0-9]+,"missed":[0-9]+\}$' || return 1
	planned=$(json_field "$line" planned)
	attempted=$(json_field "$line" attempted)
	success=$(json_field "$line" success)
	partial=$(json_field "$line" partial)
	error=$(json_field "$line" error)
	missed=$(json_field "$line" missed)
	[[ "$planned" =~ ^[0-9]+$ && "$attempted" =~ ^[0-9]+$ && "$success" =~ ^[0-9]+$ &&
		"$partial" =~ ^[0-9]+$ && "$error" =~ ^[0-9]+$ && "$missed" =~ ^[0-9]+$ ]] || return 1
	(( planned > 0 && attempted <= planned && success <= attempted && partial <= attempted &&
		error <= attempted && missed <= planned )) || return 1
	(( success + partial + error == attempted && attempted + missed == planned ))
}

write_warmup_result() {
	local dir=$1 run_id=$2 planned=$3 attempted=$4 success=$5 partial=$6 error=$7 missed=$8 temporary line
	[[ -n "$run_id" && "$planned" != null && "$attempted" != null && "$success" != null &&
		"$partial" != null && "$error" != null && "$missed" != null ]] || return 1
	temporary="$dir/control/.warmup-result.tmp"
	line=$(printf '{"timestamp":"%s","run_id":"%s","planned":%s,"attempted":%s,"success":%s,"partial":%s,"error":%s,"missed":%s}' \
		"$(timestamp_now)" "$run_id" "$planned" "$attempted" "$success" "$partial" "$error" "$missed")
	printf '%s\n' "$line" >"$temporary"
	validate_warmup_result_line "$line" || { rm -f "$temporary"; return 1; }
	chmod 640 "$temporary"
	mv "$temporary" "$dir/control/warmup-result.json"
}

validate_warmup_drained() {
	local line=$1 received duplicate gap invalid missing reader_failures
	printf '%s\n' "$line" | grep -Eq '^\{"timestamp":"[^"]+","run_id":"[^"]+","planned":[0-9]+,"attempted":[0-9]+,"success":[0-9]+,"partial":[0-9]+,"error":[0-9]+,"missed":[0-9]+,"received":[0-9]+,"duplicate":[0-9]+,"sequence_gap":[0-9]+,"invalid":[0-9]+,"missing":[0-9]+,"reader_failures":[0-9]+\}$' || return 1
	received=$(json_field "$line" received)
	duplicate=$(json_field "$line" duplicate)
	gap=$(json_field "$line" sequence_gap)
	invalid=$(json_field "$line" invalid)
	missing=$(json_field "$line" missing)
	reader_failures=$(json_field "$line" reader_failures)
	[[ "$received" =~ ^[0-9]+$ && "$duplicate" =~ ^[0-9]+$ && "$gap" =~ ^[0-9]+$ &&
		"$invalid" =~ ^[0-9]+$ && "$missing" =~ ^[0-9]+$ && "$reader_failures" =~ ^[0-9]+$ ]] || return 1
	(( duplicate == 0 && gap == 0 && invalid == 0 && reader_failures == 0 ))
}

kv_field() {
	local line=$1 key=$2 value
	value=$(printf '%s\n' "$line" | sed -n "s/.* $key=\([^ ]*\).*/\1/p")
	if [[ -n "$value" ]]; then printf '%s' "$value"; else printf 'null'; fi
}

sample_value() {
	local file=$1 name=$2 label_one=$3 label_two=$4 value
	[[ -s "$file" ]] || { printf 'null'; return; }
	value=$(awk -v name="$name" -v label_one="$label_one" -v label_two="$label_two" \
		'$1 ~ ("^" name "\\{") && $1 ~ label_one && $1 ~ label_two { print $2; exit }' "$file" || true)
	if [[ -n "$value" ]]; then printf '%s' "$value"; else printf 'null'; fi
}

metric_delta() {
	local before=$1 after=$2
	[[ "$before" != null && "$after" != null ]] || return 1
	awk -v before="$before" -v after="$after" '
		BEGIN {
			pattern = "^[+-]?[0-9]+([.][0-9]*)?([eE][+-]?[0-9]+)?$"
			if (before !~ pattern || after !~ pattern) exit 1
			delta = after - before
			if (delta < 0 || delta != int(delta)) exit 1
			printf "%.0f\n", delta
		}
	'
}

metric_values_zero() {
	local file=$1 name=$2
	awk -v name="$name" '$1 ~ ("^" name "(\\{|$)") { found=1; if ($NF != "0" && $NF != "0.0") invalid=1 } END { exit(found && !invalid ? 0 : 1) }' "$file"
}

metric_values_zero_if_present() {
	local file=$1 name=$2
	awk -v name="$name" '$1 ~ ("^" name "(\\{|$)") { found=1; if ($NF != "0" && $NF != "0.0") invalid=1 } END { exit(!found || !invalid ? 0 : 1) }' "$file"
}

metric_values_zero_for_operation() {
	local file=$1 name=$2 operation=$3
	awk -v name="$name" -v operation="$operation" \
		'$1 ~ ("^" name "\\{") && $1 ~ ("operation=\"" operation "\"") { found=1; if ($NF != "0" && $NF != "0.0") invalid=1 } END { exit(found && !invalid ? 0 : 1) }' "$file"
}

metric_counter_delta_zero() {
	local before_file=$1 after_file=$2 name=$3 label_one=${4:-} label_two=${5:-}
	local before after delta
	before=$(sample_value "$before_file" "$name" "$label_one" "$label_two")
	after=$(sample_value "$after_file" "$name" "$label_one" "$label_two")
	# Prometheus does not expose an unobserved CounterVec label. Missing in both
	# snapshots therefore means zero; appearing only in the final snapshot is a
	# positive delta and must fail the successful-workload contract.
	if [[ "$before" == null && "$after" == null ]]; then
		return 0
	fi
	delta=$(metric_delta "$before" "$after") || return 1
	[[ "$delta" == 0 ]]
}

validate_async_terminal_counters() {
	local game_before=$1 game_after=$2 gate_before=$3 gate_after=$4 operation=$5
	local reason result
	for reason in full too_large not_running; do
		metric_counter_delta_zero "$game_before" "$game_after" gaming_core_game_server_send_queue_rejected_total \
			"operation=\"$operation\"" "reason=\"$reason\"" || return 1
	done
	for result in partial error; do
		metric_counter_delta_zero "$game_before" "$game_after" gaming_core_game_server_send_worker_duration_seconds_count \
			"operation=\"$operation\"" "result=\"$result\"" || return 1
	done
	metric_counter_delta_zero "$game_before" "$game_after" gaming_core_game_server_send_discarded_total \
		"operation=\"$operation\"" 'reason="shutdown_timeout"' || return 1

	# Gate only owns the outbound Broadcast queue. Player workload delivery is
	# handled by Game's existing Player sender and has no Gate async queue.
	if [[ "$operation" == broadcast ]]; then
		for reason in full too_large not_running; do
			metric_counter_delta_zero "$gate_before" "$gate_after" gaming_core_gate_server_send_outbound_queue_rejected_total \
				'operation="broadcast"' "reason=\"$reason\"" || return 1
		done
		for result in partial error; do
			metric_counter_delta_zero "$gate_before" "$gate_after" gaming_core_gate_server_send_outbound_worker_duration_seconds_count \
				'operation="broadcast"' "result=\"$result\"" || return 1
		done
		metric_counter_delta_zero "$gate_before" "$gate_after" gaming_core_gate_server_send_outbound_discarded_total \
			'operation="broadcast"' 'reason="shutdown_timeout"' || return 1
	fi
}

validate_baseline_before_measured() {
	local dir=$1
	for required in baseline-game.prom baseline-gate.prom baseline-load.prom; do
		[[ -s "$dir/metrics/$required" ]] || return 1
	done
	validate_operation_metrics "$dir" || return 1
	validate_game_terminal_gauges "$dir/metrics/baseline-game.prom" || return 1
	validate_gate_terminal_gauges "$dir/metrics/baseline-gate.prom" || return 1
}

record_game_completion() {
	local dir=$1 line phase run_id stamp
	for phase in warmup measured; do
		line=$(grep '\"message\":\"example push workload completed\"' "$dir/game.log" | grep "$phase" | tail -n 1 || true)
		[[ -n "$line" ]] || continue
		run_id=$(printf '%s\n' "$line" | sed -n 's/.*"run_id":"\([^"]*\)".*/\1/p')
		stamp=$(printf '%s\n' "$line" | sed -n 's/.*"timestamp":"\([^"]*\)".*/\1/p')
		if ! grep -q "\"run_id\":\"$run_id\".*\"phase\":\"${phase}_running\".*\"event\":\"end\"" "$dir/phase-events.jsonl" 2>/dev/null; then
			record_event "$dir/phase-events.jsonl" "${phase}_running" end "$run_id" "$stamp"
		fi
	done
}

append_run_metadata() {
	local dir=$1 run_id=$2 finished_at=$3 phase start end line encoded
	printf 'run_id=%s\nattempt_finished_at=%s\n' "${run_id:-not_run}" "$finished_at" >>"$dir/metadata.txt"
	for phase in warmup_running measured_running measured_draining; do
		start=$(phase_timestamp "$dir/phase-events.jsonl" "$phase" start || true)
		end=$(phase_timestamp "$dir/phase-events.jsonl" "$phase" end || true)
		printf '%s_start=%s\n%s_end=%s\n' "$phase" "${start:-not_run}" "$phase" "${end:-not_run}" >>"$dir/metadata.txt"
	done
	line=$(grep '\"message\":\"example push workload completed\"' "$dir/game.log" | grep measured | tail -n 1 || true)
	encoded=$(json_field "$line" encoded_payload_bytes)
	printf 'encoded_payload_bytes=%s\n' "$encoded" >>"$dir/metadata.txt"
}

write_invalid_status() {
	local dir=$1 reason=$2 load_exit=${3:-null}
	printf '{"case":"%s","attempt":%s,"evidence_status":"environment_invalid","workload_status":"not_run","first_failure":"%s","collector_preflight":"not_validated","warmup_started":false,"measured_started":false,"baseline_ready":false,"warmup_cadence_failed":false,"warmup_sender_failed":false,"warmup_delivery_failed":false,"cadence_failed":false,"sender_failed":false,"delivery_failed":false,"load_exit_code":%s,"warmup_attempted":null,"warmup_missed":null,"measured_planned_ticks":null,"measured_attempted":null,"measured_success":null,"measured_partial":null,"measured_error":null,"measured_missed":null,"client_received":null,"profile_target":"%s","evidence":{"metadata":"metadata.txt","phase_events":"phase-events.jsonl","commands":"commands.txt"}}\n' \
		"$CASE_NAME" "$CURRENT_ATTEMPT" "$reason" "$load_exit" "$PROFILE_TARGET" >"$dir/run-status.json"
	record_event "$dir/phase-events.jsonl" classified end "attempt-$CURRENT_ATTEMPT" ""
}

configure_case() {
	local phase=$1 workload=$2 interval=$3 connections=$4 duration=$5 warmup=$6 attempts=$7
	CAMPAIGN_PHASE=$phase
	WORKLOAD=$workload
	INTERVAL=$interval
	CONNECTIONS=$connections
	DURATION=$duration
	WARMUP=$warmup
	ATTEMPTS=$attempts
	DRAIN=10s
	CASE_SUFFIX=$(printf '%s' "$INTERVAL" | sed 's/ms$//')
	CASE_NAME="$WORKLOAD-$CASE_SUFFIX"
	WARMUP_SECONDS=$(duration_seconds "$WARMUP") || return 1
	DURATION_SECONDS=$(duration_seconds "$DURATION") || return 1
	DRAIN_SECONDS=$(duration_seconds "$DRAIN") || return 1
	COLLECTOR_SAMPLES=$((WARMUP_SECONDS + DURATION_SECONDS + DRAIN_SECONDS + 10))
	CASE_ARTIFACT_ROOT="$ARTIFACT_ROOT/$CAMPAIGN_PHASE/$CASE_NAME"
	mkdir -p "$CASE_ARTIFACT_ROOT"
}

print_invalid_attempts() {
	local status evidence_status
	for status in "$CASE_ARTIFACT_ROOT"/run-*/run-status.json; do
		[[ -s "$status" ]] || continue
		evidence_status=$(sed -n 's/.*"evidence_status":"\([^"]*\)".*/\1/p' "$status")
		[[ "$evidence_status" == environment_invalid ]] || continue
		printf 'invalid_attempt=%s first_failure=%s\n' "$status" "$(sed -n 's/.*"first_failure":"\([^"]*\)".*/\1/p' "$status")" >&2
	done
}

run_case() {
	local phase=$1 workload=$2 interval=$3 connections=$4 duration=$5 warmup=$6 requested=$7
	local valid_attempts=0 attempt=0 max_total_attempts
	configure_case "$phase" "$workload" "$interval" "$connections" "$duration" "$warmup" "$requested" || return 1
	max_total_attempts=$(max_attempts_for_requested "$requested") || return 1
	while ((valid_attempts < requested && attempt < max_total_attempts)); do
		attempt=$((attempt + 1))
		if run_attempt "$attempt"; then
			valid_attempts=$((valid_attempts + 1))
			echo "$phase $CASE_NAME attempt $attempt: evidence-valid result $valid_attempts/$requested; artifacts=$CASE_ARTIFACT_ROOT/run-$attempt"
		else
			echo "$phase $CASE_NAME attempt $attempt: environment-invalid; fix environment before retry; artifacts=$CASE_ARTIFACT_ROOT/run-$attempt" >&2
		fi
		write_evidence_manifest
	done
	if ((valid_attempts < requested)); then
		print_invalid_attempts
		echo "error: $phase/$CASE_NAME stopped after $attempt/$max_total_attempts attempts; only $valid_attempts/$requested evidence-valid attempts; fix environment before retry" >&2
		return 1
	fi
}

run_correctness_attempt() {
	local workload=$1
	configure_case correctness "$workload" 33ms 10 5s 5s 1 || return 1
	if ! run_attempt 1; then
		write_evidence_manifest
		echo "error: correctness/$CASE_NAME run-1 failed; matrix was not started" >&2
		return 1
	fi
	write_evidence_manifest
	echo "correctness $CASE_NAME run-1: evidence-valid"
}

max_attempts_for_requested() {
	local requested=$1
	[[ "$requested" =~ ^[0-9]+$ && "$requested" -gt 0 ]] || return 1
	printf '%s\n' "$((requested + 2))"
}

validate_correctness_gate() {
	local correctness_root="$ARTIFACT_ROOT/correctness" workload status metadata expected actual name temporary
	for workload in broadcast player; do
		status="$correctness_root/$workload-33/run-1/run-status.json"
		metadata="$correctness_root/$workload-33/run-1/metadata.txt"
		[[ -s "$status" && -s "$metadata" ]] || return 1
		grep -q '"evidence_status":"evidence_valid"' "$status" || return 1
		grep -q '"workload_status":"delivery_complete"' "$status" || return 1
		grep -q '^connections=10$' "$metadata" || return 1
		grep -q '^interval=33ms$' "$metadata" || return 1
		for name in game gate load; do
			expected=$(awk -v name="$name" '{path=$2; sub(/^.*\//, "", path); if (path == name) {print $1; exit}}' "$CAMPAIGN_CHECKSUMS")
			actual=$(sed -n "s/^binary_sha256_$name=//p" "$metadata")
			[[ -n "$expected" && "$actual" == "$expected" ]] || return 1
		done
	done
	temporary="$ARTIFACT_ROOT/.correctness-gate.tmp"
	printf '{"timestamp":"%s","broadcast":"correctness/broadcast-33/run-1","player":"correctness/player-33/run-1","binary_sha256_game":"%s","binary_sha256_gate":"%s","binary_sha256_load":"%s"}\n' \
		"$(timestamp_now)" \
		"$(awk '{path=$2; sub(/^.*\//, "", path); if (path == "game") {print $1; exit}}' "$CAMPAIGN_CHECKSUMS")" \
		"$(awk '{path=$2; sub(/^.*\//, "", path); if (path == "gate") {print $1; exit}}' "$CAMPAIGN_CHECKSUMS")" \
		"$(awk '{path=$2; sub(/^.*\//, "", path); if (path == "load") {print $1; exit}}' "$CAMPAIGN_CHECKSUMS")" >"$temporary"
	chmod 640 "$temporary"
	mv "$temporary" "$ARTIFACT_ROOT/correctness-gate.json"
}

run_correctness_gate() {
	run_correctness_attempt broadcast || return 1
	run_correctness_attempt player || return 1
	validate_correctness_gate || {
		echo "error: correctness gate failed; matrix was not started" >&2
		return 1
	}
	echo "correctness gate passed; matrix may start"
}

run_matrix_campaign() {
	local -a workloads=(broadcast player broadcast player)
	local -a intervals=(33ms 33ms 16ms 16ms)
	local -a valid=(0 0 0 0)
	local -a total=(0 0 0 0)
	local -a order
	local index round=0 complete eligible max_total_attempts
	max_total_attempts=$(max_attempts_for_requested 3) || return 1
	while :; do
		complete=1
		eligible=0
		for index in 0 1 2 3; do
			((valid[index] >= 3)) || complete=0
			if ((valid[index] < 3 && total[index] < max_total_attempts)); then eligible=1; fi
		done
		((complete == 1)) && return 0
		if ((eligible == 0)); then
			for index in 0 1 2 3; do
				((valid[index] >= 3)) && continue
				CASE_ARTIFACT_ROOT="$ARTIFACT_ROOT/matrix/${workloads[index]}-${intervals[index]%ms}"
				print_invalid_attempts
			done
			return 1
		fi
		round=$((round + 1))
		if ((round % 2 == 0)); then order=(3 2 1 0); else order=(0 1 2 3); fi
		for index in "${order[@]}"; do
			((valid[index] < 3 && total[index] < max_total_attempts)) || continue
			total[index]=$((total[index] + 1))
			configure_case matrix "${workloads[index]}" "${intervals[index]}" 1000 30s 5s 3 || return 1
			if run_attempt "${total[index]}"; then
				valid[index]=$((valid[index] + 1))
				echo "matrix $CASE_NAME attempt ${total[index]}: evidence-valid result ${valid[index]}/3; artifacts=$CASE_ARTIFACT_ROOT/run-${total[index]}"
			else
				echo "matrix $CASE_NAME attempt ${total[index]}: environment-invalid; fix environment before retry; artifacts=$CASE_ARTIFACT_ROOT/run-${total[index]}" >&2
			fi
			write_evidence_manifest
		done
	done
}

main() {
	local setup_status
	if main_setup "$@"; then
		:
	else
		setup_status=$?
		return "$setup_status"
	fi
	if [[ -n "$VALIDATE_ONLY" ]]; then
		return 0
	fi
	trap cleanup EXIT INT TERM
	build_campaign_binaries
	if [[ "$FULL_CAMPAIGN" -eq 1 ]]; then
		run_correctness_gate || { write_evidence_manifest; return 1; }
		run_matrix_campaign || { write_evidence_manifest; echo "error: matrix campaign did not obtain three valid attempts per case" >&2; return 1; }
	else
		CAMPAIGN_PHASE=single
		[[ -n "$PROFILE_TARGET" ]] && CAMPAIGN_PHASE=profile
		run_case "$CAMPAIGN_PHASE" "$WORKLOAD" "$INTERVAL" "$CONNECTIONS" "$DURATION" "$WARMUP" "$ATTEMPTS" || {
			write_evidence_manifest
			return 1
		}
	fi
	write_evidence_manifest
}

wait_for_marker() {
	local marker=$1 dir=$2 timeout=${3:-120} deadline
	deadline=$((SECONDS + timeout))
	while ((SECONDS < deadline)); do
		[[ -s "$dir/control/$marker" ]] && return 0
		process_alive "$LOAD_PID" || return 1
		sleep 0.2
	done
	return 1
}

wait_for_run_marker() {
	local marker=$1 dir=$2 expected=$3 timeout=${4:-120} data actual
	wait_for_marker "$marker" "$dir" "$timeout" || return 1
	data=$(cat "$dir/control/$marker")
	actual=$(printf '%s\n' "$data" | sed -n 's/.*"run_id":"\([^\"]*\)".*/\1/p')
	[[ -n "$actual" && "$actual" == "$expected" ]]
}

preflight_metrics() {
	local dir=$1 endpoint family attempt
	local -a required_families
	: >"$dir/control/metrics-preflight.txt"
	for endpoint in \
		http://127.0.0.1:19080/metrics \
		http://127.0.0.1:18081/metrics \
		http://127.0.0.1:22081/metrics; do
		for attempt in 1 2; do
			if ! curl -fsS --max-time 2 "$endpoint" >"$dir/control/metrics-${attempt}.tmp"; then
				printf '%s\tattempt=%d\terror\n' "$endpoint" "$attempt" >>"$dir/control/metrics-preflight.txt"
				return 1
			fi
			printf '%s\tattempt=%d\tok\n' "$endpoint" "$attempt" >>"$dir/control/metrics-preflight.txt"
			case "$endpoint" in
				*19080*) required_families=(gaming_core_game_gate_commands_in_flight go_sched_latencies_seconds process_cpu_seconds_total) ;;
				*18081*) required_families=(gaming_core_gate_websocket_connections go_sched_latencies_seconds process_cpu_seconds_total) ;;
				*) required_families=(gaming_core_example_load_push_readers go_sched_latencies_seconds process_cpu_seconds_total) ;;
			esac
			for family in "${required_families[@]}"; do
				grep -q "^${family}" "$dir/control/metrics-${attempt}.tmp" || {
					printf '%s\tattempt=%d\tmissing=%s\n' "$endpoint" "$attempt" "$family" >>"$dir/control/metrics-preflight.txt"
					return 1
				}
			done
			rm -f "$dir/control/metrics-${attempt}.tmp"
		done
	done
	return 0
}

ports_in_use() {
	local port
	for port in 18080 18081 19080 19090 19091 22081 19082 18082 22083; do
		if lsof -nP -iTCP:"$port" -sTCP:LISTEN -t 2>/dev/null | grep -q .; then
			printf '%s\n' "$port"
		fi
	done
	return 0
}

wait_for_collector_preflight() {
	local dir=$1 deadline=$((SECONDS + 15))
	while ((SECONDS < deadline)); do
		if grep -q 'Load Avg:' "$dir/os/host-top.txt" 2>/dev/null &&
			validate_host_top "$dir/os/host-top.txt" &&
			validate_process_top "$dir/os/process-top.txt" "$GAME_PID" "$GATE_PID" "$LOAD_PID" &&
			validate_vm_stat "$dir/os/vm-stat.txt" &&
			process_alive "$NET_PID" && process_alive "$REDIS_PID" &&
			grep -q $'\tok$' "$dir/os/redis.tsv" 2>/dev/null; then
			for role in game gate load; do
				grep -q $'\t200\t' "$dir/metrics/$role.tsv" 2>/dev/null || return 1
				if grep -q $'\terror\t' "$dir/metrics/$role.tsv" 2>/dev/null; then
					return 1
				fi
			done
			return 0
		fi
		sleep 1
	done
	# workload 尚未開始前 nettop 可能只有空檔案；此時只判斷 collector
	# process 是否仍存活，active window 結束後 validate_attempt 才要求
	# 三個 service process row 都存在。
	if ! process_alive "$NET_PID"; then
		printf 'network_process_unavailable\n' >"$dir/os/network_process_unavailable"
	fi
	return 1
}

write_start_marker() {
	local dir=$1 temporary
	temporary="$dir/control/.start.tmp"
	printf '{"timestamp":"%s","case":"%s","attempt":%s}\n' \
		"$(timestamp_now)" "$CASE_NAME" "$CURRENT_ATTEMPT" >"$temporary"
	mv "$temporary" "$dir/control/start"
}

write_baseline_marker() {
	local dir=$1 run_id=$2 temporary
	temporary="$dir/control/.baseline-ready.tmp"
	printf '{"timestamp":"%s","run_id":"%s"}\n' \
		"$(timestamp_now)" "$run_id" >"$temporary"
	mv "$temporary" "$dir/control/baseline-ready.json"
}

write_final_scrape_marker() {
	local dir=$1 run_id=$2 temporary
	temporary="$dir/control/.final-scrape.tmp"
	printf '{"timestamp":"%s","run_id":"%s"}\n' \
		"$(timestamp_now)" "$run_id" >"$temporary"
	mv "$temporary" "$dir/control/final-scrape.json"
}

record_command() {
	local name=$1
	shift
	printf '%s_command=' "$name" >>"$ATTEMPT_DIR/commands.txt"
	printf '%q ' "$@" >>"$ATTEMPT_DIR/commands.txt"
	printf '\n' >>"$ATTEMPT_DIR/commands.txt"
}

capture_campaign_source_state() {
	{
		printf 'git_commit=%s\n' "$(git -C "$ROOT_DIR" rev-parse HEAD 2>/dev/null || printf unknown)"
		printf 'tracked_worktree_diff_sha256='
		(git -C "$ROOT_DIR" diff --no-ext-diff --binary || true) | shasum -a 256 | awk '{print $1}'
		printf 'tracked_index_diff_sha256='
		(git -C "$ROOT_DIR" diff --cached --no-ext-diff --binary || true) | shasum -a 256 | awk '{print $1}'
		printf 'git_status_begin\n'
		git -C "$ROOT_DIR" status --short || true
		printf 'git_status_end\n'
		printf 'untracked_source_sha256_begin\n'
		git -C "$ROOT_DIR" ls-files --others --exclude-standard | while IFS= read -r file; do
			case "$file" in
				*.go|*.proto|*.yaml|*.yml|*.md|*.sh) [[ -f "$ROOT_DIR/$file" ]] || continue; shasum -a 256 "$ROOT_DIR/$file" | awk -v path="$file" '{print $1 "\t" path}' ;;
			esac
		done
		printf 'untracked_source_sha256_end\n'
	} >"$ARTIFACT_ROOT/source-state.txt"
}

build_campaign_binaries() {
	[[ ! -e "$CAMPAIGN_CHECKSUMS" ]] || die "campaign binary checksum file already exists: $CAMPAIGN_CHECKSUMS"
	mkdir -p "$CAMPAIGN_BIN_DIR"
	capture_campaign_source_state
	{
		printf 'build_timestamp=%s\n' "$(timestamp_now)"
		printf 'build_command=cd %q && go build -o %q ./examples/metrics/game\n' "$ROOT_DIR" "$CAMPAIGN_BIN_DIR/game"
		printf 'build_command=cd %q && go build -o %q ./examples/metrics/gate\n' "$ROOT_DIR" "$CAMPAIGN_BIN_DIR/gate"
		printf 'build_command=cd %q && go build -o %q ./examples/metrics/load\n' "$ROOT_DIR" "$CAMPAIGN_BIN_DIR/load"
	} >"$ARTIFACT_ROOT/build-commands.txt"
	if ! (cd "$ROOT_DIR" && go build -o "$CAMPAIGN_BIN_DIR/game" ./examples/metrics/game &&
		go build -o "$CAMPAIGN_BIN_DIR/gate" ./examples/metrics/gate &&
		go build -o "$CAMPAIGN_BIN_DIR/load" ./examples/metrics/load); then
		die "campaign binary build failed; artifacts=$ARTIFACT_ROOT"
	fi
	shasum -a 256 "$CAMPAIGN_BIN_DIR/game" "$CAMPAIGN_BIN_DIR/gate" "$CAMPAIGN_BIN_DIR/load" >"$CAMPAIGN_CHECKSUMS"
}

copy_campaign_binaries() {
	local dir=$1 name
	for name in game gate load; do
		[[ -x "$CAMPAIGN_BIN_DIR/$name" ]] || return 1
		cp -p "$CAMPAIGN_BIN_DIR/$name" "$dir/bin/$name" || return 1
	done
	shasum -a 256 "$dir/bin/game" "$dir/bin/gate" "$dir/bin/load" >"$dir/checksums.txt"
	verify_binary_checksums "$CAMPAIGN_CHECKSUMS" "$dir/bin"
}

file_size_bytes() {
	local file=$1 size
	if size=$(stat -f '%z' "$file" 2>/dev/null); then
		printf '%s\n' "$size"
	elif size=$(stat -c '%s' "$file" 2>/dev/null); then
		printf '%s\n' "$size"
	else
		return 1
	fi
}

write_evidence_manifest() {
	local manifest="$ARTIFACT_ROOT/evidence-manifest.tsv" file size sha
	local temporary="$manifest.tmp"
	printf 'path\tsize_bytes\tsha256\n' >"$temporary"
	(
		cd "$ARTIFACT_ROOT"
		find . -type f ! -name 'evidence-manifest.tsv' ! -name 'evidence-manifest.tsv.tmp' -print | sort
	) | while IFS= read -r file; do
		file=${file#./}
		size=$(file_size_bytes "$ARTIFACT_ROOT/$file") || continue
		sha=$(shasum -a 256 "$ARTIFACT_ROOT/$file" | awk '{print $1}') || continue
		printf '%s\t%s\t%s\n' "$file" "$size" "$sha" >>"$temporary"
	done
	mv "$temporary" "$manifest"
}

run_attempt() {
	local attempt=$1 load_exit=1 warmup_marker measured_marker run_id measured_started=0 final_snapshot_ready=0 attempt_started_at
	attempt_started_at=$(timestamp_now)
	CURRENT_ATTEMPT=$attempt
	ATTEMPT_DIR="$CASE_ARTIFACT_ROOT/run-$CURRENT_ATTEMPT"
	cleanup
	GAME_PID=""; GATE_PID=""; LOAD_PID=""; REDIS_PID=""
	HOST_PID=""; PROCESS_PID=""; VM_PID=""; NET_PID=""; SNAPSHOT_PID=""
	SCRAPE_GAME_PID=""; SCRAPE_GATE_PID=""; SCRAPE_LOAD_PID=""; PROFILE_PID=""
	mkdir -p "$ATTEMPT_DIR/bin" "$ATTEMPT_DIR/metrics" "$ATTEMPT_DIR/os" "$ATTEMPT_DIR/control"
	if [[ -n "$PROFILE_TARGET" ]]; then mkdir -p "$ATTEMPT_DIR/pprof"; fi
	: >"$ATTEMPT_DIR/phase-events.jsonl"
	: >"$ATTEMPT_DIR/commands.txt"
	printf 'case=%s attempt=%s workload=%s interval=%s\n' "$CASE_NAME" "$CURRENT_ATTEMPT" "$WORKLOAD" "$INTERVAL" >"$ATTEMPT_DIR/game.log"
	printf 'case=%s attempt=%s workload=%s interval=%s\n' "$CASE_NAME" "$CURRENT_ATTEMPT" "$WORKLOAD" "$INTERVAL" >"$ATTEMPT_DIR/gate.log"
	printf 'case=%s attempt=%s workload=%s interval=%s\n' "$CASE_NAME" "$CURRENT_ATTEMPT" "$WORKLOAD" "$INTERVAL" >"$ATTEMPT_DIR/load.log"
	{
		printf 'case=%s\n' "$CASE_NAME"
		printf 'attempt=%s\n' "$CURRENT_ATTEMPT"
		printf 'campaign_root=%s\ncampaign_phase=%s\n' "$ARTIFACT_ROOT" "$CAMPAIGN_PHASE"
		printf 'workload=%s\ninterval=%s\n' "$WORKLOAD" "$INTERVAL"
		printf 'connections=%s\nroom_members=%s\nplayer_targets=%s\n' "$CONNECTIONS" "$CONNECTIONS" "$CONNECTIONS"
		printf 'payload_bytes=32\nduration=%s\nwarmup=%s\ndrain=%s\n' "$DURATION" "$WARMUP" "$DRAIN"
		printf 'gomaxprocs=4\nbuild_timestamp=%s\nattempt_started_at=%s\n' "$(sed -n 's/^build_timestamp=//p' "$ARTIFACT_ROOT/build-commands.txt")" "$attempt_started_at"
		printf 'git_commit=%s\n' "$(git -C "$ROOT_DIR" rev-parse HEAD 2>/dev/null || printf unknown)"
		printf 'go_version=%s\ngoos=%s\ngoarch=%s\n' "$(go version)" "$(go env GOOS)" "$(go env GOARCH)"
		printf 'grpc_go_version=%s\ngo_redis_version=%s\n' \
			"$(go list -m -f '{{.Version}}' google.golang.org/grpc 2>/dev/null || printf unknown)" \
			"$(go list -m -f '{{.Version}}' github.com/redis/go-redis/v9 2>/dev/null || printf unknown)"
		printf 'host=%s\n' "$(uname -a)"
		printf 'host_model=%s\nhost_logical_cpu=%s\nhost_memory_bytes=%s\nmacos_version=%s\n' \
			"$(sysctl -n hw.model 2>/dev/null || printf unknown)" \
			"$(sysctl -n hw.logicalcpu 2>/dev/null || printf unknown)" \
			"$(sysctl -n hw.memsize 2>/dev/null || printf unknown)" \
			"$(sw_vers -productVersion 2>/dev/null || printf unknown)"
		printf 'metrics_endpoints=game:19080,gate:18081,load:22081\npprof_target=%s\npprof_endpoint=%s\ntrace=%s\ncampaign_binary_checksums=%s\ncampaign_source_state=%s\n' \
			"$PROFILE_TARGET" "$(profile_endpoint)" "$TRACE" "$CAMPAIGN_CHECKSUMS" "$ARTIFACT_ROOT/source-state.txt"
		for name in game gate load; do
			printf 'binary_sha256_%s=%s\n' "$name" "$(awk -v name="$name" '{path=$2; sub(/^.*\//, "", path); if (path == name) {print $1; exit}}' "$CAMPAIGN_CHECKSUMS")"
		done
		printf 'config_game=%s\nconfig_gate=%s\n' "$CONFIG_DIR/game.yaml" "$CONFIG_DIR/gate.yaml"
		printf 'redis_endpoint=127.0.0.1:6379\nredis_runtime=external_dependency; see redis-server-info.txt\n'
	} >"$ATTEMPT_DIR/metadata.txt"
	git -C "$ROOT_DIR" status --short >"$ATTEMPT_DIR/git-status.txt" 2>&1 || true

	record_event "$ATTEMPT_DIR/phase-events.jsonl" build start "attempt-$CURRENT_ATTEMPT" ""
	if ! copy_campaign_binaries "$ATTEMPT_DIR"; then
		write_invalid_status "$ATTEMPT_DIR" build
		cleanup
		return 1
	fi
	cp -p "$ARTIFACT_ROOT/source-state.txt" "$ATTEMPT_DIR/source-state.txt"
	record_event "$ATTEMPT_DIR/phase-events.jsonl" build end "attempt-$CURRENT_ATTEMPT" ""

	record_event "$ATTEMPT_DIR/phase-events.jsonl" dependency_ready start "attempt-$CURRENT_ATTEMPT" ""
	if ! redis-cli -h 127.0.0.1 -p 6379 PING >"$ATTEMPT_DIR/redis-ping.txt" 2>&1; then
		write_invalid_status "$ATTEMPT_DIR" dependency/redis
		cleanup
		return 1
	fi
	redis-cli -h 127.0.0.1 -p 6379 INFO server >"$ATTEMPT_DIR/redis-server-info.txt" 2>&1 || true
	printf 'redis_version=%s\nredis_mode=%s\n' \
		"$(sed -n 's/^redis_version://p' "$ATTEMPT_DIR/redis-server-info.txt" | head -n 1 || printf unknown)" \
		"$(sed -n 's/^redis_mode://p' "$ATTEMPT_DIR/redis-server-info.txt" | head -n 1 || printf unknown)" >>"$ATTEMPT_DIR/metadata.txt"
	record_event "$ATTEMPT_DIR/phase-events.jsonl" dependency_ready end "attempt-$CURRENT_ATTEMPT" ""
	ports_in_use >"$ATTEMPT_DIR/port-conflicts.txt"
	printf 'port_preflight=lsof -nP -iTCP:<port> -sTCP:LISTEN -t\n' >>"$ATTEMPT_DIR/commands.txt"
	if [[ -s "$ATTEMPT_DIR/port-conflicts.txt" ]]; then
		write_invalid_status "$ATTEMPT_DIR" environment/port-conflict
		cleanup
		return 1
	fi

	GAME_CMD=(env GOMAXPROCS=4 "$ATTEMPT_DIR/bin/game" -config "$CONFIG_DIR/game.yaml")
	GATE_CMD=(env GOMAXPROCS=4 "$ATTEMPT_DIR/bin/gate" -config "$CONFIG_DIR/gate.yaml")
	if [[ "$PROFILE_TARGET" == game ]]; then GAME_CMD+=( -pprof-addr 127.0.0.1:19082 ); fi
	if [[ "$PROFILE_TARGET" == gate ]]; then GATE_CMD+=( -pprof-addr 127.0.0.1:18082 ); fi
	record_event "$ATTEMPT_DIR/phase-events.jsonl" services_ready start "attempt-$CURRENT_ATTEMPT" ""
	"${GAME_CMD[@]}" >"$ATTEMPT_DIR/game.log" 2>&1 & GAME_PID=$!
	"${GATE_CMD[@]}" >"$ATTEMPT_DIR/gate.log" 2>&1 & GATE_PID=$!
	printf 'game_pid=%s\ngate_pid=%s\n' "$GAME_PID" "$GATE_PID" >>"$ATTEMPT_DIR/metadata.txt"
	record_command GAME "${GAME_CMD[@]}"
	record_command GATE "${GATE_CMD[@]}"
	if ! wait_http http://127.0.0.1:19080/ready || ! wait_http http://127.0.0.1:18081/ready; then
		write_invalid_status "$ATTEMPT_DIR" services/readiness
		cleanup
		return 1
	fi
	record_event "$ATTEMPT_DIR/phase-events.jsonl" services_ready end "attempt-$CURRENT_ATTEMPT" ""

	LOAD_CMD=(env GOMAXPROCS=4 "$ATTEMPT_DIR/bin/load" -gate-url ws://127.0.0.1:18080/ws -workload "$WORKLOAD" -connections "$CONNECTIONS" -duration "$DURATION" -payload-bytes 32 -push-interval "$INTERVAL" -push-warmup-duration "$WARMUP" -drain-timeout "$DRAIN" -setup-concurrency 32 -setup-timeout 2m -request-timeout 10s -metrics-addr 127.0.0.1:22081 -orchestration-dir "$ATTEMPT_DIR/control")
	if [[ "$PROFILE_TARGET" == load ]]; then LOAD_CMD+=( -pprof-addr 127.0.0.1:22083 ); fi
	record_event "$ATTEMPT_DIR/phase-events.jsonl" load_started start "attempt-$CURRENT_ATTEMPT" ""
	"${LOAD_CMD[@]}" >"$ATTEMPT_DIR/load.log" 2>&1 & LOAD_PID=$!
	printf 'load_pid=%s\n' "$LOAD_PID" >>"$ATTEMPT_DIR/metadata.txt"
	record_command LOAD "${LOAD_CMD[@]}"
	record_event "$ATTEMPT_DIR/phase-events.jsonl" load_started end "attempt-$CURRENT_ATTEMPT" ""

	record_event "$ATTEMPT_DIR/phase-events.jsonl" clients_ready start "attempt-$CURRENT_ATTEMPT" ""
	if ! wait_for_marker clients-ready.json "$ATTEMPT_DIR"; then
		write_invalid_status "$ATTEMPT_DIR" load/clients-ready
		cleanup
		return 1
	fi
	record_event "$ATTEMPT_DIR/phase-events.jsonl" clients_ready end "attempt-$CURRENT_ATTEMPT" ""

	# 只在所有 reader ready 後啟動 active-window collectors，避免 setup
	# 時間消耗固定 sample count，導致 measured phase 沒有完整覆蓋。
	scrape_loop game http://127.0.0.1:19080/metrics "$ATTEMPT_DIR" & SCRAPE_GAME_PID=$!
	scrape_loop gate http://127.0.0.1:18081/metrics "$ATTEMPT_DIR" & SCRAPE_GATE_PID=$!
	scrape_loop load http://127.0.0.1:22081/metrics "$ATTEMPT_DIR" & SCRAPE_LOAD_PID=$!
	start_collectors "$ATTEMPT_DIR" "$COLLECTOR_SAMPLES"
	printf 'collector_samples=%s\n' "$COLLECTOR_SAMPLES" >>"$ATTEMPT_DIR/metadata.txt"
	printf 'open_file_limit=%s\n' "$(ulimit -n)" >>"$ATTEMPT_DIR/metadata.txt"
	netstat -s -p tcp >"$ATTEMPT_DIR/os/netstat-before.txt" 2>&1 || true
	{
		printf 'host_top=LC_ALL=C '; printf '%q ' top -l "$COLLECTOR_SAMPLES" -s 1 -n 0; printf '\n'
		printf 'process_top=LC_ALL=C '; printf '%q ' top -l "$COLLECTOR_SAMPLES" -s 1 -pid "$GAME_PID" -pid "$GATE_PID" -pid "$LOAD_PID" -stats pid,command,cpu,mem,threads,csw,state; printf '\n'
		printf 'vm_stat=LC_ALL=C '; printf '%q ' vm_stat -c "$COLLECTOR_SAMPLES" 1; printf '\n'
			printf 'nettop=LC_ALL=C '; printf '%q ' nettop -L "$COLLECTOR_SAMPLES" -s 1 -n -P -p "$GAME_PID" -p "$GATE_PID" -p "$LOAD_PID"; printf '\n'
		printf 'netstat_before=LC_ALL=C '; printf '%q ' netstat -s -p tcp; printf '\n'
		printf 'redis_before='; printf '%q ' redis-cli -h 127.0.0.1 -p 6379 INFO stats cpu memory clients commandstats; printf '\n'
		printf 'redis_collector='; printf '%q ' redis-cli -h 127.0.0.1 -p 6379 INFO stats cpu clients; printf '\n'
		for pid in "$GAME_PID" "$GATE_PID" "$LOAD_PID"; do
			printf 'lsof_%s=LC_ALL=C ' "$pid"; printf '%q ' lsof -nP -a -p "$pid"; printf '\n'
		done
	} >>"$ATTEMPT_DIR/commands.txt"

	record_event "$ATTEMPT_DIR/phase-events.jsonl" collectors_ready start "attempt-$CURRENT_ATTEMPT" ""
	if ! preflight_metrics "$ATTEMPT_DIR" || ! wait_for_collector_preflight "$ATTEMPT_DIR"; then
		write_invalid_status "$ATTEMPT_DIR" collector/preflight
		cleanup
		return 1
	fi
	capture_process_snapshot "$ATTEMPT_DIR" collectors-ready
	record_event "$ATTEMPT_DIR/phase-events.jsonl" collectors_ready end "attempt-$CURRENT_ATTEMPT" ""
	redis-cli -h 127.0.0.1 -p 6379 INFO stats cpu memory clients commandstats >"$ATTEMPT_DIR/redis-before.txt" 2>&1 || true
	if [[ -n "$PROFILE_TARGET" ]]; then
		printf 'pprof_profile=curl -fsS --max-time 25 http://%s/debug/pprof/profile?seconds=20\n' "$(profile_endpoint)" >>"$ATTEMPT_DIR/commands.txt"
		printf 'pprof_heap=curl -fsS --max-time 5 http://%s/debug/pprof/heap\n' "$(profile_endpoint)" >>"$ATTEMPT_DIR/commands.txt"
		printf 'pprof_goroutine=curl -fsS --max-time 5 http://%s/debug/pprof/goroutine?debug=1\n' "$(profile_endpoint)" >>"$ATTEMPT_DIR/commands.txt"
		if [[ "$TRACE" -eq 1 ]]; then
			printf 'pprof_trace=curl -fsS --max-time 10 http://%s/debug/pprof/trace?seconds=5\n' "$(profile_endpoint)" >>"$ATTEMPT_DIR/commands.txt"
		fi
	fi
	write_start_marker "$ATTEMPT_DIR"

	if ! wait_for_marker warmup-started.json "$ATTEMPT_DIR"; then
		:
	else
		warmup_marker=$(cat "$ATTEMPT_DIR/control/warmup-started.json")
		run_id=$(printf '%s\n' "$warmup_marker" | sed -n 's/.*"run_id":"\([^"]*\)".*/\1/p')
		record_event "$ATTEMPT_DIR/phase-events.jsonl" warmup_running start "$run_id" "$(printf '%s\n' "$warmup_marker" | sed -n 's/.*"timestamp":"\([^"]*\)".*/\1/p')"
		capture_baseline_when_warmup_completes "$ATTEMPT_DIR" & SNAPSHOT_PID=$!
		if [[ -n "$PROFILE_TARGET" ]]; then profile_loop "$ATTEMPT_DIR" "$ATTEMPT_DIR/control" & PROFILE_PID=$!; fi
		if wait_for_marker measured-started.json "$ATTEMPT_DIR"; then
			measured_marker=$(cat "$ATTEMPT_DIR/control/measured-started.json")
			run_id=$(printf '%s\n' "$measured_marker" | sed -n 's/.*"run_id":"\([^"]*\)".*/\1/p')
			record_event "$ATTEMPT_DIR/phase-events.jsonl" measured_running start "$run_id" "$(printf '%s\n' "$measured_marker" | sed -n 's/.*"timestamp":"\([^"]*\)".*/\1/p')"
			measured_started=1
		fi
	fi

	record_game_completion "$ATTEMPT_DIR"
	stop_pid "$SNAPSHOT_PID"; SNAPSHOT_PID=""
	if [[ "$measured_started" -eq 1 ]] && wait_for_run_marker final-ready.json "$ATTEMPT_DIR" "$run_id"; then
		record_game_completion "$ATTEMPT_DIR"
		if grep -q '"phase":"measured_running".*"event":"end"' "$ATTEMPT_DIR/phase-events.jsonl" 2>/dev/null &&
			! grep -q '"phase":"measured_draining".*"event":"start"' "$ATTEMPT_DIR/phase-events.jsonl" 2>/dev/null; then
			record_event "$ATTEMPT_DIR/phase-events.jsonl" measured_draining start "$run_id" ""
		fi
		record_event "$ATTEMPT_DIR/phase-events.jsonl" final_snapshot start "attempt-$CURRENT_ATTEMPT" ""
		capture_final_metrics "$ATTEMPT_DIR" || true
		record_event "$ATTEMPT_DIR/phase-events.jsonl" final_snapshot end "attempt-$CURRENT_ATTEMPT" ""
		write_final_scrape_marker "$ATTEMPT_DIR" "$run_id"
		# final snapshot 已完成；load 會在看見 marker 後關閉自己的
		# metrics listener，因此此刻停止 scraper，避免下一次 loop
		# 將正常的 listener 關閉誤記成 error row。
		: >"$ATTEMPT_DIR/control/collectors-stop"
		stop_pid "$SCRAPE_GAME_PID"; SCRAPE_GAME_PID=""
		stop_pid "$SCRAPE_GATE_PID"; SCRAPE_GATE_PID=""
		stop_pid "$SCRAPE_LOAD_PID"; SCRAPE_LOAD_PID=""
		stop_pid "$REDIS_PID"; REDIS_PID=""
		final_snapshot_ready=1
	fi
	stop_pid "$PROFILE_PID"; PROFILE_PID=""
	set +e
	wait "$LOAD_PID"
	load_exit=$?
	set -e
	redis-cli -h 127.0.0.1 -p 6379 INFO stats cpu memory clients commandstats >"$ATTEMPT_DIR/redis-after.txt" 2>&1 || true
	netstat -s -p tcp >"$ATTEMPT_DIR/os/netstat-after.txt" 2>&1 || true
	printf 'netstat_after=LC_ALL=C ' >>"$ATTEMPT_DIR/commands.txt"; printf '%q ' netstat -s -p tcp >>"$ATTEMPT_DIR/commands.txt"; printf '\n' >>"$ATTEMPT_DIR/commands.txt"
	printf 'redis_after=' >>"$ATTEMPT_DIR/commands.txt"; printf '%q ' redis-cli -h 127.0.0.1 -p 6379 INFO stats cpu memory clients commandstats >>"$ATTEMPT_DIR/commands.txt"; printf '\n' >>"$ATTEMPT_DIR/commands.txt"
	capture_process_snapshot "$ATTEMPT_DIR" final
	if [[ "$final_snapshot_ready" -eq 0 ]]; then
		record_game_completion "$ATTEMPT_DIR"
		if [[ "$measured_started" -eq 1 ]] && grep -q '"phase":"measured_running".*"event":"end"' "$ATTEMPT_DIR/phase-events.jsonl" 2>/dev/null &&
			! grep -q '"phase":"measured_draining".*"event":"start"' "$ATTEMPT_DIR/phase-events.jsonl" 2>/dev/null; then
			record_event "$ATTEMPT_DIR/phase-events.jsonl" measured_draining start "$run_id" ""
		fi
		record_event "$ATTEMPT_DIR/phase-events.jsonl" final_snapshot start "attempt-$CURRENT_ATTEMPT" ""
		capture_final_metrics "$ATTEMPT_DIR" || true
		record_event "$ATTEMPT_DIR/phase-events.jsonl" final_snapshot end "attempt-$CURRENT_ATTEMPT" ""
	fi
	if [[ "$measured_started" -eq 1 ]]; then
		record_event "$ATTEMPT_DIR/phase-events.jsonl" measured_draining end "$run_id" ""
	fi
	# final scrape 完成後先停止 metrics scraper，再進行分類與驗證；否則
	# load listener 已關閉時，scraper 可能追加一筆 error row，污染可重驗
	# 的 metrics index。
	: >"$ATTEMPT_DIR/control/collectors-stop"
	stop_pid "$SCRAPE_GAME_PID"; SCRAPE_GAME_PID=""
	stop_pid "$SCRAPE_GATE_PID"; SCRAPE_GATE_PID=""
	stop_pid "$SCRAPE_LOAD_PID"; SCRAPE_LOAD_PID=""
	stop_pid "$REDIS_PID"; REDIS_PID=""
	append_run_metadata "$ATTEMPT_DIR" "$run_id" "$(timestamp_now)"
	classify_attempt "$ATTEMPT_DIR" "$load_exit"
	if ! validate_attempt "$ATTEMPT_DIR"; then
		cleanup
		return 1
	fi
	cleanup
	return 0
}

classify_attempt() {
	local dir=$1 load_exit=$2 warmup_line measured_line load_line warmup_drained_line
	local warmup_attempted warmup_success warmup_partial warmup_error warmup_missed measured_attempted measured_success measured_partial measured_error measured_missed planned received
	local warmup_received warmup_duplicate warmup_gap warmup_invalid warmup_reader_failures load_duplicate load_gap load_invalid load_missing load_reader_failures expected
	local baseline_gate final_gate gate_terminal baseline_write final_write write_success baseline_sender final_sender sender_success
	local evidence_status workload_status first_failure warmup_started measured_started baseline_ready collector_preflight terminal_gauges_zero async_terminal_counters_zero
	local warmup_workload_status warmup_first_failure warmup_cadence_failed warmup_sender_failed warmup_delivery_failed cadence_failed sender_failed delivery_failed
	warmup_line=$(grep '\"message\":\"example push workload completed\"' "$dir/game.log" | grep warmup | tail -n 1 || true)
	measured_line=$(grep '\"message\":\"example push workload completed\"' "$dir/game.log" | grep measured | tail -n 1 || true)
	load_line=$(grep 'push load complete:' "$dir/load.log" | tail -n 1 || true)
	warmup_attempted=$(json_field "$warmup_line" attempted); warmup_success=$(json_field "$warmup_line" success); warmup_partial=$(json_field "$warmup_line" partial); warmup_error=$(json_field "$warmup_line" error); warmup_missed=$(json_field "$warmup_line" missed)
	measured_attempted=$(json_field "$measured_line" attempted); measured_success=$(json_field "$measured_line" success)
	measured_partial=$(json_field "$measured_line" partial); measured_error=$(json_field "$measured_line" error); measured_missed=$(json_field "$measured_line" missed)
	warmup_drained_line=$(cat "$dir/control/warmup-drained.json" 2>/dev/null || true)
	warmup_received=$(json_field "$warmup_drained_line" received)
	warmup_duplicate=$(json_field "$warmup_drained_line" duplicate)
	warmup_gap=$(json_field "$warmup_drained_line" sequence_gap)
	warmup_invalid=$(json_field "$warmup_drained_line" invalid)
	warmup_reader_failures=$(json_field "$warmup_drained_line" reader_failures)
	planned=$(kv_field "$load_line" planned_ticks); received=$(kv_field "$load_line" received)
	load_duplicate=$(kv_field "$load_line" duplicate); load_gap=$(kv_field "$load_line" sequence_gap)
	load_invalid=$(kv_field "$load_line" invalid); load_missing=$(kv_field "$load_line" missing_per_reader_at_least)
	load_reader_failures=$(kv_field "$load_line" reader_failures)
	baseline_gate=$(sample_value "$dir/metrics/baseline-gate.prom" "gaming_core_gate_server_send_delivery_duration_seconds_count" 'target="room"' 'result="success"')
	final_gate=$(sample_value "$dir/metrics/final-gate.prom" "gaming_core_gate_server_send_delivery_duration_seconds_count" 'target="room"' 'result="success"')
	if [[ "$WORKLOAD" == player ]]; then
		baseline_gate=$(sample_value "$dir/metrics/baseline-gate.prom" "gaming_core_gate_server_send_requests_total" 'target="player"' 'result="queued"')
		final_gate=$(sample_value "$dir/metrics/final-gate.prom" "gaming_core_gate_server_send_requests_total" 'target="player"' 'result="queued"')
	fi
	baseline_sender=$(sample_value "$dir/metrics/baseline-game.prom" "gaming_core_game_server_send_requests_total" "operation=\"$WORKLOAD\"" 'result="success"')
	final_sender=$(sample_value "$dir/metrics/final-game.prom" "gaming_core_game_server_send_requests_total" "operation=\"$WORKLOAD\"" 'result="success"')
	baseline_write=$(sample_value "$dir/metrics/baseline-gate.prom" "gaming_core_gate_websocket_writes_total" 'source="server_send"' 'result="success"')
	final_write=$(sample_value "$dir/metrics/final-gate.prom" "gaming_core_gate_websocket_writes_total" 'source="server_send"' 'result="success"')
	gate_terminal=null; write_success=null; sender_success=null
	if gate_terminal=$(metric_delta "$baseline_gate" "$final_gate"); then :; else gate_terminal=null; fi
	if write_success=$(metric_delta "$baseline_write" "$final_write"); then :; else write_success=null; fi
	if sender_success=$(metric_delta "$baseline_sender" "$final_sender"); then :; else sender_success=null; fi
	warmup_started=false; measured_started=false; baseline_ready=false; collector_preflight=failed
	[[ -s "$dir/control/warmup-started.json" ]] && warmup_started=true
	[[ -s "$dir/control/measured-started.json" ]] && measured_started=true
	[[ -s "$dir/control/baseline-ready.json" ]] && baseline_ready=true
	if [[ -s "$dir/control/metrics-preflight.txt" && -s "$dir/os/host-top.txt" && -s "$dir/os/process-top.txt" &&
		-s "$dir/os/vm-stat.txt" && -s "$dir/os/nettop.txt" && -s "$dir/os/redis.tsv" && ! -e "$dir/os/network_process_unavailable" ]]; then
		collector_preflight=passed
		validate_redis_tsv "$dir/os/redis.tsv" || collector_preflight=failed
		for role in game gate load; do
			if ! validate_metrics_index "$dir/metrics/$role.tsv"; then
				collector_preflight=failed
			fi
		done
	fi
	terminal_gauges_zero=true
	validate_game_terminal_gauges "$dir/metrics/final-game.prom" 2>/dev/null || terminal_gauges_zero=false
	validate_gate_terminal_gauges "$dir/metrics/final-gate.prom" 2>/dev/null || terminal_gauges_zero=false
	async_terminal_counters_zero=true
	if [[ "$measured_started" == true ]]; then
		validate_async_terminal_counters \
			"$dir/metrics/baseline-game.prom" "$dir/metrics/final-game.prom" \
			"$dir/metrics/baseline-gate.prom" "$dir/metrics/final-gate.prom" "$WORKLOAD" ||
			async_terminal_counters_zero=false
	fi
	evidence_status=evidence_valid; workload_status=not_run; first_failure=
	warmup_workload_status=; warmup_first_failure=
	warmup_cadence_failed=false; warmup_sender_failed=false; warmup_delivery_failed=false
	cadence_failed=false; sender_failed=false; delivery_failed=false
	# Warm-up workload failure 仍保留在 attempt 的主要分類；它不會阻止
	# measured，但若 measured 成功，報告仍需指出較早失守的 boundary。
	if [[ -n "$warmup_line" ]]; then
		if [[ "$warmup_partial" != null && "$warmup_partial" != 0 || "$warmup_error" != null && "$warmup_error" != 0 ]]; then
			warmup_sender_failed=true; sender_failed=true
			warmup_workload_status=sender_failed
			warmup_first_failure=warmup/game_sender
		fi
		if [[ "$warmup_missed" != null && "$warmup_missed" != 0 ]]; then
			warmup_cadence_failed=true; cadence_failed=true
			if [[ -z "$warmup_first_failure" ]]; then
				warmup_workload_status=cadence_failed
				warmup_first_failure=warmup/producer_cadence
			fi
		fi
		if [[ "$warmup_duplicate" != null && "$warmup_duplicate" != 0 || "$warmup_gap" != null && "$warmup_gap" != 0 ||
			"$warmup_invalid" != null && "$warmup_invalid" != 0 || "$warmup_reader_failures" != null && "$warmup_reader_failures" != 0 ]]; then
			warmup_delivery_failed=true; delivery_failed=true
			if [[ -z "$warmup_first_failure" ]]; then
				warmup_workload_status=delivery_failed
				warmup_first_failure=warmup/client_delivery
			fi
		fi
		if [[ "$warmup_received" != null && "$warmup_success" != null ]]; then
			expected=$((warmup_success * CONNECTIONS))
			[[ "$warmup_received" -eq "$expected" ]] || {
				warmup_delivery_failed=true; delivery_failed=true
				if [[ -z "$warmup_first_failure" ]]; then
					warmup_workload_status=delivery_failed
					warmup_first_failure=warmup/client_delivery
				fi
			}
		fi
	fi
	if [[ "$collector_preflight" != passed ]]; then
		evidence_status=environment_invalid; first_failure=collector/coverage
	elif [[ "$measured_started" == true && ! -s "$dir/metrics/baseline-gate.prom" ]]; then
		evidence_status=environment_invalid; first_failure=measured/baseline
	elif [[ "$load_exit" -ne 0 && -z "$warmup_line" && -z "$measured_line" ]]; then
		evidence_status=environment_invalid; first_failure=environment/startup
	elif [[ "$measured_started" == true && -z "$measured_line" ]]; then
		evidence_status=environment_invalid; first_failure=measured/summary
	elif [[ "$measured_started" == true && "$sender_success" == null ]]; then
		evidence_status=environment_invalid; first_failure=metrics/game_sender
	elif [[ "$measured_started" == true && "$measured_success" != null && "$sender_success" != "$measured_success" ]]; then
		evidence_status=environment_invalid; first_failure=metrics/game_sender_summary_mismatch
	elif [[ -e "$dir/control/warmup-workload-invalid" || -e "$dir/control/warmup-drained-timeout" ||
		-e "$dir/control/warmup-drained-invalid" || -e "$dir/control/warmup-process-exited" ||
		-e "$dir/control/baseline-queue-timeout" || -e "$dir/control/baseline-scrape-failed" ||
		-e "$dir/control/baseline-metrics-invalid" ]]; then
		evidence_status=environment_invalid; first_failure=warmup/lifecycle
		[[ -e "$dir/control/baseline-metrics-invalid" ]] && first_failure=measured/baseline_metrics
	elif [[ "$warmup_started" == true && "$measured_started" == false ]]; then
		evidence_status=environment_invalid; first_failure=warmup/barrier
	elif [[ -z "$measured_line" ]]; then
		if [[ -n "$warmup_first_failure" ]]; then
			workload_status=$warmup_workload_status
			first_failure=$warmup_first_failure
		elif [[ -e "$dir/control/baseline-queue-timeout" ]]; then
			delivery_failed=true; workload_status=delivery_failed; first_failure=warmup/gate_queue
		else
			delivery_failed=true; workload_status=delivery_failed; first_failure=warmup/client_delivery
		fi
	else
		if [[ "$measured_partial" != 0 && "$measured_partial" != null || "$measured_error" != 0 && "$measured_error" != null ]]; then
			sender_failed=true
		fi
		if [[ "$measured_missed" != 0 && "$measured_missed" != null ]]; then
			cadence_failed=true
		fi
		if [[ "$load_exit" -ne 0 ]]; then
			delivery_failed=true
		fi
		if ! client_delivery_matches_emitted "$received" "$sender_success" "$CONNECTIONS" \
			"$load_duplicate" "$load_gap" "$load_invalid" "$load_reader_failures"; then
			delivery_failed=true
		fi
		if [[ "$gate_terminal" != null && "$sender_success" != null ]]; then
			expected=$((sender_success * CONNECTIONS))
			[[ "$gate_terminal" -eq "$expected" ]] || delivery_failed=true
		fi
		if [[ "$write_success" != null && "$sender_success" != null ]]; then
			expected=$((sender_success * CONNECTIONS + 1))
			[[ "$write_success" -eq "$expected" ]] || delivery_failed=true
		fi
		[[ "$terminal_gauges_zero" == true ]] || delivery_failed=true
		[[ "$async_terminal_counters_zero" == true ]] || delivery_failed=true
		if [[ -n "$warmup_first_failure" ]]; then
			workload_status=$warmup_workload_status; first_failure=$warmup_first_failure
		elif [[ "$cadence_failed" == true ]]; then
			workload_status=cadence_failed; first_failure=measured/producer_cadence
		elif [[ "$sender_failed" == true ]]; then
			workload_status=sender_failed; first_failure=measured/game_sender
		elif [[ "$delivery_failed" == true ]]; then
			workload_status=delivery_failed; first_failure=gate/client_delivery
		else
			workload_status=delivery_complete
		fi
	fi
	cat >"$dir/run-status.json" <<EOF
	{"case":"$CASE_NAME","attempt":$CURRENT_ATTEMPT,"evidence_status":"$evidence_status","workload_status":"$workload_status","first_failure":"$first_failure","workload":"$WORKLOAD","interval":"$INTERVAL","connections":$CONNECTIONS,"load_exit_code":$load_exit,"collector_preflight":"$collector_preflight","warmup_started":$warmup_started,"measured_started":$measured_started,"baseline_ready":$baseline_ready,"terminal_gauges_zero":$terminal_gauges_zero,"async_terminal_counters_zero":$async_terminal_counters_zero,"warmup_cadence_failed":$warmup_cadence_failed,"warmup_sender_failed":$warmup_sender_failed,"warmup_delivery_failed":$warmup_delivery_failed,"cadence_failed":$cadence_failed,"sender_failed":$sender_failed,"delivery_failed":$delivery_failed,"warmup_attempted":$warmup_attempted,"warmup_success":$warmup_success,"warmup_partial":$warmup_partial,"warmup_error":$warmup_error,"warmup_missed":$warmup_missed,"measured_planned_ticks":$planned,"measured_attempted":$measured_attempted,"measured_success":$measured_success,"measured_partial":$measured_partial,"measured_error":$measured_error,"measured_missed":$measured_missed,"sender_success":$sender_success,"client_received":$received,"client_duplicate":$load_duplicate,"client_sequence_gap":$load_gap,"client_invalid":$load_invalid,"client_missing_per_reader_at_least":$load_missing,"client_reader_failures":$load_reader_failures,"gate_delivery_terminal_delta":$gate_terminal,"websocket_server_send_write_success_delta":$write_success,"profile_target":"$PROFILE_TARGET","evidence":{"metadata":"metadata.txt","phase_events":"phase-events.jsonl","commands":"commands.txt","metrics":"metrics/","os":"os/","redis_before":"redis-before.txt","redis_after":"redis-after.txt","redis_samples":"os/redis.tsv","pprof":"pprof/","warmup_result":"control/warmup-result.json","warmup_drained":"control/warmup-drained.json","binary_checksums":"checksums.txt","source_state":"source-state.txt"}}
EOF
	record_event "$dir/phase-events.jsonl" classified end "attempt-$CURRENT_ATTEMPT" ""
}

if [[ "${BASH_SOURCE[0]}" == "$0" ]]; then
	main "$@"
	exit $?
fi
