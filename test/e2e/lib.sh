# What run.sh and memory_drift.sh share: the paths, a controller's environment, the
# package, the runner and the broker helpers. Sourced, never run.
HERE=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
PLUGIN=$(cd "$HERE/../.." && pwd)
CORE=$(cd "$PLUGIN/../.." && pwd)
# Never a default: a release someone keeps for their own use must not be run from here.
REL=${FLEZHA_RELEASE:?set FLEZHA_RELEASE to a controller release built for this run, e.g. mix release flezha --path /tmp/flzrel}
COMPOSE=(docker compose -f "$CORE/docker/dev/docker-compose.yml")
# The dev broker's management API, as the host reaches it.
API=${RABBITMQ_E2E_API:-http://localhost:15672/api}
# A Unix socket path must fit in 108 bytes, so the directory stays short.
TMP=$(mktemp -d /tmp/flzrabbit.XXXX)
RUN=$(date +%s)
FAILED=0
mkdir -p "$TMP/out"

free_port() { python3 -c 'import socket; s = socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1])'; }

say() { echo "[e2e] $*"; }
pass() { echo "PASS ($1) $2"; }
fail() { echo "FAIL ($1) $2"; FAILED=1; }

# Poll `command` every 0.5 s for up to `seconds`.
until_ok() {
  local n=$(($1 * 2))
  shift
  for _ in $(seq "$n"); do eval "$*" >/dev/null 2>&1 && return 0; sleep 0.5; done
  return 1
}

# `controller_env <dir> <node> <release>`: the environment `flz` and `flzvm` of
# one controller run in, its node name, state, plugin root, sockets and ports all
# its own under <dir>; authorization off.
controller_env() {
  export PATH=$3/bin:$PATH
  export FLEZHA_CTL_DIR=$1/ctl
  export FLEZHA_SOCKET_FILES_PATH=$1/s
  export FLEZHA_NODE_NAME=$2
  export FLEZHA_STATE_DIR=$1/state
  export FLEZHA_PLUGIN_ROOT=$1/plugins
  export FLEZHA_AUTHZ_ENABLED=false
  FLEZHA_HEALTHZ_PORT=$(free_port) FLEZHA_PROMETHEUS_PORT=$(free_port) FLEZHA_TESTNET_WS_PORT=$(free_port)
  FLEZHA_QUIC_LISTEN_ADDRESS=127.0.0.1:$(free_port)
  export FLEZHA_HEALTHZ_PORT FLEZHA_PROMETHEUS_PORT FLEZHA_TESTNET_WS_PORT FLEZHA_QUIC_LISTEN_ADDRESS
  mkdir -p "$1/ctl" "$1/s"
}

# The release a run uses, and when it was built: it must be built from the core
# checkout as it is now, or every check reads an older controller.
say_release() {
  say "release $1, built $(date -r "$1/bin/flzvm" '+%Y-%m-%d %H:%M:%S')"
}

package_plugin() {
  say "packaging the plugin"
  (cd "$PLUGIN" && MIX_ENV=prod RUSTLER_SKIP_COMPILATION=true mix flezha.plugin.package --output "$TMP") \
    >"$TMP/out/package.log" 2>&1 || { cat "$TMP/out/package.log"; exit 1; }
}

install_plugin() {
  (cd "$FLEZHA_CTL_DIR" && flz plugin install "$TMP/flezha_rabbitmq-0.1.0.fpz") >>"$TMP/out/install.log" 2>&1 ||
    { cat "$TMP/out/install.log"; exit 1; }
}

start_controller() {
  (cd "$FLEZHA_CTL_DIR" && nohup flzvm start </dev/null >>"$TMP/out/controller-$FLEZHA_NODE_NAME.log" 2>&1 &)
  until_ok 90 "flz controller status" || { say "controller $FLEZHA_NODE_NAME did not come up"; exit 1; }
  until_ok 30 "flz plugin list --installed | grep -q ' loaded'" || { say "the package did not load"; exit 1; }
}

stop_controller() {
  (cd "$FLEZHA_CTL_DIR" && flzvm stop >/dev/null 2>&1)
  until_ok 30 "! pgrep -f 'beam.smp.* $FLEZHA_NODE_NAME@'" || pkill -KILL -f "beam.smp.* $FLEZHA_NODE_NAME@"
}

build_runner() {
  (cd "$HERE/runner" && mix deps.get && mix compile) >"$TMP/out/runner-build.log" 2>&1 ||
    { cat "$TMP/out/runner-build.log"; exit 1; }
}

# `start_runner <name> <namespace>`: the e2e runner as a runner of the
# controller whose environment is set. Its standard input stays open until
# `stop_runner`, whose closing it reads as its stop.
start_runner() {
  mkfifo "$TMP/$1.in"
  (sleep infinity >"$TMP/$1.in" & echo $! >"$TMP/$1.hold")
  (cd "$HERE/runner" && FLEZHA_RUNNER_ID_PATH="$TMP/runner-id-$1" \
    setsid mix run --no-compile --no-deps-check --no-halt -- "$2" <"$TMP/$1.in" >"$TMP/out/$1.log" 2>&1 &
    echo $! >"$TMP/$1.pid")
}

stop_runner() {
  [ -f "$TMP/$1.pid" ] || return 0
  kill "$(cat "$TMP/$1.hold")" 2>/dev/null
  local pid
  pid=$(cat "$TMP/$1.pid")
  until_ok 10 "! kill -0 $pid" || kill -KILL -- "-$pid" 2>/dev/null
}

# `start_relay <factory> <id>`, retried until the runner serves the factory.
start_relay() {
  for _ in $(seq 60); do
    flz relay start "$1" --id "$2" >>"$TMP/out/start-$2.log" 2>&1 && return 0
    sleep 1
  done
  say "$2 did not start: $(tail -3 "$TMP/out/start-$2.log")"
  exit 1
}

# `producer <name> <routing key> [X<key>=<value>...]`: a rabbitmq_producer plugin
# publishing to the default exchange, enabled.
producer() {
  local name=$1 key=$2
  shift 2
  flz plugin enable "$name" rabbitmq_producer "Xrouting_key=$key" "$@" >>"$TMP/out/enable-$name.log" 2>&1 ||
    { cat "$TMP/out/enable-$name.log"; exit 1; }
}

# `bind <plugin> <relay>`: the relay's outputs go to the plugin.
bind() {
  flz plugin bind "$1" --relay_id "$2" --hook=on_output >>"$TMP/out/bind-$1.log" 2>&1 ||
    { cat "$TMP/out/bind-$1.log"; exit 1; }
}

# `consumer <name> <queue> <relay>`: a rabbitmq_consumer plugin reading
# `queue` onto the relay's entry channel `in`.
consumer() {
  flz plugin enable "$1" rabbitmq_consumer "Xqueue=$2" Xchannel=in >>"$TMP/out/enable-$1.log" 2>&1 &&
    flz plugin bind "$1" --relay_id "$3" --hook=on_terminate >>"$TMP/out/bind-$1.log" 2>&1 ||
    { cat "$TMP/out/enable-$1.log" "$TMP/out/bind-$1.log"; exit 1; }
}

# `send_all <relay> <from> <to> [prefix]`: one input per number, all in one
# confirming batch (one `flz` call per input costs a round trip each).
send_all() {
  local accepted
  accepted=$(flzvm rpc "
    inputs = for i <- $2..$3, do: {\"${4:-m}-#{i}\", %{}}
    verdicts = Flezha.Service.RelayManager.send_io_confirming_many(\"$1\", \"default\", inputs)
    IO.puts(Enum.count(verdicts, &(&1 == :ok)))" 2>&1 | tail -1)
  [ "$accepted" = $(($3 - $2 + 1)) ] || { say "sending $2..$3 to $1: $accepted accepted"; return 1; }
}

api() { curl -sf -u guest:guest -H 'content-type: application/json' "$@"; }

# `declare <queue>`: a durable queue, as RabbitMQ 4 wants every shared one.
declare_queue() { api -X PUT "$API/queues/%2F/$1" -d '{"durable":true}' >/dev/null; }

# `feed <queue> <from> <to> <prefix>`: one persistent message per number, each its
# own `message_id`, published from inside the controller with the package's client.
feed() {
  flzvm rpc "
    {:ok, conn} = AMQP.Connection.open()
    {:ok, chan} = AMQP.Channel.open(conn)
    for i <- $2..$3, do: :ok = AMQP.Basic.publish(chan, \"\", \"$1\", \"$4-#{i}\", message_id: \"$4-#{i}\", persistent: true)
    AMQP.Connection.close(conn)" >/dev/null 2>&1
}

# Every message of `queue` as `<message_id> <payload>`, one per line, read
# through the management API and left in the queue.
queue_messages() {
  api -X POST "$API/queues/%2F/$1/get" \
    -d '{"count":100000,"ackmode":"ack_requeue_true","encoding":"auto"}' |
    python3 -c 'import json, sys; [print(m["properties"].get("message_id", "-"), m["payload"]) for m in json.load(sys.stdin)]'
}

# The messages `queue` holds, ready and unacknowledged.
queue_depth() { api "$API/queues/%2F/$1" | python3 -c 'import json, sys; print(json.load(sys.stdin)["messages"])'; }

# `await_count <queue> <n> [seconds]`: wait until the queue holds `n` distinct payloads.
await_count() {
  until_ok "${3:-60}" "[ \"\$(queue_messages $1 | cut -d' ' -f2 | sort -u | wc -l)\" -ge $2 ]"
}
