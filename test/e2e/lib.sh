# What run.sh uses: the plugin e2e library, and the broker helpers. Sourced, never
# run.
HERE=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
PLUGIN=$(cd "$HERE/../.." && pwd)
CORE=$(cd "$PLUGIN/../.." && pwd)
PACKAGE=flezha_rabbitmq
TMP_PREFIX=flzrabbit
# shellcheck source=../../../../test/helpers/plugin_e2e.sh
. "$CORE/test/helpers/plugin_e2e.sh"

# The dev broker's management API, as the host reaches it.
API=${RABBITMQ_E2E_API:-http://localhost:15672/api}

# `producer <name> <routing key> [X<key>=<value>...]`: a rabbitmq_producer plugin
# publishing to the default exchange, enabled.
producer() {
  local name=$1 key=$2
  shift 2
  flz plugin enable "$name" rabbitmq_producer "Xrouting_key=$key" "$@" >>"$TMP/out/enable-$name.log" 2>&1 ||
    { cat "$TMP/out/enable-$name.log"; exit 1; }
}

# `consumer <name> <queue> <relay>`: a rabbitmq_consumer plugin reading
# `queue` onto the relay's entry channel `in`.
consumer() {
  flz plugin enable "$1" rabbitmq_consumer "Xqueue=$2" Xchannel=in >>"$TMP/out/enable-$1.log" 2>&1 &&
    flz plugin bind "$1" --relay_id "$3" --hook=on_terminate >>"$TMP/out/bind-$1.log" 2>&1 ||
    { cat "$TMP/out/enable-$1.log" "$TMP/out/bind-$1.log"; exit 1; }
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
