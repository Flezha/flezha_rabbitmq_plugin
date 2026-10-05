#!/usr/bin/env bash
# The RabbitMQ package's consumer and producer against a real controller release
# and the core's dev broker: it packages the plugin, installs it into a release
# under a temporary plugin root, starts the controller and the e2e runner's
# runner, and checks what a user would see. PASS or FAIL per check; exits non-zero
# on any failure.
#
#   docker compose -f ../../docker/dev/docker-compose.yml up -d rabbitmq   # in the core
#   MIX_ENV=prod mix release flezha --path /tmp/flzrel                     # in the core
#   FLEZHA_RELEASE=/tmp/flzrel test/e2e/run.sh
set -uo pipefail
. "$(dirname "$0")/lib.sh"

cleanup() {
  local code=$?
  "${COMPOSE[@]}" start rabbitmq >/dev/null 2>&1
  stop_runner runner
  stop_controller
  if [ "$FAILED" -eq 0 ] && [ "$code" -eq 0 ]; then rm -rf "$TMP"; else say "logs kept in $TMP/out"; fi
}
trap cleanup EXIT

controller_env "$TMP/c1" "flzrabbit$RUN" "$REL"
say_release "$REL"
until_ok 60 "api $API/overview" || { say "the dev broker does not answer"; exit 1; }

# ---------- without the package ----------

check_z() {
  local out
  out=$(flz plugin enable nope rabbitmq_producer Xrouting_key=q 2>&1)
  if [ $? -ne 0 ] && grep -q "rabbitmq_producer" <<<"$out" && ! ls "$REL/lib" | grep -q '^amqp'; then
    pass z "without the package the provider is unknown and the release carries no AMQP client: $(grep -m1 -i error <<<"$out")"
  else
    fail z "enable without the package answered: $out"
  fi
}

say "starting the controller without the package"
(cd "$FLEZHA_CTL_DIR" && nohup flzvm start </dev/null >>"$TMP/out/controller-bare.log" 2>&1 &)
until_ok 90 "flz controller status" || { say "the controller did not come up"; exit 1; }
check_z
stop_controller

# ---------- with it ----------

package_plugin
install_plugin
say "starting the controller with the package"
start_controller
build_runner
start_runner runner default

check_a() {
  local apps
  apps=$(flzvm rpc 'IO.inspect(for a <- [:amqp, :amqp_client, :rabbit_common, :credentials_obfuscation], do: List.keymember?(Application.started_applications(), a, 0))' 2>&1)
  if [ "$apps" = "[true, true, true, true]" ] && ! ls "$REL/lib" | grep -q '^rabbit_common-'; then
    pass a "the package loaded and started its client's applications, which the release does not ship"
  else
    fail a "the client's applications at boot: $apps"
  fi
}

check_b() {
  local in=e2e_rt_in_$RUN out=e2e_rt_out_$RUN
  declare_queue "$in"
  declare_queue "$out"
  start_relay echo rt_writer
  start_relay echo rt_reader
  producer rt_writer_out "$in"
  bind rt_writer_out rt_writer
  producer rt_reader_out "$out"
  bind rt_reader_out rt_reader
  consumer rt_reader_in "$in" rt_reader
  send_all rt_writer 1 100 rt
  if await_count "$out" 100; then
    pass b "one relay published 100 outputs to $in, a second consumed them all: $(queue_messages "$out" | cut -d' ' -f2 | sort -u | wc -l) in $out"
  else
    fail b "$out holds $(queue_messages "$out" | cut -d' ' -f2 | sort -u | wc -l) of 100"
  fi
}

check_c() {
  local in=e2e_kill_in_$RUN out=e2e_kill_out_$RUN total
  declare_queue "$in"
  declare_queue "$out"
  feed "$in" 1 10000 c
  start_relay echo_once kill_echo
  producer kill_out "$out"
  bind kill_out kill_echo
  consumer kill_in "$in" kill_echo
  await_count "$out" 1 || { fail c "nothing reached $out"; return; }
  say "killing the controller mid-way"
  pkill -KILL -f "beam.smp.* $FLEZHA_NODE_NAME@"
  start_controller
  if await_count "$out" 10000 300 && until_ok 60 "[ \"\$(queue_depth $in)\" = 0 ]"; then
    total=$(queue_messages "$out" | wc -l)
    pass c "an effectively-once relay lost nothing across a controller kill: 10000 distinct of $total messages in $out, $in empty"
  else
    fail c "$out holds $(queue_messages "$out" | cut -d' ' -f2 | sort -u | wc -l) of 10000; $in holds $(queue_depth "$in")"
  fi
}

check_d() {
  local out=e2e_restart_$RUN mixed copies
  declare_queue "$out"
  start_relay echo_once restarted
  producer restart_out "$out"
  bind restart_out restarted
  send_all restarted 1 500 d
  say "restarting the broker"
  "${COMPOSE[@]}" restart rabbitmq >/dev/null 2>&1
  until_ok 60 "api $API/overview" || { fail d "the broker did not come back"; return; }
  send_all restarted 501 1000 d
  if await_count "$out" 1000 180; then
    copies=$(queue_messages "$out" | wc -l)
    mixed=$(queue_messages "$out" | sort -u | cut -d' ' -f2 | uniq -d | wc -l)
    if [ "$mixed" = 0 ] && [ -z "$(flz dlq list restarted 2>/dev/null | grep -i rabbitmq)" ]; then
      pass d "1000 outputs across a broker restart in $copies messages, every copy of one output under one message_id"
    else
      fail d "$mixed outputs carry more than one message_id; dead letters: $(flz dlq list restarted 2>&1 | head -3)"
    fi
  else
    fail d "$out holds $(queue_messages "$out" | cut -d' ' -f2 | sort -u | wc -l) of 1000 after the restart"
  fi
}

# The broker's connections named for the plugin `shared_out`.
shared_connections() {
  api "$API/connections" | python3 -c "import json, sys; print(sum(1 for c in json.load(sys.stdin) if c.get('client_properties', {}).get('connection_name', '').startswith('flezha shared_out ')))"
}

check_e() {
  local out=e2e_shared_$RUN
  declare_queue "$out"
  start_relay echo share_a
  start_relay echo share_b
  producer shared_out "$out"
  bind shared_out share_a
  bind shared_out share_b
  send_all share_a 1 50 a
  send_all share_b 1 50 b
  # The management API lists a connection once its statistics are next collected.
  if await_count "$out" 100 && until_ok 30 "[ \"\$(shared_connections)\" = 1 ]"; then
    pass e "two relays bound to one producer publish through one broker connection: 100 messages"
  else
    fail e "$out holds $(queue_messages "$out" | cut -d' ' -f2 | sort -u | wc -l) of 100, $(shared_connections) connection(s)"
  fi
}

check_f() {
  local listed
  start_relay echo_once unroutable
  producer void_out nowhere Xexchange=amq.direct Xmandatory=true
  bind void_out unroutable
  send_all unroutable 1 1 f
  if until_ok 30 "flz dlq list unroutable 2>&1 | grep -q unroutable"; then
    listed=$(flz dlq list unroutable 2>&1 | grep -m1 unroutable)
    pass f "an unroutable guaranteed output is a dead letter: $listed"
  else
    fail f "no dead letter: $(flz dlq list unroutable 2>&1 | tail -3)"
  fi
}

check_a
check_b
check_c
check_e
check_f
check_d

[ "$FAILED" -eq 0 ] && say "all checks passed"
exit "$FAILED"
