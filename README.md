# flezha_rabbitmq

The RabbitMQ plugin for Flezha: a plugin package, `flezha_rabbitmq`, with two providers.
`rabbitmq_consumer` consumes one queue with a prefetch window and hands its messages to the
relay in batches, acknowledging each one only after the relay took it. `rabbitmq_producer`
publishes the payloads passing through a relay to an exchange: every relay bound to one
plugin on a controller casts its payloads to one worker, which publishes them on a channel in
confirm mode and confirms, retries or parks a delivery-guaranteed output. Both are built on the
`amqp` client and reconnect after the broker goes away.

The contracts (options, acknowledgement, message ids, routing, confirms, health, telemetry) are
the core's page `docs/plugins/rabbitmq.md`; this README covers the repository.

## Layout

```
lib/flezha_rabbitmq/connection.ex          the connection options, password, open, failures, backoff
lib/flezha_rabbitmq/consumer.ex            the consumer provider: options, refusals, health
lib/flezha_rabbitmq/consumer/reader.ex     one reader per binding: deliveries, batches, acks
lib/flezha_rabbitmq/producer.ex            the producer provider: options, the hook, health
lib/flezha_rabbitmq/producer/publisher.ex  one publisher per plugin: queue, confirms, retries, parks
test/flezha_rabbitmq/                      unit tests against a bare channel, the integration
                                           test against a broker, the catalogue and docs checks
test/support/                              the stand-ins for a controller and a channel
test/e2e/                                  the e2e runner (an Elixir SDK runner) and run.sh
```

## Building and testing

The project depends on the Flezha core as a path dependency: it is the core's submodule at
`plugins/flezha_rabbitmq_plugin`, so the core is `../..`. Nothing is published before go-live.
`mix.lock` pins every library the core also ships to the core's version, which the packaging
task checks.

```
mix deps.get
RUSTLER_SKIP_COMPILATION=true mix compile --warnings-as-errors
RUSTLER_SKIP_COMPILATION=true mix test
RUSTLER_SKIP_COMPILATION=true mix format --check-formatted
```

`mix test` runs with `--no-start`: no controller boots, and the tests start the providers'
worker families and stand in for the broker's channel and the relay. The catalogue test
fails when a code the providers report is missing from the core's `flz_transport/errors.json`,
and the docs test when an option table of `docs/plugins/rabbitmq.md` differs from its
provider's declaration, printing the table to put there.

The integration tests need the core's dev broker on `localhost:5672`, its management API on
15672:

```
docker compose -f ../../docker/dev/docker-compose.yml up -d rabbitmq
RABBITMQ_INTEGRATION=1 RUSTLER_SKIP_COMPILATION=true mix test --include rabbitmq_integration
```

One of them stops and starts the broker (`docker compose stop rabbitmq`, then `start`).

## Packaging

On the host, with the same OTP and Elixir as the controller release:

```
MIX_ENV=prod mix deps.get
MIX_ENV=prod RUSTLER_SKIP_COMPILATION=true mix flezha.plugin.package
```

The archive is `_build/flezha_rabbitmq-0.1.0.fpz`. It bundles `amqp`, `amqp_client`,
`rabbit_common`, `credentials_obfuscation`, `thoas`, `ranch` and OTP's `syntax_tools`, none of
them native code; `recon`, which `rabbit_common` names, is the release's own at the core's
version.

## Installing into a local release

```
flz plugin install _build/flezha_rabbitmq-0.1.0.fpz
flz controller stop && flz controller start
flz plugin list --installed
```

Then enable `rabbitmq_consumer` or `rabbitmq_producer` on a relay as its page shows.

## The end-to-end run

`test/e2e/run.sh` checks the package against a controller release built from the core
(`MIX_ENV=prod mix release flezha --path /tmp/flzrel` there, then
`FLEZHA_RELEASE=/tmp/flzrel test/e2e/run.sh`; with no `FLEZHA_RELEASE` it refuses to run) and
the dev broker. It first starts the controller without the package and checks the provider
is unknown, then packages the plugin, installs it under a temporary plugin root, starts the
controller and the e2e runner, and checks: the client's applications started; one
relay publishes 100 outputs to a queue and a second consumes them all; an effectively-once
relay consuming 10 000 messages loses none across a killed and restarted controller; two
relays bound to one producer publish through one broker connection; an unroutable
guaranteed output becomes a dead letter; and 1 000 outputs of an effectively-once relay all
arrive across `docker compose restart rabbitmq`, every copy of one under one `message_id`. It
prints PASS or FAIL per check, keeps its logs when one fails, and exits non-zero on any
failure.
