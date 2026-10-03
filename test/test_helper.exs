# The suite runs with `--no-start`: no controller boots. What a controller would
# have running for the providers is started here: each provider family's
# supervisor, which the tests start workers under, and the client library.
alias Flezha.Plugin.Provider.WorkerSup

{:ok, _} = Application.ensure_all_started([:telemetry, :amqp, :mimic])

for provider <- [FlezhaRabbitmq.Producer, FlezhaRabbitmq.Consumer] do
  {:ok, family} = WorkerSup.start_link(WorkerSup.family(provider))
  Process.unlink(family)
end

for module <- [AMQP.Basic, AMQP.Channel, AMQP.Confirm, AMQP.Connection, Flezha.State.Outbox],
    do: Mimic.copy(module)

# The core's own suite runs at this level: a log line a test provokes on purpose
# is captured and asserted where it is.
Logger.configure(level: :error)

# A broker on the dev stack is needed by the integration test alone. What a
# worker does in answer to a test's message is awaited, not timed: a loaded host
# can take longer than ExUnit's default 100 ms.
ExUnit.start(exclude: [:rabbitmq_integration], assert_receive_timeout: 2_000)
