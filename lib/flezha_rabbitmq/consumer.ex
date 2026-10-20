defmodule FlezhaRabbitmq.Consumer do
  @moduledoc """
  The `rabbitmq_consumer` provider: one RabbitMQ queue read onto the relay's
  entry channel, each message one input.

  Each binding runs one `FlezhaRabbitmq.Consumer.Reader`, which consumes the
  queue with a prefetch window, hands the deliveries to the relay in
  batches and acknowledges each one only after the relay took it. Several
  relays bound to consumers of one queue are competing consumers, as
  RabbitMQ intends. One consumer per binding: not shareable, and refused on a
  partitioned relay, whose members a message's key decides between.
  Options, the input's metadata, acknowledgement, health and telemetry:
  `docs/public/plugins/rabbitmq.md` in the core.
  """

  alias Flezha.Plugin.ProviderBehaviour
  alias Flezha.Plugin.ProviderBehaviour.Options
  alias FlezhaRabbitmq.Connection
  alias FlezhaRabbitmq.Consumer.Reader

  use ProviderBehaviour,
    name: "rabbitmq_consumer",
    shareable: false,
    source: [partitioned: :refuse],
    telemetry: [reconnected: :event, unacked: :metric],
    options:
      Connection.rows() ++
        [
          queue: [type: :string, required: true, doc: "The queue to consume"],
          prefetch: [
            type: :int,
            default: 200,
            min: 1,
            max: 65_535,
            doc: "Deliveries the broker sends before the first of them is acknowledged"
          ],
          batch_ms: [
            type: :int,
            default: 10,
            min: 0,
            doc: "Longest a delivery waits for others before its batch goes to the relay"
          ],
          msg_id_header: [
            type: :string,
            doc: "A header whose value is the message's id, in place of its `message_id`"
          ],
          declare: [
            type: :bool,
            default: false,
            doc: "Declare `queue` durable when it does not exist"
          ],
          bind_exchange: [
            type: :string,
            doc: "With `declare`: an exchange the queue is bound to"
          ],
          routing_keys: [
            type: :names,
            doc:
              "With `bind_exchange`: the binding keys, comma-separated; one empty key when unset"
          ]
        ] ++
        Options.channel(:in, "message") ++
        Options.plugin_id("the id it was enabled under") ++ Options.failure_threshold(),
    worker: [module: Reader, restart: :temporary]

  @doc "Validate and prepare. Connects to nothing: the reader does, at activation."
  @impl true
  def init_provider(opts) do
    with :ok <- check_topology(opts), do: {:ok, Connection.prepare(opts)}
  end

  defp check_topology(%{queue: ""}), do: ProviderBehaviour.refuse_value(:queue, "")

  defp check_topology(%{bind_exchange: exchange, declare: false}) when is_binary(exchange),
    do: ProviderBehaviour.refuse_value(:bind_exchange, exchange)

  defp check_topology(%{routing_keys: keys, bind_exchange: nil}) when is_list(keys),
    do: ProviderBehaviour.refuse_value(:routing_keys, Enum.join(keys, ","))

  defp check_topology(_opts), do: :ok

  # Before the source's shared verdicts: a lost connection or channel names its
  # failure (a missing queue is a channel the broker closed with 404).
  defp verdict(%{failure: failure}, _opts) when failure != nil, do: {:unhealthy, failure}

  @doc "What `flz relay info` shows beneath the plugin: the queue and its verdict."
  @impl true
  def inspect_state(opts), do: %{queue: opts.queue, health: health(opts)}
end
