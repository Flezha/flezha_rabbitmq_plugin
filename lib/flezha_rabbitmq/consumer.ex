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

  alias Flezha.Plugin.Extension.Ctx
  alias Flezha.Plugin.Provider.WorkerSup
  alias Flezha.Plugin.ProviderBehaviour
  alias FlezhaRabbitmq.Connection
  alias FlezhaRabbitmq.Consumer.Reader

  use ProviderBehaviour,
    name: "rabbitmq_consumer",
    feeds: true,
    shareable: false,
    telemetry: [reconnected: :event, unacked: :metric],
    options:
      Connection.rows() ++
        [
          queue: [type: :string, required: true, doc: "The queue to consume"],
          channel: [
            type: :names,
            doc:
              "The channel this plugin is attached to: the entry channel every message is injected on, and one name. Required, unless `channel_in` names it"
          ],
          channel_in: [
            type: :names,
            doc:
              "Entry channel every message is injected on, and one name; the explicit spelling of `channel`. Required, unless `channel` names it"
          ],
          plugin_id: [type: :string, doc: "Plugin id; defaults to the id it was enabled under"],
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
          ],
          failure_threshold: [
            type: :int,
            default: 3,
            min: 1,
            doc: "Refused batches in a row before the plugin reports unhealthy"
          ]
        ],
    worker: [module: Reader, restart: :temporary]

  @doc "Validate and prepare. Connects to nothing: the reader does, at activation."
  @impl true
  def init_provider(opts) do
    with :ok <- ProviderBehaviour.require_entry_channel(opts, __MODULE__),
         :ok <- ProviderBehaviour.refuse_partitioned(opts, __MODULE__),
         :ok <- check_topology(opts) do
      {:ok,
       opts
       |> Connection.prepare()
       |> Map.put(:plugin_id, opts.plugin_id || opts[:source_plugin_id])}
    end
  end

  defp check_topology(%{queue: ""}), do: ProviderBehaviour.refuse_value(:queue, "")

  defp check_topology(%{bind_exchange: exchange, declare: false}) when is_binary(exchange),
    do: ProviderBehaviour.refuse_value(:bind_exchange, exchange)

  defp check_topology(%{routing_keys: keys, bind_exchange: nil}) when is_list(keys),
    do: ProviderBehaviour.refuse_value(:routing_keys, Enum.join(keys, ","))

  defp check_topology(_opts), do: :ok

  @doc "One reader per binding, started with `Consumer.Reader.args/1`."
  @impl true
  def worker_specs(opts), do: [{WorkerSup.key(opts), Reader.args(opts)}]

  @doc """
  Teardown is the host's. This callback exists so the provider can be hooked at
  all: every hook requires the matching export.
  """
  @impl true
  def on_terminate(%Ctx.Terminate{}, _opts), do: :ok

  @doc """
  Health verdict from the reader: a lost connection or channel names its
  failure (a missing queue is a channel the broker closed with 404), and
  `failure_threshold` refused batches in a row name the refusal.
  """
  @impl true
  @spec health(map()) :: ProviderBehaviour.health_verdict()
  def health(opts) do
    ProviderBehaviour.worker_health(
      opts,
      [
        absent: :no_reader,
        dead: :reader_dead,
        budget_ms: 2 * ProviderBehaviour.probe_budget_ms()
      ],
      fn pid ->
        case Reader.probe(pid) do
          {:ok, facts} -> verdict(facts, opts)
          :timeout -> {:fail_open, :probe_timeout}
        end
      end
    )
  end

  defp verdict(%{failure: failure}, _opts) when failure != nil, do: {:unhealthy, failure}

  defp verdict(%{consecutive_failures: n, last_error: error}, opts)
       when n >= opts.failure_threshold,
       do: {:unhealthy, {:read_failed, error}}

  defp verdict(_facts, _opts), do: :healthy

  @doc "What `flz relay info` shows beneath the plugin: the queue and its verdict."
  @impl true
  def inspect_state(opts), do: %{queue: opts.queue, health: health(opts)}
end
