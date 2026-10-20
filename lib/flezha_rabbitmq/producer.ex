defmodule FlezhaRabbitmq.Producer do
  @moduledoc """
  The `rabbitmq_producer` provider: an `:on_input` / `:on_output` output plugin that
  publishes each payload to a RabbitMQ exchange.

  Every relay bound to one plugin on a controller casts its payloads to ONE
  worker (`FlezhaRabbitmq.Producer.Publisher`), which owns the plugin's one
  connection, its queue, its confirm-mode channel and the per-output headers.
  The dispatch holds a delivery-guaranteed output for the worker to confirm or
  park, and casts; nothing else runs per payload. Options, routing, headers,
  message ids, confirms and telemetry: `docs/public/plugins/rabbitmq.md` in the core.
  """

  alias Flezha.Plugin.Extension.Ctx
  alias Flezha.Plugin.Provider.WorkerSup
  alias Flezha.Plugin.ProviderBehaviour
  alias Flezha.Plugin.ProviderBehaviour.Options
  alias Flezha.Process.Backlog
  alias Flezha.State.Outbox
  alias FlezhaRabbitmq.Connection
  alias FlezhaRabbitmq.Producer.Publisher

  use ProviderBehaviour,
    name: "rabbitmq_producer",
    shareable: true,
    telemetry: [reconnected: :event],
    options:
      Connection.rows() ++
        [
          exchange: [
            type: :string,
            default: "",
            doc: "The exchange each payload is published to; the default exchange when empty"
          ],
          routing_key: [
            type: :string,
            default: "",
            doc: "The routing key of every payload, unless `routing_key_from` names another"
          ],
          routing_key_from: [
            type: :enum,
            values: ["flz_key"],
            doc: "`flz_key`: each output's own `flz.key` is its routing key"
          ],
          persistent: [
            type: :bool,
            default: true,
            doc: "Delivery mode 2: the broker writes it to disk"
          ],
          mandatory: [
            type: :bool,
            default: false,
            doc: "The broker returns a payload no queue is bound for; a guaranteed one is parked"
          ],
          headers: [
            type: :headers,
            default: %{},
            doc: "Static message headers, a JSON object; names starting `flz.` are refused"
          ],
          content_type: [type: :string, doc: "The message's `content_type` property"],
          declare_exchange: [
            type: :bool,
            default: false,
            doc: "Declare `exchange` durable when it does not exist"
          ],
          exchange_type: [
            type: :enum,
            values: ["direct", "fanout", "topic", "headers"],
            default: "direct",
            doc: "With `declare_exchange`: the exchange's type"
          ],
          retries: [
            type: :int,
            default: 5,
            min: 0,
            max: 100,
            doc:
              "Publishes again of a guaranteed output the broker refused or a lost connection left unconfirmed"
          ],
          # The worker this plugin's payloads are cast to, named once here so the
          # dispatch builds nothing per payload.
          publisher: [type: :any, internal: true, transient: true]
        ] ++
        Options.channel(:out, "payload") ++
        Options.plugin_id("the id it was enabled under") ++
        Options.retry(retry_max_backoff_ms: 10_000) ++
        Options.queue(),
    worker: [module: Publisher, restart: :transient]

  @impl true
  def init_provider(opts) do
    opts = %{opts | plugin_id: opts.plugin_id || opts[:source_plugin_id]}

    with :ok <- refuse_reserved(opts.headers),
         :ok <- check_exchange(opts) do
      {:ok,
       opts
       |> Connection.prepare()
       |> Map.put(:publisher, WorkerSup.worker(__MODULE__, key(opts)))}
    end
  end

  # A static header may not pass for one the producer writes from the output.
  defp refuse_reserved(headers) do
    case Enum.find(Map.keys(headers), &String.starts_with?(&1, "flz.")) do
      nil -> :ok
      name -> {:error, {:headers_invalid, "rabbitmq_producer", :headers, name}}
    end
  end

  # The default exchange always exists and cannot be declared.
  defp check_exchange(%{declare_exchange: true, exchange: ""}),
    do: ProviderBehaviour.refuse_value(:declare_exchange, true)

  defp check_exchange(_opts), do: :ok

  @doc "One publisher per `{namespace, plugin_id}`: the relays bound to the plugin share it."
  @impl true
  def worker_specs(opts), do: [{key(opts), Publisher.args(opts)}]

  @impl true
  def on_input(%Ctx.IO{} = ctx, opts), do: publish(ctx, opts)
  @impl true
  def on_output(%Ctx.IO{} = ctx, opts), do: publish(ctx, opts)

  # The hold and the dispatch, nothing else: the worker builds the message.
  defp publish(%Ctx.IO{message: message, metadata: metadata} = ctx, opts) do
    if ProviderBehaviour.in_channel?(metadata, opts.filter_channel) do
      Outbox.hold(metadata)
      Backlog.dispatch(opts.publisher, {:publish, message, metadata})
    end

    {:ok, ctx}
  end

  @doc """
  Healthy while the publisher's connection and channel are up; otherwise the
  failure that took them, until they are back.
  """
  @impl true
  @spec health(map()) :: ProviderBehaviour.health_verdict()
  def health(opts) do
    ProviderBehaviour.pid_health(
      WorkerSup.worker_pid(__MODULE__, key(opts)),
      [absent: :not_running, dead: :worker_dead],
      &Publisher.probe_verdict(&1, fn
        %{failure: nil} -> :healthy
        %{failure: failure} -> {:unhealthy, failure}
      end)
    )
  end

  defp key(opts), do: {opts[:source_namespace], opts.plugin_id}
end
