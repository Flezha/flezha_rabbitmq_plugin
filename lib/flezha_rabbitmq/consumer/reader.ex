defmodule FlezhaRabbitmq.Consumer.Reader do
  @moduledoc """
  The reading half of `FlezhaRabbitmq.Consumer`: one process per binding,
  owning one connection and channel, the deliveries not yet settled and the
  health facts.

  It consumes `queue` with manual acknowledgement and a prefetch of
  `prefetch`. Deliveries collect until half the prefetch (so the broker keeps
  sending while a batch is injected) or `batch_ms`, then go to the relay
  with `Host.inject_many/4` in `:verdict` mode. The longest prefix the relay
  took is acknowledged with one `basic.ack`, and the rest are returned to the
  queue with one `basic.nack`. A returned delivery comes straight back, so after
  a refusal the reader waits along its backoff curve before it injects again.

  A delivery tag belongs to its channel: when the connection or the channel is
  lost the deliveries held are forgotten, never acknowledged, the broker
  delivers them again, and the reader reconnects along its curve. A delivery
  that reaches the reader after that, from the consumer it had, is dropped by
  its consumer tag.

  Log lines, telemetry and verdicts name the queue, a count, the broker's
  reply code and text or a reason, never a message or the connection's
  options.
  """

  alias Flezha.Plugin.Host
  alias Flezha.Plugin.Provider.Worker
  alias Flezha.Plugin.ProviderBehaviour
  alias FlezhaRabbitmq.Connection

  # The facts the provider's `health/1` judges: the connection's failure, the
  # run of refused batches and its last error. A probe that times out is also
  # what a reader waiting on the relay looks like.
  use Worker, facts: [:failure, :consecutive_failures, :last_error]
  use Flezha.Process.Timer, keys: [:connect, :batch, :resume]

  require Logger

  @provider :rabbitmq_consumer

  @doc """
  The reader's start arguments, from the plugin's prepared options. The
  password reaches it only inside the connection's closure.
  """
  @spec args(map()) :: map()
  def args(opts) do
    opts
    |> Map.take([
      :plugin_id,
      :overwatcher_pid,
      :entry_channel,
      :queue,
      :prefetch,
      :batch_ms,
      :msg_id_header,
      :declare,
      :bind_exchange,
      :routing_keys
    ])
    |> Map.put(:link_args, Connection.args(opts))
  end

  @impl true
  def init(args) do
    # A stop closes the connection, which only a trapped shutdown reaches.
    Process.flag(:trap_exit, true)

    state =
      args
      |> Map.drop([:name])
      |> Map.merge(Connection.initial(args.link_args))
      |> Map.merge(Worker.fields(args))
      |> Map.merge(%{consumer: nil, held: []})

    # Scheduled, never run here: activation is synchronous in the host.
    {:ok, arm_timer(state, :connect, 0, :connect)}
  end

  @impl true
  def handle_info(:connect, state), do: {:noreply, state |> timer_fired(:connect) |> connect()}

  def handle_info(
        {:basic_deliver, payload, %{consumer_tag: tag} = meta},
        %{consumer: tag} = state
      ),
      do: {:noreply, held(state, payload, meta)}

  def handle_info(:batch, state), do: {:noreply, state |> timer_fired(:batch) |> flush()}

  def handle_info(:resume, state),
    do: {:noreply, state |> timer_fired(:resume) |> flush()}

  # The broker cancelled the consumer: the queue was deleted.
  def handle_info({:basic_cancel, %{consumer_tag: tag}}, %{consumer: tag} = state),
    do:
      {:noreply,
       lose(state, {:rabbitmq_channel_closed, %{code: 404, text: "consumer cancelled"}})}

  def handle_info({:DOWN, ref, :process, _pid, reason}, state) do
    case Connection.down(state.link, ref, reason, state.link_args) do
      {:lost, failure} -> {:noreply, lose(state, failure)}
      :other -> {:noreply, state}
    end
  end

  def handle_info(_other, state), do: {:noreply, state}

  @impl true
  def terminate(_reason, state) do
    Connection.close(state.link)
    :ok
  end

  # ---------- the connection ----------

  defp connect(state) do
    case Connection.open(state.link_args, &subscribe(&1, state)) do
      {:ok, link, consumer} ->
        if state.attempt > 0, do: emit(:reconnected, %{count: 1}, labels(state))
        %{state | link: link, consumer: consumer, failure: nil, attempt: 0}

      {:error, failure} ->
        retry(state, failure)
    end
  end

  # A missing queue is a channel the broker closes with 404, which the consume
  # call sees as its exit.
  defp subscribe(chan, state) do
    with :ok <- declare(chan, state),
         :ok <- AMQP.Basic.qos(chan, prefetch_count: state.prefetch),
         do: AMQP.Basic.consume(chan, state.queue)
  end

  defp declare(_chan, %{declare: false}), do: :ok

  defp declare(chan, state) do
    with {:ok, _info} <- AMQP.Queue.declare(chan, state.queue, durable: true) do
      bind(chan, state)
    end
  end

  defp bind(_chan, %{bind_exchange: nil}), do: :ok

  defp bind(chan, state) do
    Enum.reduce_while(state.routing_keys || [""], :ok, fn key, :ok ->
      case AMQP.Queue.bind(chan, state.queue, state.bind_exchange, routing_key: key) do
        :ok -> {:cont, :ok}
        error -> {:halt, error}
      end
    end)
  end

  defp lose(state, failure) do
    if state.failure == nil do
      Logger.warning(
        "[FlezhaRabbitmq.Consumer] queue=#{state.queue} lost its connection: " <>
          "#{inspect(failure)}; #{length(state.held)} deliveries return to the queue"
      )
    end

    Connection.close(state.link)
    state = %{state | consumer: nil, held: []}
    state |> cancel_timer(:batch) |> cancel_timer(:resume) |> retry(failure)
  end

  defp retry(state, failure) do
    {state, delay} = Connection.failed(state, failure, {@provider, state.plugin_id})
    arm_timer(state, :connect, delay, :connect)
  end

  # ---------- batching ----------

  defp held(state, payload, meta) do
    state = %{state | held: [{payload, meta} | state.held]}

    cond do
      length(state.held) >= batch_max(state) -> flush(state)
      timer_armed?(state, :batch) -> state
      true -> arm_timer(state, :batch, state.batch_ms, :batch)
    end
  end

  defp batch_max(state), do: max(div(state.prefetch, 2), 1)

  # Nothing is injected while a refusal's wait, the `:resume` timer, runs.
  defp flush(%{held: []} = state), do: state

  defp flush(state) do
    if timer_armed?(state, :resume) do
      state
    else
      state = cancel_timer(state, :batch)
      {batch, rest} = state.held |> Enum.reverse() |> Enum.split(batch_max(state))
      state |> Map.put(:held, Enum.reverse(rest)) |> deliver(batch) |> flush()
    end
  end

  # ---------- delivery ----------

  defp deliver(state, batch) do
    verdicts =
      Host.inject_many(
        state.overwatcher_pid,
        Enum.map(batch, fn {payload, meta} -> input(payload, meta, state) end),
        :verdict,
        :infinity
      )

    {taken, refused} = split(batch, verdicts)
    if taken != [], do: settle(state.link, &AMQP.Basic.ack/3, taken, [])

    state =
      case refused do
        [] ->
          emit(:shipped, %{count: length(taken)}, labels(state))
          Worker.recovered(state)

        # The refused deliveries and every one held after them go back, in
        # their order, before anything newer is injected. The refusal is
        # logged first, so it is on record even when the return blocks.
        [{_delivery, {:error, reason}} | _] ->
          returned = Enum.map(refused, &elem(&1, 0)) ++ Enum.reverse(state.held)

          state =
            refused(%{state | held: []}, length(returned), "ingest refused: #{inspect(reason)}")

          settle(state.link, &AMQP.Basic.nack/3, returned, requeue: true)
          state
      end

    emit(:unacked, %{count: length(state.held)}, labels(state))
    state
  end

  @doc """
  The deliveries the relay took, the longest prefix whose verdict is `:ok`,
  and the rest, each with its verdict.
  """
  @spec split([term()], [term()]) :: {[term()], [{term(), term()}]}
  def split(batch, verdicts) do
    {taken, refused} = batch |> Enum.zip(verdicts) |> Enum.split_while(&(elem(&1, 1) == :ok))
    {Enum.map(taken, &elem(&1, 0)), refused}
  end

  # One acknowledgement, or one return, settles every delivery up to the
  # newest of `deliveries`. A channel lost meanwhile is seen by its monitor.
  defp settle(link, answer, deliveries, options) do
    {_payload, meta} = List.last(deliveries)
    answer.(link.chan, meta.delivery_tag, [multiple: true] ++ options)
  catch
    :exit, _channel_gone -> :ok
  end

  defp refused(state, count, error) do
    state = Worker.failed(state, error)

    Logger.warning(
      "[FlezhaRabbitmq.Consumer] queue=#{state.queue} deliveries=#{count} #{error}; " <>
        "returned to the queue"
    )

    emit(:failed, %{count: count}, Map.put(labels(state), :reason, error))

    delay =
      Connection.backoff(
        state.link_args,
        state.consecutive_failures,
        {@provider, state.plugin_id}
      )

    arm_timer(state, :resume, delay, :resume)
  end

  # ---------- one delivery ----------

  @doc """
  A delivery as the input it becomes: its body, and its metadata, every value
  text as every input's is. `rabbitmq.headers` is the headers as a JSON object;
  `flz.msg_id` is `<plugin_id>/<id>` when the message has an id.
  """
  @spec input(binary(), map(), map()) :: {binary(), map()}
  def input(payload, meta, state) do
    headers = table(meta[:headers])

    metadata =
      %{
        "channel" => state.entry_channel,
        "rabbitmq.exchange" => meta.exchange,
        "rabbitmq.routing_key" => meta.routing_key,
        "rabbitmq.redelivered" => to_string(meta.redelivered),
        "rabbitmq.headers" => Jason.encode!(headers)
      }
      |> put_set("rabbitmq.message_id", meta[:message_id])
      |> put_set("rabbitmq.timestamp", meta[:timestamp])

    case id(meta, headers, state.msg_id_header) do
      nil -> {payload, metadata}
      id -> {payload, Map.put(metadata, "flz.msg_id", "#{state.plugin_id}/#{id}")}
    end
  end

  defp id(meta, _headers, nil), do: text(meta[:message_id])
  defp id(_meta, headers, header), do: text(headers[header])

  defp put_set(metadata, name, value) do
    case text(value) do
      nil -> metadata
      text -> Map.put(metadata, name, text)
    end
  end

  # A property or header value as metadata text; unset, empty or a table is none.
  defp text(value) when value in [nil, :undefined, ""], do: nil
  defp text(value) when is_binary(value), do: value
  defp text(value) when is_number(value) or is_boolean(value), do: to_string(value)
  defp text(_value), do: nil

  # An AMQP table as JSON values: text that is not UTF-8 is Base64.
  defp table(:undefined), do: %{}
  defp table(nil), do: %{}

  defp table(fields),
    do: Map.new(fields, fn {name, type, value} -> {name, value(type, value)} end)

  defp value(:table, fields), do: table(fields)
  defp value(:array, items), do: Enum.map(items, fn {type, item} -> value(type, item) end)
  defp value(:void, _value), do: nil
  defp value(:timestamp, value), do: value

  defp value(_type, value) when is_binary(value),
    do: if(String.valid?(value), do: value, else: Base.encode64(value))

  defp value(_type, value), do: value

  defp labels(state), do: %{plugin_id: state.plugin_id, queue: state.queue}

  defp emit(verb, measurements, metadata),
    do: ProviderBehaviour.emit(@provider, verb, measurements, metadata)
end
