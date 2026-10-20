defmodule FlezhaRabbitmq.Producer.Publisher do
  @moduledoc """
  The publishing half of `FlezhaRabbitmq.Producer`: one process per plugin per
  controller, owning its connection and its channel in confirm mode, the queue
  of payloads not yet published, the publish sequence numbers awaiting the
  broker's confirm and every message's headers.

  A payload is published as soon as it is taken off the mailbox while the
  channel is up. While it is down payloads wait in a `Flezha.Process.Backlog`:
  a guaranteed output always, past `queue_max` if it must, and a plain one as
  `queue_overflow` decides (`Backlog.admit/5`).

  A guaranteed output carries its `Outbox.dedup_key/1` as `message_id` and is
  confirmed on the broker's ack. A nack, or a lost connection before the ack,
  publishes it again with the same `message_id` along the retry curve while
  its attempts stay within `retries`, and then parks it with
  `rabbitmq.exhausted`; a `mandatory` return parks it at once with
  `rabbitmq.unroutable`. A plain output is never awaited: its ack is counted
  `shipped`, and a nack, a return or a lost connection is counted `failed`.
  A stop parks every guaranteed output still held, as `http_push` does.
  """

  # The fact the provider's `health/1` judges: the connection's failure. A
  # publisher serves every relay bound to its plugin, so no one relay is its host.
  use Flezha.Plugin.Provider.Worker, facts: [:failure], host: false
  use Flezha.Process.Timer, keys: [:connect, :retry]

  require Logger

  alias Flezha.Config.Backoff
  alias Flezha.Error
  alias Flezha.Plugin.ProviderBehaviour
  alias Flezha.Process.Backlog
  alias Flezha.State.Outbox
  alias FlezhaRabbitmq.Connection

  @provider :rabbitmq_producer

  @identity ["flz.key", "flz.cell", "flz.seq", "flz.epoch"]

  @doc "The publisher's start arguments, from the plugin's prepared options."
  @spec args(map()) :: map()
  def args(opts) do
    opts
    |> Map.take([
      :plugin_id,
      :exchange,
      :routing_key,
      :routing_key_from,
      :persistent,
      :mandatory,
      :content_type,
      :declare_exchange,
      :exchange_type,
      :retries,
      :queue_max,
      :queue_max_bytes,
      :queue_overflow
    ])
    |> Map.merge(%{
      headers: Map.to_list(opts.headers),
      retry: %{
        name: :rabbitmq_publish,
        base: opts.retry_backoff_ms,
        cap: opts.retry_max_backoff_ms,
        jitter: true,
        reset: :per_call
      },
      link_args: Connection.args(opts)
    })
  end

  @doc "Counters and queue depths, for tests; `%{}` when the publisher does not answer."
  @spec stats(GenServer.server()) :: map()
  def stats(publisher) do
    GenServer.call(publisher, :stats, ProviderBehaviour.probe_budget_ms())
  catch
    :exit, _ -> %{}
  end

  @impl true
  def init(args) do
    # A stop parks what it holds and closes the connection.
    Process.flag(:trap_exit, true)

    state =
      args
      |> Map.drop([:name, :queue_max, :queue_max_bytes])
      |> Map.merge(Connection.initial(args.link_args))
      |> Map.merge(%{
        backlog: Backlog.new(args.queue_max_bytes, args.queue_max),
        seqno: 1,
        unconfirmed: :gb_trees.empty(),
        ids: %{},
        delayed: [],
        overflowed: 0,
        timers: %{}
      })

    {:ok, arm_timer(state, :connect, 0, :connect)}
  end

  @impl true
  def handle_cast({:publish, body, metadata}, state),
    do: {:noreply, state |> admit(entry(body, metadata)) |> dispatch()}

  @impl true
  def handle_call(:stats, _from, state) do
    {:reply,
     %{
       queued: state.backlog.frames,
       unconfirmed: :gb_trees.size(state.unconfirmed),
       delayed: length(state.delayed),
       overflowed: state.overflowed
     }, state}
  end

  @impl true
  def handle_info(:connect, state), do: {:noreply, state |> timer_fired(:connect) |> connect()}

  def handle_info({:basic_ack, seqno, multiple}, state),
    do: {:noreply, acked(confirmed(state, seqno, multiple))}

  def handle_info({:basic_nack, seqno, multiple}, state) do
    {entries, state} = confirmed(state, seqno, multiple)
    {:noreply, again(state, entries, :nacked)}
  end

  def handle_info({:basic_return, _payload, meta}, state), do: {:noreply, returned(state, meta)}

  def handle_info(:retry, state) do
    delayed = Enum.map(state.delayed, &{&1, byte_size(&1.body)})
    state = timer_fired(state, :retry)
    {:noreply, dispatch(%{state | backlog: Backlog.requeue(state.backlog, delayed), delayed: []})}
  end

  def handle_info({:DOWN, ref, :process, _pid, reason}, state) do
    case Connection.down(state.link, ref, reason, state.link_args) do
      {:lost, failure} -> {:noreply, lose(state, failure)}
      :other -> {:noreply, state}
    end
  end

  def handle_info(_other, state), do: {:noreply, state}

  @impl true
  def terminate(_reason, state) do
    held =
      Backlog.to_list(state.backlog) ++ state.delayed ++ :gb_trees.values(state.unconfirmed)

    for entry <- held,
        Outbox.guaranteed?(entry.metadata),
        do: park(entry, "rabbitmq.exhausted", %{reason: :stopped}, entry.attempt)

    Connection.close(state.link)
    :ok
  end

  # ---------- the connection ----------

  defp connect(state) do
    case Connection.open(state.link_args, &confirm_mode(&1, state)) do
      {:ok, link, :confirming} ->
        if state.attempt > 0, do: emit(state, :reconnected, 1, %{})
        dispatch(%{state | link: link, failure: nil, attempt: 0, seqno: 1})

      {:error, failure} ->
        retry(state, failure)
    end
  end

  defp confirm_mode(chan, state) do
    with :ok <- declare(chan, state),
         :ok <- AMQP.Confirm.select(chan),
         :ok <- AMQP.Confirm.register_handler(chan, self()),
         :ok <- AMQP.Basic.return(chan, self()),
         do: {:ok, :confirming}
  end

  defp declare(_chan, %{declare_exchange: false}), do: :ok

  defp declare(chan, state),
    do:
      AMQP.Exchange.declare(chan, state.exchange, String.to_existing_atom(state.exchange_type),
        durable: true
      )

  # What was published and not confirmed may or may not have reached the
  # broker: a guaranteed output goes again, a plain one is counted lost.
  defp lose(state, failure) do
    if state.failure == nil do
      Logger.warning(
        "[FlezhaRabbitmq.Producer] #{state.plugin_id} lost its connection: #{inspect(failure)}; " <>
          "#{:gb_trees.size(state.unconfirmed)} unconfirmed outputs are published again or counted"
      )
    end

    Connection.close(state.link)
    unconfirmed = :gb_trees.values(state.unconfirmed)
    state = %{state | link: nil, unconfirmed: :gb_trees.empty(), ids: %{}}
    state |> again(unconfirmed, :connection_lost) |> retry(failure)
  end

  defp retry(state, failure) do
    {state, delay} = Connection.failed(state, failure, {@provider, state.plugin_id})
    arm_timer(state, :connect, delay, :connect)
  end

  # ---------- admission ----------

  defp entry(body, metadata),
    do: %{body: body, metadata: metadata, message_id: Outbox.dedup_key(metadata), attempt: 0}

  # A guaranteed output always waits; the policy decides for a plain one.
  defp admit(state, entry) do
    size = byte_size(entry.body)

    if Outbox.guaranteed?(entry.metadata) do
      %{state | backlog: Backlog.push(state.backlog, entry, size)}
    else
      plain? = &(not Outbox.guaranteed?(&1.metadata))

      case Backlog.admit(state.backlog, entry, size, state.queue_overflow, plain?) do
        {:queued, backlog, dropped} -> overflow(%{state | backlog: backlog}, dropped)
        {:refused, backlog, dropped} -> overflow(%{state | backlog: backlog}, dropped ++ [entry])
        {:full, backlog} -> %{state | backlog: Backlog.push(backlog, entry, size)}
      end
    end
  end

  defp overflow(state, []), do: state

  defp overflow(state, dropped) do
    n = length(dropped)
    emit(state, :overflowed, n, %{policy: state.queue_overflow})
    %{state | overflowed: state.overflowed + n}
  end

  # ---------- publishing ----------

  defp dispatch(%{link: nil} = state), do: state

  defp dispatch(state) do
    case Backlog.out(state.backlog) do
      {:empty, _backlog} -> state
      {entry, backlog} -> dispatch(publish(%{state | backlog: backlog}, entry))
    end
  end

  defp publish(state, entry) do
    case AMQP.Basic.publish(
           state.link.chan,
           state.exchange,
           routing_key(state, entry.metadata),
           entry.body,
           properties(state, entry.metadata, entry.message_id)
         ) do
      :ok ->
        %{
          state
          | seqno: state.seqno + 1,
            unconfirmed: :gb_trees.insert(state.seqno, entry, state.unconfirmed),
            ids: index(state.ids, entry.message_id, state.seqno)
        }

      {:error, reason} ->
        again(state, [entry], reason)
    end
  end

  # A guaranteed output's publish number by its message id, for a return.
  defp index(ids, nil, _seqno), do: ids
  defp index(ids, message_id, seqno), do: Map.put(ids, message_id, seqno)

  @doc "The routing key of an output with `metadata` under the publisher's `conf`."
  @spec routing_key(map(), map()) :: binary()
  def routing_key(%{routing_key_from: "flz_key"} = conf, metadata),
    do: to_string(metadata["flz.key"] || conf.routing_key)

  def routing_key(conf, _metadata), do: conf.routing_key

  @doc """
  The publish options of an output with `metadata`: its identity and the static
  headers, persistence, `mandatory`, the content type and, for a guaranteed
  output, `message_id`.
  """
  @spec properties(map(), map(), binary() | nil) :: keyword()
  def properties(conf, metadata, message_id) do
    identity =
      for name <- @identity,
          value = metadata[name],
          value not in [nil, ""],
          do: {name, to_string(value)}

    [
      headers: identity ++ conf.headers,
      persistent: conf.persistent,
      mandatory: conf.mandatory
    ]
    |> put_set(:content_type, conf.content_type)
    |> put_set(:message_id, message_id)
  end

  defp put_set(options, _name, nil), do: options
  defp put_set(options, name, value), do: Keyword.put(options, name, value)

  # ---------- confirms ----------

  # The unconfirmed entries a confirm of `seqno` settles, oldest first, and the
  # publisher without them.
  defp confirmed(state, seqno, false) do
    case :gb_trees.lookup(seqno, state.unconfirmed) do
      {:value, entry} -> {[entry], forget(state, [{seqno, entry}])}
      :none -> {[], state}
    end
  end

  defp confirmed(state, seqno, true) do
    {settled, _rest} =
      Enum.split_while(:gb_trees.to_list(state.unconfirmed), &(elem(&1, 0) <= seqno))

    {Enum.map(settled, &elem(&1, 1)), forget(state, settled)}
  end

  defp forget(state, settled) do
    unconfirmed = Enum.reduce(settled, state.unconfirmed, &:gb_trees.delete(elem(&1, 0), &2))
    ids = Map.drop(state.ids, Enum.map(settled, &elem(&1, 1).message_id))
    %{state | unconfirmed: unconfirmed, ids: ids}
  end

  defp acked({[], state}), do: state

  defp acked({entries, state}) do
    Outbox.confirm_many(Enum.map(entries, & &1.metadata))
    emit(state, :shipped, length(entries), %{})
    state
  end

  # A guaranteed output the broker returned is parked at once; its ack, which
  # follows, then settles nothing.
  defp returned(state, meta) do
    case Map.fetch(state.ids, meta[:message_id]) do
      {:ok, seqno} ->
        {[entry], state} = confirmed(state, seqno, false)
        details = %{exchange: meta.exchange, routing_key: meta.routing_key}
        park(entry, "rabbitmq.unroutable", details, entry.attempt + 1)
        emit(state, :failed, 1, %{reason: :unroutable})
        state

      :error ->
        emit(state, :failed, 1, %{reason: :unroutable})
        state
    end
  end

  # Entries that must go again: a guaranteed one waits out the retry curve,
  # or is parked once its attempts run out; a plain one is counted lost.
  defp again(state, entries, why) do
    {guaranteed, plain} = Enum.split_with(entries, &Outbox.guaranteed?(&1.metadata))
    if plain != [], do: emit(state, :failed, length(plain), %{reason: why})

    {retried, exhausted} =
      guaranteed
      |> Enum.map(&%{&1 | attempt: &1.attempt + 1})
      |> Enum.split_with(&(&1.attempt <= state.retries))

    for entry <- exhausted, do: park(entry, "rabbitmq.exhausted", %{reason: why}, entry.attempt)
    if exhausted != [], do: emit(state, :failed, length(exhausted), %{reason: why})
    retry_later(state, retried, why)
  end

  defp retry_later(state, [], _why), do: state

  defp retry_later(state, retried, why) do
    emit(state, :retried, length(retried), %{reason: why})
    state = %{state | delayed: state.delayed ++ retried}

    if timer_armed?(state, :retry) do
      state
    else
      attempt = retried |> Enum.map(& &1.attempt) |> Enum.min()

      wait = Backoff.arm(state.retry, attempt, {@provider, state.plugin_id})

      arm_timer(state, :retry, wait, :retry)
    end
  end

  defp park(entry, code, details, attempts),
    do: Outbox.park(entry.metadata, entry.body, Error.object(Error.new(code, details)), attempts)

  defp emit(state, verb, count, metadata),
    do:
      ProviderBehaviour.emit(
        @provider,
        verb,
        %{count: count},
        Map.put(metadata, :plugin_id, state.plugin_id)
      )
end
