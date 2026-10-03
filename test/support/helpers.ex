defmodule FlezhaRabbitmq.Test.Helpers do
  @moduledoc """
  What the package's tests need from a running controller and nothing more: a
  poll, a telemetry capture, a plugin prepared and activated as the extension
  table does it, a stand-in operator, a channel without a broker, and the
  outbox owner a delivery-guaranteed output answers to.
  """

  import ExUnit.Callbacks, only: [on_exit: 1]

  alias Flezha.Plugin.Extension.Table.Entry
  alias Flezha.Plugin.Extension.Table.Workers
  alias Flezha.Plugin.ProviderBehaviour.Options
  alias Flezha.State.Outbox
  alias Flezha.State.Supervisor, as: StateSupervisor

  @doc "Poll `fun` every `interval_ms` until it is truthy or `timeout_ms` passes."
  def wait_until(fun, timeout_ms, interval_ms \\ 50) do
    deadline = System.monotonic_time(:millisecond) + timeout_ms
    poll(fun, deadline, interval_ms)
  end

  defp poll(fun, deadline, interval_ms) do
    case fun.() do
      falsy when falsy in [nil, false] ->
        if System.monotonic_time(:millisecond) >= deadline do
          falsy
        else
          Process.sleep(interval_ms)
          poll(fun, deadline, interval_ms)
        end

      truthy ->
        truthy
    end
  end

  @doc """
  Forward every emission of `events` to the calling process as
  `{:telemetry, event, measurements, metadata}` until the test exits.
  """
  def capture(events) do
    test_pid = self()
    handler_id = {__MODULE__, events, System.unique_integer([:positive])}

    :ok =
      :telemetry.attach_many(
        handler_id,
        events,
        fn event, measurements, metadata, _config ->
          send(test_pid, {:telemetry, event, measurements, metadata})
        end,
        nil
      )

    on_exit(fn -> :telemetry.detach(handler_id) end)
  end

  @doc "The options `provider` prepares from `opts`, as the extension table builds them."
  def prepare(provider, opts, host \\ %{}) do
    host =
      Map.merge(
        %{
          overwatcher_pid: self(),
          source_namespace: :default,
          source_operator_id: "op",
          source_plugin_id: "rabbit",
          subscribed_extensions: [:on_output]
        },
        host
      )

    with {:ok, normalised} <- Options.normalise_opts(provider, Map.merge(host, opts)),
         do: provider.init_provider(normalised)
  end

  @doc """
  Prepare `opts` and start the declared workers through the table's own
  `Workers.start/5`; answers the prepared opts and the plugin's worker.
  """
  def activate(provider, opts, host \\ %{}) do
    {:ok, prepared} = prepare(provider, opts, host)
    plugin_id = prepared.plugin_id

    {started, :ok} =
      Workers.start(%{entries: %{}, monitors: %{}}, plugin_id, provider, prepared, & &1.())

    pid = Entry.owned_pid(started.entries[plugin_id])
    on_exit(fn -> stop(pid) end)
    {prepared, pid}
  end

  @doc "Stop the worker `pid`, as its supervisor would, if it is still alive."
  def stop(pid) do
    GenServer.stop(pid, :shutdown)
  catch
    :exit, _gone -> :ok
  end

  @doc """
  The host operator of a feeding worker, linked to the caller: it answers each
  confirming ingest with the next of `verdicts`, `:ok` once they run out, and
  tells the caller `{:ingest, body, metadata}`.
  """
  def stand_in_host(verdicts \\ []) do
    test = self()
    spawn_link(fn -> host_loop(test, verdicts) end)
  end

  defp host_loop(test, verdicts) do
    receive do
      {:"$gen_call", from, {:ingest, body, metadata}} ->
        send(test, {:ingest, body, metadata})
        {verdict, rest} = if verdicts == [], do: {:ok, []}, else: {hd(verdicts), tl(verdicts)}
        GenServer.reply(from, verdict)
        host_loop(test, rest)
    end
  end

  @doc """
  A broker whose connections and channels are processes and nothing else,
  each outliving the worker that opens it: the client's `open` calls are
  stubbed, so a worker's other calls into `AMQP` reach the stubs the test sets.
  """
  def stub_broker() do
    Mimic.stub(AMQP.Connection, :open, fn _options ->
      {:ok, %AMQP.Connection{pid: outliving(self())}}
    end)

    Mimic.stub(AMQP.Channel, :open, fn conn ->
      {:ok, %AMQP.Channel{pid: outliving(self()), conn: conn}}
    end)

    Mimic.stub(AMQP.Connection, :close, fn _conn -> :ok end)
  end

  defp outliving(worker), do: spawn(fn -> outlive(worker) end)

  defp outlive(pid) do
    ref = Process.monitor(pid)

    receive do
      {:DOWN, ^ref, :process, ^pid, _reason} -> :ok
    end
  end

  @doc "Register the calling test as the outbox owner of `cell_id`."
  def stub_outbox_owner(cell_id) do
    case Registry.start_link(keys: :unique, name: StateSupervisor.registry_name()) do
      {:ok, _} -> :ok
      {:error, {:already_started, _}} -> :ok
    end

    {:ok, _} = Registry.register(StateSupervisor.registry_name(), cell_id, nil)
    :ok
  end

  @doc """
  Metadata a durable outbox flush would stamp on one output of `cell_id`'s
  entry `seq`: identity plus the confirmation token the owning writer waits on.
  """
  def guaranteed_metadata({rc, sh, pk} = cell_id, seq, extra \\ %{}) do
    Map.merge(
      %{
        "channel" => "out",
        "flz.key" => "EUR.USD",
        "flz.cell" => "#{rc}/#{sh}/#{pk}",
        "flz.seq" => "#{seq}.0",
        "flz.epoch" => "0",
        Outbox.ack_key() => Outbox.token(cell_id, {:cursor, seq})
      },
      extra
    )
  end

  @doc "Wait until every one of `keys` has been confirmed to the calling owner."
  def assert_confirmed(keys, timeout \\ 2_000), do: await_confirmed(MapSet.new(keys), timeout)

  defp await_confirmed(remaining, timeout) do
    if MapSet.size(remaining) == 0 do
      :ok
    else
      receive do
        {:"$gen_cast", {:outbox_confirmed_many, keys}} ->
          await_confirmed(Enum.reduce(keys, remaining, &MapSet.delete(&2, &1)), timeout)

        {:"$gen_cast", {:outbox_confirmed, key}} ->
          await_confirmed(MapSet.delete(remaining, key), timeout)
      after
        timeout ->
          ExUnit.Assertions.flunk("never confirmed: #{inspect(MapSet.to_list(remaining))}")
      end
    end
  end
end
