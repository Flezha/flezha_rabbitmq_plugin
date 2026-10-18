defmodule FlezhaRabbitmq.Test.Helpers do
  @moduledoc """
  What the package's tests need beyond the core's shared helpers: a plugin
  prepared and activated as the extension table does it, a stand-in relay, and
  a channel without a broker.
  """

  import ExUnit.Callbacks, only: [on_exit: 1]

  alias Flezha.Plugin.Extension.Table.Entry
  alias Flezha.Plugin.Extension.Table.Workers
  alias Flezha.Plugin.ProviderBehaviour.Options
  alias FlezhaTest.Helper.NodeTestHelper

  @doc "The options `provider` prepares from `opts`, as the extension table builds them."
  def prepare(provider, opts, host \\ %{}) do
    host =
      Map.merge(
        %{
          overwatcher_pid: self(),
          source_namespace: :default,
          source_relay_id: "op",
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
    on_exit(fn -> NodeTestHelper.ensure_stopped(pid, :shutdown) end)
    {prepared, pid}
  end

  @doc """
  The host relay of a feeding worker, linked to the caller: it answers each
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
end
