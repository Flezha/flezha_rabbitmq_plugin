defmodule FlezhaRabbitmq.IntegrationTest do
  @moduledoc """
  Both providers against the RabbitMQ of `docker/dev/docker-compose.yml`, with
  a stand-in relay. Tagged `:rabbitmq_integration`, which the default run
  excludes:

      RABBITMQ_INTEGRATION=1 mix test --include rabbitmq_integration

  The broker is `docker compose up -d rabbitmq` in the core, on
  `localhost:5672`, its management API on 15672. The reconnect test restarts
  it with `docker compose restart rabbitmq`.
  """

  # async: false: one broker, restarted by one test; Mimic's global mode.
  use ExUnit.Case, async: false
  use Mimic

  import ExUnit.CaptureLog

  alias Flezha.Plugin.Extension.Ctx
  alias Flezha.State.Outbox
  alias FlezhaRabbitmq.Consumer
  alias FlezhaRabbitmq.Producer
  alias FlezhaRabbitmq.Producer.Publisher
  alias FlezhaRabbitmq.Test.Helpers

  @moduletag :rabbitmq_integration
  @moduletag timeout: 120_000

  @compose [
    "compose",
    "-f",
    Path.join(Mix.Project.deps_paths()[:flezha], "docker/dev/docker-compose.yml")
  ]
  @cell {"Rates", "t1", "EUR.USD"}

  setup_all do
    if System.get_env("RABBITMQ_INTEGRATION") != "1",
      do: raise("Refusing to run rabbitmq_integration tests without RABBITMQ_INTEGRATION=1")

    {:ok, _} = Application.ensure_all_started(:inets)
    :ok
  end

  setup :set_mimic_global

  setup do
    {:ok, conn} = AMQP.Connection.open()
    {:ok, chan} = AMQP.Channel.open(conn)
    on_exit(fn -> Helpers.stop(conn.pid) end)
    {:ok, chan: chan, run: System.unique_integer([:positive])}
  end

  # The provider's warnings are captured: the suite logs errors only.
  defp warnings(fun) do
    Logger.configure(level: :warning)
    capture_log(fun)
  after
    Logger.configure(level: :error)
  end

  # RabbitMQ 4 refuses a transient queue that is not exclusive.
  defp queue(chan, name) do
    {:ok, _} = AMQP.Queue.declare(chan, name, durable: true)
    on_exit(fn -> delete_queue(name) end)
    name
  end

  defp delete_queue(name) do
    {:ok, conn} = AMQP.Connection.open()
    {:ok, chan} = AMQP.Channel.open(conn)
    AMQP.Queue.delete(chan, name)
    AMQP.Connection.close(conn)
  end

  defp producer(extra) do
    {opts, pid} = Helpers.activate(Producer, Map.merge(%{reconnect_backoff_ms: 100}, extra))
    assert Helpers.wait_until(fn -> Producer.health(opts) == :healthy end, 5_000)
    {opts, pid}
  end

  defp consumer(queue, host, extra) do
    opts = Map.merge(%{queue: queue, channel: "in", reconnect_backoff_ms: 100}, extra)
    {opts, pid} = Helpers.activate(Consumer, opts, %{overwatcher_pid: host})
    opts = Map.put(opts, :owned_pid, pid)
    {opts, pid}
  end

  defp out(opts, body, metadata \\ %{"channel" => "out"}),
    do: {:ok, _ctx} = Producer.on_output(%Ctx.IO{message: body, metadata: metadata}, opts)

  defp ingested(acc \\ []) do
    receive do
      {:ingest, body, metadata} -> ingested([{body, metadata} | acc])
    after
      500 -> Enum.reverse(acc)
    end
  end

  test "100 outputs published to a queue are read back by a consumer", %{chan: chan, run: run} do
    q = queue(chan, "flz-round-#{run}")
    {popts, _} = producer(%{plugin_id: "rt-#{run}", routing_key: q})
    {_copts, _} = consumer(q, Helpers.stand_in_host(), %{plugin_id: "in-#{run}"})

    for n <- 1..100, do: out(popts, "m#{n}")
    bodies = ingested() |> Enum.map(&elem(&1, 0))
    assert Enum.sort(bodies) == Enum.sort(for n <- 1..100, do: "m#{n}")
  end

  test "a guaranteed output is held until the broker confirms it", %{chan: chan, run: run} do
    Helpers.stub_outbox_owner(@cell)
    q = queue(chan, "flz-confirm-#{run}")
    {opts, _} = producer(%{plugin_id: "gc-#{run}", routing_key: q})

    for seq <- 1..3, do: out(opts, "g#{seq}", Helpers.guaranteed_metadata(@cell, seq))
    assert :ok = Helpers.assert_confirmed(for(seq <- 1..3, do: {:cursor, seq}), 5_000)

    {:ok, "g1", meta} = AMQP.Basic.get(chan, q)
    assert meta.message_id == "Rates/t1/EUR.USD/1.0/0"
    assert {"flz.key", :longstr, "EUR.USD"} in meta.headers
  end

  test "outputs are routed by their key and carry their identity", %{chan: chan, run: run} do
    fx = "flz-fx-#{run}"
    :ok = AMQP.Exchange.declare(chan, fx, :topic, auto_delete: true)
    eur = queue(chan, "flz-eur-#{run}")
    usd = queue(chan, "flz-usd-#{run}")
    :ok = AMQP.Queue.bind(chan, eur, fx, routing_key: "EUR.*")
    :ok = AMQP.Queue.bind(chan, usd, fx, routing_key: "USD.*")
    {opts, pid} = producer(%{plugin_id: "rk-#{run}", exchange: fx, routing_key_from: "flz_key"})

    out(opts, "e", %{
      "channel" => "out",
      "flz.key" => "EUR.USD",
      "flz.cell" => "c",
      "flz.seq" => "1.0"
    })

    out(opts, "u", %{
      "channel" => "out",
      "flz.key" => "USD.JPY",
      "flz.cell" => "c",
      "flz.seq" => "2.0"
    })

    assert Helpers.wait_until(fn -> Publisher.stats(pid).unconfirmed == 0 end, 5_000)

    assert {:ok, "e", %{headers: eur_headers}} = AMQP.Basic.get(chan, eur)
    assert {:ok, "u", _meta} = AMQP.Basic.get(chan, usd)
    assert {"flz.seq", :longstr, "1.0"} in eur_headers
    assert {:empty, _} = AMQP.Basic.get(chan, eur)
  end

  test "an unroutable guaranteed output is parked as rabbitmq.unroutable", %{chan: chan, run: run} do
    test = self()
    Helpers.stub_outbox_owner(@cell)

    stub(Outbox, :park, fn md, _body, error, _n ->
      send(test, {:parked, md["flz.seq"], error}) && :ok
    end)

    fx = "flz-void-#{run}"
    :ok = AMQP.Exchange.declare(chan, fx, :direct, auto_delete: false)
    on_exit(fn -> delete_exchange(fx) end)

    {opts, _} =
      producer(%{plugin_id: "ur-#{run}", exchange: fx, routing_key: "nowhere", mandatory: true})

    out(opts, "g1", Helpers.guaranteed_metadata(@cell, 1))
    assert_receive {:parked, "1.0", error}, 5_000

    assert %{"code" => "rabbitmq.unroutable", "details" => %{"routing_key" => "nowhere"}} =
             Jason.decode!(error)
  end

  defp delete_exchange(name) do
    {:ok, conn} = AMQP.Connection.open()
    {:ok, chan} = AMQP.Channel.open(conn)
    AMQP.Exchange.delete(chan, name)
    AMQP.Connection.close(conn)
  end

  test "a refused delivery goes back to the queue and is delivered again", %{chan: chan, run: run} do
    q = queue(chan, "flz-refuse-#{run}")
    host = Helpers.stand_in_host([{:error, :full}])

    log =
      warnings(fn ->
        {_opts, _} = consumer(q, host, %{plugin_id: "rf-#{run}", reconnect_backoff_ms: 1})
        :ok = AMQP.Basic.publish(chan, "", q, "once", message_id: "m-1")
        assert_receive {:ingest, "once", %{"rabbitmq.redelivered" => "false"}}, 5_000
        assert_receive {:ingest, "once", %{"rabbitmq.redelivered" => "true"} = md}, 5_000
        assert md["flz.msg_id"] == "rf-#{run}/m-1"
      end)

    assert log =~ "queue=#{q} deliveries=1 ingest refused: :full"
    assert {:empty, _} = AMQP.Basic.get(chan, q)
  end

  test "two consumers of one queue each take a share, every message once", %{chan: chan, run: run} do
    q = queue(chan, "flz-compete-#{run}")
    host = Helpers.stand_in_host()
    {_, _} = consumer(q, host, %{plugin_id: "c1-#{run}", prefetch: 10})
    {_, _} = consumer(q, host, %{plugin_id: "c2-#{run}", prefetch: 10})
    assert Helpers.wait_until(fn -> consumers(q) == 2 end, 5_000)

    for n <- 1..1_000, do: :ok = AMQP.Basic.publish(chan, "", q, "m#{n}", message_id: "#{n}")
    taken = ingested()

    assert taken |> Enum.map(&elem(&1, 0)) |> Enum.sort() ==
             Enum.sort(for n <- 1..1_000, do: "m#{n}")

    # Each consumer names itself in the id it gives a message, and both took some.
    owners =
      taken |> Enum.map(&(elem(&1, 1)["flz.msg_id"] |> String.split("/") |> hd())) |> Enum.uniq()

    assert Enum.sort(owners) == Enum.sort(["c1-#{run}", "c2-#{run}"])
  end

  test "a missing queue is a channel closed with 404", %{run: run} do
    log =
      warnings(fn ->
        {opts, _} =
          consumer("flz-absent-#{run}", Helpers.stand_in_host(), %{
            plugin_id: "mq-#{run}",
            reconnect_backoff_ms: 5_000
          })

        assert Helpers.wait_until(
                 fn ->
                   match?(
                     {:unhealthy, {:rabbitmq_channel_closed, %{code: 404}}},
                     Consumer.health(opts)
                   )
                 end,
                 5_000
               )
      end)

    assert log == ""
  end

  test "both providers survive a broker restart and resume", %{chan: chan, run: run} do
    q = queue(chan, "flz-restart-#{run}")
    {popts, _} = producer(%{plugin_id: "rp-#{run}", routing_key: q})
    {copts, _} = consumer(q, Helpers.stand_in_host(), %{plugin_id: "rc-#{run}"})
    assert Helpers.wait_until(fn -> Consumer.health(copts) == :healthy end, 5_000)

    log =
      warnings(fn ->
        {_, 0} = System.cmd("docker", @compose ++ ["stop", "rabbitmq"], stderr_to_stdout: true)
        assert Helpers.wait_until(fn -> Producer.health(popts) != :healthy end, 10_000)
        assert Consumer.health(copts) != :healthy
        {_, 0} = System.cmd("docker", @compose ++ ["start", "rabbitmq"], stderr_to_stdout: true)
        assert Helpers.wait_until(fn -> Producer.health(popts) == :healthy end, 60_000)
        assert Helpers.wait_until(fn -> Consumer.health(copts) == :healthy end, 60_000)
      end)

    assert log =~ "rp-#{run} lost its connection"
    assert log =~ "queue=#{q} lost its connection"

    out(popts, "after")
    assert_receive {:ingest, "after", _}, 10_000
  end

  test "relays sharing one producer publish through one connection", %{run: run} do
    plugin = "share-#{run}"
    {opts, pid} = producer(%{plugin_id: plugin})
    {_again, ^pid} = Helpers.activate(Producer, %{plugin_id: opts.plugin_id})
    assert Helpers.wait_until(fn -> connections("flezha #{plugin} ") == 1 end, 10_000)
  end

  # The broker's own view, through its management API.
  defp consumers(queue), do: api("/api/queues/%2F/#{queue}")["consumers"]

  defp connections(prefix) do
    api("/api/connections")
    |> Enum.count(
      &String.starts_with?(get_in(&1, ["client_properties", "connection_name"]) || "", prefix)
    )
  end

  defp api(path) do
    auth = {~c"authorization", ~c"Basic " ++ String.to_charlist(Base.encode64("guest:guest"))}

    {:ok, {{_, 200, _}, _headers, body}} =
      :httpc.request(
        :get,
        {~c"http://localhost:15672" ++ String.to_charlist(path), [auth]},
        [],
        []
      )

    Jason.decode!(body)
  end
end
