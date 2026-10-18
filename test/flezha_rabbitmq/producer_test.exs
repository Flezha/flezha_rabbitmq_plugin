defmodule FlezhaRabbitmq.ProducerTest do
  @moduledoc """
  The producer without a broker: its options and refusals, each message's
  routing key, headers and message id, the queue's overflow policy while the
  connection is down, and the publisher's confirms, retries, returns, parks
  and reconnect against a channel that is a bare process.
  """

  # async: false: Mimic's global mode and the primary Logger.
  use ExUnit.Case, async: false
  use Mimic

  import ExUnit.CaptureLog

  alias Flezha.Plugin.Extension.Ctx
  alias Flezha.State.Outbox
  alias FlezhaRabbitmq.Producer
  alias FlezhaRabbitmq.Producer.Publisher
  alias FlezhaRabbitmq.Test.Helpers
  alias FlezhaTest.Helper.NodeTestHelper
  alias FlezhaTest.Helper.ProviderPrepare
  alias FlezhaTest.Helper.Telemetry

  @base %{exchange: "fx", password: "hunter2"}
  @cell {"Rates", "t1", "EUR.USD"}
  @key %{"flz.key" => "EUR.USD"}

  describe "options" do
    test "a plugin is prepared with its password held in a closure and its worker named" do
      assert {:ok, opts} = Helpers.prepare(Producer, @base)
      assert opts.password == "**redacted**"
      assert {opts.queue_overflow, opts.persistent, opts.retries} == {:drop_oldest, true, 5}
      assert {:via, Registry, {_registry, {:default, "rabbit"}}} = opts.publisher
      refute inspect(Map.delete(Publisher.args(opts), :link_args)) =~ "hunter2"
    end

    test "a static header the producer writes, or headers that are no object, are refused" do
      assert {:error, {:headers_invalid, "rabbitmq_producer", :headers, "flz.key"}} =
               Helpers.prepare(Producer, Map.put(@base, :headers, ~s({"flz.key":"x"})))

      assert {:error, {:headers_invalid, "rabbitmq_producer", :headers}} =
               Helpers.prepare(Producer, Map.put(@base, :headers, ~s(["x"])))
    end

    test "the default exchange cannot be declared" do
      assert {:error, {:invalid_option_value, :declare_exchange, true}} =
               Helpers.prepare(Producer, %{declare_exchange: true})
    end

    test "it is shareable and declares its own verb" do
      decl = Producer.__provider__()
      assert {decl.name, decl.shareable, decl.feeds} == {"rabbitmq_producer", true, false}
      assert decl.telemetry == [reconnected: :event]
    end
  end

  describe "a message" do
    test "carries the output's identity, the static headers and, guaranteed, its message id" do
      {:ok, opts} = Helpers.prepare(Producer, Map.put(@base, :headers, ~s({"app":"rates","v":2})))
      conf = Publisher.args(opts)
      metadata = ProviderPrepare.guaranteed_metadata(@cell, 7, @key)

      assert Publisher.properties(conf, metadata, Outbox.dedup_key(metadata)) == [
               message_id: "Rates/t1/EUR.USD/7.0/0",
               headers: [
                 {"flz.key", "EUR.USD"},
                 {"flz.cell", "Rates/t1/EUR.USD"},
                 {"flz.seq", "7.0"},
                 {"flz.epoch", "0"},
                 {"app", "rates"},
                 {"v", "2"}
               ],
               persistent: true,
               mandatory: false
             ]

      plain = Publisher.properties(%{conf | content_type: "text/plain"}, %{"flz.key" => "k"}, nil)
      assert plain[:content_type] == "text/plain"
      refute Keyword.has_key?(plain, :message_id)
    end

    test "is routed by its static key, or by its own flz.key" do
      {:ok, opts} = Helpers.prepare(Producer, Map.put(@base, :routing_key, "rates"))
      conf = Publisher.args(opts)
      assert Publisher.routing_key(conf, @key) == "rates"

      by_key = %{conf | routing_key_from: "flz_key"}
      assert Publisher.routing_key(by_key, @key) == "EUR.USD"
      assert Publisher.routing_key(by_key, %{}) == "rates"
    end
  end

  describe "the publisher" do
    setup :set_mimic_global

    setup do
      test = self()
      ProviderPrepare.stub_outbox_owner(@cell)
      Helpers.stub_broker()
      stub(AMQP.Confirm, :select, fn _chan -> :ok end)
      stub(AMQP.Confirm, :register_handler, fn _chan, _pid -> :ok end)
      stub(AMQP.Basic, :return, fn _chan, _pid -> :ok end)

      stub(AMQP.Basic, :publish, fn _chan, exchange, key, body, options ->
        send(test, {:published, exchange, key, body, options})
        :ok
      end)

      stub(Outbox, :park, fn metadata, body, error, attempts ->
        send(test, {:parked, metadata["flz.seq"], body, Jason.decode!(error), attempts})
        :ok
      end)

      Telemetry.capture(
        for verb <- [:shipped, :retried, :failed, :overflowed, :reconnected],
            do: [:flezha, :plugin, :rabbitmq_producer, verb]
      )

      :ok
    end

    defp publisher(connected?, extra \\ %{}) do
      unless connected?,
        do: stub(AMQP.Connection, :open, fn _options -> {:error, :econnrefused} end)

      opts =
        Map.merge(
          %{@base | exchange: ""},
          Map.merge(%{routing_key: "q", retry_backoff_ms: 1}, extra)
        )

      {opts, pid} = Helpers.activate(Producer, opts)

      if connected?,
        do: assert(NodeTestHelper.wait_until(fn -> health(opts) == :healthy end, 2_000))

      {opts, pid}
    end

    defp health(opts), do: Producer.health(opts)

    defp out(opts, body, metadata \\ %{"channel" => "out"}) do
      ctx = %Ctx.IO{message: body, metadata: metadata}
      assert {:ok, ^ctx} = Producer.on_output(ctx, opts)
    end

    defp g(seq), do: ProviderPrepare.guaranteed_metadata(@cell, seq, @key)

    test "while the connection is down it is unhealthy and drop_oldest keeps the newest plain ones" do
      {opts, pid} = publisher(false, %{queue_max: 2})
      for n <- 1..3, do: out(opts, "p#{n}")

      assert %{queued: 2, overflowed: 1} = Publisher.stats(pid)

      assert Enum.map(Flezha.Process.Backlog.to_list(:sys.get_state(pid).backlog), & &1.body) == [
               "p2",
               "p3"
             ]

      assert_receive {:telemetry, [_, _, _, :overflowed], %{count: 1}, %{policy: :drop_oldest}}

      refused = {:rabbitmq_connection_failed, %{host: "localhost:5672", reason: "econnrefused"}}
      assert NodeTestHelper.wait_until(fn -> health(opts) == {:unhealthy, refused} end, 2_000)
    end

    test "drop_newest keeps the oldest, drop_nothing keeps every one" do
      {opts, pid} = publisher(false, %{queue_max: 2, queue_overflow: :drop_newest})
      for n <- 1..3, do: out(opts, "p#{n}")

      assert Enum.map(Flezha.Process.Backlog.to_list(:sys.get_state(pid).backlog), & &1.body) == [
               "p1",
               "p2"
             ]

      {opts, pid} =
        publisher(false, %{plugin_id: "keeper", queue_max: 2, queue_overflow: :drop_nothing})

      for n <- 1..3, do: out(opts, "p#{n}")
      assert %{queued: 3, overflowed: 0} = Publisher.stats(pid)
    end

    test "a guaranteed output always waits, and a plain one finds no plain one to drop" do
      {opts, pid} = publisher(false, %{queue_max: 1})
      out(opts, "g1", g(1))
      out(opts, "g2", g(2))
      out(opts, "plain")
      assert %{queued: 2, overflowed: 1} = Publisher.stats(pid)
    end

    test "a guaranteed output is published with its message id and confirmed on the ack" do
      {opts, pid} = publisher(true)
      out(opts, "g1", g(1))
      out(opts, "p")

      assert_receive {:published, "", "q", "g1", options}
      assert options[:message_id] == "Rates/t1/EUR.USD/1.0/0"
      assert_receive {:published, "", "q", "p", plain}
      refute Keyword.has_key?(plain, :message_id)

      send(pid, {:basic_ack, 2, true})
      assert :ok = ProviderPrepare.assert_confirmed([{:cursor, 1}])
      assert_receive {:telemetry, [_, _, _, :shipped], %{count: 2}, %{plugin_id: "rabbit"}}
      assert %{unconfirmed: 0} = Publisher.stats(pid)
    end

    test "a nack publishes it again with the same message id, and an ack then confirms it" do
      {opts, pid} = publisher(true)
      out(opts, "g1", g(1))
      assert_receive {:published, _, _, "g1", first}

      send(pid, {:basic_nack, 1, false})
      assert_receive {:telemetry, [_, _, _, :retried], %{count: 1}, %{reason: :nacked}}
      assert_receive {:published, _, _, "g1", again}, 2_000
      assert again[:message_id] == first[:message_id]

      send(pid, {:basic_ack, 2, false})
      assert :ok = ProviderPrepare.assert_confirmed([{:cursor, 1}])
    end

    test "retries that run out park it as rabbitmq.exhausted, and a plain nack is counted" do
      {opts, pid} = publisher(true, %{retries: 0})
      out(opts, "g1", g(1))
      out(opts, "p")
      send(pid, {:basic_nack, 2, true})

      assert_receive {:parked, "1.0", "g1",
                      %{"code" => "rabbitmq.exhausted", "details" => %{"reason" => "nacked"}}, 1}

      assert_receive {:telemetry, [_, _, _, :failed], %{count: 1}, %{reason: :nacked}}
      assert_receive {:telemetry, [_, _, _, :failed], %{count: 1}, %{reason: :nacked}}
      assert %{unconfirmed: 0, delayed: 0} = Publisher.stats(pid)
    end

    test "a returned guaranteed output is parked as rabbitmq.unroutable and its ack settles nothing" do
      {opts, pid} = publisher(true, %{mandatory: true})
      out(opts, "g1", g(1))
      assert_receive {:published, _, _, "g1", options}
      assert options[:mandatory]

      send(
        pid,
        {:basic_return, "g1", %{message_id: options[:message_id], exchange: "", routing_key: "q"}}
      )

      send(pid, {:basic_ack, 1, false})

      assert_receive {:parked, "1.0", "g1",
                      %{
                        "code" => "rabbitmq.unroutable",
                        "details" => %{"exchange" => "", "routing_key" => "q"}
                      }, 1}

      refute_receive {:"$gen_cast", {:outbox_confirmed_many, _}}, 100
      refute_received {:telemetry, [_, _, _, :shipped], _, _}
    end

    test "a lost connection publishes the unconfirmed guaranteed outputs again and counts the plain ones" do
      Logger.configure(level: :warning)
      on_exit(fn -> Logger.configure(level: :error) end)
      {opts, pid} = publisher(true, %{reconnect_backoff_ms: 1})
      out(opts, "g1", g(1))
      out(opts, "p")
      assert_receive {:published, _, _, "g1", _}
      assert_receive {:published, _, _, "p", _}
      %{link: %{chan: chan}} = :sys.get_state(pid)

      log =
        capture_log(fn ->
          Process.exit(chan.pid, :kill)
          assert_receive {:telemetry, [_, _, _, :reconnected], %{count: 1}, _}, 2_000
        end)

      assert log =~ "rabbit lost its connection"
      assert log =~ "2 unconfirmed outputs are published again or counted"
      assert_receive {:telemetry, [_, _, _, :failed], %{count: 1}, %{reason: :connection_lost}}
      assert_receive {:published, "", "q", "g1", again}, 2_000
      assert again[:message_id] == "Rates/t1/EUR.USD/1.0/0"
    end

    test "a stop parks every guaranteed output it still holds" do
      {opts, pid} = publisher(false)
      out(opts, "g1", g(1))
      out(opts, "p")
      assert %{queued: 2} = Publisher.stats(pid)
      NodeTestHelper.ensure_stopped(pid, :shutdown)

      assert_receive {:parked, "1.0", "g1",
                      %{"code" => "rabbitmq.exhausted", "details" => %{"reason" => "stopped"}}, 0}

      refute_received {:parked, _, "p", _, _}
    end
  end
end
