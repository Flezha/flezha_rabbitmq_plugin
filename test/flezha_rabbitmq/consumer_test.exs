defmodule FlezhaRabbitmq.ConsumerTest do
  @moduledoc """
  The consumer without a broker: its options and refusals, a delivery as the
  input it becomes, and the reader's acknowledgements, returns, back-off and
  reconnect against a channel that is a bare process and a stand-in relay.
  """

  # async: false: Mimic's global mode and the primary Logger.
  use ExUnit.Case, async: false
  use Mimic

  import ExUnit.CaptureLog

  alias FlezhaRabbitmq.Consumer
  alias FlezhaRabbitmq.Consumer.Reader
  alias FlezhaRabbitmq.Test.Helpers
  alias FlezhaTest.Helper.NodeTestHelper
  alias FlezhaTest.Helper.Telemetry

  @base %{queue: "orders", channel: "in", password: "hunter2"}

  describe "options" do
    test "a binding is prepared with its password held in a closure" do
      assert {:ok, opts} = Helpers.prepare(Consumer, @base)

      assert {opts.plugin_id, opts.port, opts.prefetch, opts.batch_ms} ==
               {"rabbit", 5672, 200, 10}

      assert opts.password == "**redacted**"

      args = Reader.args(opts)
      refute inspect(Map.delete(args, :link_args)) =~ "hunter2"
      assert args.link_args.host == "localhost:5672"

      assert {:ok, %{port: 5671}} = Helpers.prepare(Consumer, Map.put(@base, :ssl, true))
    end

    test "a binding without a queue or a declare it binds with is refused" do
      assert {:error, {:invalid_option_value, :queue, ""}} =
               Helpers.prepare(Consumer, %{@base | queue: ""})

      assert {:error, {:invalid_option_value, :bind_exchange, "fx"}} =
               Helpers.prepare(Consumer, Map.put(@base, :bind_exchange, "fx"))

      assert {:error, {:invalid_option_value, :routing_keys, "a,b"}} =
               Helpers.prepare(Consumer, Map.merge(@base, %{declare: true, routing_keys: "a,b"}))
    end

    test "one name in two namespaces is two readers" do
      keys =
        for ns <- ["ns_a", "ns_b"] do
          {:ok, opts} = Helpers.prepare(Consumer, @base, %{source_namespace: ns})
          [{key, _args}] = Consumer.worker_specs(opts)
          key
        end

      assert keys == [{"ns_a", "rabbit"}, {"ns_b", "rabbit"}]
    end

    test "it feeds, is not shareable and declares its own verbs" do
      decl = Consumer.__provider__()
      assert {decl.name, decl.feeds, decl.shareable} == {"rabbitmq_consumer", true, false}
      assert decl.telemetry == [reconnected: :event, unacked: :metric]

      assert Application.get_env(:flezha_rabbitmq, :flezha_providers)["rabbitmq_consumer"] ==
               Consumer
    end
  end

  describe "a delivery" do
    @state %{entry_channel: "in", plugin_id: "orders-in", msg_id_header: nil}

    defp meta(extra \\ %{}) do
      Map.merge(
        %{
          exchange: "fx",
          routing_key: "EUR.USD",
          redelivered: true,
          message_id: "m-1",
          timestamp: 1_790_000_000,
          headers: [
            {"region", :longstr, "eu"},
            {"depth", :long, 3},
            {"raw", :binary, <<0xFF, 0x00>>},
            {"nested", :table, [{"ok", :bool, true}]}
          ]
        },
        extra
      )
    end

    test "becomes one input carrying where it came from, its id prefixed with the plugin" do
      assert {"body", metadata} = Reader.input("body", meta(), @state)

      assert metadata == %{
               "channel" => "in",
               "flz.msg_id" => "orders-in/m-1",
               "rabbitmq.exchange" => "fx",
               "rabbitmq.routing_key" => "EUR.USD",
               "rabbitmq.redelivered" => "true",
               "rabbitmq.message_id" => "m-1",
               "rabbitmq.timestamp" => "1790000000",
               "rabbitmq.headers" => metadata["rabbitmq.headers"]
             }

      assert Jason.decode!(metadata["rabbitmq.headers"]) == %{
               "region" => "eu",
               "depth" => 3,
               "raw" => Base.encode64(<<0xFF, 0x00>>),
               "nested" => %{"ok" => true}
             }
    end

    test "takes its id from a named header, and has none without one" do
      state = %{@state | msg_id_header: "region"}
      assert {_body, %{"flz.msg_id" => "orders-in/eu"}} = Reader.input("b", meta(), state)

      bare = meta(%{message_id: :undefined, timestamp: :undefined, headers: :undefined})
      assert {_body, metadata} = Reader.input("b", bare, @state)
      refute Map.has_key?(metadata, "flz.msg_id")
      refute Map.has_key?(metadata, "rabbitmq.message_id")
      assert metadata["rabbitmq.headers"] == "{}"
    end

    # Fails when: an absorbed or suspended delivery reads as refused and is
    # returned to the queue the relay already took it from.
    test "split takes the longest prefix of taken verdicts" do
      assert Reader.split([:a, :b, :c], [:ok, :ok, :ok]) == {[:a, :b, :c], []}

      assert Reader.split([:a, :b, :c], [{:ok, :absorbed}, {:ok, :suspended}, :ok]) ==
               {[:a, :b, :c], []}

      assert Reader.split([:a, :b, :c], [:ok, {:error, :x}, :ok]) ==
               {[:a], [b: {:error, :x}, c: :ok]}

      assert Reader.split([:a, :b], [{:error, :x}, :ok]) == {[], [a: {:error, :x}, b: :ok]}
    end
  end

  describe "the reader" do
    setup :set_mimic_global

    setup do
      # The reader's warnings reach the captures: the suite logs errors only.
      Logger.configure(level: :warning)
      on_exit(fn -> Logger.configure(level: :error) end)
      test = self()
      Helpers.stub_broker()
      stub(AMQP.Basic, :qos, fn _chan, _opts -> :ok end)
      stub(AMQP.Basic, :consume, fn _chan, queue -> {:ok, "ctag-" <> queue} end)

      stub(AMQP.Basic, :ack, fn _chan, tag, opts ->
        send(test, {:ack, tag, opts})
        :ok
      end)

      stub(AMQP.Basic, :nack, fn _chan, tag, opts ->
        send(test, {:nack, tag, opts})
        :ok
      end)

      Telemetry.capture([
        [:flezha, :plugin, :rabbitmq_consumer, :shipped],
        [:flezha, :plugin, :rabbitmq_consumer, :failed],
        [:flezha, :plugin, :rabbitmq_consumer, :unacked],
        [:flezha, :plugin, :rabbitmq_consumer, :reconnected]
      ])

      :ok
    end

    # A reader of `orders` with batches of two, fed by a stand-in relay.
    defp reader(verdicts, extra \\ %{}) do
      host = Helpers.stand_in_host(verdicts)

      opts =
        Map.merge(@base, %{prefetch: 4, batch_ms: 60_000, reconnect_backoff_ms: 60_000})
        |> Map.merge(extra)

      {opts, pid} = Helpers.activate(Consumer, opts, %{overwatcher_pid: host})

      assert NodeTestHelper.wait_until(
               fn -> Consumer.health(owned(opts, pid)) == :healthy end,
               2_000
             )

      {opts, pid}
    end

    defp owned(opts, pid), do: Map.put(opts, :owned_pid, pid)

    defp deliver(pid, tags, ctag \\ "ctag-orders") do
      for tag <- tags do
        meta = %{
          consumer_tag: ctag,
          delivery_tag: tag,
          exchange: "",
          routing_key: "orders",
          redelivered: false,
          message_id: "m-#{tag}",
          timestamp: :undefined,
          headers: :undefined
        }

        send(pid, {:basic_deliver, "p#{tag}", meta})
      end
    end

    test "a batch the relay took is acknowledged once, up to its newest delivery" do
      {_opts, pid} = reader([])
      deliver(pid, [1, 2])

      assert_receive {:ingest, "p1", %{"flz.msg_id" => "rabbit/m-1"}}
      assert_receive {:ingest, "p2", _}
      assert_receive {:ack, 2, [multiple: true]}
      assert_receive {:telemetry, [_, _, _, :shipped], %{count: 2}, %{queue: "orders"}}
      assert_receive {:telemetry, [_, _, _, :unacked], %{count: 0}, _}
      refute_received {:nack, _, _}
    end

    test "a refusal in the middle acks the prefix, returns the rest and waits before the next" do
      {opts, pid} = reader([:ok, {:error, :parked}], %{prefetch: 8})

      log =
        capture_log([level: :warning], fn ->
          deliver(pid, [1, 2, 3, 4])
          assert_receive {:ack, 1, [multiple: true]}
          assert_receive {:nack, 4, [multiple: true, requeue: true]}
        end)

      assert log =~ "queue=orders deliveries=3 ingest refused: :parked"
      assert_receive {:telemetry, [_, _, _, :failed], %{count: 3}, %{reason: reason}}
      assert reason =~ "parked"

      # Nothing is injected while it waits; then the returned deliveries go.
      deliver(pid, [5, 6, 7, 8])
      assert NodeTestHelper.wait_until(fn -> length(:sys.get_state(pid).held) == 4 end, 2_000)
      refute_received {:ingest, "p5", _}
      send(pid, :resume)
      assert_receive {:ingest, "p5", _}
      assert_receive {:ack, 8, [multiple: true]}
      assert Consumer.health(owned(opts, pid)) == :healthy
    end

    test "a refusal returns every delivery held behind its batch" do
      {_opts, pid} = reader([{:error, :full}, :ok, {:error, :full}, :ok])

      log =
        capture_log([level: :warning], fn ->
          deliver(pid, [1, 2])
          assert_receive {:nack, 2, [multiple: true, requeue: true]}
          deliver(pid, [3, 4, 5])
          assert NodeTestHelper.wait_until(fn -> length(:sys.get_state(pid).held) == 3 end, 2_000)
          send(pid, :resume)
          assert_receive {:nack, 5, [multiple: true, requeue: true]}
        end)

      assert log =~ "deliveries=2 ingest refused: :full"
      assert log =~ "deliveries=3 ingest refused: :full"
      refute_received {:ack, _, _}
      assert :sys.get_state(pid).held == []
    end

    test "refused batches in a row turn health" do
      {opts, pid} = reader(List.duplicate({:error, :full}, 4), %{failure_threshold: 2})

      log =
        capture_log([level: :warning], fn ->
          deliver(pid, [1, 2])
          assert_receive {:nack, 2, _}
          send(pid, :resume)
          deliver(pid, [3, 4])
          assert_receive {:nack, 4, _}
        end)

      assert log =~ "deliveries=2 ingest refused: :full"

      assert {:unhealthy, {:read_failed, "ingest refused: :full"}} =
               Consumer.health(owned(opts, pid))
    end

    test "a delivery of a consumer it no longer has is dropped" do
      {_opts, pid} = reader([])
      deliver(pid, [1, 2], "ctag-old")
      deliver(pid, [3, 4])
      assert_receive {:ingest, "p3", _}
      assert_receive {:ack, 4, [multiple: true]}
      refute_received {:ingest, "p1", _}
    end

    test "a lost channel forgets what it held, reports it and reconnects" do
      {opts, pid} = reader([], %{prefetch: 10, reconnect_backoff_ms: 1})
      deliver(pid, [1])
      %{link: %{chan: chan}} = :sys.get_state(pid)

      log =
        capture_log([level: :warning], fn ->
          Process.exit(chan.pid, {:shutdown, {:server_initiated_close, 404, "NOT_FOUND - gone"}})
          assert_receive {:telemetry, [_, _, _, :reconnected], %{count: 1}, _}, 2_000
        end)

      assert log =~ "queue=orders lost its connection"
      assert log =~ "1 deliveries return to the queue"
      refute_received {:ingest, "p1", _}
      assert Consumer.health(owned(opts, pid)) == :healthy
    end

    test "a missing queue is a channel closed with 404 until it exists" do
      test = self()

      stub(AMQP.Basic, :consume, fn _chan, _queue ->
        send(test, :consume)

        exit(
          {{:shutdown, {:server_initiated_close, 404, "NOT_FOUND - no queue 'orders'"}},
           {:gen_server, :call, []}}
        )
      end)

      host = Helpers.stand_in_host([])
      opts = Map.merge(@base, %{reconnect_backoff_ms: 1_000})
      {opts, pid} = Helpers.activate(Consumer, opts, %{overwatcher_pid: host})
      assert_receive :consume, 2_000

      assert NodeTestHelper.wait_until(
               fn ->
                 Consumer.health(owned(opts, pid)) ==
                   {:unhealthy,
                    {:rabbitmq_channel_closed,
                     %{code: 404, text: "NOT_FOUND - no queue 'orders'"}}}
               end,
               2_000
             )

      assert %Flezha.Error{code: "rabbitmq.channel_closed", details: %{"code" => 404}} =
               Flezha.Error.from_reason(elem(Consumer.health(owned(opts, pid)), 1), nil)
    end
  end
end
