defmodule FlezhaRabbitmq.ConnectionTest do
  @moduledoc """
  What a lost connection or channel is reported as, the password that never
  leaves its closure, and the types the connection's rows publish.
  """
  use ExUnit.Case, async: true

  alias Flezha.Plugin.ProviderBehaviour.Decl
  alias FlezhaRabbitmq.Connection
  alias FlezhaRabbitmq.Consumer
  alias FlezhaRabbitmq.Producer
  alias FlezhaRabbitmq.Test.Helpers

  @args %{host: "broker:5672"}

  defp link(), do: %{refs: [make_ref(), make_ref()]}

  test "the connection's own exit is the connection's failure, whatever the broker said" do
    %{refs: [conn, _chan]} = link = link()
    forced = {:shutdown, {:server_initiated_close, 320, "CONNECTION_FORCED - broker shutdown"}}

    assert Connection.down(link, conn, forced, @args) ==
             {:lost,
              {:rabbitmq_connection_failed,
               %{host: "broker:5672", reason: "320 CONNECTION_FORCED - broker shutdown"}}}
  end

  test "a channel the broker closed names its code and text; one its connection took does not" do
    %{refs: [_conn, chan]} = link = link()
    closed = {:shutdown, {:server_initiated_close, 404, "NOT_FOUND - no queue 'q'"}}

    assert Connection.down(link, chan, closed, @args) ==
             {:lost, {:rabbitmq_channel_closed, %{code: 404, text: "NOT_FOUND - no queue 'q'"}}}

    taken =
      {:shutdown, {:connection_closing, {:server_initiated_close, 320, "CONNECTION_FORCED"}}}

    assert {:lost, {:rabbitmq_connection_failed, %{reason: "320 CONNECTION_FORCED"}}} =
             Connection.down(link, chan, taken, @args)
  end

  test "a monitor that is not the link's is someone else's" do
    assert Connection.down(link(), make_ref(), :normal, @args) == :other
    assert Connection.down(nil, make_ref(), :normal, @args) == :other
  end

  test "a failure names the tag of a reason, never its payload" do
    %{refs: [conn, _chan]} = link = link()

    assert {:lost, {_, %{reason: "auth_failure"}}} =
             Connection.down(link, conn, {:auth_failure, "user 'u' password 'hunter2'"}, @args)

    assert {:lost, {_, %{reason: "econnrefused"}}} =
             Connection.down(link, conn, :econnrefused, @args)
  end

  describe "ssl_options" do
    @base %{queue: "orders", channel: "in", password: "hunter2"}

    test "a keyword list is taken as it is" do
      assert {:ok, %{ssl_options: [verify: :verify_none]}} =
               Helpers.prepare(Consumer, Map.put(@base, :ssl_options, verify: :verify_none))
    end

    test "a string is refused naming the option" do
      assert {:error, {:invalid_option_value, :ssl_options, "verify_none"}} =
               Helpers.prepare(Consumer, Map.put(@base, :ssl_options, "verify_none"))
    end
  end

  test "no public row of either provider is typed :any" do
    untyped =
      for provider <- [Consumer, Producer],
          {name, row} <- Decl.public_options(provider.__provider__()),
          Keyword.get(row, :type, :any) == :any,
          do: "#{inspect(provider)}.#{name}"

    assert untyped == []
  end
end
