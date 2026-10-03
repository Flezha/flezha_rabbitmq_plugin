defmodule FlezhaRabbitmq.Connection do
  @moduledoc """
  What both RabbitMQ providers share about their connection: the option rows
  that describe it, the password, the one connection and channel a worker
  opens, the failures that make it unhealthy, and its reconnect curve.

  The password is the core's Postgres one (`Flezha.Plugin.Provider.Postgres.Connection`):
  a literal one is held in the transient row `password_secret` inside a
  closure and the prepared `password` is a placeholder; `connect/1` resolves
  it, or `password_env`, at every connect, so it never lands in a worker's
  state, a log line or a telemetry label. The client keeps it obfuscated in
  its own connection process.

  The connection and the channel are the client's processes, not linked to
  the worker: the worker monitors both and closes the connection when it
  stops. A link is `%{conn, chan, refs}`, `refs` the two monitors. A worker
  keeps it in its state beside `failure`, the reason it is unhealthy (`nil`
  while the link is up), and `attempt`, the connects failed in a row.
  """

  alias Flezha.Plugin.Provider.Postgres.Connection, as: Password
  alias Flezha.SDK.Knob

  # The client waits this long for the broker to accept a connection; a worker
  # is busy while it does.
  @connect_timeout_ms 5_000

  @rows [
          host: [type: :string, default: "localhost", doc: "Broker host"],
          port: [
            type: :int,
            min: 1,
            max: 65_535,
            doc: "AMQP port; 5672, or 5671 with `ssl`"
          ],
          virtual_host: [type: :string, default: "/", doc: "Virtual host"],
          username: [type: :string, default: "guest", doc: "User the plugin connects as"]
        ] ++
          Keyword.take(Password.rows(), [:password, :password_env, :password_secret]) ++
          [
            ssl: [
              type: :bool,
              default: false,
              doc: "TLS to the broker, verifying it against the system's CA store"
            ],
            ssl_options: [type: :any, doc: "Erlang `:ssl` options, passed through"],
            heartbeat_s: [
              type: :int,
              default: 30,
              min: 0,
              doc: "AMQP heartbeat; 0 turns it off"
            ],
            reconnect_backoff_ms: [
              type: :int,
              default: 500,
              min: 1,
              doc: "First wait before connecting again after a loss"
            ],
            reconnect_max_backoff_ms: [
              type: :int,
              default: 30_000,
              min: 1,
              doc: "Ceiling for that wait"
            ]
          ]

  @doc "The connection's option rows, for a provider's declaration."
  @spec rows() :: keyword()
  def rows(), do: @rows

  @doc "`opts` with the password held in its closure and the port defaulted for `ssl`."
  @spec prepare(map()) :: map()
  def prepare(opts) do
    port = opts.port || if(opts.ssl, do: 5671, else: 5672)
    Password.prepare(%{opts | port: port})
  end

  @doc """
  What a worker connects with, from prepared `opts`: where (for its log lines
  and verdicts), the backoff curve, and a closure that opens the connection
  under the name the broker's management API shows, `flezha <plugin_id> <node>`.
  """
  @spec args(map()) :: map()
  def args(opts) do
    name = "flezha #{opts.plugin_id} #{node()}"
    password = Password.password_fun(opts)
    base = Map.take(opts, [:host, :port, :virtual_host, :username, :heartbeat_s])
    ssl = if opts.ssl, do: ssl_options(opts), else: :none

    %{
      host: "#{opts.host}:#{opts.port}",
      backoff: %{
        base: opts.reconnect_backoff_ms,
        cap: opts.reconnect_max_backoff_ms,
        jitter: true,
        reset: :per_call
      },
      open: fn -> open(base, password.(), ssl, name) end
    }
  end

  defp ssl_options(opts) do
    Keyword.merge(
      [
        verify: :verify_peer,
        cacerts: :public_key.cacerts_get(),
        server_name_indication: ~c"#{opts.host}"
      ],
      opts.ssl_options || []
    )
  end

  defp open(base, password, ssl, name) do
    options =
      [
        host: base.host,
        port: base.port,
        virtual_host: base.virtual_host,
        username: base.username,
        heartbeat: base.heartbeat_s,
        connection_timeout: @connect_timeout_ms,
        ssl_options: ssl,
        name: name
      ] ++ if(password, do: [password: password], else: [])

    AMQP.Connection.open(options)
  end

  @doc "A worker's link fields before its first connect: no link, unhealthy while connecting."
  @spec initial(map()) :: map()
  def initial(args),
    do: %{link: nil, failure: connection_failed(args, :connecting), attempt: 0}

  @doc """
  Open the connection and a channel, monitor both, and set the channel up with
  `setup` (a consumer, confirm mode): the link and what `setup` answered, or
  the failure a health verdict names, with the connection closed. A call into
  the channel that exits, because the broker closed it, is its failure.
  """
  @spec open(map(), (AMQP.Channel.t() -> {:ok, term()} | {:error, term()})) ::
          {:ok, map(), term()} | {:error, term()}
  def open(args, setup) do
    with {:ok, link} <- connect(args) do
      case set_up(link.chan, setup, args) do
        {:ok, result} ->
          {:ok, link, result}

        {:error, failure} ->
          close(link)
          {:error, failure}
      end
    end
  end

  defp set_up(chan, setup, args) do
    case setup.(chan) do
      {:ok, result} -> {:ok, result}
      {:error, reason} -> {:error, lost(reason, args)}
    end
  catch
    :exit, {reason, {:gen_server, :call, _args}} -> {:error, lost(reason, args)}
  end

  @doc """
  `state` after a failed connect or a lost link: unhealthy with `failure`, one
  more attempt counted, and the wait before the next connect, announced for
  `target`.
  """
  @spec failed(map(), term(), term()) :: {map(), non_neg_integer()}
  def failed(state, failure, target) do
    attempt = state.attempt + 1

    {%{state | link: nil, failure: failure, attempt: attempt},
     backoff(state.link_args, attempt, target)}
  end

  defp connect(args) do
    with {:ok, conn} <- open_conn(args),
         {:ok, chan} <- open_chan(conn, args) do
      {:ok,
       %{conn: conn, chan: chan, refs: [Process.monitor(conn.pid), Process.monitor(chan.pid)]}}
    end
  end

  defp open_conn(args) do
    case args.open.() do
      {:ok, conn} -> {:ok, conn}
      {:error, reason} -> {:error, connection_failed(args, reason)}
    end
  end

  defp open_chan(conn, args) do
    case AMQP.Channel.open(conn) do
      {:ok, chan} ->
        {:ok, chan}

      {:error, reason} ->
        close(%{conn: conn, refs: []})
        {:error, connection_failed(args, reason)}
    end
  end

  @doc """
  The failure a monitor's `{:DOWN, ref, ...}` means for `link`, or `:other` when
  `ref` is none of its monitors: the connection's own exit is its failure; a
  channel the broker closed names its reply code and text.
  """
  @spec down(map() | nil, reference(), term(), map()) :: {:lost, term()} | :other
  def down(%{refs: [conn, chan]}, ref, reason, args) do
    cond do
      ref == conn -> {:lost, connection_failed(args, reason)}
      ref == chan -> {:lost, lost(reason, args)}
      true -> :other
    end
  end

  def down(nil, _ref, _reason, _args), do: :other

  # A channel the broker closed names its reply code and text; one its
  # connection took is the connection's failure.
  defp lost({:shutdown, {:server_initiated_close, code, text}}, _args),
    do: {:rabbitmq_channel_closed, %{code: code, text: to_string(text)}}

  defp lost({:shutdown, {:connection_closing, reason}}, args), do: connection_failed(args, reason)
  defp lost(reason, args), do: connection_failed(args, reason)

  defp connection_failed(args, reason),
    do: {:rabbitmq_connection_failed, %{host: args.host, reason: describe(reason)}}

  # The tag alone, or the broker's own text: a reason's payload may hold the
  # client's parameters.
  defp describe({:server_initiated_close, code, text}), do: "#{code} #{text}"
  defp describe({:shutdown, reason}), do: describe(reason)
  defp describe({:auth_failure, _text}), do: "auth_failure"
  defp describe(reason) when is_atom(reason), do: Atom.to_string(reason)
  defp describe(reason) when is_binary(reason), do: reason

  defp describe(reason) when is_tuple(reason) and is_atom(elem(reason, 0)),
    do: describe(elem(reason, 0))

  defp describe(_reason), do: "error"

  @doc "Stop watching `link` and close its connection; a connection already gone is fine."
  @spec close(map() | nil) :: :ok
  def close(nil), do: :ok

  def close(%{conn: conn, refs: refs}) do
    Enum.each(refs, &Process.demonitor(&1, [:flush]))

    try do
      AMQP.Connection.close(conn)
    catch
      :exit, _gone -> :ok
    end

    :ok
  end

  @doc "The wait before connect `attempt` (1-based) on `args`' curve, announced for `target`."
  @spec backoff(map(), pos_integer(), term()) :: non_neg_integer()
  def backoff(args, attempt, target),
    do: Knob.announce(:rabbitmq_reconnect, attempt, target, Knob.delay(args.backoff, attempt))
end
