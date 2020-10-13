defmodule Lepus.BasicClient.ConnectionServer do
  @moduledoc false

  alias Lepus.BasicClient.ServerNames

  use GenServer

  require Logger

  @timeout 1000

  def start_link(init_arg) do
    name = init_arg |> Keyword.fetch!(:client_name) |> ServerNames.connection_server()
    GenServer.start_link(__MODULE__, init_arg, name: name)
  end

  def get_conn(name) do
    GenServer.call(name, :get_conn)
  end

  @impl GenServer
  def init(opts) do
    conn_opts = opts |> Keyword.fetch!(:conn_opts)
    rabbit_client = opts |> Keyword.fetch!(:rabbit_client)

    {:ok,
     %{
       conn_opts: conn_opts,
       rabbit_client: rabbit_client,
       conn: connect(rabbit_client, conn_opts)
     }}
  end

  @impl GenServer
  def handle_call(:get_conn, _ref, %{conn: %{} = conn} = state) do
    {:reply, conn, state}
  end

  @impl GenServer
  def handle_call(:get_conn, _ref, %{rabbit_client: rabbit_client, conn_opts: conn_opts} = state) do
    conn = connect(rabbit_client, conn_opts)
    {:reply, conn, %{state | conn: conn}}
  end

  @impl GenServer
  def handle_info(:connect, %{rabbit_client: rabbit_client, conn_opts: conn_opts} = state) do
    {:noreply, %{state | conn: connect(rabbit_client, conn_opts)}}
  end

  @impl GenServer
  # if conn process dies
  def handle_info({:DOWN, _, :process, conn_pid, reason}, %{conn: %{pid: conn_pid}} = state) do
    {:stop, {:connection_lost, reason}, state}
  end

  @impl GenServer
  def terminate(_, %{rabbit_client: rabbit_client, conn: conn}) do
    rabbit_client.close_connection(conn)
  end

  defp connect(rabbit_client, conn_opts) do
    case rabbit_client.open_connection(conn_opts) do
      {:ok, conn} ->
        Process.monitor(conn.pid)
        conn

      {:error, err} ->
        Process.send_after(self(), :connect, @timeout)
        Logger.error("Connecting to broker failed: #{inspect(err)}")
        nil
    end
  end
end
