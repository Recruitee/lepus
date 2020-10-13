defmodule Lepus.BasicClient.ChannelServer do
  @moduledoc false

  alias Lepus.BasicClient.ConnectionServer
  alias Lepus.BasicClient.ServerNames

  use GenServer

  require Logger

  @timeout 1000

  def start_link(init_arg) do
    client_name = init_arg |> Keyword.fetch!(:client_name)
    exchange = init_arg |> Keyword.fetch!(:exchange)
    name = client_name |> ServerNames.channel_server(exchange)

    GenServer.start_link(__MODULE__, init_arg, name: name)
  end

  def publish(client_name, exchange, routing_key, payload, amqp_opts) do
    client_name
    |> ServerNames.channel_server(exchange)
    |> GenServer.call({:publish, routing_key, payload, amqp_opts})
  end

  @impl GenServer
  def init(init_arg) do
    client_name = init_arg |> Keyword.fetch!(:client_name)
    exchange = init_arg |> Keyword.fetch!(:exchange)
    rabbit_client = init_arg |> Keyword.fetch!(:rabbit_client)

    {:ok,
     %{
       rabbit_client: rabbit_client,
       exchange: exchange,
       client_name: client_name,
       channel: nil
     }, {:continue, :open_channel_first}}
  end

  @impl GenServer
  def handle_continue(
        :open_channel_first,
        %{rabbit_client: rabbit_client, exchange: exchange, client_name: client_name} = state
      ) do
    channel = open_channel(rabbit_client, client_name)
    rabbit_client.declare_direct_exchange(channel, exchange, durable: true)

    {:noreply, %{state | channel: channel}}
  end

  @impl GenServer
  def handle_call(
        {:publish, routing_key, payload, amqp_opts},
        _ref,
        %{rabbit_client: rabbit_client, exchange: exchange, channel: channel} = state
      ) do
    result = rabbit_client.publish(channel, exchange, routing_key, payload, amqp_opts)
    {:reply, result, state}
  end

  @impl GenServer
  def handle_info(
        :open_channel,
        %{rabbit_client: rabbit_client, client_name: client_name} = state
      ) do
    {:noreply, %{state | channel: open_channel(rabbit_client, client_name)}}
  end

  @impl GenServer
  def handle_info({:DOWN, _, :process, channel_pid, reason}, %{channel: %{pid: channel_pid}}) do
    {:stop, {:connection_lost, reason}, nil}
  end

  @impl GenServer
  def terminate(_reason, %{rabbit_client: rabbit_client, channel: channel})
      when not is_nil(channel) do
    rabbit_client.close_channel(channel)
  end

  @impl GenServer
  def terminate(_reason, _state), do: nil

  defp open_channel(rabbit_client, client_name) do
    client_name
    |> ServerNames.connection_server()
    |> ConnectionServer.get_conn()
    |> rabbit_client.open_channel()
    |> case do
      {:ok, channel} ->
        Process.monitor(channel.pid)
        channel

      {:error, err} ->
        Process.send_after(self(), :open_channel, @timeout)
        Logger.error("Connecting to channel failed: #{inspect(err)}")
        nil
    end
  end
end
