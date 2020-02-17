defmodule Lepus.Client.Channel do
  use GenServer

  require Logger

  @timeout 1000

  def start_link(init_arg) do
    GenServer.start_link(__MODULE__, init_arg, name: via_tuple(init_arg))
  end

  def publish(registry_name, exchange, routing_key, payload, options) do
    via_tuple(registry_name, exchange)
    |> GenServer.call({:publish, routing_key, payload, options})
  end

  @impl GenServer
  def init(init_arg) do
    exchange = init_arg |> Keyword.fetch!(:exchange)
    connection_name = init_arg |> Keyword.fetch!(:connection_name)
    rabbit_client = init_arg |> Keyword.fetch!(:rabbit_client)

    {:ok,
     %{
       rabbit_client: rabbit_client,
       exchange: exchange,
       connection_name: connection_name,
       channel: nil
     }, {:continue, :open_channel_first}}
  end

  @impl GenServer
  def handle_continue(
        :open_channel_first,
        %{rabbit_client: rabbit_client, exchange: exchange, connection_name: connection_name} =
          state
      ) do
    channel = open_channel(rabbit_client, connection_name)
    rabbit_client.declare_direct_exchange(channel, exchange, durable: true)

    {:noreply, %{state | channel: channel}}
  end

  @impl GenServer
  def handle_call(
        {:publish, routing_key, payload, options},
        _ref,
        %{rabbit_client: rabbit_client, exchange: exchange, channel: channel} = state
      ) do
    result = rabbit_client.publish(channel, exchange, routing_key, payload, options)
    {:reply, result, state}
  end

  @impl GenServer
  def handle_info(
        :open_channel,
        %{rabbit_client: rabbit_client, connection_name: connection_name} = state
      ) do
    {:noreply, %{state | channel: open_channel(rabbit_client, connection_name)}}
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

  defp open_channel(rabbit_client, connection_name) do
    connection_name
    |> Lepus.Client.Connection.get_conn()
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

  defp via_tuple(keyword) when is_list(keyword) do
    registry_name = keyword |> Keyword.fetch!(:registry_name)
    exchange = keyword |> Keyword.fetch!(:exchange)
    via_tuple(registry_name, exchange)
  end

  defp via_tuple(registry_name, exchange) do
    {:via, Registry, {registry_name, exchange}}
  end
end
