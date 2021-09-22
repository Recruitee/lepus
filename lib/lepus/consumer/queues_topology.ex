defmodule Lepus.Consumer.QueuesTopology do
  @moduledoc false

  alias Lepus.Rabbit

  @spec declare_function(keyword) :: (Rabbit.Client.channel() -> :ok | {:error, any})
  def declare_function(opts) do
    functions =
      [&declare_base_topology/2]
      |> maybe_add_retry_topology(opts)
      |> maype_add_failed_topology(opts)
      |> Enum.reverse()

    fn channel ->
      functions |> run(channel, opts)
    end
  end

  defp run(functions, channel, opts) do
    functions
    |> Enum.reduce_while(:ok, fn function, _ ->
      channel
      |> function.(opts)
      |> case do
        :ok -> {:cont, :ok}
        error -> {:halt, error}
      end
    end)
  end

  defp maype_add_failed_topology(functions, opts) do
    opts
    |> Keyword.fetch!(:store_failed)
    |> case do
      true -> [(&declare_failed_topology/2) | functions]
      _ -> functions
    end
  end

  defp maybe_add_retry_topology(functions, opts) do
    opts
    |> Keyword.fetch!(:max_retry_count)
    |> case do
      count when count > 0 -> [(&declare_retry_topology/2) | functions]
      _ -> functions
    end
  end

  defp declare_base_topology(channel, opts) do
    [rabbit_client, exchange, routing_key, queue] =
      opts |> get_options([:rabbit_client, :exchange, :routing_key, :queue])

    with :ok <- channel |> rabbit_client.declare_direct_exchange(exchange, durable: true),
         :ok <- channel |> rabbit_client.declare_queue(queue, durable: true) do
      channel |> rabbit_client.bind_queue(queue, exchange, routing_key: routing_key)
    else
      error -> error
    end
  end

  defp declare_retry_topology(channel, opts) do
    [rabbit_client, routing_key, delay_exchange, retry_exchange, queue, retry_queue] =
      [:rabbit_client, :routing_key, :delay_exchange, :retry_exchange, :queue, :retry_queue]
      |> Enum.map(&Keyword.fetch!(opts, &1))

    with :ok <- channel |> rabbit_client.declare_direct_exchange(delay_exchange, durable: true),
         :ok <- channel |> rabbit_client.declare_direct_exchange(retry_exchange, durable: true),
         :ok <-
           channel
           |> rabbit_client.declare_queue(retry_queue,
             durable: true,
             arguments: [{"x-dead-letter-exchange", :longstr, retry_exchange}]
           ),
         :ok <-
           channel |> rabbit_client.bind_queue(queue, retry_exchange, routing_key: routing_key) do
      channel |> rabbit_client.bind_queue(retry_queue, delay_exchange, routing_key: routing_key)
    else
      error -> error
    end
  end

  defp declare_failed_topology(channel, opts) do
    [rabbit_client, routing_key, failed_exchange, failed_queue] =
      [:rabbit_client, :routing_key, :failed_exchange, :failed_queue]
      |> Enum.map(&Keyword.fetch!(opts, &1))

    with :ok <- channel |> rabbit_client.declare_direct_exchange(failed_exchange, durable: true),
         :ok <- channel |> rabbit_client.declare_queue(failed_queue, durable: true) do
      channel |> rabbit_client.bind_queue(failed_queue, failed_exchange, routing_key: routing_key)
    else
      error -> error
    end
  end

  defp get_options(opts, keys), do: keys |> Enum.map(&Keyword.fetch!(opts, &1))
end
