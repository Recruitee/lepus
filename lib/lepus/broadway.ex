defmodule Lepus.Broadway do
  @moduledoc false

  use Broadway

  def start_link(consumer_module, opts) do
    %{
      private_data: %{
        broadway_producer_module: broadway_producer_module,
        rabbit_client: rabbit_client,
        name: name
      },
      strategy_data: strategy_data,
      connection: connection_opts,
      processor: processor_opts
    } = opts

    declare_strategy(rabbit_client, connection_opts, strategy_data)

    Broadway.start_link(__MODULE__,
      name: name || consumer_module,
      producer: [
        module:
          {broadway_producer_module,
           [
             on_failure: :ack,
             connection: connection_opts,
             queue: strategy_data.queue,
             metadata: [:headers, :routing_key, :content_type, :timestamp]
           ]},
        transformer: {__MODULE__, :transform, []}
      ],
      processors: [default: processor_opts],
      context: %{
        rabbit_client: rabbit_client,
        delay_exchange: strategy_data.delay_exchange,
        consumer_module: consumer_module
      }
    )
  end

  def transform(%{metadata: %{content_type: content_type}} = message, _) do
    message |> Broadway.Message.update_data(&from_binary(&1, content_type))
  end

  @impl Broadway
  def handle_message(_, message, context) do
    %{consumer_module: consumer_module} = context
    %{data: data, metadata: metadata} = message

    case consumer_module.handle_message(data, metadata) do
      :ok -> message
      {:error, error} -> message |> Broadway.Message.failed(error)
    end
  end

  @impl Broadway
  def handle_failed(messages, context) do
    %{
      rabbit_client: rabbit_client,
      delay_exchange: delay_exchange,
      consumer_module: consumer_module
    } = context

    with [%{acknowledger: {_, %{conn: connection}, _}} | _] <- messages do
      with_new_channel(rabbit_client, connection, fn channel ->
        messages
        |> Enum.map(&{&1, retry_failed_message(rabbit_client, &1, delay_exchange, channel)})
        |> Enum.each(fn {%{data: data, metadata: metadata}, retry_number} ->
          consumer_module.handle_failed(data, metadata, retry_number)
        end)
      end)
    end

    messages
  end

  defp retry_failed_message(
         rabbit_client,
         %{
           data: data,
           metadata:
             %{headers: headers, routing_key: routing_key, content_type: content_type} = meta
         },
         delay_exchange,
         channel
       ) do
    payload = data |> to_binary(content_type)

    retry_number = get_header_value(headers, "x-retries", 0) + 1
    new_headers = headers |> update_headers([{"x-retries", :long, retry_number}])

    opts =
      meta
      |> Map.put(:headers, new_headers)
      |> Map.put(:expiration, expiration_prop(retry_number))
      |> Map.to_list()

    rabbit_client.publish(channel, delay_exchange, routing_key, payload, opts)

    retry_number
  end

  defp expiration_prop(retry_number) do
    expiration_sec = :math.pow(2, retry_number) |> Kernel.trunc()
    "#{expiration_sec * 1000}"
  end

  defp get_header_value(headers, name, default) when is_list(headers) do
    headers
    |> Enum.split_with(fn
      {^name, _, _} -> true
      _ -> false
    end)
    |> case do
      {[{_, _, value} | _], _} -> value
      _ -> default
    end
  end

  defp get_header_value(:undefined, _, default), do: default

  defp update_headers(headers, tuples) when is_list(headers) do
    [tuples | headers] |> List.flatten() |> Enum.uniq_by(fn {k, _, _} -> k end)
  end

  defp update_headers(:undefined, tuples), do: tuples

  defp to_binary(data, "application/json") do
    case Jason.encode(data) do
      {:ok, encoded_data} -> encoded_data
      _ -> data
    end
  end

  defp to_binary(data, _), do: data

  defp declare_strategy(rabbit_client, connection_opts, %{
         exchange: exchange,
         routing_key: routing_key,
         delay_exchange: delay_exchange,
         retry_exchange: retry_exchange,
         queue: queue,
         retry_queue: retry_queue
       }) do
    with_new_connection(rabbit_client, connection_opts, fn channel ->
      [exchange, delay_exchange, retry_exchange]
      |> Enum.each(&rabbit_client.declare_direct_exchange(channel, &1, durable: true))

      rabbit_client.declare_queue(channel, queue, durable: true)

      rabbit_client.declare_queue(channel, retry_queue,
        durable: true,
        arguments: [{"x-dead-letter-exchange", :longstr, retry_exchange}]
      )

      [{queue, exchange}, {queue, retry_exchange}, {retry_queue, delay_exchange}]
      |> Enum.each(fn {q, ex} ->
        rabbit_client.bind_queue(channel, q, ex, routing_key: routing_key)
      end)
    end)
  end

  defp with_new_connection(rabbit_client, connection_opts, fun) do
    {:ok, connection} = rabbit_client.open_connection(connection_opts)
    with_new_channel(rabbit_client, connection, fun)
    rabbit_client.close_connection(connection)
  end

  defp with_new_channel(rabbit_client, connection, fun) do
    {:ok, channel} = rabbit_client.open_channel(connection)
    fun.(channel)
    rabbit_client.close_channel(channel)
  end

  defp from_binary(data, "application/json") when is_binary(data) do
    case Jason.decode(data) do
      {:ok, decoded_data} -> decoded_data
      _ -> data
    end
  end

  defp from_binary(data, _), do: data
end
