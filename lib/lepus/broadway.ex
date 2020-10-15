defmodule Lepus.Broadway do
  @moduledoc false

  alias Lepus.BroadwayHelpers

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
        module: {
          broadway_producer_module,
          on_failure: :ack,
          connection: connection_opts,
          queue: strategy_data.queue,
          metadata: [
            :content_type,
            :correlation_id,
            :headers,
            :reply_to,
            :routing_key,
            :timestamp
          ]
        }
      ],
      processors: [default: processor_opts],
      context: %{
        rabbit_client: rabbit_client,
        delay_exchange: strategy_data.delay_exchange,
        consumer_module: consumer_module
      }
    )
  end

  @impl Broadway
  def handle_message(_, message, context) do
    message
    |> handle_by_consumer(context)
    |> case do
      :ok ->
        message

      {:ok, response} ->
        publish_success(message, response, context)
        message

      {:error, error} ->
        message |> Broadway.Message.failed(error)
    end
  end

  @impl Broadway
  def handle_failed(messages, context) do
    {sync_messages, async_messages} = messages |> Enum.split_with(&sync_message?/1)

    context.rabbit_client
    |> with_new_channel(get_connection(messages), fn channel ->
      sync_messages |> Enum.each(&publish_error(&1, channel, context))

      async_messages
      |> Enum.map(&{&1, retry_failed_message(&1, channel, context)})
      |> call_consumer_failed_callback(context)
    end)

    messages
  end

  defp handle_by_consumer(message, context) do
    %{data: payload} = message |> BroadwayHelpers.maybe_decode_json()
    %{consumer_module: consumer_module} = context
    consumer_metadata = build_consumer_metadata(message)

    {consumer_metadata, consumer_module.handle_message(payload, consumer_metadata)}
    |> case do
      {_, :error} -> {:error, ""}
      {_, {:error, error}} -> {:error, error}
      {%{sync: true}, :ok} -> {:ok, ""}
      {%{sync: true}, {:ok, response}} -> {:ok, response}
      _ -> :ok
    end
  end

  defp publish_success(message, response, context) do
    %{rabbit_client: rabbit_client} = context

    with_new_channel(rabbit_client, get_connection(message), fn channel ->
      message |> publish_response(channel, response, "ok", context)
    end)
  end

  defp publish_error(message, channel, context) do
    %{status: status} = message

    response =
      case status do
        {_, response} -> response
        {_, response, _} -> inspect(response)
        _ -> ""
      end

    message |> publish_response(channel, response, "error", context)
  end

  defp publish_response(message, channel, response, status, context) do
    %{rabbit_client: rabbit_client} = context
    {reply_to, correlation_id} = message |> get_sync_params()
    content_type = message |> get_content_type()
    response = message |> BroadwayHelpers.maybe_encode_json(response)

    opts = [
      correlation_id: correlation_id,
      content_type: content_type,
      headers: [{"x-reply-status", :binary, status}]
    ]

    opts =
      message
      |> BroadwayHelpers.get_header_value("x-reply-timeout", :infinity)
      |> case do
        expiration when is_integer(expiration) -> opts |> Keyword.put(:expiration, expiration)
        _ -> opts
      end

    channel |> rabbit_client.publish("", reply_to, response, opts)
  end

  defp call_consumer_failed_callback(messages_and_retries, context) do
    %{consumer_module: consumer_module} = context

    if function_exported?(consumer_module, :handle_failed, 2) do
      messages_and_retries
      |> Enum.each(fn {message, retries_count} ->
        %{data: payload, status: status} = message |> BroadwayHelpers.maybe_decode_json()

        consumer_metadata =
          build_consumer_metadata(message)
          |> Map.put(:status, status)
          |> Map.put(:retries_count, retries_count)

        consumer_module.handle_failed(payload, consumer_metadata)
      end)
    end
  end

  defp retry_failed_message(message, channel, context) do
    %{
      data: payload,
      metadata: %{headers: headers, routing_key: routing_key} = meta
    } = message

    %{rabbit_client: rabbit_client, delay_exchange: delay_exchange} = context
    retry_number = BroadwayHelpers.get_header_value(headers, "x-retries", 0) + 1
    new_headers = headers |> BroadwayHelpers.update_headers([{"x-retries", :long, retry_number}])

    opts =
      meta
      |> Map.put(:headers, new_headers)
      |> Map.put(:expiration, expiration_prop(retry_number))
      |> Map.drop([:reply_to, :correlation_id])
      |> Map.to_list()

    rabbit_client.publish(channel, delay_exchange, routing_key, payload, opts)

    retry_number
  end

  defp expiration_prop(retry_number) do
    expiration_sec = :math.pow(2, retry_number) |> Kernel.trunc()
    "#{expiration_sec * 1000}"
  end

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

  defp sync_message?(message) do
    {reply_to, correlation_id} = get_sync_params(message)
    [reply_to, correlation_id] |> Enum.all?(&(is_binary(&1) and &1 != ""))
  end

  defp get_sync_params(%{metadata: %{reply_to: reply_to, correlation_id: correlation_id}}) do
    {reply_to, correlation_id}
  end

  defp get_sync_params(_message), do: {nil, nil}

  defp get_connection(%{acknowledger: {_, %{conn: connection}, _}}), do: connection
  defp get_connection([message | _]), do: get_connection(message)

  defp get_content_type(%{metadata: %{content_type: content_type}}), do: content_type

  defp build_consumer_metadata(message) do
    %{metadata: rabbit_mq_metadata} = message

    %{
      sync: sync_message?(message),
      rabbit_mq_metadata: rabbit_mq_metadata,
      retries_count: BroadwayHelpers.get_header_value(rabbit_mq_metadata, "x-retries", 0),
      client: BroadwayHelpers.get_header_value(rabbit_mq_metadata, "x-client", nil)
    }
  end
end
