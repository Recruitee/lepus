defmodule Lepus.Consumer.Broadway do
  @moduledoc false

  alias Lepus.BroadwayHelpers
  alias Lepus.Consumer.QueuesTopology

  use Broadway

  @spec start_link(module, keyword) :: :ignore | {:error, any} | {:ok, pid}
  def start_link(consumer_module, opts) do
    Broadway.start_link(__MODULE__,
      name: opts |> Keyword.get(:name, consumer_module),
      producer: [
        module: {
          Keyword.fetch!(opts, :broadway_producer_module),
          after_connect: opts |> QueuesTopology.declare_function(),
          on_failure: :ack,
          connection: opts |> Keyword.fetch!(:connection),
          queue: opts |> Keyword.fetch!(:queue),
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
      processors: [default: opts |> Keyword.fetch!(:processor)],
      context: %{
        consumer_module: consumer_module,
        rabbit_client: opts |> Keyword.fetch!(:rabbit_client),
        delay_exchange: opts |> Keyword.fetch!(:delay_exchange),
        failed_exchange: opts |> Keyword.fetch!(:failed_exchange),
        max_retry_count: opts |> Keyword.fetch!(:max_retry_count),
        store_failed: opts |> Keyword.fetch!(:store_failed)
      }
    )
  end

  @impl Broadway
  def prepare_messages(messages, context) do
    messages
    |> Enum.map(fn message ->
      retry_count = get_retry_count(message)
      rpc? = rpc?(message)
      retriable = not rpc? and retry_count < context.max_retry_count
      client = BroadwayHelpers.get_header_value(message, "x-client", nil)

      message
      |> BroadwayHelpers.set_lepus_metadata(%{
        rpc: rpc?,
        retry_count: retry_count,
        retriable: retriable,
        client: client
      })
    end)
  end

  @impl Broadway
  def handle_message(_, %{metadata: %{lepus: %{rpc: true}}} = message, context) do
    message
    |> handle_by_consumer(context)
    |> case do
      {:ok, _} = term -> term
      {:error, _} = term -> term
      :error -> {:error, ""}
      _ -> {:ok, ""}
    end
    |> case do
      {:ok, response} ->
        message |> publish_success(response, context)
        message

      {:error, error} ->
        message |> Broadway.Message.failed(error)
    end
  end

  def handle_message(_, message, context) do
    message
    |> handle_by_consumer(context)
    |> case do
      {:error, _} = term -> term
      :error -> {:error, ""}
      {:retry, _} = term -> term
      :retry -> {:retry, ""}
      _ -> :ok
    end
    |> case do
      :ok ->
        message

      {:retry, reason} ->
        message
        |> BroadwayHelpers.put_in_lepus_metadata(:retriable, true)
        |> Broadway.Message.failed(reason)

      {:error, error} ->
        message |> Broadway.Message.failed(error)
    end
  end

  @impl Broadway
  def handle_failed(messages, context) do
    messages
    |> Enum.each(fn message ->
      handle_failed_message(message, context, message.metadata.lepus)
    end)

    messages
  end

  defp handle_failed_message(message, context, %{rpc: true}), do: publish_error(message, context)
  defp handle_failed_message(message, context, %{retriable: true}), do: retry(message, context)

  defp handle_failed_message(message, %{store_failed: true} = context, _lepus_metadata) do
    store_failed(message, context)
  end

  defp handle_failed_message(_message, _context, _lepus_metadata), do: :ok

  defp handle_by_consumer(message, context) do
    %{data: payload} = message |> BroadwayHelpers.maybe_decode_json()
    consumer_metadata = message |> build_consumer_metadata()
    %{consumer_module: consumer_module} = context

    message
    |> case do
      %{metadata: %{lepus: %{retriable: true}}} ->
        try do
          consumer_module.handle_message(payload, consumer_metadata)
        rescue
          error -> {:error, inspect(error)}
        end

      _ ->
        consumer_module.handle_message(payload, consumer_metadata)
    end
  end

  defp publish_success(message, response, context) do
    message |> publish_response(response, "ok", context)
  end

  defp publish_error(message, context) do
    response =
      message
      |> case do
        %{status: {_, response}} -> response
        %{status: {_, response, _}} -> inspect(response)
        _ -> ""
      end

    message |> publish_response(response, "error", context)
  end

  defp publish_response(message, response, status, context) do
    %{metadata: metadata} = message
    response = message |> BroadwayHelpers.maybe_encode_json(response)

    opts =
      message
      |> BroadwayHelpers.get_header_value("x-reply-timeout", :infinity)
      |> case do
        expiration when is_integer(expiration) and expiration > 0 -> [expiration: "#{expiration}"]
        _ -> []
      end
      |> Keyword.merge(
        correlation_id: metadata.correlation_id,
        content_type: metadata.content_type,
        headers: [{"x-reply-status", :binary, status}]
      )

    message.metadata.amqp_channel
    |> context.rabbit_client.publish("", metadata.reply_to, response, opts)
  end

  defp retry(message, context) do
    %{metadata: %{lepus: %{retry_count: retry_count}} = metadata} = message

    new_headers =
      metadata.headers
      |> BroadwayHelpers.update_headers([{"x-retries", :long, retry_count + 1}])
      |> put_status_to_headers(message)

    opts =
      metadata
      |> Map.take([:content_type, :routing_key, :timestamp])
      |> Map.put(:headers, new_headers)
      |> Map.put(:expiration, expiration_prop(retry_count))
      |> Map.to_list()

    metadata.amqp_channel
    |> context.rabbit_client.publish(
      context.delay_exchange,
      metadata.routing_key,
      message.data,
      opts
    )
  end

  defp store_failed(message, context) do
    %{metadata: metadata} = message
    new_headers = metadata.headers |> put_status_to_headers(message)

    opts =
      metadata
      |> Map.take([:content_type, :routing_key, :timestamp])
      |> Map.put(:headers, new_headers)
      |> Map.to_list()

    metadata.amqp_channel
    |> context.rabbit_client.publish(
      context.failed_exchange,
      metadata.routing_key,
      message.data,
      opts
    )
  end

  defp put_status_to_headers(headers, message) do
    message.status
    |> case do
      {_, ""} -> :error
      {_, binary} when is_binary(binary) -> {:ok, binary}
      {_, term} -> {:ok, inspect(term)}
      {_, binary, _} when is_binary(binary) -> {:ok, binary}
      {_, term, _} -> {:ok, inspect(term)}
      term -> {:ok, inspect(term)}
    end
    |> case do
      {:ok, status} ->
        headers
        |> BroadwayHelpers.update_headers([
          {"x-status-#{message.metadata.lepus.retry_count}", :binary,
           String.slice(status, 0, 1000)}
        ])

      _ ->
        headers
    end
  end

  defp expiration_prop(retry_number) do
    expiration_sec = :math.pow(2, retry_number) |> Kernel.trunc()
    "#{expiration_sec * 1000}"
  end

  defp rpc?(message) do
    %{metadata: %{reply_to: reply_to, correlation_id: correlation_id}} = message
    [reply_to, correlation_id] |> Enum.all?(&(is_binary(&1) and &1 != ""))
  end

  defp get_retry_count(message_or_metadata_or_headers) do
    message_or_metadata_or_headers
    |> BroadwayHelpers.get_header_value("x-retries", 0)
    |> case do
      non_negat_int when is_integer(non_negat_int) and non_negat_int >= 0 -> non_negat_int
      _ -> 0
    end
  end

  defp build_consumer_metadata(message) do
    {%{lepus: consumer_metadata}, rabbit_mq_metadata} = message.metadata |> Map.split([:lepus])
    consumer_metadata |> Map.put(:rabbit_mq_metadata, rabbit_mq_metadata)
  end
end
