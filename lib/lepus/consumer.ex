defmodule Lepus.Consumer do
  @moduledoc """
  Defines a consumer.
  Example usage:

      defmodule MyApp.MyConsumer do
        use Lepus.Consumer,
          exchange: "my_exchange",
          routing_key: "my_routing_key"

        # Read more here
        # https://hexdocs.pm/broadway_rabbitmq/BroadwayRabbitMQ.Producer.html
        # about producer options
        #
        # Read more here
        # https://hexdocs.pm/broadway/Broadway.html#start_link/2
        # about other options
        @impl Lepus.Consumer
        def options do
          [producer: [concurrency: 1], processors: [default: [concurrency: 1]]]
        end

        # Read more here
        # https://hexdocs.pm/broadway/Broadway.html#c:handle_message/3
        # aboud messages handling
        @impl Broadway
        def handle_message(_, message, _) do
          # do someting with message
          message
        end
      end

  """

  @callback options() :: Keyword.t()

  alias Lepus.RabbitClient

  defmacro __using__(opts) do
    strategy_data = Lepus.Consumer.build_strategy_data(opts)
    rabbit_client = opts |> Keyword.get(:rabbit_client, RabbitClient)

    broadway_producer_module =
      opts |> Keyword.get(:broadway_producer_module, BroadwayRabbitMQ.Producer)

    quote do
      use Broadway

      @behaviour Lepus.Consumer

      def start_link(opts \\ []) do
        Lepus.Consumer.start_link(
          __MODULE__,
          unquote(broadway_producer_module),
          unquote(rabbit_client),
          unquote(Macro.escape(strategy_data)),
          options(),
          opts
        )
      end

      @impl Broadway
      def handle_failed(messages, _) do
        Lepus.Consumer.handle_failed(
          unquote(rabbit_client),
          messages,
          unquote(strategy_data.delay_exchange)
        )

        messages
      end
    end
  end

  def build_strategy_data(opts) do
    exchange = opts |> Keyword.fetch!(:exchange)
    routing_key = opts |> Keyword.fetch!(:routing_key)

    %{
      exchange: exchange,
      routing_key: routing_key,
      delay_exchange: strategy_opt(opts, :delay_exchange, [exchange, "delay"]),
      retry_exchange: strategy_opt(opts, :retry_exchange, [exchange, "retry"]),
      queue: strategy_opt(opts, :queue, [exchange, routing_key]),
      retry_queue: strategy_opt(opts, :retry_queue, [exchange, routing_key, "retry"])
    }
  end

  defp strategy_opt(opts, key, default_list) do
    opts
    |> Keyword.get_lazy(key, fn ->
      default_list |> Enum.reject(&(&1 in ["", nil])) |> Enum.join(".")
    end)
  end

  def start_link(
        module,
        broadway_producer_module,
        rabbit_client,
        %{queue: queue} = strategy_data,
        internal_opts,
        external_opts
      ) do
    connection_opts =
      internal_opts
      |> Keyword.get(:producer, [])
      |> Keyword.get(:module, [])
      |> Keyword.fetch(:connection)
      |> case do
        {:ok, opts} -> opts
        _ -> external_opts |> Keyword.fetch!(:connection)
      end

    process_name =
      external_opts
      |> Keyword.get_lazy(:name, fn -> internal_opts |> Keyword.get(:name, module) end)

    modified_opts =
      internal_opts
      |> Keyword.put(:name, process_name)
      |> update_option(:producer, fn producer_opts ->
        producer_opts
        |> Keyword.put(:transformer, {__MODULE__, :transform, []})
        |> update_option(:module, fn producer_module_opts ->
          updated_producer_module_opts =
            producer_module_opts
            |> Keyword.put(:connection, connection_opts)
            |> Keyword.merge(on_failure: :ack, queue: queue)
            |> update_option(:metadata, fn metadata ->
              [[:headers, :routing_key, :content_type, :timestamp] | metadata]
              |> List.flatten()
              |> Enum.uniq()
            end)

          {broadway_producer_module, updated_producer_module_opts}
        end)
      end)

    declare_strategy(rabbit_client, connection_opts, strategy_data)

    Broadway.start_link(module, modified_opts)
  end

  def transform(%{metadata: %{content_type: content_type}} = message, _) do
    message |> Broadway.Message.update_data(&from_binary(&1, content_type))
  end

  defp update_option(keyword, key, fun) do
    keyword |> Keyword.put_new(key, []) |> Keyword.update!(key, fun)
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

  def handle_failed(rabbit_client, messages, delay_exchange) do
    with [%{acknowledger: {_, %{conn: connection}, _}} | _] <- messages do
      with_new_channel(rabbit_client, connection, fn channel ->
        messages |> Enum.each(&handle_failed_message(rabbit_client, &1, delay_exchange, channel))
      end)
    end

    messages
  end

  defp handle_failed_message(
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

  defp to_binary(data, "application/json") do
    case Jason.encode(data) do
      {:ok, encoded_data} -> encoded_data
      _ -> data
    end
  end

  defp to_binary(data, _), do: data
end
