defmodule Lepus.Consumer do
  @moduledoc """
  Defines a consumer.
  Usage

      defmodule MyConsumer do
        use Lepus.Consumer

        def options do
          [
            connection: [
              host: "localhost",
              port: 5672,
              virtual_host: "/",
              username: "guest",
              password: "guest"
            ],
            exchange: "my_exchange",
            routing_key: "my_routing_key"
            processor: [concurrency: 10]
          ]
        end
      end

  ## Options:

    * `:connection` - Required. AMQP URI or a set of options used by the RabbitMQ client.
      See `AMQP.Connection.open/1` for the full list of options.
    * `:exchange` - Required. RabbitMQ exchange name.
    * `:routing_key` - Required. RabbitMQ routing key name.
    * `:delay_exchange` - Optional. Redefines delay exchange name.
      Default value is `"my_exchange.delay"`
    * `:retry_exchange` - Optional. Redefines retry exchange name.
      Default value is `"my_exchange.retry"`
    * `:queue` - Optional. Redefines queue name.
      Default value is `"my_exchange.my_routing_key"`
    * `:retry_queue` - Optional. Redefines retry queue name.
      Default value is `"my_exchange.my_routing_key.retry"`
    * `processor`  - Optional. Broadway processor options.

  """

  @callback options() :: Keyword.t()
  @callback handle_message(any(), map()) :: :ok | {:error, String.t()}
  @callback handle_failed(any(), map()) :: any()

  @optional_callbacks handle_failed: 2

  alias Lepus.RabbitClient

  defmacro __using__(_) do
    quote do
      @behaviour Lepus.Consumer

      def start_link(opts \\ []) do
        built_opts = __MODULE__ |> Lepus.Consumer.build_all_opts(opts)
        __MODULE__ |> Lepus.Broadway.start_link(built_opts)
      end

      def child_spec(opts) do
        %{
          id: __MODULE__,
          shutdown: :infinity,
          start: {__MODULE__, :start_link, [opts]}
        }
      end
    end
  end

  def build_all_opts(consumer_module, dynamic_opts) do
    static_opts = consumer_module.options()

    %{
      connection: fetch_opt!(static_opts, dynamic_opts, :connection),
      private_data: build_private_data(static_opts, dynamic_opts),
      strategy_data: build_strategy_data(static_opts, dynamic_opts),
      processor: build_processor_opts(static_opts, dynamic_opts)
    }
  end

  def build_strategy_data(static_opts, dynamic_opts) do
    exchange = fetch_opt!(static_opts, dynamic_opts, :exchange)
    routing_key = fetch_opt!(static_opts, dynamic_opts, :routing_key)

    %{
      exchange: exchange,
      routing_key: routing_key,
      delay_exchange:
        strategy_opt(static_opts, dynamic_opts, :delay_exchange, [exchange, "delay"]),
      retry_exchange:
        strategy_opt(static_opts, dynamic_opts, :retry_exchange, [exchange, "retry"]),
      queue: strategy_opt(static_opts, dynamic_opts, :queue, [exchange, routing_key]),
      retry_queue:
        strategy_opt(static_opts, dynamic_opts, :retry_queue, [exchange, routing_key, "retry"])
    }
  end

  defp build_private_data(static_opts, dynamic_opts) do
    %{
      broadway_producer_module:
        get_opt(static_opts, dynamic_opts, :broadway_producer_module, BroadwayRabbitMQ.Client),
      rabbit_client: get_opt(static_opts, dynamic_opts, :rabbit_client, RabbitClient),
      name: get_opt(static_opts, dynamic_opts, :name, nil)
    }
  end

  defp build_processor_opts(static_opts, dynamic_opts) do
    processor_opts = get_opt(static_opts, dynamic_opts, :processor, [])
    [concurrency: 1] |> Keyword.merge(processor_opts)
  end

  defp strategy_opt(static_opts, dynamic_opts, key, default_list) do
    case fetch_opt(static_opts, dynamic_opts, key) do
      {:ok, value} -> value
      _ -> default_list |> Enum.reject(&(&1 in ["", nil])) |> Enum.join(".")
    end
  end

  defp fetch_opt(static_opts, dynamic_opts, key) do
    [dynamic_opts, static_opts]
    |> Enum.reduce_while(:error, fn opts, acc ->
      opts
      |> Keyword.fetch(key)
      |> case do
        {:ok, value} -> {:halt, {:ok, value}}
        _ -> {:cont, acc}
      end
    end)
  end

  defp fetch_opt!(static_opts, dynamic_opts, key) do
    case fetch_opt(static_opts, dynamic_opts, key) do
      {:ok, value} -> value
      _ -> raise "option #{inspect(key)} is required"
    end
  end

  defp get_opt(static_opts, dynamic_opts, key, default) do
    case fetch_opt(static_opts, dynamic_opts, key) do
      {:ok, value} -> value
      _ -> default
    end
  end
end
