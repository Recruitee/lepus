defmodule Lepus do
  @moduledoc """
  Defines a RabbitMQ client.
  Example usage:

      use Lepus,
        client: Lepus.Client,
        exchanges: ["my_exchange1", "my_exchange2"]

  """

  @callback publish(String.t(), String.t(), String.t(), keyword()) :: :ok | AMQP.error()
  @callback publish_json(String.t(), String.t(), map() | list(), keyword()) :: :ok | AMQP.error()

  def wrong_exchange(exchange, exchanges) do
    "#{inspect(exchange)} is not allowed. Only one of #{inspect(exchanges)} is allowed"
  end

  defmacro __using__(opts) do
    client = opts |> Keyword.fetch!(:client)
    exchanges = opts |> Keyword.fetch!(:exchanges)
    name = opts |> Keyword.get(:name, __CALLER__.module())

    quote do
      @behaviour Lepus

      def start_link(init_arg) do
        init_arg
        |> Keyword.take([:connection])
        |> Keyword.put_new(:connection, unquote(Macro.escape(Keyword.get(opts, :connection))))
        |> Keyword.put(:name, unquote(name))
        |> Keyword.put(:exchanges, unquote(exchanges))
        |> unquote(client).start_link()
      end

      def child_spec(init_arg) do
        %{
          id: __MODULE__,
          start: {__MODULE__, :start_link, [init_arg]},
          type: :supervisor
        }
      end

      @impl Lepus
      def publish(exchange, routing_key, payload, options \\ [])

      def publish(exchange, routing_key, payload, options)
          when exchange in unquote(exchanges) do
        unquote(client).publish(unquote(name), exchange, routing_key, payload, options)
      end

      def publish(exchange, _routing_key, _payload, _options) do
        ArgumentError |> raise(Lepus.wrong_exchange(exchange, unquote(exchanges)))
      end

      @impl Lepus
      def publish_json(exchange, routing_key, payload, options \\ [])

      def publish_json(exchange, routing_key, payload, options)
          when exchange in unquote(exchanges) do
        unquote(client).publish_json(unquote(name), exchange, routing_key, payload, options)
      end

      def publish_json(exchange, _routing_key, _payload, _options) do
        ArgumentError |> raise(Lepus.wrong_exchange(exchange, unquote(exchanges)))
      end
    end
  end
end
