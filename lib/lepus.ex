defmodule Lepus do
  @moduledoc """
  Defines a RabbitMQ client.
  Example usage:

      use Lepus, client: Lepus.BasicClient
  """

  @callback publish(String.t(), String.t(), String.t(), keyword()) :: :ok | AMQP.Basic.error()
  @callback publish_json(String.t(), String.t(), map() | list(), keyword()) ::
              :ok | AMQP.Basic.error()
  @optional_callbacks publish: 4, publish_json: 4

  def wrong_exchange(exchange, exchanges) do
    "#{inspect(exchange)} is not allowed. Only one of #{inspect(exchanges)} is allowed"
  end

  defmacro __using__(opts) do
    client = opts |> Keyword.fetch!(:client)
    name = opts |> Keyword.get(:name, __CALLER__.module())

    quote do
      @behaviour Lepus

      @spec publish(String.t(), String.t(), String.t(), keyword()) :: :ok | AMQP.Basic.error()
      def publish(exchange, routing_key, payload, options \\ []) do
        unquote(client).publish(unquote(name), exchange, routing_key, payload, options)
      end

      @spec publish_json(String.t(), String.t(), map() | list(), keyword()) ::
              :ok | AMQP.Basic.error()
      def publish_json(exchange, routing_key, payload, options \\ []) do
        unquote(client).publish_json(unquote(name), exchange, routing_key, payload, options)
      end
    end
  end
end
