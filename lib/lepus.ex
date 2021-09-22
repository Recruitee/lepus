defmodule Lepus do
  @moduledoc """
  Defines a RabbitMQ client.
  Example usage:

      use Lepus, client: Lepus.BasicClient
  """
  alias Lepus.Client

  @callback publish(String.t(), String.t(), String.t(), keyword()) :: Client.response()
  @callback publish_json(String.t(), String.t(), map() | list(), keyword()) :: Client.response()
  @optional_callbacks publish: 4, publish_json: 4

  defmacro __using__(opts) do
    client = opts |> Keyword.fetch!(:client)
    name = opts |> Keyword.get(:name, __CALLER__.module)

    quote do
      alias Lepus.Client

      @behaviour Lepus

      @spec publish(String.t(), String.t(), any(), keyword()) :: Client.response()
      def publish(exchange, routing_key, payload, options \\ []) do
        unquote(client).publish(unquote(name), exchange, routing_key, payload, options)
      end

      @spec publish_json(String.t(), String.t(), map() | list(), keyword()) :: Client.response()
      def publish_json(exchange, routing_key, payload, options \\ []) do
        unquote(client).publish_json(unquote(name), exchange, routing_key, payload, options)
      end
    end
  end
end
