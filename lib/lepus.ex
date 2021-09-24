defmodule Lepus do
  @moduledoc """
  Defines a RabbitMQ client.

  ## Example usage

      defmodule MyApp.RabbitMQ do
        use Lepus, client: Lepus.BasicClient
      end

  Add `Lepus.BasicClient` to your supervision tree (see the `Lepus.BasicClient` documentation).

  Use it to publish a message to Rabbit MQ:

      MyApp.RabbitMQ.publish_json("my-exchange", "my-routing-key", %{a: 1, b: 2})

  ## Testing

  For testing purposes you can use another `:client`.
  It could be [Mox](https://hexdocs.pm/mox/Mox.html) mock.


      defmodule MyApp.RabbitMQ do
        use Lepus, client: Application.get_env(:my_app, __MODULE__, []) |> Keyword.fetch!(:client)
      end

  In the `prod.exs` or `dev.exs`:

      config :my_app, MyApp.RabbitMQ, client: Lepus.BasicClient

  In the `test.exs`:

      config :my_app, MyApp.RabbitMQ, client: MyApp.RabbitMQClientMock

  In the tests:

      test "uses Mox for Lepus" do
        MyApp.RabbitMQClientMock
        |> expect(:publish_json, fn _, "my-exchange", "my-routing-key", payload, _opts ->
          assert %{my_key: "My Value"} = payload
        end)

        MyApp.RabbitMQ.publish_json("my-exchange", "my-routing-key", %{my_key: "My Value"})
      end
  """
  alias Lepus.Client

  @doc """
  Publishes message to RabbitMQ exchange.

  ## Options

  * `amqp_opts` - `AMQP.Basic.publish/5` options.
  * `rpc` - Defines if the message uses
    [RPC](https://www.rabbitmq.com/tutorials/tutorial-six-elixir.html) pattern.
    The default value is `false`.
  * `timeout` - number of milliseconds or `:infinity`.
    Valid for [RPC](https://www.rabbitmq.com/tutorials/tutorial-six-elixir.html).
    The default value is `#{inspect(Lepus.BasicClient.Publisher.default_timeout())}`.

  ## Returns

  Without `rpc: true`:

  * `:ok` - in case of success
  * `{:error, any}` in case of error

  With `rpc: true`:

  * `{:ok, any}` - in case of success
  * `{:error, :timeout}` - in case of timeout error
  * `{:error, any}` - in case of error
  """
  @callback publish(
              Client.exchange(),
              Client.routing_key(),
              Client.binary_payload(),
              Client.opts()
            ) :: Client.response()

  @doc """
  The same as `c:publish/4` but `payload` can be any type that is convertable to JSON (map, list, etc.).
  Sends JSON string to RabbitMQ exchange.
  """
  @callback publish_json(Client.exchange(), Client.routing_key(), Client.payload(), Client.opts()) ::
              Client.response()
  @optional_callbacks publish: 4, publish_json: 4

  defmacro __using__(opts) do
    client = opts |> Keyword.fetch!(:client)
    name = opts |> Keyword.get(:name, __CALLER__.module)

    quote do
      alias Lepus.Client

      @behaviour Lepus

      @spec publish(
              Client.exchange(),
              Client.routing_key(),
              Client.binary_payload(),
              Client.opts()
            ) :: Client.response()
      def publish(exchange, routing_key, payload, options \\ []) do
        unquote(client).publish(unquote(name), exchange, routing_key, payload, options)
      end

      @spec publish_json(Client.exchange(), Client.routing_key(), Client.payload(), Client.opts()) ::
              Client.response()
      def publish_json(exchange, routing_key, payload, options \\ []) do
        unquote(client).publish_json(unquote(name), exchange, routing_key, payload, options)
      end
    end
  end
end
