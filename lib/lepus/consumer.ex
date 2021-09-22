defmodule Lepus.Consumer do
  alias Lepus.Consumer.{Options, Server}

  @moduledoc """
  Consumer receives messages from a RabbitMQ queue.
  It uses `Broadway` and `BroadwayRabbitMQ.Producer` under the hood.

  ## Features
  * Optionally retries failed messages with exponential backoff
  * Optionally publishes failed messages to separate queue
  * Supports [RPC](https://www.rabbitmq.com/tutorials/tutorial-six-elixir.html)

  ## Topology

  ```txt
  ┌►   [queue]
  │       │
  │       ▼
  │ (Lepus.Consumer) ───► {failed exchange} ───► [failed queue]
  │       │
  │       ▼
  │ {delay exchange}
  │       │
  │       ▼
  │ [retry queue]
  │       │
  │       ▼
  └ {retry exchange}
  ```

  ## Example

  Define a consumer:

      defmodule MyApp.MyConsumer do
        @behaviour Lepus.Consumer

        @impl Lepus.Consumer
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
            routing_key: "my_routing_key",
            queue: "my_queue",
            store_errors: true,
            max_retry_count: 5
          ]
        end

        @impl Lepus.Consumer
        def handle_message(data, metadata) do
          Do.something_with(data, metadata)
        end
      end

  Then add it to your supervision tree (usually in `lib/my_app/application.ex`):

      children = [
        {Lepus.Consumer, consumers: [MyApp.MyConsumer]}
      ]

      children |> Supervisor.start_link(strategy: :one_for_one)

  You can also define some global options for all the consumers in the supervision tree:

      children = [
        {Lepus.Consumer,
          consumers: [MyApp.MyConsumer, MyApp.OtherConsumer],
          options: [connection: rabbit_connection, exchange: "my_exchange"]}
      ]

      children |> Supervisor.start_link(strategy: :one_for_one)
  """

  use Supervisor

  @type payload :: any()

  @type metadata :: %{
          rpc: boolean(),
          retry_count: non_neg_integer(),
          retriable: boolean(),
          client: String.t() | nil,
          rabbit_mq_metadata: map()
        }

  @doc """
  Should return keyword list of options (if defined).
  You can also define options in the supervision tree as described above.

  ## Options

  #{Options.definition() |> NimbleOptions.docs()}
  """
  @callback options() :: Keyword.t()

  @doc """
  Receives message `payload` and `metadata`.

  ## `payload`

  Usually it's a binary RabbitMQ message payload.
  But it could be parsed JSON (map, list, etc.)
  if content type of the message is "application/json" or it was sent via `c:Lepus.publish_json/4`

  ## `metadata`

  It's a map with the keys:

  * `rpc` - Defines if the message was sent using
    [RPC](https://www.rabbitmq.com/tutorials/tutorial-six-elixir.html) pattern
    (`reply_to` and `correlation_id` is not empty).
    The function should return `{:ok, result}` if `rpc` is `true`.

  * `retry_count` - Count of retries if the message was retried. Otherwise `0`.

  * `retriable` – Defines if the message will be retried in case of error.

  * `client` – `"lepus"` if the message was sent via `c:Lepus.publish/4` or `c:Lepus.publish_json/4`.

  * `rabbit_mq_metadata` - `metadata` field from `BroadwayRabbitMQ.Producer`.
  """
  @callback handle_message(payload, metadata) ::
              :ok | {:ok, binary()} | :error | {:error, binary()}

  @optional_callbacks options: 0

  @spec start_link(keyword) :: Supervisor.on_start()
  def start_link(opts) do
    __MODULE__ |> Supervisor.start_link(opts, name: __MODULE__)
  end

  @impl Supervisor
  def init(opts) do
    options = opts |> Keyword.get(:options, [])

    opts
    |> Keyword.get(:consumers, [])
    |> Enum.map(fn
      module -> {Server, [consumer: module, options: options]}
    end)
    |> Supervisor.init(strategy: :one_for_one)
  end
end
