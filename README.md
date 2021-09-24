# Lepus

![lepus](https://user-images.githubusercontent.com/1102853/73840963-c1bd1c80-4819-11ea-9112-d17120ad3a77.jpg)

Opinionated library for [RabbitMQ](https://www.rabbitmq.com/).
Connections and channels are supervised.
Consumers are retryable with an exponential backoff.

Uses [AMQP](https://github.com/pma/amqp) and [BroadwayRabbitMQ](https://github.com/dashbitco/broadway_rabbitmq) under the hood.

## Installation

Add `:lepus` to the list of dependencies in `mix.exs`:

```elixir
def deps do
  [
    {:lepus, github: "recruitee/lepus"}
  ]
end
```

## Usage

### Consumer Configuration

Create a consumer if you need to receive messages from RabbitMQ.

```elixir
defmodule MyApp.MyConsumer do
  use Lepus.Consumer

  @impl Lepus.Consumer
  def options do
    [exchange: "my_exchange", routing_key: "my_routing_key"]
  end

  @impl Lepus.Consumer
  def handle_message(data, %{rpc: true} = metadata) do
    # do someting
    {:ok, response} # or `{:error, error}`
  end

  def handle_message(data, metadata) do
    # do someting
    :ok # or `:error` or `{:error, error}`
  end

  @impl Lepus.Consumer
  def handle_failed(data, metadata) do
    # do someting
  end
end
```

And add it to your supervision tree.
```elixir
children = [
  # ...
  # `connection` is a keyword or URI from AMQP
  # https://hexdocs.pm/amqp/AMQP.Connection.html#open/2
  {MyApp.MyConsumer, connection: rabbit_connection},
  # ...
]

Supervisor.init(children, strategy: :one_for_one)
```

### BasicClient Configuration

Create a client if you need to send messages to RabbitMQ.

```elixir
defmodule MyApp.RabbitMQ do
  use Lepus, client: Lepus.BasicClient
end
```

And add it to your supervision tree.
```elixir
children = [
  # ...
  # `connection` is a keyword or URI from AMQP
  # https://hexdocs.pm/amqp/AMQP.Connection.html#open/2
  {
    Lepus.BasicClient
    name: MyApp.RabbitMQ
    connection: rabbit_connection
    exchanges: ["my_exchange1", "my_exchange2"],
    # if you wand use `rpc: true` option
    rpc_opts: [
        pubsub: MyApp.PubSub
        reply_to_queue: "my_app.reply_to"
    ]
  },
  # ...
]

Supervisor.init(children, strategy: :one_for_one)
```

Now you can use the client.
```elixir
MyApp.MyRabbitMQClient.publish(
  "my_exchange1",
  "my_routing_key",
  "My payload"
)

MyApp.MyRabbitMQClient.publish_json(
  "my_exchange2",
  "my_routing_key",
  %{
    key: "Value",
    list: [1, 2, 3]
  }
)

MyApp.MyRabbitMQClient.publish_json(
  "my_exchange2",
  "my_routing_key",
  %{
    key: "Value",
    list: [1, 2, 3]
  },
  rpc: true
)
```
