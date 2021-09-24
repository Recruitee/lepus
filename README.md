# Lepus

Opinionated library for [RabbitMQ](https://www.rabbitmq.com/).

![lepus](https://user-images.githubusercontent.com/1102853/73840963-c1bd1c80-4819-11ea-9112-d17120ad3a77.jpg)

## Documentation

[HexDocs](https://hexdocs.pm/lepus)

## Features

* Supervises `AMQP.Connection` and `AMQP.Channel` processes.
* Optionally retries failed messages with exponential backoff.
* Optionally publishes failed messages to separate queue.
* Supports [RPC](https://www.rabbitmq.com/tutorials/tutorial-six-elixir.html).

## Installation

Add `:lepus` to the list of dependencies in `mix.exs`:

```elixir
def deps do
  [
    {:lepus, "~> 0.1.0"},
  ]
end
```
