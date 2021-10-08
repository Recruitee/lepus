# Lepus

Opinionated library for [RabbitMQ](https://www.rabbitmq.com/).

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
