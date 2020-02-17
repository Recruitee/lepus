defmodule Lepus.RabbitClient do
  alias AMQP.Basic
  alias AMQP.Channel
  alias AMQP.Connection
  alias AMQP.Exchange
  alias AMQP.Queue

  def open_connection(opts), do: Connection.open(opts)
  def close_connection(connection), do: Connection.close(connection)
  def open_channel(connection), do: Channel.open(connection)
  def close_channel(channel), do: Channel.close(channel)
  def declare_queue(channel, queue, opts), do: Queue.declare(channel, queue, opts)
  def bind_queue(channel, queue, exchange, opts), do: Queue.bind(channel, queue, exchange, opts)

  def declare_direct_exchange(_channel, "", _opts), do: :ok

  def declare_direct_exchange(channel, exchange, opts) do
    Exchange.direct(channel, exchange, opts)
  end

  def publish(channel, exchange, routing_key, payload, opts) do
    Basic.publish(channel, exchange, routing_key, payload, opts)
  end
end
