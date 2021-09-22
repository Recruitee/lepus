defmodule Lepus.Rabbit.BasicClient do
  @moduledoc false

  alias AMQP.Basic
  alias AMQP.Channel
  alias AMQP.Connection
  alias AMQP.Exchange
  alias AMQP.Queue

  alias Lepus.Rabbit.Client

  @behaviour Client

  @spec open_connection(binary | keyword) :: {:ok, Client.connection()} | Client.error()
  def open_connection(opts), do: Connection.open(opts)

  @spec close_connection(Client.connection()) :: :ok | Client.error()
  def close_connection(connection), do: Connection.close(connection)

  @spec open_channel(Client.connection()) :: {:ok, Client.channel()} | Client.error()
  def open_channel(connection), do: Channel.open(connection)

  @spec close_channel(Client.channel()) :: :ok | Client.error()
  def close_channel(channel), do: Channel.close(channel)

  @spec declare_queue(Client.channel(), Client.queue(), keyword) :: :ok | Client.error()
  def declare_queue(channel, queue, opts) do
    channel
    |> Queue.declare(queue, opts)
    |> case do
      :ok -> :ok
      {:ok, _} -> :ok
      error -> error
    end
  end

  @spec bind_queue(Client.channel(), Client.queue(), Client.exchange(), keyword) ::
          :ok | Client.error()
  def bind_queue(channel, queue, exchange, opts), do: channel |> Queue.bind(queue, exchange, opts)

  @spec declare_direct_exchange(Client.channel(), Client.exchange(), keyword) ::
          :ok | Client.error()
  def declare_direct_exchange(_channel, "", _opts), do: :ok

  def declare_direct_exchange(channel, exchange, opts) do
    channel |> Exchange.direct(exchange, opts)
  end

  @spec publish(Client.channel(), Client.exchange(), Client.routing_key(), binary, keyword) ::
          :ok | Client.error()
  def publish(channel, exchange, routing_key, payload, opts) do
    channel |> Basic.publish(exchange, routing_key, payload, opts)
  end
end
