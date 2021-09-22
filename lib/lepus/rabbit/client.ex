defmodule Lepus.Rabbit.Client do
  @moduledoc false

  alias AMQP.Channel
  alias AMQP.Connection

  @type connection :: Connection.t()
  @type channel :: Channel.t()
  @type error :: {:error, any}
  @type exchange :: String.t()
  @type queue :: String.t()
  @type routing_key :: String.t()

  @callback open_connection(binary | keyword) :: {:ok, connection} | error
  @callback close_connection(connection) :: :ok | error
  @callback open_channel(connection) :: {:ok, channel} | error
  @callback close_channel(channel) :: :ok | error
  @callback declare_queue(channel, queue, keyword) :: :ok | error
  @callback bind_queue(channel, queue, exchange, keyword) :: :ok | error
  @callback declare_direct_exchange(channel, exchange, keyword) :: :ok | error
  @callback publish(channel, exchange, routing_key, binary, keyword) :: :ok | error
end
