defmodule Lepus.BasicClient do
  @moduledoc """
  Default `Lepus.Client` implementation.
  It uses supervised RabbitMQ connection and channels
  """

  alias Lepus.BasicClient.Channel
  alias Lepus.BasicClient.Channels
  alias Lepus.BasicClient.Connection
  alias Lepus.Client
  alias Lepus.RabbitClient

  @behaviour Client

  use Supervisor

  def start_link(init_arg) do
    name = init_arg |> Keyword.fetch!(:name)
    Supervisor.start_link(__MODULE__, init_arg, name: name)
  end

  @impl Supervisor
  def init(init_arg) do
    name = init_arg |> Keyword.fetch!(:name)
    connection_name = :"#{name}.Connection"
    registry_name = get_registry_name(name)
    conn_opts = init_arg |> Keyword.fetch!(:connection)
    exchanges = init_arg |> Keyword.fetch!(:exchanges)
    rabbit_client = init_arg |> Keyword.get(:rabbit_client, RabbitClient)

    children = [
      {Connection, conn_opts: conn_opts, name: connection_name, rabbit_client: rabbit_client},
      {Registry, keys: :unique, name: registry_name},
      {Channels,
       name: :"#{name}.Channels",
       registry_name: registry_name,
       exchanges: exchanges,
       connection_name: connection_name,
       rabbit_client: rabbit_client}
    ]

    Supervisor.init(children, strategy: :one_for_all)
  end

  @impl Client
  def publish(name, exchange, routing_key, payload, opts) do
    amqp_opts = opts |> Keyword.get(:amqp_opts, []) |> put_timestamp()
    {payload, amqp_opts} = opts |> Keyword.get(:json, false) |> maybe_json(payload, amqp_opts)

    do_puplish(name, exchange, routing_key, payload, amqp_opts)
  end

  @impl Client
  def publish_json(name, exchange, routing_key, payload, opts) do
    opts = opts |> Keyword.put(:json, true)
    publish(name, exchange, routing_key, payload, opts)
  end

  defp get_registry_name(name), do: :"#{name}.Registry"

  defp do_puplish(name, exchange, routing_key, payload, amqp_opts) do
    name |> get_registry_name() |> Channel.publish(exchange, routing_key, payload, amqp_opts)
  end

  defp put_timestamp(amqp_opts) do
    amqp_opts
    |> Keyword.put_new_lazy(:timestamp, fn ->
      DateTime.utc_now() |> DateTime.to_unix(:microsecond)
    end)
  end

  defp maybe_json(_json? = true, payload, amqp_opts) do
    payload = payload |> Jason.encode!()
    amqp_opts = amqp_opts |> Keyword.put_new(:content_type, "application/json")
    {payload, amqp_opts}
  end

  defp maybe_json(_json?, payload, amqp_opts), do: {payload, amqp_opts}
end
