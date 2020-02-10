defmodule Lepus.Client do
  alias Lepus.Client.Connection
  alias Lepus.Client.Channel
  alias Lepus.Client.Channels
  alias Lepus.Producer
  alias Lepus.RabbitClient

  @behaviour Producer

  use Supervisor

  def start_link(init_arg) do
    name = init_arg |> Keyword.fetch!(:name)
    Supervisor.start_link(__MODULE__, init_arg, name: name)
  end

  @impl Producer
  def publish(name, exchange, routing_key, payload, options) do
    options =
      options
      |> Keyword.put_new_lazy(:timestamp, fn -> DateTime.utc_now() |> DateTime.to_unix() end)

    name
    |> get_registry_name()
    |> Channel.publish(exchange, routing_key, payload, options)
  end

  @impl Producer
  def publish_json(name, exchange, routing_key, payload, options) do
    options = options |> Keyword.put_new(:content_type, "application/json")
    payload = payload |> Jason.encode!()

    publish(name, exchange, routing_key, payload, options)
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

  defp get_registry_name(name), do: :"#{name}.Registry"
end
