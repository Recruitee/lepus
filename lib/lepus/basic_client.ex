defmodule Lepus.BasicClient do
  @moduledoc """
  Basic `Lepus.Client` implementation.
  It uses supervised RabbitMQ connection and channels

  Example usage:

      {Lepus.BasicClient,
       name: MyApp.RabbitMQ,
       exchanges: ["exchange1", "exchange2"],
       connection: [
         host: "localhost",
         port: 5672,
         virtual_host: "/",
         username: "guest",
         password: "guest"
       ],
       sync_opts: [
         pubsub: MyApp.PubSub
         reply_to_queue: "my_app.reply_to"
       ]}
  """

  alias Lepus.BasicClient.ChannelsSupervisor
  alias Lepus.BasicClient.ConnectionServer
  alias Lepus.BasicClient.Publisher
  alias Lepus.BasicClient.RepliesBroadway
  alias Lepus.BasicClient.ServerNames
  alias Lepus.BasicClient.Store
  alias Lepus.Client
  alias Lepus.Rabbit

  @behaviour Client

  use Supervisor

  def child_spec(init_arg) do
    name = init_arg |> Keyword.fetch!(:name)

    %{
      id: name,
      start: {__MODULE__, :start_link, [init_arg]},
      type: :supervisor
    }
  end

  def start_link(init_arg) do
    name = init_arg |> Keyword.fetch!(:name)

    sync_opts =
      init_arg
      |> Keyword.fetch(:sync_opts)
      |> case do
        {:ok, [_ | _] = opts} ->
          [
            pubsub: opts |> Keyword.fetch!(:pubsub),
            reply_to_queue: opts |> Keyword.fetch!(:reply_to_queue)
          ]

        _ ->
          []
      end

    init_arg = %{
      name: name,
      connection: init_arg |> Keyword.fetch!(:connection),
      exchanges: init_arg |> Keyword.fetch!(:exchanges),
      rabbit_client: init_arg |> Keyword.get(:rabbit_client, Rabbit.BasicClient),
      producer_module: init_arg |> Keyword.get(:producer_module, BroadwayRabbitMQ.Producer),
      sync_opts: sync_opts
    }

    Supervisor.start_link(__MODULE__, init_arg, name: name)
  end

  @impl Supervisor
  def init(%{
        name: name,
        connection: conn_opts,
        exchanges: exchanges,
        rabbit_client: rabbit_client,
        sync_opts: sync_opts,
        producer_module: producer_module
      }) do
    children = [
      {Store, client_name: name, sync_opts: sync_opts},
      {ConnectionServer, conn_opts: conn_opts, client_name: name, rabbit_client: rabbit_client},
      {Registry, keys: :unique, name: ServerNames.registry(name)},
      {ChannelsSupervisor, client_name: name, exchanges: exchanges, rabbit_client: rabbit_client},
      {RepliesBroadway,
       client_name: name,
       sync_opts: sync_opts,
       conn_opts: conn_opts,
       producer_module: producer_module}
    ]

    Supervisor.init(children, strategy: :one_for_all)
  end

  @impl Client
  def publish(name, exchange, routing_key, payload, opts) do
    Publisher.call(name, exchange, routing_key, payload, opts)
  end

  @impl Client
  def publish_json(name, exchange, routing_key, payload, opts) do
    opts = opts |> Keyword.put(:json, true)
    publish(name, exchange, routing_key, payload, opts)
  end
end
