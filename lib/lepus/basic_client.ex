defmodule Lepus.BasicClient do
  alias Lepus.BasicClient.{
    ChannelsSupervisor,
    ConnectionServer,
    Options,
    Publisher,
    RepliesBroadway,
    ServerNames,
    Store
  }

  alias Lepus.Client

  @moduledoc """
  Basic `Lepus.Client` implementation.

  ## Features
  * Supervises `AMQP.Connection` process and many `AMQP.Channel` processes
    (one `AMQP.Channel` process per exchange).
  * Supports [RPC](https://www.rabbitmq.com/tutorials/tutorial-six-elixir.html)

  ## Example usage

  Add it to your supervision tree

      children = [
        {
          Lepus.BasicClient,
          name: MyApp.RabbitMQ,
          exchanges: ["exchange1", "exchange2"],
          connection: [
            host: "localhost",
            port: 5672,
            virtual_host: "/",
            username: "guest",
            password: "guest"
          ],
          rpc_opts: [
            pubsub: MyApp.PubSub,
            reply_to_queue: "my_app.reply_to"
          ]
        }
      ]

      children |> Supervisor.start_link(strategy: :one_for_one)

  ## Options

  #{Options.definition() |> NimbleOptions.docs()}
  """

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

  def start_link(opts) do
    with {:ok, bult_opts} <- opts |> Options.build() do
      name = bult_opts |> Keyword.fetch!(:name)
      __MODULE__ |> Supervisor.start_link(bult_opts, name: name)
    end
  end

  @impl Supervisor
  def init(opts) do
    [conn_opts, name, rabbit_client, rpc_opts] =
      [:connection, :name, :rabbit_client, :rpc_opts]
      |> Enum.map(&Keyword.fetch!(opts, &1))

    [
      {Store, client_name: name, rpc_opts: rpc_opts},
      {ConnectionServer, conn_opts: conn_opts, client_name: name, rabbit_client: rabbit_client},
      {Registry, keys: :unique, name: ServerNames.registry(name)},
      {ChannelsSupervisor,
       client_name: name,
       exchanges: opts |> Keyword.fetch!(:exchanges),
       rabbit_client: rabbit_client},
      {RepliesBroadway,
       client_name: name,
       rpc_opts: rpc_opts,
       conn_opts: conn_opts,
       broadway_producer_module: opts |> Keyword.fetch!(:broadway_producer_module)}
    ]
    |> Supervisor.init(strategy: :one_for_all)
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
