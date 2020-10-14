defmodule Lepus.BasicClient.ChannelsSupervisor do
  @moduledoc false

  use Supervisor

  alias Lepus.BasicClient.ChannelServer
  alias Lepus.BasicClient.ServerNames

  def start_link(init_arg) do
    name = init_arg |> Keyword.fetch!(:client_name) |> ServerNames.channels_supervisor()
    Supervisor.start_link(__MODULE__, init_arg, name: name)
  end

  @impl Supervisor
  def init(init_arg) do
    client_name = init_arg |> Keyword.fetch!(:client_name)
    exchanges = init_arg |> Keyword.fetch!(:exchanges)
    rabbit_client = init_arg |> Keyword.fetch!(:rabbit_client)

    children =
      ["" | exchanges]
      |> Enum.uniq()
      |> Enum.map(fn exchange ->
        Supervisor.child_spec(
          {
            ChannelServer,
            client_name: client_name, exchange: exchange, rabbit_client: rabbit_client
          },
          id: exchange
        )
      end)

    Supervisor.init(children, strategy: :one_for_one)
  end
end
