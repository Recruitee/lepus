defmodule Lepus.Client.Channels do
  use Supervisor

  alias Lepus.Client.Channel

  def start_link(init_arg) do
    name = init_arg |> Keyword.fetch!(:name)
    Supervisor.start_link(__MODULE__, init_arg, name: name)
  end

  @impl Supervisor
  def init(init_arg) do
    exchanges = init_arg |> Keyword.fetch!(:exchanges)
    registry_name = init_arg |> Keyword.fetch!(:registry_name)
    connection_name = init_arg |> Keyword.fetch!(:connection_name)
    rabbit_client = init_arg |> Keyword.fetch!(:rabbit_client)

    children =
      exchanges
      |> Enum.map(fn exchange ->
        Supervisor.child_spec(
          {Channel,
           exchange: exchange,
           registry_name: registry_name,
           connection_name: connection_name,
           rabbit_client: rabbit_client},
          id: exchange
        )
      end)

    Supervisor.init(children, strategy: :one_for_one)
  end
end
