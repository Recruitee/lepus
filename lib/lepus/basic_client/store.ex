defmodule Lepus.BasicClient.Store do
  @moduledoc false

  use Agent

  alias Lepus.BasicClient.ServerNames

  def start_link(init_arg) do
    name = init_arg |> Keyword.fetch!(:client_name) |> ServerNames.store()
    Agent.start_link(fn -> init_arg end, name: name)
  end

  @spec get_sync_opts(atom) :: keyword()
  def get_sync_opts(client_name) do
    client_name |> ServerNames.store() |> Agent.get(&Keyword.fetch!(&1, :rpc_opts))
  end
end
