defmodule Lepus.BasicClient.ServerNames do
  @moduledoc false

  @spec connection_server(atom) :: atom
  def connection_server(client_name), do: :"#{client_name}.ConnectionServer"

  @spec channels_supervisor(atom) :: atom
  def channels_supervisor(client_name), do: :"#{client_name}.ChannelsSupervisor"

  @spec registry(atom) :: atom
  def registry(client_name), do: :"#{client_name}.Registry"

  @spec channel_server(atom, binary) :: {atom, atom, any}
  def channel_server(client_name, exchange) do
    {:via, Registry, {registry(client_name), exchange}}
  end
end
