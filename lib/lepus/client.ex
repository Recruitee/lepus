defmodule Lepus.Client do
  @moduledoc """
  Client behaviour used in `Lepus`.
  """

  @typep success() :: :ok | {:ok, any()}
  @typep error() :: {:error, any()}

  @type exchange :: String.t()
  @type routing_key :: String.t()
  @type binary_payload :: binary()
  @type payload :: term()
  @type opts :: keyword()
  @type response() :: success() | error()

  @callback publish(Supervisor.supervisor(), exchange, routing_key, binary_payload, opts) ::
              response()

  @callback publish_json(Supervisor.supervisor(), exchange, routing_key, payload, opts) ::
              response()
end
