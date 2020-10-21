defmodule Lepus.Client do
  @moduledoc """
  Client behaviour
  """

  @typep success() :: :ok | {:ok, any()}
  @typep error() :: AMQP.Basic.error() | {:error, reason :: :timeout | any()}

  @type response() :: success() | error()

  @callback publish(Supervisor.supervisor(), String.t(), String.t(), any(), keyword()) ::
              response()

  @callback publish_json(Supervisor.supervisor(), String.t(), String.t(), any(), keyword()) ::
              response()
end
