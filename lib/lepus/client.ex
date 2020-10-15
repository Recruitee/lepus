defmodule Lepus.Client do
  @moduledoc """
  Client behaviour
  """

  @callback publish(Supervisor.supervisor(), String.t(), String.t(), String.t(), keyword()) ::
              :ok | AMQP.Basic.error()
  @callback publish_json(
              Supervisor.supervisor(),
              String.t(),
              String.t(),
              map() | list(),
              keyword()
            ) :: :ok | AMQP.Basic.error() | {:error, :timeout}
end
