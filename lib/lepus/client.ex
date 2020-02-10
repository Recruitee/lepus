defmodule Lepus.Client do
  @callback publish(Supervisor.supervisor(), String.t(), String.t(), String.t(), keyword()) ::
              :ok | AMQP.error()
  @callback publish_json(
              Supervisor.supervisor(),
              String.t(),
              String.t(),
              map() | list(),
              keyword()
            ) :: :ok | AMQP.error()
end
