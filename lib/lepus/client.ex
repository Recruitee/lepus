defmodule Lepus.Client do
  @callback start_link(keyword()) :: Supervisor.on_start()
  @callback publish(Supervisor.supervisor(), String.t(), String.t(), String.t(), keyword()) ::
              :ok | AMQP.error()
  @callback publish_json(
              Supervisor.supervisor(),
              String.t(),
              String.t(),
              map() | list(),
              keyword()
            ) :: :ok | AMQP.error()

  @optional_callbacks start_link: 1
end
