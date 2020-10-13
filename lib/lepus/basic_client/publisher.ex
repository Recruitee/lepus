defmodule Lepus.BasicClient.Publisher do
  @moduledoc false

  alias Lepus.BasicClient.ChannelServer

  def call(client_name, exchange, routing_key, payload, opts) do
    amqp_opts = opts |> Keyword.get(:amqp_opts, []) |> put_timestamp()
    {payload, amqp_opts} = opts |> Keyword.get(:json, false) |> maybe_json(payload, amqp_opts)

    opts
    |> Keyword.fetch(:sync)
    |> case do
      {:ok, true} -> publish_sync(client_name, exchange, routing_key, payload, amqp_opts)
      _ -> publish(client_name, exchange, routing_key, payload, amqp_opts)
    end
  end

  defp publish_sync(client_name, exchange, routing_key, payload, amqp_opts) do
    publish(client_name, exchange, routing_key, payload, amqp_opts)
  end

  defp publish(client_name, exchange, routing_key, payload, amqp_opts) do
    ChannelServer.publish(client_name, exchange, routing_key, payload, amqp_opts)
  end

  defp put_timestamp(amqp_opts) do
    amqp_opts
    |> Keyword.put_new_lazy(:timestamp, fn ->
      DateTime.utc_now() |> DateTime.to_unix(:microsecond)
    end)
  end

  defp maybe_json(true = _json?, payload, amqp_opts) do
    payload = payload |> Jason.encode!()
    amqp_opts = amqp_opts |> Keyword.put_new(:content_type, "application/json")
    {payload, amqp_opts}
  end

  defp maybe_json(_json?, payload, amqp_opts), do: {payload, amqp_opts}
end
