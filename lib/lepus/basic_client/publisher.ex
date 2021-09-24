defmodule Lepus.BasicClient.Publisher do
  @moduledoc false

  alias Lepus.BasicClient.ChannelServer
  alias Lepus.BasicClient.Store
  alias Lepus.BroadwayHelpers
  alias Phoenix.PubSub

  def default_timeout, do: :infinity

  def call(client_name, exchange, routing_key, payload, opts) do
    amqp_opts = opts |> Keyword.get(:amqp_opts, []) |> put_timestamp() |> put_client()
    {payload, amqp_opts} = opts |> Keyword.get(:json, false) |> maybe_json(payload, amqp_opts)
    opts = opts |> Keyword.put(:amqp_opts, amqp_opts)

    opts
    |> Keyword.fetch(:rpc)
    |> case do
      {:ok, true} -> publish_sync(client_name, exchange, routing_key, payload, opts)
      _ -> publish(client_name, exchange, routing_key, payload, opts)
    end
  end

  def pubsub_topic(reply_to_queue, correlation_id) do
    "lepus:#{reply_to_queue}:#{correlation_id}"
  end

  defp publish_sync(client_name, exchange, routing_key, payload, opts) do
    client_name
    |> Store.get_sync_opts()
    |> case do
      [] ->
        raise "Add `rpc_opts` to the `Lepus.BasicClient` configuration for using `rpc` option"

      rpc_opts ->
        reply_to_queue = rpc_opts |> Keyword.fetch!(:reply_to_queue)
        correlation_id = gen_correlation_id()
        timeout = opts |> Keyword.get(:timeout, default_timeout())

        amqp_opts =
          opts
          |> Keyword.fetch!(:amqp_opts)
          |> Keyword.merge(reply_to: reply_to_queue, correlation_id: correlation_id)
          |> put_reply_timeout(timeout)

        rpc_opts
        |> Keyword.fetch!(:pubsub)
        |> PubSub.subscribe(pubsub_topic(reply_to_queue, correlation_id))

        ChannelServer.publish(client_name, exchange, routing_key, payload, amqp_opts)
        timeout |> wait_for_reply()
    end
  end

  defp wait_for_reply(:infinity) do
    receive do
      {:lepus, :error, :timeout} -> {:error, :timeout}
      {:lepus, status, value} -> {status, value}
    end
  end

  defp wait_for_reply(timeout) do
    timer_ref = self() |> Process.send_after({:lepus, :error, :timeout}, timeout)

    case wait_for_reply(:infinity) do
      {:ok, _} = reply ->
        timer_ref |> Process.cancel_timer()
        reply

      error ->
        error
    end
  end

  defp publish(client_name, exchange, routing_key, payload, opts) do
    amqp_opts = opts |> Keyword.fetch!(:amqp_opts)
    ChannelServer.publish(client_name, exchange, routing_key, payload, amqp_opts)
  end

  defp put_timestamp(amqp_opts) do
    amqp_opts
    |> Keyword.put_new_lazy(:timestamp, fn ->
      DateTime.utc_now() |> DateTime.to_unix(:microsecond)
    end)
  end

  defp put_client(amqp_opts) do
    amqp_opts |> update_headers([{"x-client", :binary, "lepus"}])
  end

  defp put_reply_timeout(amqp_opts, timeout) when is_integer(timeout) do
    amqp_opts |> update_headers([{"x-reply-timeout", :long, timeout}])
  end

  defp put_reply_timeout(amqp_opts, _timeout), do: amqp_opts

  defp maybe_json(true = _json?, payload, amqp_opts) do
    payload = payload |> Jason.encode!()
    amqp_opts = amqp_opts |> Keyword.put_new(:content_type, "application/json")
    {payload, amqp_opts}
  end

  defp maybe_json(_json?, payload, amqp_opts), do: {payload, amqp_opts}

  defp gen_correlation_id do
    16 |> :crypto.strong_rand_bytes() |> Base.url_encode64(padding: false)
  end

  defp update_headers(amqp_opts, headers) do
    amqp_opts |> Keyword.update(:headers, headers, &BroadwayHelpers.update_headers(&1, headers))
  end
end
