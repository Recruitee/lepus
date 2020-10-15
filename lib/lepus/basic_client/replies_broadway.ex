defmodule Lepus.BasicClient.RepliesBroadway do
  @moduledoc false

  alias Lepus.BasicClient.Publisher
  alias Lepus.BasicClient.ServerNames
  alias Lepus.BroadwayHelpers
  alias Phoenix.PubSub

  use Broadway

  def start_link(init_arg), do: init_arg |> Keyword.fetch!(:sync_opts) |> start_link(init_arg)

  def start_link([_ | _] = sync_opts, init_arg) do
    [client_name, conn_opts, producer_module] =
      [:client_name, :conn_opts, :producer_module]
      |> Enum.map(&Keyword.fetch!(init_arg, &1))

    name = client_name |> ServerNames.replies_broadway()
    [queue, pubsub] = [:reply_to_queue, :pubsub] |> Enum.map(&Keyword.fetch!(sync_opts, &1))

    Broadway.start_link(__MODULE__,
      name: name,
      producer: [
        module: {
          producer_module,
          on_failure: :ack,
          connection: conn_opts,
          queue: queue,
          declare: [durable: true],
          metadata: [:content_type, :correlation_id, :headers]
        },
        concurrency: 1
      ],
      processors: [default: [concurrency: 1]],
      context: %{queue: queue, pubsub: pubsub}
    )
  end

  def start_link(_sync_opts, _init_arg), do: :ignore

  @impl Broadway

  def handle_message(_, %{metadata: %{correlation_id: correlation_id}} = message, context) do
    %{queue: queue, pubsub: pubsub} = context
    %{data: data} = updated_message = message |> BroadwayHelpers.maybe_decode_json()
    topic = Publisher.pubsub_topic(queue, correlation_id)

    pubsub_message =
      message
      |> BroadwayHelpers.get_header_value("x-reply-status", "error")
      |> case do
        "error" -> {:lepus, :error, data}
        _ -> {:lepus, :ok, data}
      end

    pubsub |> PubSub.broadcast!(topic, pubsub_message)

    updated_message
  end

  def handle_message(_, message, _), do: message
end
