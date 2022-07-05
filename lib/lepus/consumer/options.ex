defmodule Lepus.Consumer.Options do
  @moduledoc false

  alias Lepus.Rabbit

  definition = [
    # private options
    broadway_producer_module: [type: :atom, doc: false, default: BroadwayRabbitMQ.Producer],
    rabbit_client: [type: :atom, doc: false, default: Lepus.Rabbit.BasicClient],

    # public options
    connection: [
      type: {:or, [:string, :keyword_list]},
      required: true,
      doc: """
      Defines an AMQP URI or a set of options used by the RabbitMQ client to open the connection
      with the RabbitMQ broker.
      See `AMQP.Connection.open/1` for the full list of options.
      """
    ],
    exchange: [
      type: :string,
      required: true,
      doc: """
      The name of the RabbitMQ exchange.
      """
    ],
    routing_key: [
      type: :string,
      required: true,
      doc: """
      The name of the RabbitMQ routing_key.
      """
    ],
    delay_exchange: [
      type: :string,
      doc: """
      Redefines delay exchange name. The default value is `"\#{exchange}.delay"`
      """
    ],
    retry_exchange: [
      type: :string,
      doc: """
      Redefines retry exchange name. The default value is `"\#{exchange}.retry"`
      """
    ],
    failed_exchange: [
      type: :string,
      doc: """
      Redefines error exchange name. The default value is `"\#{exchange}.failed"`
      """
    ],
    queue: [
      type: :string,
      doc: """
      Redefines queue name. The default value is `"\#{exchange}.\#{routing_key}"`
      """
    ],
    retry_queue: [
      type: :string,
      doc: """
      Redefines retry queue name. The default value is `"\#{queue}.retry"`
      """
    ],
    failed_queue: [
      type: :string,
      doc: """
      Redefines error queue name. The default value is `"\#{queue}.failed"`
      """
    ],
    processor: [
      type: :keyword_list,
      default: [],
      doc: """
      Broadway processor options.
      Only one processor is used.
      See `Broadway.start_link/2` for the full list of options.
      """
    ],
    max_retry_count: [
      type: :non_neg_integer,
      default: 0,
      doc: """
      The maximum count of message retries.
      """
    ],
    store_failed: [
      type: :boolean,
      default: false,
      doc: """
      Defines if the failed message should be published to `failed_queue`.
      """
    ],
    queues_type: [
      type: {:in, Rabbit.Config.allowed_queues_types()},
      doc: """
      Defines queues type (one of `#{inspect(Rabbit.Config.allowed_queues_types())}`). Used during queue declaration.
      """
    ]
  ]

  @definition NimbleOptions.new!(definition)

  @spec definition :: NimbleOptions.t()
  def definition, do: @definition

  @spec build(atom, keyword) :: {:ok, keyword} | {:error, String.t()}
  def build(consumer_module, global_opts) do
    with {:ok, local_opts} <- consumer_module |> fetch_options(),
         {:ok, opts} <-
           global_opts |> Keyword.merge(local_opts) |> NimbleOptions.validate(definition()) do
      {:ok, set_defaults(opts)}
    else
      {:error, %{message: reason}} -> {:error, reason}
      {:error, reason} -> {:error, reason}
    end
  end

  defp fetch_options(consumer_module) do
    with :ok <- consumer_module |> ensure_loaded(),
         true <- consumer_module |> function_exported?(:options, 0) do
      {:ok, consumer_module.options()}
    else
      false -> {:ok, []}
      {:error, reason} -> {:error, reason}
    end
  end

  defp ensure_loaded(module) do
    module
    |> Code.ensure_loaded()
    |> case do
      {:module, _} -> :ok
      {:error, reason} -> {:error, "Can't load #{inspect(module)} (#{inspect(reason)})"}
    end
  end

  defp set_defaults(opts) do
    exchange = opts |> Keyword.fetch!(:exchange)
    routing_key = opts |> Keyword.fetch!(:routing_key)
    queue = opts |> Keyword.get(:queue, default_value(exchange, routing_key))

    opts
    |> Keyword.put_new(:queue, queue)
    |> set_default(exchange,
      delay_exchange: "delay",
      retry_exchange: "retry",
      failed_exchange: "failed"
    )
    |> set_default(queue, retry_queue: "retry", failed_queue: "failed")
  end

  defp set_default(opts, preffix, suffixes) do
    suffixes
    |> Enum.reduce(opts, fn {k, v}, acc ->
      acc |> Keyword.put_new(k, default_value(preffix, v))
    end)
  end

  defp default_value("", suffix), do: suffix
  defp default_value(preffix, suffix), do: "#{preffix}.#{suffix}"
end
