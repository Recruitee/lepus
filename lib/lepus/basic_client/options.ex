defmodule Lepus.BasicClient.Options do
  @moduledoc false

  definition = [
    # private options
    broadway_producer_module: [type: :atom, doc: false, default: BroadwayRabbitMQ.Producer],
    rabbit_client: [type: :atom, doc: false, default: Lepus.Rabbit.BasicClient],

    # public options
    name: [
      type: :atom,
      required: true,
      doc: """
      Module name of your client. If you use

      ```elixir
      defmodule MyApp.RabbitMQ do
        use Lepus, client: Lepus.BasicClient
      end
      ```

      the `name` should be `MyApp.RabbitMQ`.
      """
    ],
    connection: [
      type: {:or, [:string, :keyword_list]},
      required: true,
      doc: """
      Defines an AMQP URI or a set of options used by the RabbitMQ client to open the connection
      with the RabbitMQ broker.
      See `AMQP.Connection.open/1` for the full list of options.
      """
    ],
    exchanges: [
      type: {:list, :string},
      default: [],
      doc: """
      List of exchange names you are going to use.
      `Lepus.BasicClient` starts and supervises separate process per exchange.
      Process for the default exchange (`""`) is always started.
      """
    ],
    rpc_opts: [
      type: :keyword_list,
      default: [],
      doc: """
      Required only if you are going to use
      [RPC](https://www.rabbitmq.com/tutorials/tutorial-six-elixir.html)
      (`c:Lepus.publish/4` or `c:Lepus.publish_json/4` with `rpc: true`).
      """,
      keys: [
        pubsub: [
          type: :atom,
          doc: """
          Your PubSub process name. See the `Phoenix.PubSub`.
          """
        ],
        reply_to_queue: [
          type: :string,
          doc: """
          Defines a queue name for the replies.
          """
        ]
      ]
    ]
  ]

  @definition NimbleOptions.new!(definition)

  @spec definition :: NimbleOptions.t()
  def definition, do: @definition

  @spec build(keyword) :: {:ok, keyword} | {:error, String.t()}
  def build(opts) do
    opts
    |> NimbleOptions.validate(definition())
    |> case do
      {:ok, opts} -> {:ok, opts}
      {:error, %{message: reason}} -> {:error, reason}
    end
  end
end
