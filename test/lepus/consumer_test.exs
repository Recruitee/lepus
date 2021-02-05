defmodule Lepus.ConsumerTest do
  use ExUnit.Case, async: true

  defmodule RabbitClientMock do
    def start_link, do: Agent.start_link(fn -> [] end)

    def open_connection(pid) do
      pid |> log(:open_connection, [pid])
      {:ok, {:connection, pid}}
    end

    def close_connection({:connection, pid} = connection) do
      pid |> log(:close_connection, [connection])
    end

    def open_channel({:connection, pid}) do
      pid |> log(:open_channel, [{:connection, pid}])
      {:ok, {:channel, System.unique_integer(), pid}}
    end

    def close_channel({:channel, _, pid} = channel) do
      pid |> log(:close_channel, [channel])
    end

    def declare_queue({:channel, _, pid} = channel, queue, opts) do
      pid |> log(:declare_queue, [channel, queue, opts])
    end

    def bind_queue({:channel, _, pid} = channel, queue, exchange, opts) do
      pid |> log(:bind_queue, [channel, queue, exchange, opts])
    end

    def declare_direct_exchange({:channel, _, pid} = channel, exchange, opts) do
      pid |> log(:declare_direct_exchange, [channel, exchange, opts])
    end

    def publish({:channel, _, pid} = channel, exchange, routing_key, payload, opts) do
      pid |> log(:publish, [channel, exchange, routing_key, payload, opts])
    end

    def get_log(pid), do: pid |> Agent.get(& &1) |> Enum.reverse()

    def clear(pid), do: pid |> Agent.update(fn _ -> [] end)

    defp log(pid, name, args) do
      pid |> Agent.update(fn state -> [{name, args} | state] end)
    end
  end

  defmodule BroadwayProducerMock do
    use GenStage

    def push_message(producer, data, metadata \\ %{}, rabbit_client \\ nil) do
      metadata =
        metadata
        |> Map.put(:test_pid, self())
        |> Map.put_new(:content_type, :undefined)
        |> Map.put_new(:headers, :undefined)
        |> Map.put_new(:routing_key, :undefined)
        |> Map.put_new(:timestamp, :undefined)

      acknowledger = {BroadwayProducerMock, %{conn: {:connection, rabbit_client}}, :ok}
      message = %Broadway.Message{data: data, metadata: metadata, acknowledger: acknowledger}

      producer |> GenStage.cast({:push_message, message})
    end

    def init(init_arg), do: {:producer, init_arg}
    def handle_demand(_demand, init_arg), do: {:noreply, [], init_arg}

    def ack(_, successful, failed) do
      [successful: successful, failed: failed]
      |> Enum.each(fn {k, messages} ->
        messages
        |> Enum.each(fn %{metadata: %{test_pid: test_pid}} = message ->
          Process.send(test_pid, {k, message}, [])
        end)
      end)
    end

    def handle_call(:get_state, _, init_arg), do: {:reply, init_arg, [], init_arg}
    def handle_cast({:push_message, message}, init_arg), do: {:noreply, [message], init_arg}
  end

  defmodule RetriableConsumer do
    use Lepus.Consumer

    @impl Lepus.Consumer
    def options do
      [
        exchange: "test_exchange",
        routing_key: "test_routing_key",
        rabbit_client: RabbitClientMock,
        broadway_producer_module: BroadwayProducerMock,
        processor: [concurrency: 1]
      ]
    end

    @impl Lepus.Consumer
    def handle_message(data, %{rabbit_mq_metadata: %{test_pid: test_pid}} = metadata) do
      Process.send(test_pid, {:message_handled, data, metadata}, [])

      data
      |> case do
        "Failed!" -> {:error, "Failed!"}
        %{"str" => "Failed!"} -> {:error, %{"error" => "Failed!"}}
        "With response" -> {:ok, "Response"}
        %{"str" => "With response"} -> {:ok, %{"str" => "Response"}}
        "Raised!" -> raise ArgumentError, "Test Argument Error!"
        %{"str" => "Raised!"} -> raise ArgumentError, "Test Argument Error!"
        _ -> {:ok, ""}
      end
    end

    @impl Lepus.Consumer
    def handle_failed(data, %{rabbit_mq_metadata: %{test_pid: test_pid}} = metadata) do
      Process.send(test_pid, {:failed_handled, data, metadata}, [])
    end
  end

  defmodule NotRetriableConsumer do
    use Lepus.Consumer

    @impl Lepus.Consumer
    def options, do: RetriableConsumer.options() |> Keyword.put(:retriable, false)

    @impl Lepus.Consumer
    def handle_message(data, metadata), do: RetriableConsumer.handle_message(data, metadata)

    @impl Lepus.Consumer
    def handle_failed(data, metadata), do: RetriableConsumer.handle_failed(data, metadata)
  end

  describe ".start_link/1" do
    setup tag do
      {:ok, rabbit_client} = RabbitClientMock.start_link()
      consumer_module = tag |> Map.get(:consumer_module, RetriableConsumer)

      {:ok, consumer} =
        consumer_module.start_link(connection: rabbit_client, name: new_consumer_name())

      log = rabbit_client |> RabbitClientMock.get_log()

      {:ok, log: log, rabbit_client: rabbit_client, consumer: consumer}
    end

    test "declares exchanges and queues", %{log: log, rabbit_client: rabbit_client} do
      assert [
               open_connection: [^rabbit_client],
               open_channel: [connection],
               declare_direct_exchange: [channel, "test_exchange", [durable: true]],
               declare_direct_exchange: [channel, "test_exchange.delay", [durable: true]],
               declare_direct_exchange: [channel, "test_exchange.retry", [durable: true]],
               declare_queue: [channel, "test_exchange.test_routing_key", [durable: true]],
               declare_queue: [
                 channel,
                 "test_exchange.test_routing_key.retry",
                 [
                   durable: true,
                   arguments: [{"x-dead-letter-exchange", :longstr, "test_exchange.retry"}]
                 ]
               ],
               bind_queue: [
                 channel,
                 "test_exchange.test_routing_key",
                 "test_exchange",
                 [routing_key: "test_routing_key"]
               ],
               bind_queue: [
                 channel,
                 "test_exchange.test_routing_key",
                 "test_exchange.retry",
                 [routing_key: "test_routing_key"]
               ],
               bind_queue: [
                 channel,
                 "test_exchange.test_routing_key.retry",
                 "test_exchange.delay",
                 [routing_key: "test_routing_key"]
               ],
               close_channel: [channel],
               close_connection: [connection]
             ] = log
    end

    @tag consumer_module: NotRetriableConsumer
    test "declares exchanges and queues for NotRetriableConsumer", %{
      log: log,
      rabbit_client: rabbit_client
    } do
      assert [
               open_connection: [^rabbit_client],
               open_channel: [connection],
               declare_direct_exchange: [channel, "test_exchange", [durable: true]],
               declare_queue: [channel, "test_exchange.test_routing_key", [durable: true]],
               bind_queue: [
                 channel,
                 "test_exchange.test_routing_key",
                 "test_exchange",
                 [routing_key: "test_routing_key"]
               ],
               close_channel: [channel],
               close_connection: [connection]
             ] = log
    end

    test "starts broadway with proper options", %{
      consumer: consumer,
      rabbit_client: rabbit_client
    } do
      broadway_opts =
        consumer
        |> Broadway.producer_names()
        |> List.first()
        |> GenStage.call(:get_state)
        |> Keyword.fetch!(:broadway)

      processor_opts = broadway_opts |> Keyword.fetch!(:processors) |> Keyword.fetch!(:default)

      assert Keyword.fetch!(processor_opts, :concurrency) == 1

      producer_opts = broadway_opts |> Keyword.fetch!(:producer)

      assert {BroadwayProducerMock, producer_module_opts} =
               producer_opts |> Keyword.fetch!(:module)

      assert producer_module_opts |> Keyword.fetch!(:connection) == rabbit_client
      assert producer_module_opts |> Keyword.fetch!(:on_failure) == :ack
      assert producer_module_opts |> Keyword.fetch!(:queue) == "test_exchange.test_routing_key"

      assert producer_module_opts |> Keyword.fetch!(:metadata) == [
               :content_type,
               :correlation_id,
               :headers,
               :reply_to,
               :routing_key,
               :timestamp
             ]
    end
  end

  describe ".handle_message/3" do
    setup tag do
      {:ok, rabbit_client} = RabbitClientMock.start_link()
      consumer_module = tag |> Map.get(:consumer_module, RetriableConsumer)

      {:ok, consumer} =
        consumer_module.start_link(connection: rabbit_client, name: new_consumer_name())

      [producer] = consumer |> Broadway.producer_names()
      rabbit_client |> RabbitClientMock.clear()

      {:ok, producer: producer, rabbit_client: rabbit_client}
    end

    test "handles message", %{producer: producer} do
      producer |> BroadwayProducerMock.push_message("Hello!")

      assert_receive {:message_handled, "Hello!",
                      %{rabbit_mq_metadata: _, retries_count: 0, sync: false, client: nil}}

      assert_receive {:successful, %{data: "Hello!"}}
    end

    test "handles message with x-client header", %{producer: producer} do
      producer
      |> BroadwayProducerMock.push_message("Hello!", %{
        headers: [{"x-client", :binary, "test_client"}]
      })

      assert_receive {:message_handled, "Hello!",
                      %{
                        rabbit_mq_metadata: _,
                        retries_count: 0,
                        sync: false,
                        client: "test_client"
                      }}

      assert_receive {:successful, %{data: "Hello!"}}
    end

    test "handles JSON message", %{producer: producer} do
      payload = ~s({"str":"value","int":1})

      producer
      |> BroadwayProducerMock.push_message(~s({"str":"value","int":1}), %{
        content_type: "application/json"
      })

      assert_receive {:message_handled, %{"str" => "value", "int" => 1},
                      %{rabbit_mq_metadata: _, retries_count: 0, sync: false}}

      assert_receive {:successful, %{data: ^payload}}
    end

    test "handles sync message", %{producer: producer, rabbit_client: rabbit_client} do
      producer
      |> BroadwayProducerMock.push_message(
        "Hello!",
        %{
          reply_to: "my_reply_to",
          correlation_id: "my_correlation_id"
        },
        rabbit_client
      )

      assert_receive {:message_handled, "Hello!",
                      %{rabbit_mq_metadata: _, retries_count: 0, sync: true}}

      assert_receive {:successful, %{data: "Hello!"}}

      assert [
               open_channel: _connection,
               publish: [channel, "", "my_reply_to", "", options],
               close_channel: [channel]
             ] = RabbitClientMock.get_log(rabbit_client)

      assert options |> Keyword.fetch!(:correlation_id) == "my_correlation_id"
      assert options |> Keyword.fetch!(:headers) == [{"x-reply-status", :binary, "ok"}]
    end

    test "handles sync message with x-reply-timeout header", %{
      producer: producer,
      rabbit_client: rabbit_client
    } do
      producer
      |> BroadwayProducerMock.push_message(
        "Hello!",
        %{
          reply_to: "my_reply_to",
          correlation_id: "my_correlation_id",
          headers: [{"x-reply-timeout", :long, 10_000}]
        },
        rabbit_client
      )

      assert_receive {:message_handled, "Hello!",
                      %{rabbit_mq_metadata: _, retries_count: 0, sync: true}}

      assert_receive {:successful, %{data: "Hello!"}}

      assert [
               open_channel: _connection,
               publish: [channel, "", "my_reply_to", "", options],
               close_channel: [channel]
             ] = RabbitClientMock.get_log(rabbit_client)

      assert options |> Keyword.fetch!(:expiration) == 10_000
      assert options |> Keyword.fetch!(:correlation_id) == "my_correlation_id"
      assert options |> Keyword.fetch!(:headers) == [{"x-reply-status", :binary, "ok"}]
    end

    test "handles sync message with response", %{producer: producer, rabbit_client: rabbit_client} do
      producer
      |> BroadwayProducerMock.push_message(
        "With response",
        %{
          reply_to: "my_reply_to",
          correlation_id: "my_correlation_id"
        },
        rabbit_client
      )

      assert_receive {:message_handled, "With response",
                      %{rabbit_mq_metadata: _, retries_count: 0, sync: true}}

      assert_receive {:successful, %{data: "With response"}}

      assert [
               open_channel: _connection,
               publish: [channel, "", "my_reply_to", "Response", options],
               close_channel: [channel]
             ] = RabbitClientMock.get_log(rabbit_client)

      assert options |> Keyword.fetch!(:correlation_id) == "my_correlation_id"
      assert options |> Keyword.fetch!(:headers) == [{"x-reply-status", :binary, "ok"}]
    end

    test "handles sync message with error response", %{
      producer: producer,
      rabbit_client: rabbit_client
    } do
      producer
      |> BroadwayProducerMock.push_message(
        "Failed!",
        %{
          routing_key: "my_routing_key",
          reply_to: "my_reply_to",
          correlation_id: "my_correlation_id"
        },
        rabbit_client
      )

      assert_receive {:message_handled, "Failed!",
                      %{rabbit_mq_metadata: _, retries_count: 0, sync: true}}

      assert_receive {:failed, %{data: "Failed!"}}

      assert [
               open_channel: _connection,
               publish: [channel, "", "my_reply_to", "Failed!", options],
               close_channel: [channel]
             ] = RabbitClientMock.get_log(rabbit_client)

      assert options |> Keyword.fetch!(:correlation_id) == "my_correlation_id"
      assert options |> Keyword.fetch!(:headers) == [{"x-reply-status", :binary, "error"}]
    end

    test "handles sync message with raised exception", %{
      producer: producer,
      rabbit_client: rabbit_client
    } do
      payload = "Raised!"

      producer
      |> BroadwayProducerMock.push_message(
        payload,
        %{
          routing_key: "my_routing_key",
          reply_to: "my_reply_to",
          correlation_id: "my_correlation_id"
        },
        rabbit_client
      )

      assert_receive {:message_handled, ^payload,
                      %{rabbit_mq_metadata: _, retries_count: 0, sync: true}}

      assert_receive {:failed, %{data: ^payload}}

      assert [
               open_channel: _connection,
               publish: [
                 channel,
                 "",
                 "my_reply_to",
                 ~s(%ArgumentError{message: "Test Argument Error!"}),
                 options
               ],
               close_channel: [channel]
             ] = RabbitClientMock.get_log(rabbit_client)

      assert options |> Keyword.fetch!(:correlation_id) == "my_correlation_id"
      assert options |> Keyword.fetch!(:headers) == [{"x-reply-status", :binary, "error"}]
    end

    test "handles sync JSON message", %{producer: producer, rabbit_client: rabbit_client} do
      payload = ~s({"str":"value","int":1})

      producer
      |> BroadwayProducerMock.push_message(
        payload,
        %{
          reply_to: "my_reply_to",
          correlation_id: "my_correlation_id",
          content_type: "application/json"
        },
        rabbit_client
      )

      assert_receive {:message_handled, %{"int" => 1, "str" => "value"},
                      %{rabbit_mq_metadata: _, retries_count: 0, sync: true}}

      assert_receive {:successful, %{data: ^payload}}

      assert [
               open_channel: _connection,
               publish: [channel, "", "my_reply_to", ~s(""), options],
               close_channel: [channel]
             ] = RabbitClientMock.get_log(rabbit_client)

      assert options |> Keyword.fetch!(:correlation_id) == "my_correlation_id"
      assert options |> Keyword.fetch!(:headers) == [{"x-reply-status", :binary, "ok"}]
      assert options |> Keyword.fetch!(:content_type) == "application/json"
    end

    test "handles sync JSON message with response", %{
      producer: producer,
      rabbit_client: rabbit_client
    } do
      payload = ~s({"str":"With response"})

      producer
      |> BroadwayProducerMock.push_message(
        payload,
        %{
          reply_to: "my_reply_to",
          correlation_id: "my_correlation_id",
          content_type: "application/json"
        },
        rabbit_client
      )

      assert_receive {:message_handled, %{"str" => "With response"},
                      %{rabbit_mq_metadata: _, retries_count: 0, sync: true}}

      assert_receive {:successful, %{data: ^payload}}

      assert [
               open_channel: _connection,
               publish: [channel, "", "my_reply_to", ~s({"str":"Response"}), options],
               close_channel: [channel]
             ] = RabbitClientMock.get_log(rabbit_client)

      assert options |> Keyword.fetch!(:correlation_id) == "my_correlation_id"
      assert options |> Keyword.fetch!(:headers) == [{"x-reply-status", :binary, "ok"}]
      assert options |> Keyword.fetch!(:content_type) == "application/json"
    end

    test "handles sync JSON message with error response", %{
      producer: producer,
      rabbit_client: rabbit_client
    } do
      payload = ~s({"str":"Failed!"})
      parsed_payload = %{"str" => "Failed!"}

      producer
      |> BroadwayProducerMock.push_message(
        payload,
        %{
          routing_key: "my_routing_key",
          reply_to: "my_reply_to",
          correlation_id: "my_correlation_id",
          content_type: "application/json"
        },
        rabbit_client
      )

      assert_receive {:message_handled, ^parsed_payload,
                      %{rabbit_mq_metadata: _, retries_count: 0, sync: true}}

      assert_receive {:failed, %{data: ^payload}}

      assert [
               open_channel: _connection,
               publish: [channel, "", "my_reply_to", ~s({"error":"Failed!"}), options],
               close_channel: [channel]
             ] = RabbitClientMock.get_log(rabbit_client)

      assert options |> Keyword.fetch!(:correlation_id) == "my_correlation_id"
      assert options |> Keyword.fetch!(:headers) == [{"x-reply-status", :binary, "error"}]
      assert options |> Keyword.fetch!(:content_type) == "application/json"
    end

    test "handles sync JSON message with raised exception", %{
      producer: producer,
      rabbit_client: rabbit_client
    } do
      payload = ~s({"str": "Raised!"})
      parsed_payload = %{"str" => "Raised!"}

      producer
      |> BroadwayProducerMock.push_message(
        payload,
        %{
          routing_key: "my_routing_key",
          reply_to: "my_reply_to",
          correlation_id: "my_correlation_id",
          content_type: "application/json"
        },
        rabbit_client
      )

      assert_receive {:message_handled, ^parsed_payload,
                      %{rabbit_mq_metadata: _, retries_count: 0, sync: true}}

      assert_receive {:failed, %{data: ^payload}}

      assert [
               open_channel: _connection,
               publish: [
                 channel,
                 "",
                 "my_reply_to",
                 ~s("%ArgumentError{message: \\"Test Argument Error!\\"}"),
                 options
               ],
               close_channel: [channel]
             ] = RabbitClientMock.get_log(rabbit_client)

      assert options |> Keyword.fetch!(:correlation_id) == "my_correlation_id"
      assert options |> Keyword.fetch!(:headers) == [{"x-reply-status", :binary, "error"}]
      assert options |> Keyword.fetch!(:content_type) == "application/json"
    end

    test "reschedules failed message", %{producer: producer, rabbit_client: rabbit_client} do
      producer
      |> BroadwayProducerMock.push_message(
        "Failed!",
        %{routing_key: "my_routing_key"},
        rabbit_client
      )

      assert_receive {:message_handled, "Failed!",
                      %{rabbit_mq_metadata: _, retries_count: 0, sync: false}}

      assert_receive {:failed_handled, "Failed!",
                      %{
                        rabbit_mq_metadata: _,
                        retries_count: 0,
                        status: {:failed, "Failed!"},
                        sync: false
                      }}

      assert_receive {:failed, %{data: "Failed!"}}

      assert [
               open_channel: _connection,
               publish: [channel, "test_exchange.delay", "my_routing_key", "Failed!", options],
               close_channel: [channel]
             ] = RabbitClientMock.get_log(rabbit_client)

      assert options |> Keyword.fetch!(:expiration) == "2000"
      assert options |> Keyword.fetch!(:headers) == [{"x-retries", :long, 1}]
      assert options |> Keyword.fetch!(:routing_key) == "my_routing_key"
    end

    test "reschedules failed JSON message", %{producer: producer, rabbit_client: rabbit_client} do
      payload = ~s({"str":"Failed!"})
      parsed_payload = %{"str" => "Failed!"}

      producer
      |> BroadwayProducerMock.push_message(
        payload,
        %{routing_key: "my_routing_key", content_type: "application/json"},
        rabbit_client
      )

      assert_receive {:message_handled, ^parsed_payload,
                      %{rabbit_mq_metadata: _, retries_count: 0, sync: false}}

      assert_receive {:failed_handled, ^parsed_payload,
                      %{
                        rabbit_mq_metadata: _,
                        retries_count: 0,
                        status: {:failed, %{"error" => "Failed!"}},
                        sync: false
                      }}

      assert_receive {:failed, %{data: ^payload}}

      assert [
               open_channel: _connection,
               publish: [channel, "test_exchange.delay", "my_routing_key", ^payload, options],
               close_channel: [channel]
             ] = RabbitClientMock.get_log(rabbit_client)

      assert options |> Keyword.fetch!(:expiration) == "2000"
      assert options |> Keyword.fetch!(:headers) == [{"x-retries", :long, 1}]
      assert options |> Keyword.fetch!(:routing_key) == "my_routing_key"
    end

    test "reschedules failed message using exp backoff", %{
      producer: producer,
      rabbit_client: rabbit_client
    } do
      producer
      |> BroadwayProducerMock.push_message(
        "Failed!",
        %{routing_key: "my_routing_key", headers: [{"x-retries", :long, 3}]},
        rabbit_client
      )

      assert_receive {:message_handled, "Failed!",
                      %{rabbit_mq_metadata: _, retries_count: 3, sync: false}}

      assert_receive {:failed_handled, "Failed!",
                      %{
                        rabbit_mq_metadata: _,
                        retries_count: 3,
                        status: {:failed, "Failed!"},
                        sync: false
                      }}

      assert_receive {:failed, %{data: "Failed!"}}

      assert [
               open_channel: _connection,
               publish: [channel, "test_exchange.delay", "my_routing_key", "Failed!", options],
               close_channel: [channel]
             ] = RabbitClientMock.get_log(rabbit_client)

      assert options |> Keyword.fetch!(:expiration) == "16000"
      assert options |> Keyword.fetch!(:headers) == [{"x-retries", :long, 4}]
      assert options |> Keyword.fetch!(:routing_key) == "my_routing_key"
    end

    test "reschedules failed message after raising exception", %{
      producer: producer,
      rabbit_client: rabbit_client
    } do
      payload = "Raised!"

      producer
      |> BroadwayProducerMock.push_message(
        payload,
        %{routing_key: "my_routing_key"},
        rabbit_client
      )

      assert_receive {:message_handled, ^payload,
                      %{rabbit_mq_metadata: _, retries_count: 0, sync: false}}

      assert_receive {:failed_handled, ^payload,
                      %{
                        rabbit_mq_metadata: _,
                        retries_count: 0,
                        status: {:error, %ArgumentError{message: "Test Argument Error!"}, _},
                        sync: false
                      }}

      assert_receive {:failed, %{data: ^payload}}

      assert [
               open_channel: _connection,
               publish: [channel, "test_exchange.delay", "my_routing_key", ^payload, options],
               close_channel: [channel]
             ] = RabbitClientMock.get_log(rabbit_client)

      assert options |> Keyword.fetch!(:expiration) == "2000"
      assert options |> Keyword.fetch!(:headers) == [{"x-retries", :long, 1}]
      assert options |> Keyword.fetch!(:routing_key) == "my_routing_key"
    end

    @tag consumer_module: NotRetriableConsumer
    test "doesn't reschedule failed message for NotRetriableConsumer", %{
      producer: producer,
      rabbit_client: rabbit_client
    } do
      producer
      |> BroadwayProducerMock.push_message(
        "Failed!",
        %{routing_key: "my_routing_key"},
        rabbit_client
      )

      assert_receive {:message_handled, "Failed!",
                      %{rabbit_mq_metadata: _, retries_count: 0, sync: false}}

      assert_receive {:failed_handled, "Failed!",
                      %{
                        rabbit_mq_metadata: _,
                        retries_count: 0,
                        status: {:failed, "Failed!"},
                        sync: false
                      }}

      assert_receive {:failed, %{data: "Failed!"}}

      assert [] = RabbitClientMock.get_log(rabbit_client)
    end
  end

  describe "build_strategy_data/1" do
    test "with usual exchange" do
      assert %{
               exchange: "usual_exchange",
               routing_key: "routing_key",
               delay_exchange: "usual_exchange.delay",
               retry_exchange: "usual_exchange.retry",
               queue: "usual_exchange.routing_key",
               retry_queue: "usual_exchange.routing_key.retry"
             } =
               Lepus.Consumer.build_strategy_data([],
                 exchange: "usual_exchange",
                 routing_key: "routing_key"
               )
    end

    test "with default exchange" do
      assert %{
               exchange: "",
               routing_key: "routing_key",
               delay_exchange: "delay",
               retry_exchange: "retry",
               queue: "routing_key",
               retry_queue: "routing_key.retry"
             } = Lepus.Consumer.build_strategy_data([], exchange: "", routing_key: "routing_key")
    end

    test "with delay_exchange defined" do
      assert %{
               exchange: "exchange",
               routing_key: "routing_key",
               delay_exchange: "delay_exchange",
               retry_exchange: "exchange.retry",
               queue: "exchange.routing_key",
               retry_queue: "exchange.routing_key.retry"
             } =
               Lepus.Consumer.build_strategy_data([],
                 exchange: "exchange",
                 routing_key: "routing_key",
                 delay_exchange: "delay_exchange"
               )
    end

    test "with retry_exchange defined" do
      assert %{
               exchange: "exchange",
               routing_key: "routing_key",
               delay_exchange: "exchange.delay",
               retry_exchange: "retry_exchange",
               queue: "exchange.routing_key",
               retry_queue: "exchange.routing_key.retry"
             } =
               Lepus.Consumer.build_strategy_data([],
                 exchange: "exchange",
                 routing_key: "routing_key",
                 retry_exchange: "retry_exchange"
               )
    end

    test "with queue defined" do
      assert %{
               exchange: "exchange",
               routing_key: "routing_key",
               delay_exchange: "exchange.delay",
               retry_exchange: "exchange.retry",
               queue: "queue",
               retry_queue: "exchange.routing_key.retry"
             } =
               Lepus.Consumer.build_strategy_data([],
                 exchange: "exchange",
                 routing_key: "routing_key",
                 queue: "queue"
               )
    end

    test "with retry_queue defined" do
      assert %{
               exchange: "exchange",
               routing_key: "routing_key",
               delay_exchange: "exchange.delay",
               retry_exchange: "exchange.retry",
               queue: "exchange.routing_key",
               retry_queue: "retry_queue"
             } =
               Lepus.Consumer.build_strategy_data([],
                 exchange: "exchange",
                 routing_key: "routing_key",
                 retry_queue: "retry_queue"
               )
    end
  end

  defp new_consumer_name do
    :"consumer-#{System.unique_integer([:positive, :monotonic])}"
  end
end
