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

    defp log(pid, name, args) do
      pid |> Agent.update(fn state -> [{name, args} | state] end)
    end

    def get_log(pid), do: pid |> Agent.get(& &1) |> Enum.reverse()
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

  defmodule Consumer do
    use Lepus.Consumer,
      exchange: "test_exchange",
      routing_key: "test_routing_key",
      rabbit_client: RabbitClientMock,
      broadway_producer_module: BroadwayProducerMock

    @impl Lepus.Consumer
    def options do
      [producer: [concurrency: 1], processors: [my_processor_name: [concurrency: 1]]]
    end

    def handle_message(_, %{metadata: %{test_pid: test_pid}, data: data} = message, _) do
      message =
        if data in ["Failed!", %{"str" => "Failed!"}] do
          message |> Broadway.Message.failed(":(")
        else
          message
        end

      Process.send(test_pid, {:message_handled, message}, [])
      message
    end
  end

  describe ".start_link/1" do
    setup do
      {:ok, rabbit_client} = RabbitClientMock.start_link()
      {:ok, consumer} = Consumer.start_link(connection: rabbit_client, name: new_consumer_name())
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

      processor_opts =
        broadway_opts |> Keyword.fetch!(:processors) |> Keyword.fetch!(:my_processor_name)

      assert Keyword.fetch!(processor_opts, :concurrency) == 1

      producer_opts = broadway_opts |> Keyword.fetch!(:producer)

      assert producer_opts |> Keyword.fetch!(:transformer) == {Lepus.Consumer, :transform, []}

      assert {BroadwayProducerMock, producer_module_opts} =
               producer_opts |> Keyword.fetch!(:module)

      assert producer_module_opts |> Keyword.fetch!(:connection) == rabbit_client
      assert producer_module_opts |> Keyword.fetch!(:on_failure) == :ack
      assert producer_module_opts |> Keyword.fetch!(:queue) == "test_exchange.test_routing_key"

      assert producer_module_opts |> Keyword.fetch!(:metadata) == [
               :headers,
               :routing_key,
               :content_type,
               :timestamp
             ]
    end
  end

  describe ".handle_message/3" do
    setup do
      {:ok, rabbit_client} = RabbitClientMock.start_link()
      {:ok, consumer} = Consumer.start_link(connection: rabbit_client, name: new_consumer_name())
      [producer] = consumer |> Broadway.producer_names()

      {:ok, producer: producer}
    end

    test "handles message", %{producer: producer} do
      producer |> BroadwayProducerMock.push_message("Hello!")
      assert_receive {:message_handled, %{data: "Hello!"}}
      assert_receive {:successful, %{data: "Hello!"}}
    end

    test "handles JSON message", %{producer: producer} do
      producer
      |> BroadwayProducerMock.push_message(~s({"str":"value","int":1}), %{
        content_type: "application/json"
      })

      parsed_data = %{"str" => "value", "int" => 1}

      assert_receive {:message_handled, %{data: ^parsed_data}}
      assert_receive {:successful, %{data: ^parsed_data}}
    end

    test "reschedules failed message", %{producer: producer} do
      {:ok, rabbit_client} = RabbitClientMock.start_link()

      producer
      |> BroadwayProducerMock.push_message(
        "Failed!",
        %{routing_key: "my_routing_key"},
        rabbit_client
      )

      assert_receive {:message_handled, %{data: "Failed!"}}
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

    test "reschedules failed JSON message", %{producer: producer} do
      {:ok, rabbit_client} = RabbitClientMock.start_link()

      payload = ~s({"str":"Failed!"})

      producer
      |> BroadwayProducerMock.push_message(
        payload,
        %{routing_key: "my_routing_key", content_type: "application/json"},
        rabbit_client
      )

      assert_receive {:message_handled, %{data: %{"str" => "Failed!"}}}
      assert_receive {:failed, %{data: %{"str" => "Failed!"}}}

      assert [
               open_channel: _connection,
               publish: [channel, "test_exchange.delay", "my_routing_key", ^payload, options],
               close_channel: [channel]
             ] = RabbitClientMock.get_log(rabbit_client)

      assert options |> Keyword.fetch!(:expiration) == "2000"
      assert options |> Keyword.fetch!(:headers) == [{"x-retries", :long, 1}]
      assert options |> Keyword.fetch!(:routing_key) == "my_routing_key"
    end

    test "reschedules failed message using exp backoff", %{producer: producer} do
      {:ok, rabbit_client} = RabbitClientMock.start_link()

      producer
      |> BroadwayProducerMock.push_message(
        "Failed!",
        %{routing_key: "my_routing_key", headers: [{"x-retries", :long, 3}]},
        rabbit_client
      )

      assert_receive {:message_handled, %{data: "Failed!"}}
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
               Lepus.Consumer.build_strategy_data(
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
             } = Lepus.Consumer.build_strategy_data(exchange: "", routing_key: "routing_key")
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
               Lepus.Consumer.build_strategy_data(
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
               Lepus.Consumer.build_strategy_data(
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
               Lepus.Consumer.build_strategy_data(
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
               Lepus.Consumer.build_strategy_data(
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
