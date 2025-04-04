defmodule Lepus.BasicClient.ServerTest do
  alias Lepus.{BasicClient, Rabbit, TestProducer}

  use ExUnit.Case, async: false

  import Mox

  describe ".start_link/1" do
    setup :set_mox_global
    setup :verify_on_exit!

    setup do
      test_process = self()

      Rabbit.TestClient
      |> stub(:open_connection, fn conn_opts ->
        test_process |> send({:open_connection, [conn_opts]})
        {:ok, connection_pid} = Agent.start_link(fn -> :connection end)

        {:ok, %{pid: connection_pid}}
      end)

      Rabbit.TestClient
      |> stub(:open_channel, fn connection ->
        test_process |> send({:open_channel, [connection]})
        {:ok, channel_pid} = Agent.start_link(fn -> :channel end)

        {:ok, %{pid: channel_pid}}
      end)

      Rabbit.TestClient
      |> stub(:declare_direct_exchange, fn channel, exchange, opts ->
        test_process
        |> send({:declare_direct_exchange, [channel, exchange, opts]})

        :ok
      end)

      TestProducer
      |> stub(:init, fn opts ->
        test_process |> send({:producer_init, [opts]})
        {:producer, []}
      end)

      TestProducer |> stub(:handle_demand, fn _, _ -> {:noreply, [], []} end)
      TestProducer |> stub(:terminate, fn _, _ -> :ok end)

      :ok
    end

    test "starts connection, channels and declares exchanges without RPC" do
      assert {:ok, _server} =
               BasicClient.start_link(
                 rabbit_client: Rabbit.TestClient,
                 name: MyApp.RabbitMQ,
                 connection: "my-rabbit-mq-connection",
                 exchanges: ["my-exchange1", "my-exchange2"]
               )

      assert_receive {:open_connection, ["my-rabbit-mq-connection"]}

      assert_receive {:open_channel, [%{pid: connection_pid}]}
      assert_receive {:open_channel, [%{pid: ^connection_pid}]}
      assert_receive {:open_channel, [%{pid: ^connection_pid}]}

      assert_receive {:declare_direct_exchange, [%{pid: channel0_pid}, "", [durable: true]]}

      assert_receive {:declare_direct_exchange,
                      [%{pid: channel1_pid}, "my-exchange1", [durable: true]]}

      assert_receive {:declare_direct_exchange,
                      [%{pid: channel2_pid}, "my-exchange2", [durable: true]]}

      refute channel0_pid == channel1_pid
      refute channel0_pid == channel2_pid
      refute channel1_pid == channel2_pid

      assert :ok = MyApp.RabbitMQ |> GenServer.stop(:normal)
    end

    test "starts connection, channels and declares exchanges with RPC" do
      assert {:ok, _server} =
               BasicClient.start_link(
                 rabbit_client: Rabbit.TestClient,
                 name: MyApp.RabbitMQ,
                 connection: "my-rabbit-mq-connection",
                 exchanges: ["my-exchange1", "my-exchange2"],
                 broadway_producer_module: TestProducer,
                 rpc_opts: [pubsub: MyApp.PubSub, reply_to_queue: "my-reply-to-queue"]
               )

      assert_receive {:open_connection, ["my-rabbit-mq-connection"]}

      assert_receive {:open_channel, [%{pid: connection_pid}]}
      assert_receive {:open_channel, [%{pid: ^connection_pid}]}
      assert_receive {:open_channel, [%{pid: ^connection_pid}]}

      assert_receive {:declare_direct_exchange, [%{pid: channel0_pid}, "", [durable: true]]}

      assert_receive {:declare_direct_exchange,
                      [%{pid: channel1_pid}, "my-exchange1", [durable: true]]}

      assert_receive {:declare_direct_exchange,
                      [%{pid: channel2_pid}, "my-exchange2", [durable: true]]}

      refute channel0_pid == channel1_pid
      refute channel0_pid == channel2_pid
      refute channel1_pid == channel2_pid

      assert_receive {:producer_init, [broadway_opts]}

      producer_opts = broadway_opts |> Keyword.fetch!(:broadway) |> Keyword.fetch!(:producer)

      assert {TestProducer, producer_module_opts} = producer_opts |> Keyword.fetch!(:module)

      assert :ack = producer_module_opts |> Keyword.fetch!(:on_failure)
      assert "my-rabbit-mq-connection" = producer_module_opts |> Keyword.fetch!(:connection)
      assert "my-reply-to-queue" = producer_module_opts |> Keyword.fetch!(:queue)

      assert [arguments: [{"x-queue-type", :longstr, "classic"}], durable: true] =
               producer_module_opts |> Keyword.fetch!(:declare) |> Enum.sort()

      assert [:content_type, :correlation_id, :headers] =
               producer_module_opts |> Keyword.fetch!(:metadata) |> Enum.sort()

      assert :ok = MyApp.RabbitMQ |> GenServer.stop(:normal)
    end

    test "RPC with quorum reply_to queue" do
      assert {:ok, _server} =
               BasicClient.start_link(
                 rabbit_client: Rabbit.TestClient,
                 name: MyApp.RabbitMQ,
                 connection: "my-rabbit-mq-connection",
                 exchanges: ["my-exchange1", "my-exchange2"],
                 broadway_producer_module: TestProducer,
                 rpc_opts: [
                   pubsub: MyApp.PubSub,
                   reply_to_queue: "my-reply-to-queue",
                   queues_type: "quorum"
                 ]
               )

      assert_receive {:open_connection, ["my-rabbit-mq-connection"]}

      assert_receive {:open_channel, [%{pid: connection_pid}]}
      assert_receive {:open_channel, [%{pid: ^connection_pid}]}
      assert_receive {:open_channel, [%{pid: ^connection_pid}]}

      assert_receive {:declare_direct_exchange, [%{pid: channel0_pid}, "", [durable: true]]}

      assert_receive {:declare_direct_exchange,
                      [%{pid: channel1_pid}, "my-exchange1", [durable: true]]}

      assert_receive {:declare_direct_exchange,
                      [%{pid: channel2_pid}, "my-exchange2", [durable: true]]}

      refute channel0_pid == channel1_pid
      refute channel0_pid == channel2_pid
      refute channel1_pid == channel2_pid

      assert_receive {:producer_init, [broadway_opts]}

      producer_opts = broadway_opts |> Keyword.fetch!(:broadway) |> Keyword.fetch!(:producer)

      assert {TestProducer, producer_module_opts} = producer_opts |> Keyword.fetch!(:module)

      assert :ack = producer_module_opts |> Keyword.fetch!(:on_failure)
      assert "my-rabbit-mq-connection" = producer_module_opts |> Keyword.fetch!(:connection)
      assert "my-reply-to-queue" = producer_module_opts |> Keyword.fetch!(:queue)

      assert [arguments: [{"x-queue-type", :longstr, "quorum"}], durable: true] =
               producer_module_opts |> Keyword.fetch!(:declare) |> Enum.sort()

      assert [:content_type, :correlation_id, :headers] =
               producer_module_opts |> Keyword.fetch!(:metadata) |> Enum.sort()

      assert :ok = MyApp.RabbitMQ |> GenServer.stop(:normal)
    end
  end

  describe ".publish/5 & .publish_json/5 without rpc_opts" do
    setup :set_mox_global
    setup :verify_on_exit!

    setup do
      test_process = self()

      open_something = fn _ ->
        {:ok, pid} = Agent.start_link(fn -> [] end)
        {:ok, %{pid: pid}}
      end

      Rabbit.TestClient |> stub(:open_connection, open_something)
      Rabbit.TestClient |> stub(:open_channel, open_something)
      Rabbit.TestClient |> stub(:close_channel, fn _ -> :ok end)
      Rabbit.TestClient |> stub(:close_connection, fn _ -> :ok end)

      Rabbit.TestClient
      |> stub(:declare_direct_exchange, fn _channel, _exchange, _opts -> :ok end)

      Rabbit.TestClient
      |> stub(:publish, fn channel, exchange, routing_key, payload, opts ->
        test_process |> send({:publish, [channel, exchange, routing_key, payload, opts]})
        :ok
      end)

      start_supervised!(
        {BasicClient,
         rabbit_client: Rabbit.TestClient,
         name: MyApp.RabbitMQ,
         connection: "my-rabbit-mq-connection",
         exchanges: ["my-exchange1", "my-exchange2"]},
        restart: :temporary
      )

      :ok
    end

    test ".publish/5 publishes to rabbit and adds timestamp and x-client to opts" do
      assert :ok =
               MyApp.RabbitMQ
               |> BasicClient.publish("my-exchange1", "my-routing-key", "my_payload",
                 amqp_opts: [
                   my_key: "my_value",
                   headers: [{"my-header", :binary, "my-header-value"}]
                 ]
               )

      assert_receive {:publish, [_channel, "my-exchange1", "my-routing-key", "my_payload", opts]}

      assert opts |> Keyword.fetch!(:my_key) == "my_value"
      assert :error = opts |> Keyword.fetch(:content_type)

      assert {:ok, _datetime} =
               opts |> Keyword.fetch!(:timestamp) |> DateTime.from_unix(:microsecond)

      assert [{"my-header", :binary, "my-header-value"}, {"x-client", :binary, "lepus"}] =
               opts |> Keyword.fetch!(:headers) |> Enum.sort()
    end

    test ".publish_json/5 publishes to rabbit and adds content_type, timestamp and x-client to opts" do
      assert :ok =
               MyApp.RabbitMQ
               |> BasicClient.publish_json(
                 "my-exchange1",
                 "my-routing-key",
                 %{my_key: "my value"},
                 amqp_opts: [
                   my_key: "my_value",
                   headers: [{"my-header", :binary, "my-header-value"}]
                 ]
               )

      assert_receive {:publish,
                      [
                        _channel,
                        "my-exchange1",
                        "my-routing-key",
                        ~s({"my_key":"my value"}),
                        opts
                      ]}

      assert opts |> Keyword.fetch!(:my_key) == "my_value"
      assert "application/json" = opts |> Keyword.fetch!(:content_type)

      assert {:ok, _datetime} =
               opts |> Keyword.fetch!(:timestamp) |> DateTime.from_unix(:microsecond)

      assert [{"my-header", :binary, "my-header-value"}, {"x-client", :binary, "lepus"}] =
               opts |> Keyword.fetch!(:headers) |> Enum.sort()
    end

    test ".publish/5 uses 1 channel per exchange" do
      assert :ok = MyApp.RabbitMQ |> BasicClient.publish("", "my-routing-key", "first", [])

      assert :ok =
               MyApp.RabbitMQ
               |> BasicClient.publish("my-exchange1", "my-routing-key", "first", [])

      assert :ok =
               MyApp.RabbitMQ
               |> BasicClient.publish("my-exchange2", "my-routing-key", "first", [])

      assert :ok = MyApp.RabbitMQ |> BasicClient.publish("", "my-routing-key", "second", [])

      assert :ok =
               MyApp.RabbitMQ
               |> BasicClient.publish("my-exchange1", "my-routing-key", "second", [])

      assert :ok =
               MyApp.RabbitMQ
               |> BasicClient.publish("my-exchange2", "my-routing-key", "second", [])

      assert_receive {:publish, [%{pid: channel0_pid}, "", "my-routing-key", "first", _]}

      assert_receive {:publish,
                      [%{pid: channel1_pid}, "my-exchange1", "my-routing-key", "first", _]}

      assert_receive {:publish,
                      [%{pid: channel2_pid}, "my-exchange2", "my-routing-key", "first", _]}

      assert_receive {:publish, [%{pid: ^channel0_pid}, "", "my-routing-key", "second", _]}

      assert_receive {:publish,
                      [%{pid: ^channel1_pid}, "my-exchange1", "my-routing-key", "second", _]}

      assert_receive {:publish,
                      [%{pid: ^channel2_pid}, "my-exchange2", "my-routing-key", "second", _]}

      refute channel0_pid == channel1_pid
      refute channel0_pid == channel2_pid
      refute channel1_pid == channel2_pid
    end

    test ".publish_json/5 uses 1 channel per exchange" do
      assert :ok = MyApp.RabbitMQ |> BasicClient.publish_json("", "my-routing-key", %{n: 1}, [])

      assert :ok =
               MyApp.RabbitMQ
               |> BasicClient.publish_json("my-exchange1", "my-routing-key", %{n: 1}, [])

      assert :ok =
               MyApp.RabbitMQ
               |> BasicClient.publish_json("my-exchange2", "my-routing-key", %{n: 1}, [])

      assert :ok = MyApp.RabbitMQ |> BasicClient.publish_json("", "my-routing-key", %{n: 2}, [])

      assert :ok =
               MyApp.RabbitMQ
               |> BasicClient.publish_json("my-exchange1", "my-routing-key", %{n: 2}, [])

      assert :ok =
               MyApp.RabbitMQ
               |> BasicClient.publish_json("my-exchange2", "my-routing-key", %{n: 2}, [])

      assert_receive {:publish, [%{pid: channel0_pid}, "", "my-routing-key", ~s({"n":1}), _]}

      assert_receive {:publish,
                      [%{pid: channel1_pid}, "my-exchange1", "my-routing-key", ~s({"n":1}), _]}

      assert_receive {:publish,
                      [%{pid: channel2_pid}, "my-exchange2", "my-routing-key", ~s({"n":1}), _]}

      assert_receive {:publish, [%{pid: ^channel0_pid}, "", "my-routing-key", ~s({"n":2}), _]}

      assert_receive {:publish,
                      [%{pid: ^channel1_pid}, "my-exchange1", "my-routing-key", ~s({"n":2}), _]}

      assert_receive {:publish,
                      [%{pid: ^channel2_pid}, "my-exchange2", "my-routing-key", ~s({"n":2}), _]}

      refute channel0_pid == channel1_pid
      refute channel0_pid == channel2_pid
      refute channel1_pid == channel2_pid
    end

    test ".publish/5 doesn't publish to wrong exchange" do
      assert_raise RuntimeError,
                   "There is no exchange with the 'wrong-exchange' name. Check the `:exchanges` option",
                   fn ->
                     MyApp.RabbitMQ
                     |> BasicClient.publish("wrong-exchange", "my-routing-key", "payload", [])
                   end
    end

    test ".publish_json/5 doesn't publish to wrong exchange" do
      assert_raise RuntimeError,
                   "There is no exchange with the 'wrong-exchange' name. Check the `:exchanges` option",
                   fn ->
                     MyApp.RabbitMQ
                     |> BasicClient.publish_json(
                       "wrong-exchange",
                       "my-routing-key",
                       %{key: "value"},
                       []
                     )
                   end
    end

    test ".publish/5 doesn't publish with rpc opt" do
      assert_raise RuntimeError,
                   "Add `rpc_opts` to the `Lepus.BasicClient` configuration for using `rpc` option",
                   fn ->
                     MyApp.RabbitMQ
                     |> BasicClient.publish("my-exchange1", "my-routing-key", "payload",
                       rpc: true
                     )
                   end
    end

    test ".publish_json/5 doesn't publish with rpc opt" do
      assert_raise RuntimeError,
                   "Add `rpc_opts` to the `Lepus.BasicClient` configuration for using `rpc` option",
                   fn ->
                     MyApp.RabbitMQ
                     |> BasicClient.publish_json(
                       "my-exchange1",
                       "my-routing-key",
                       %{data: "my-payload"},
                       rpc: true
                     )
                   end
    end
  end

  describe ".publish/5 & .publish_json/5 with rpc_opts" do
    setup :set_mox_global
    setup :verify_on_exit!

    setup ctx do
      test_process = self()

      open_something = fn _ ->
        {:ok, pid} = Agent.start_link(fn -> [] end)
        {:ok, %{pid: pid}}
      end

      close_something = fn something ->
        something.pid |> Agent.stop()
      end

      Rabbit.TestClient |> stub(:open_connection, open_something)
      Rabbit.TestClient |> stub(:open_channel, open_something)
      Rabbit.TestClient |> stub(:close_channel, close_something)
      Rabbit.TestClient |> stub(:close_connection, close_something)

      Rabbit.TestClient
      |> stub(:declare_direct_exchange, fn _channel, _exchange, _opts -> :ok end)

      Rabbit.TestClient
      |> stub(:publish, fn channel, exchange, routing_key, payload, opts ->
        test_process |> send({:publish, [channel, exchange, routing_key, payload, opts]})

        with {:ok, reply_payload} <- ctx |> Map.fetch(:reply_payload),
             {:ok, correlation_id} when is_binary(correlation_id) <-
               opts |> Keyword.fetch(:correlation_id),
             {:ok, reply_to} when is_binary(reply_to) <- opts |> Keyword.fetch(:reply_to) do
          metadata =
            ctx
            |> Map.get(:reply_metadata, [])
            |> Keyword.new()
            |> Keyword.put(:correlation_id, correlation_id)

          reply_timeout = ctx |> Map.get(:reply_timeout, 0)

          Task.async(fn ->
            Process.sleep(reply_timeout)

            Broadway.all_running()
            |> List.first()
            |> Broadway.test_message(reply_payload, metadata: metadata)
          end)
        end

        :ok
      end)

      start_supervised!({Phoenix.PubSub, name: MyApp.PubSub})

      start_supervised!(
        {BasicClient,
         broadway_producer_module: Broadway.DummyProducer,
         rabbit_client: Rabbit.TestClient,
         name: MyApp.RabbitMQ,
         connection: "my-rabbit-mq-connection",
         exchanges: ["my-exchange"],
         rpc_opts: [pubsub: MyApp.PubSub, reply_to_queue: "my-reply-to-queue"]},
        restart: :temporary
      )

      :ok
    end

    @tag reply_payload: "Good", reply_metadata: [headers: [{"x-reply-status", :binary, "ok"}]]
    test(".publish/5 publishes to rabbit and receives response") do
      assert {:ok, "Good"} =
               MyApp.RabbitMQ
               |> BasicClient.publish("my-exchange", "my-routing-key", "my-payload",
                 amqp_opts: [
                   my_key: "my-value",
                   headers: [{"my-header", :binary, "my-header-value"}]
                 ],
                 rpc: true,
                 timeout: :infinity
               )

      assert_receive {:publish, [_channel, "my-exchange", "my-routing-key", "my-payload", opts]}

      assert opts |> Keyword.fetch!(:my_key) == "my-value"
      assert :error = opts |> Keyword.fetch(:content_type)

      assert {:ok, _datetime} =
               opts |> Keyword.fetch!(:timestamp) |> DateTime.from_unix(:microsecond)

      assert [{"my-header", :binary, "my-header-value"}, {"x-client", :binary, "lepus"}] =
               opts |> Keyword.fetch!(:headers) |> Enum.sort()
    end

    @tag reply_payload: ~s({"response": "Good"}),
         reply_metadata: [
           headers: [{"x-reply-status", :binary, "ok"}],
           content_type: "application/json"
         ]
    test ".publish_json/5 publishes to rabbit and receives response" do
      assert {:ok, %{"response" => "Good"}} =
               MyApp.RabbitMQ
               |> BasicClient.publish_json("my-exchange", "my-routing-key", %{data: "data"},
                 amqp_opts: [
                   my_key: "my-value",
                   headers: [{"my-header", :binary, "my-header-value"}]
                 ],
                 rpc: true,
                 timeout: :infinity
               )

      assert_receive {:publish,
                      [_channel, "my-exchange", "my-routing-key", ~s({"data":"data"}), opts]}

      assert opts |> Keyword.fetch!(:my_key) == "my-value"
      assert "application/json" = opts |> Keyword.fetch!(:content_type)

      assert {:ok, _datetime} =
               opts |> Keyword.fetch!(:timestamp) |> DateTime.from_unix(:microsecond)

      assert [{"my-header", :binary, "my-header-value"}, {"x-client", :binary, "lepus"}] =
               opts |> Keyword.fetch!(:headers) |> Enum.sort()
    end

    @tag reply_payload: "Bad", reply_metadata: [headers: [{"x-reply-status", :binary, "error"}]]
    test ".publish/5 publishes to rabbit and receives an error" do
      assert {:error, "Bad"} =
               MyApp.RabbitMQ
               |> BasicClient.publish("my-exchange", "my-routing-key", "my-payload", rpc: true)
    end

    @tag reply_payload: ~s({"response": "Bad"}),
         reply_metadata: [
           headers: [{"x-reply-status", :binary, "error"}],
           content_type: "application/json"
         ]
    test ".publish_json publishes to rabbit and receives an error" do
      assert {:error, %{"response" => "Bad"}} =
               MyApp.RabbitMQ
               |> BasicClient.publish_json("my-exchange", "my-routing-key", %{data: "data"},
                 rpc: true
               )
    end

    test ".publish/5 publishes to rabbit and receives timeout error" do
      assert {:error, :timeout} =
               MyApp.RabbitMQ
               |> BasicClient.publish("my-exchange", "my-routing-key", "my-payload",
                 rpc: true,
                 timeout: 100
               )
    end

    @tag reply_payload: ~s({"response": "Good"}),
         reply_metadata: [
           headers: [{"x-reply-status", :binary, "ok"}],
           content_type: "application/json"
         ],
         reply_timeout: 200
    test ".publish/5 doesn't respond with previous result", %{reply_timeout: reply_timeout} do
      assert {:error, :timeout} =
               MyApp.RabbitMQ
               |> BasicClient.publish("my-exchange", "my-routing-key", "my-payload",
                 rpc: true,
                 timeout: 100
               )

      Process.sleep(reply_timeout * 2)

      assert {:error, :timeout} =
               MyApp.RabbitMQ
               |> BasicClient.publish("my-exchange", "my-routing-key", "my-payload",
                 rpc: true,
                 timeout: 100
               )
    end

    @tag reply_payload: "Error message",
         reply_metadata: [headers: [{"x-reply-status", :binary, "error"}]],
         reply_timeout: 200
    test ".publish/5 doesn't respond with previous timeout error", %{
      reply_timeout: reply_timeout
    } do
      long_time = reply_timeout * 2

      assert {:error, "Error message"} =
               MyApp.RabbitMQ
               |> BasicClient.publish("my-exchange", "my-routing-key", "my-payload",
                 rpc: true,
                 timeout: long_time
               )

      assert {:error, "Error message"} =
               MyApp.RabbitMQ
               |> BasicClient.publish("my-exchange", "my-routing-key", "my-payload",
                 rpc: true,
                 timeout: long_time
               )
    end

    test ".publish_json/5 publishes to rabbit and receives timeout error" do
      assert {:error, :timeout} =
               MyApp.RabbitMQ
               |> BasicClient.publish("my-exchange", "my-routing-key", %{data: "data"},
                 rpc: true,
                 timeout: 100
               )
    end
  end
end
