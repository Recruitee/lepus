defmodule Lepus.ConsumerTest do
  use ExUnit.Case, async: false

  alias Lepus.Rabbit
  alias Lepus.TestConsumer
  alias Lepus.TestProducer

  import Mox

  @timestamp ~U[2022-12-31 23:59:59Z] |> DateTime.to_unix(:microsecond)

  describe ".start_link/1" do
    setup :set_mox_global
    setup :verify_on_exit!

    setup do
      TestProducer |> stub(:terminate, fn _, _ -> :ok end)
      TestProducer |> stub(:handle_demand, fn _, _ -> {:noreply, [], []} end)

      :ok
    end

    test "sets producer options" do
      TestConsumer
      |> expect(:options, fn ->
        [
          broadway_producer_module: TestProducer,
          rabbit_client: Rabbit.TestClient,
          exchange: "my-exchange",
          routing_key: "my-routing-key"
        ]
      end)

      Lepus.TestProducer
      |> expect(:init, fn opts ->
        assert {Lepus.TestProducer, module_opts} =
                 opts
                 |> Keyword.fetch!(:broadway)
                 |> Keyword.fetch!(:producer)
                 |> Keyword.fetch!(:module)

        assert "my-rabbit-mq-connection" = module_opts |> Keyword.fetch!(:connection)
        assert "my-exchange.my-routing-key" = module_opts |> Keyword.fetch!(:queue)
        assert :ack = module_opts |> Keyword.fetch!(:on_failure)

        assert [:content_type, :correlation_id, :headers, :reply_to, :routing_key, :timestamp] =
                 module_opts |> Keyword.fetch!(:metadata) |> Enum.sort()

        after_connect = module_opts |> Keyword.fetch!(:after_connect)
        assert after_connect |> is_function(1)

        assert :ok = :my_channel |> after_connect.()

        {:producer, []}
      end)

      Rabbit.TestClient
      |> expect(:declare_direct_exchange, fn :my_channel, "my-exchange", _ -> :ok end)

      Rabbit.TestClient
      |> expect(:declare_queue, fn :my_channel, "my-exchange.my-routing-key", _ -> :ok end)

      Rabbit.TestClient
      |> expect(:bind_queue, fn
        :my_channel,
        "my-exchange.my-routing-key",
        "my-exchange",
        [routing_key: "my-routing-key"] ->
          :ok
      end)

      assert {:ok, _} =
               Lepus.Consumer.start_link(
                 consumers: [TestConsumer],
                 options: [connection: "my-rabbit-mq-connection"]
               )

      assert :ok = Lepus.Consumer |> GenServer.stop(:normal)
    end

    test "local producer options rewrite global options" do
      TestConsumer
      |> expect(:options, fn ->
        [
          broadway_producer_module: TestProducer,
          rabbit_client: Rabbit.TestClient,
          exchange: "my-exchange",
          routing_key: "my-routing-key",
          connection: "other-rabbit-mq-connection"
        ]
      end)

      Lepus.TestProducer
      |> expect(:init, fn opts ->
        assert {Lepus.TestProducer, module_opts} =
                 opts
                 |> Keyword.fetch!(:broadway)
                 |> Keyword.fetch!(:producer)
                 |> Keyword.fetch!(:module)

        assert "other-rabbit-mq-connection" = module_opts |> Keyword.fetch!(:connection)

        {:producer, []}
      end)

      assert {:ok, _} =
               Lepus.Consumer.start_link(
                 consumers: [TestConsumer],
                 options: [connection: "my-rabbit-mq-connection"]
               )

      assert :ok = Lepus.Consumer |> GenServer.stop(:normal)
    end
  end

  describe ".handle_message/2" do
    setup :set_mox_global
    setup :verify_on_exit!

    setup ctx do
      routing_key = "my-routing-key"

      TestConsumer
      |> stub(:options, fn ->
        overwrite = ctx |> Map.get(:consumer_options, [])

        [
          broadway_producer_module: Broadway.DummyProducer,
          rabbit_client: Rabbit.TestClient,
          connection: "my-rabbit-mq-connection",
          exchange: "my-exchange",
          routing_key: routing_key,
          queue: "my-queue"
        ]
        |> Keyword.merge(overwrite)
      end)

      test_process = self()

      TestConsumer
      |> stub(:handle_message, fn data, metadata ->
        test_process |> send({:handle_message, [data, metadata]})

        ctx
        |> Map.fetch(:handle_message)
        |> case do
          {:ok, {:return, something}} -> something
          {:ok, {:raise, something}} -> raise something
          _ -> :ok
        end
      end)

      Rabbit.TestClient
      |> stub(:publish, fn channel, exchange, routing_key, binary, keyword ->
        test_process
        |> send({:rabbit_mq_publish, [channel, exchange, routing_key, binary, keyword]})

        :ok
      end)

      start_supervised!({Lepus.Consumer, consumers: [TestConsumer]}, restart: :temporary)

      default_rabbit_mq_metadata = %{
        amqp_channel: :test_amqp_channel,
        content_type: "my-content-type",
        correlation_id: nil,
        headers: [{"my-header", :binary, "My Header Value"}],
        reply_to: nil,
        routing_key: routing_key,
        timestamp: @timestamp
      }

      {:ok, default_rabbit_mq_metadata: default_rabbit_mq_metadata}
    end

    test "receives data and metadata", %{default_rabbit_mq_metadata: default_rabbit_mq_metadata} do
      TestConsumer |> Broadway.test_message("data", metadata: default_rabbit_mq_metadata)

      assert_receive {:handle_message,
                      [
                        "data",
                        %{
                          rabbit_mq_metadata: %{},
                          client: nil,
                          retriable: false,
                          retry_count: 0,
                          rpc: false
                        }
                      ]}

      refute_receive {:rabbit_mq_publish, _}
    end

    test "receives `client` metadata key", %{
      default_rabbit_mq_metadata: default_rabbit_mq_metadata
    } do
      TestConsumer
      |> Broadway.test_message("data",
        metadata: %{default_rabbit_mq_metadata | headers: [{"x-client", :binary, "test-client"}]}
      )

      assert_receive {:handle_message, ["data", %{client: "test-client"}]}
      refute_receive {:rabbit_mq_publish, _}
    end

    test "automatically converts JSON messages", %{
      default_rabbit_mq_metadata: default_rabbit_mq_metadata
    } do
      TestConsumer
      |> Broadway.test_message(~s({"string": "String", "int": 1}),
        metadata: %{default_rabbit_mq_metadata | content_type: "application/json"}
      )

      assert_receive {:handle_message, [%{"string" => "String", "int" => 1}, _metadata]}
      refute_receive {:rabbit_mq_publish, _}
    end

    @tag handle_message: {:raise, "Test Exception!"}
    test "doesn't retry unretriable messages", %{
      default_rabbit_mq_metadata: default_rabbit_mq_metadata
    } do
      TestConsumer |> Broadway.test_message("data", metadata: default_rabbit_mq_metadata)

      assert_receive {:handle_message, [_data, _metadata]}
      refute_receive {:rabbit_mq_publish, _}
    end

    @tag consumer_options: [store_failed: true], handle_message: {:raise, "Test Exception!"}
    test "stores failded messages in case of exception", %{
      default_rabbit_mq_metadata: default_rabbit_mq_metadata
    } do
      TestConsumer |> Broadway.test_message("data", metadata: default_rabbit_mq_metadata)

      assert_receive {:handle_message, [_data, _metadata]}

      assert_receive {:rabbit_mq_publish,
                      [:test_amqp_channel, "my-exchange.failed", "my-routing-key", "data", opts]}

      assert [
               {"my-header", :binary, "My Header Value"},
               {"x-status-0", :binary, "%RuntimeError{message: \"Test Exception!\"}"}
             ] = opts |> Keyword.fetch!(:headers) |> Enum.sort()

      assert "my-content-type" = opts |> Keyword.fetch!(:content_type)
      assert @timestamp = opts |> Keyword.fetch!(:timestamp)
    end

    @tag consumer_options: [store_failed: true], handle_message: {:return, {:error, "Test Error"}}
    test "stores failded messages in case of error with message", %{
      default_rabbit_mq_metadata: default_rabbit_mq_metadata
    } do
      TestConsumer |> Broadway.test_message("data", metadata: default_rabbit_mq_metadata)

      assert_receive {:handle_message, [_data, _metadata]}

      assert_receive {:rabbit_mq_publish,
                      [:test_amqp_channel, "my-exchange.failed", "my-routing-key", "data", opts]}

      assert [
               {"my-header", :binary, "My Header Value"},
               {"x-status-0", :binary, "Test Error"}
             ] = opts |> Keyword.fetch!(:headers) |> Enum.sort()

      assert "my-content-type" = opts |> Keyword.fetch!(:content_type)
      assert @timestamp = opts |> Keyword.fetch!(:timestamp)
    end

    @tag consumer_options: [store_failed: true], handle_message: {:return, :error}
    test "stores failded messages in case of error without message", %{
      default_rabbit_mq_metadata: default_rabbit_mq_metadata
    } do
      TestConsumer |> Broadway.test_message("data", metadata: default_rabbit_mq_metadata)

      assert_receive {:handle_message, [_data, _metadata]}

      assert_receive {:rabbit_mq_publish,
                      [:test_amqp_channel, "my-exchange.failed", "my-routing-key", "data", opts]}

      assert [{"my-header", :binary, "My Header Value"}] = opts |> Keyword.fetch!(:headers)
      assert "my-content-type" = opts |> Keyword.fetch!(:content_type)
      assert @timestamp = opts |> Keyword.fetch!(:timestamp)
    end

    @tag consumer_options: [max_retry_count: 1], handle_message: {:raise, "Test Exception!"}
    test "retries failed messages in case of exception", %{
      default_rabbit_mq_metadata: default_rabbit_mq_metadata
    } do
      TestConsumer |> Broadway.test_message("data", metadata: default_rabbit_mq_metadata)

      assert_receive {:handle_message, [_data, _metadata]}

      assert_receive {:rabbit_mq_publish,
                      [:test_amqp_channel, "my-exchange.delay", "my-routing-key", "data", opts]}

      assert [
               {"my-header", :binary, "My Header Value"},
               {"x-retries", :long, 1},
               {"x-status-0", :binary, "%RuntimeError{message: \"Test Exception!\"}"}
             ] = opts |> Keyword.fetch!(:headers) |> Enum.sort()

      assert "my-content-type" = opts |> Keyword.fetch!(:content_type)
      assert @timestamp = opts |> Keyword.fetch!(:timestamp)
    end

    @tag consumer_options: [max_retry_count: 1], handle_message: {:return, {:error, "Test Error"}}
    test "retries failed messages in case of error with message", %{
      default_rabbit_mq_metadata: default_rabbit_mq_metadata
    } do
      TestConsumer |> Broadway.test_message("data", metadata: default_rabbit_mq_metadata)

      assert_receive {:handle_message, [_data, _metadata]}

      assert_receive {:rabbit_mq_publish,
                      [:test_amqp_channel, "my-exchange.delay", "my-routing-key", "data", opts]}

      assert [
               {"my-header", :binary, "My Header Value"},
               {"x-retries", :long, 1},
               {"x-status-0", :binary, "Test Error"}
             ] = opts |> Keyword.fetch!(:headers) |> Enum.sort()

      assert "my-content-type" = opts |> Keyword.fetch!(:content_type)
      assert @timestamp = opts |> Keyword.fetch!(:timestamp)
    end

    @tag consumer_options: [max_retry_count: 1], handle_message: {:return, :error}
    test "retries failed messages in case of error without message", %{
      default_rabbit_mq_metadata: default_rabbit_mq_metadata
    } do
      TestConsumer |> Broadway.test_message("data", metadata: default_rabbit_mq_metadata)

      assert_receive {:handle_message, [_data, _metadata]}

      assert_receive {:rabbit_mq_publish,
                      [:test_amqp_channel, "my-exchange.delay", "my-routing-key", "data", opts]}

      assert [
               {"my-header", :binary, "My Header Value"},
               {"x-retries", :long, 1}
             ] = opts |> Keyword.fetch!(:headers) |> Enum.sort()

      assert "my-content-type" = opts |> Keyword.fetch!(:content_type)
      assert @timestamp = opts |> Keyword.fetch!(:timestamp)
    end

    @tag consumer_options: [max_retry_count: 2], handle_message: {:return, {:error, "Test Error"}}
    test "retries retried message", %{
      default_rabbit_mq_metadata: %{headers: headers} = default_rabbit_mq_metadata
    } do
      headers = [{"x-retries", :long, 1} | headers]

      TestConsumer
      |> Broadway.test_message("data", metadata: %{default_rabbit_mq_metadata | headers: headers})

      assert_receive {:handle_message, [_data, _metadata]}

      assert_receive {:rabbit_mq_publish,
                      [:test_amqp_channel, "my-exchange.delay", "my-routing-key", "data", opts]}

      assert [
               {"my-header", :binary, "My Header Value"},
               {"x-retries", :long, 2},
               {"x-status-1", :binary, "Test Error"}
             ] = opts |> Keyword.fetch!(:headers) |> Enum.sort()

      assert "my-content-type" = opts |> Keyword.fetch!(:content_type)
      assert @timestamp = opts |> Keyword.fetch!(:timestamp)
    end

    @tag consumer_options: [max_retry_count: 2], handle_message: {:return, {:error, "Test Error"}}
    test "doesn't retry retried message", %{
      default_rabbit_mq_metadata: %{headers: headers} = default_rabbit_mq_metadata
    } do
      headers = [{"x-retries", :long, 2} | headers]

      TestConsumer
      |> Broadway.test_message("data", metadata: %{default_rabbit_mq_metadata | headers: headers})

      assert_receive {:handle_message, [_data, _metadata]}
      refute_receive {:rabbit_mq_publish, _}
    end

    @tag consumer_options: [store_failed: true, max_retry_count: 2],
         handle_message: {:return, {:error, "Test Error"}}
    test "store failed retried message", %{
      default_rabbit_mq_metadata: %{headers: headers} = default_rabbit_mq_metadata
    } do
      headers = [{"x-retries", :long, 2} | headers]

      TestConsumer
      |> Broadway.test_message("data", metadata: %{default_rabbit_mq_metadata | headers: headers})

      assert_receive {:handle_message, [_data, _metadata]}

      assert_receive {:rabbit_mq_publish,
                      [:test_amqp_channel, "my-exchange.failed", "my-routing-key", "data", opts]}

      assert [
               {"my-header", :binary, "My Header Value"},
               {"x-retries", :long, 2},
               {"x-status-2", :binary, "Test Error"}
             ] = opts |> Keyword.fetch!(:headers) |> Enum.sort()

      assert "my-content-type" = opts |> Keyword.fetch!(:content_type)
      assert @timestamp = opts |> Keyword.fetch!(:timestamp)
    end

    @tag handle_message: {:return, :ok}
    test "RPC: respont to client", %{
      default_rabbit_mq_metadata: default_rabbit_mq_metadata
    } do
      TestConsumer
      |> Broadway.test_message("data",
        metadata: %{
          default_rabbit_mq_metadata
          | correlation_id: "my-correlation-id",
            reply_to: "my-reply-to"
        }
      )

      assert_receive {:handle_message, ["data", _]}
      assert_receive {:rabbit_mq_publish, [:test_amqp_channel, "", "my-reply-to", "", opts]}
      assert "my-correlation-id" = opts |> Keyword.fetch!(:correlation_id)
      [{"x-reply-status", :binary, "ok"}] = opts |> Keyword.fetch!(:headers)
    end

    @tag handle_message: {:return, {:ok, "Result"}}
    test "RPC: respont to client with message", %{
      default_rabbit_mq_metadata: default_rabbit_mq_metadata
    } do
      TestConsumer
      |> Broadway.test_message("data",
        metadata: %{
          default_rabbit_mq_metadata
          | correlation_id: "my-correlation-id",
            reply_to: "my-reply-to"
        }
      )

      assert_receive {:handle_message, ["data", _]}
      assert_receive {:rabbit_mq_publish, [:test_amqp_channel, "", "my-reply-to", "Result", opts]}
      assert "my-correlation-id" = opts |> Keyword.fetch!(:correlation_id)
      [{"x-reply-status", :binary, "ok"}] = opts |> Keyword.fetch!(:headers)
    end

    @tag handle_message: {:return, :error}
    test "RPC: respont to client with error", %{
      default_rabbit_mq_metadata: default_rabbit_mq_metadata
    } do
      TestConsumer
      |> Broadway.test_message("data",
        metadata: %{
          default_rabbit_mq_metadata
          | correlation_id: "my-correlation-id",
            reply_to: "my-reply-to"
        }
      )

      assert_receive {:handle_message, ["data", _]}
      assert_receive {:rabbit_mq_publish, [:test_amqp_channel, "", "my-reply-to", "", opts]}
      assert "my-correlation-id" = opts |> Keyword.fetch!(:correlation_id)
      [{"x-reply-status", :binary, "error"}] = opts |> Keyword.fetch!(:headers)
    end

    @tag handle_message: {:return, {:error, "Error Message"}},
         consumer_options: [store_failed: true, max_retry_count: 2]
    test "RPC: respont to client with error message", %{
      default_rabbit_mq_metadata: default_rabbit_mq_metadata
    } do
      TestConsumer
      |> Broadway.test_message("data",
        metadata: %{
          default_rabbit_mq_metadata
          | correlation_id: "my-correlation-id",
            reply_to: "my-reply-to"
        }
      )

      assert_receive {:handle_message, ["data", _]}

      assert_receive {:rabbit_mq_publish,
                      [:test_amqp_channel, "", "my-reply-to", "Error Message", opts]}

      assert "my-correlation-id" = opts |> Keyword.fetch!(:correlation_id)
      [{"x-reply-status", :binary, "error"}] = opts |> Keyword.fetch!(:headers)
    end

    @tag handle_message: {:raise, "Test Exception"}, consumer_options: [max_retry_count: 2]
    test "RPC: respont to client with exception", %{
      default_rabbit_mq_metadata: default_rabbit_mq_metadata
    } do
      TestConsumer
      |> Broadway.test_message("data",
        metadata: %{
          default_rabbit_mq_metadata
          | correlation_id: "my-correlation-id",
            reply_to: "my-reply-to"
        }
      )

      assert_receive {:handle_message, ["data", _]}

      assert_receive {:rabbit_mq_publish,
                      [
                        :test_amqp_channel,
                        "",
                        "my-reply-to",
                        "%RuntimeError{message: \"Test Exception\"}",
                        opts
                      ]}

      assert "my-correlation-id" = opts |> Keyword.fetch!(:correlation_id)
      [{"x-reply-status", :binary, "error"}] = opts |> Keyword.fetch!(:headers)
    end
  end
end
