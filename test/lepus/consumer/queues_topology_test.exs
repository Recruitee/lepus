defmodule Lepus.Consumer.QueuesTopologyTest do
  use ExUnit.Case, async: true

  import Mox

  alias Lepus.Consumer.Options
  alias Lepus.Consumer.QueuesTopology
  alias Lepus.Rabbit

  defmodule NullConsumer do
  end

  describe ".declare_function/2" do
    setup ctx do
      base_opts =
        [
          rabbit_client: Rabbit.TestClient,
          connection: "my-connection",
          exchange: "my-exchange",
          delay_exchange: "my-delay-exchange",
          retry_exchange: "my-retry-exchange",
          failed_exchange: "my-failed-exchange",
          routing_key: "my-routing-key",
          queue: "my-queue",
          retry_queue: "my-retry-queue",
          failed_queue: "my-failed-queue"
        ]
        |> Keyword.merge(Map.get(ctx, :redefine, []))

      {:ok, opts} = NullConsumer |> Options.build(base_opts)
      result = opts |> QueuesTopology.declare_function()

      {:ok, result: result}
    end

    setup :verify_on_exit!

    test "declares only base topology", %{result: result} do
      Rabbit.TestClient
      |> expect(:declare_direct_exchange, fn
        :test_channel, "my-exchange", [durable: true] -> :ok
      end)

      Rabbit.TestClient
      |> expect(:declare_queue, fn :test_channel, "my-queue", opts ->
        assert [{:arguments, [{"x-queue-type", :longstr, "classic"}]}, {:durable, true}] =
                 opts |> Enum.sort()

        :ok
      end)

      Rabbit.TestClient
      |> expect(:bind_queue, fn
        :test_channel, "my-queue", "my-exchange", [routing_key: "my-routing-key"] -> :ok
      end)

      assert :ok = result.(:test_channel)
    end

    @tag redefine: [max_retry_count: 1]
    test "declares base & retry topology", %{result: result} do
      Rabbit.TestClient
      |> expect(:declare_direct_exchange, 3, fn
        :test_channel, "my-exchange", [durable: true] -> :ok
        :test_channel, "my-delay-exchange", [durable: true] -> :ok
        :test_channel, "my-retry-exchange", [durable: true] -> :ok
      end)

      Rabbit.TestClient
      |> expect(:declare_queue, 2, fn
        :test_channel, "my-queue", opts ->
          assert [arguments: [{"x-queue-type", :longstr, "classic"}], durable: true] =
                   opts |> Enum.sort()

          :ok

        :test_channel, "my-retry-queue", opts ->
          assert [arguments: arguments, durable: true] = opts |> Enum.sort()

          assert [
                   {"x-dead-letter-exchange", :longstr, "my-retry-exchange"},
                   {"x-queue-type", :longstr, "classic"}
                 ] = arguments |> Enum.sort()

          :ok
      end)

      Rabbit.TestClient
      |> expect(:bind_queue, 3, fn
        :test_channel, "my-queue", "my-exchange", [routing_key: "my-routing-key"] ->
          :ok

        :test_channel, "my-queue", "my-retry-exchange", [routing_key: "my-routing-key"] ->
          :ok

        :test_channel, "my-retry-queue", "my-delay-exchange", [routing_key: "my-routing-key"] ->
          :ok
      end)

      assert :ok = result.(:test_channel)
    end

    @tag redefine: [store_failed: true]
    test "declares base & error topology", %{result: result} do
      Rabbit.TestClient
      |> expect(:declare_direct_exchange, 2, fn
        :test_channel, "my-exchange", [durable: true] -> :ok
        :test_channel, "my-failed-exchange", [durable: true] -> :ok
      end)

      Rabbit.TestClient
      |> expect(:declare_queue, 2, fn
        :test_channel, "my-queue", opts ->
          assert [{:arguments, [{"x-queue-type", :longstr, "classic"}]}, {:durable, true}] =
                   opts |> Enum.sort()

          :ok

        :test_channel, "my-failed-queue", opts ->
          assert [{:arguments, [{"x-queue-type", :longstr, "classic"}]}, {:durable, true}] =
                   opts |> Enum.sort()

          :ok
      end)

      Rabbit.TestClient
      |> expect(:bind_queue, 2, fn
        :test_channel, "my-queue", "my-exchange", [routing_key: "my-routing-key"] ->
          :ok

        :test_channel, "my-failed-queue", "my-failed-exchange", [routing_key: "my-routing-key"] ->
          :ok
      end)

      assert :ok = result.(:test_channel)
    end

    @tag redefine: [store_failed: true, max_retry_count: 1]
    test "declares base, retry & error topology", %{result: result} do
      Rabbit.TestClient
      |> expect(:declare_direct_exchange, 4, fn
        :test_channel, "my-exchange", [durable: true] -> :ok
        :test_channel, "my-delay-exchange", [durable: true] -> :ok
        :test_channel, "my-retry-exchange", [durable: true] -> :ok
        :test_channel, "my-failed-exchange", [durable: true] -> :ok
      end)

      Rabbit.TestClient
      |> expect(:declare_queue, 3, fn
        :test_channel, "my-queue", opts ->
          assert [arguments: [{"x-queue-type", :longstr, "classic"}], durable: true] =
                   opts |> Enum.sort()

          :ok

        :test_channel, "my-failed-queue", opts ->
          assert [arguments: [{"x-queue-type", :longstr, "classic"}], durable: true] =
                   opts |> Enum.sort()

          :ok

        :test_channel, "my-retry-queue", opts ->
          assert [arguments: arguments, durable: true] = opts |> Enum.sort()

          assert [
                   {"x-dead-letter-exchange", :longstr, "my-retry-exchange"},
                   {"x-queue-type", :longstr, "classic"}
                 ] = arguments |> Enum.sort()

          :ok
      end)

      Rabbit.TestClient
      |> expect(:bind_queue, 4, fn
        :test_channel, "my-queue", "my-exchange", [routing_key: "my-routing-key"] ->
          :ok

        :test_channel, "my-queue", "my-retry-exchange", [routing_key: "my-routing-key"] ->
          :ok

        :test_channel, "my-retry-queue", "my-delay-exchange", [routing_key: "my-routing-key"] ->
          :ok

        :test_channel, "my-failed-queue", "my-failed-exchange", [routing_key: "my-routing-key"] ->
          :ok
      end)

      assert :ok = result.(:test_channel)
    end

    @tag redefine: [store_failed: true, max_retry_count: 1, queues_type: "quorum"]
    test "declares base, retry & error topology with quorum queues", %{result: result} do
      Rabbit.TestClient
      |> expect(:declare_direct_exchange, 4, fn
        :test_channel, "my-exchange", [durable: true] -> :ok
        :test_channel, "my-delay-exchange", [durable: true] -> :ok
        :test_channel, "my-retry-exchange", [durable: true] -> :ok
        :test_channel, "my-failed-exchange", [durable: true] -> :ok
      end)

      Rabbit.TestClient
      |> expect(:declare_queue, 3, fn
        :test_channel, "my-queue", opts ->
          assert [arguments: [{"x-queue-type", :longstr, "quorum"}], durable: true] =
                   opts |> Enum.sort()

          :ok

        :test_channel, "my-failed-queue", opts ->
          assert [arguments: [{"x-queue-type", :longstr, "quorum"}], durable: true] =
                   opts |> Enum.sort()

          :ok

        :test_channel, "my-retry-queue", opts ->
          assert [arguments: arguments, durable: true] = opts |> Enum.sort()

          assert [
                   {"x-dead-letter-exchange", :longstr, "my-retry-exchange"},
                   {"x-queue-type", :longstr, "quorum"}
                 ] = arguments |> Enum.sort()

          :ok
      end)

      Rabbit.TestClient
      |> expect(:bind_queue, 4, fn
        :test_channel, "my-queue", "my-exchange", [routing_key: "my-routing-key"] ->
          :ok

        :test_channel, "my-queue", "my-retry-exchange", [routing_key: "my-routing-key"] ->
          :ok

        :test_channel, "my-retry-queue", "my-delay-exchange", [routing_key: "my-routing-key"] ->
          :ok

        :test_channel, "my-failed-queue", "my-failed-exchange", [routing_key: "my-routing-key"] ->
          :ok
      end)

      assert :ok = result.(:test_channel)
    end

    @tag redefine: [store_failed: true, max_retry_count: 1]
    test "doesn't declare any topology in case of error in base declaration", %{result: result} do
      Rabbit.TestClient
      |> expect(:declare_direct_exchange, fn
        :test_channel, "my-exchange", [durable: true] -> {:error, "can't declare exchange"}
      end)

      assert {:error, "can't declare exchange"} = result.(:test_channel)
    end

    @tag redefine: [store_failed: true, max_retry_count: 1]
    test "doesn't declare retry & error topologies in case of error in retry declaration", %{
      result: result
    } do
      Rabbit.TestClient
      |> expect(:declare_direct_exchange, 2, fn
        :test_channel, "my-exchange", [durable: true] -> :ok
        :test_channel, "my-delay-exchange", [durable: true] -> {:error, "can't declare exchange"}
      end)

      Rabbit.TestClient
      |> expect(:declare_queue, fn :test_channel, "my-queue", opts ->
        assert [arguments: [{"x-queue-type", :longstr, "classic"}], durable: true] =
                 opts |> Enum.sort()

        :ok
      end)

      Rabbit.TestClient
      |> expect(:bind_queue, 1, fn
        :test_channel, "my-queue", "my-exchange", [routing_key: "my-routing-key"] ->
          :ok
      end)

      assert {:error, "can't declare exchange"} = result.(:test_channel)
    end

    @tag redefine: [store_failed: true, max_retry_count: 1]
    test "doesn't declare error topology in case of error in error declaration", %{result: result} do
      Rabbit.TestClient
      |> expect(:declare_direct_exchange, 4, fn
        :test_channel, "my-exchange", [durable: true] -> :ok
        :test_channel, "my-delay-exchange", [durable: true] -> :ok
        :test_channel, "my-retry-exchange", [durable: true] -> :ok
        :test_channel, "my-failed-exchange", [durable: true] -> {:error, "can't declare exchange"}
      end)

      Rabbit.TestClient
      |> expect(:declare_queue, 2, fn
        :test_channel, "my-queue", opts ->
          assert [arguments: [{"x-queue-type", :longstr, "classic"}], durable: true] =
                   opts |> Enum.sort()

          :ok

        :test_channel, "my-retry-queue", opts ->
          assert [arguments: arguments, durable: true] = opts |> Enum.sort()

          [
            {"x-dead-letter-exchange", :longstr, "my-retry-exchange"},
            {"x-queue-type", :longstr, "classic"}
          ] = arguments |> Enum.sort()

          :ok
      end)

      Rabbit.TestClient
      |> expect(:bind_queue, 3, fn
        :test_channel, "my-queue", "my-exchange", [routing_key: "my-routing-key"] ->
          :ok

        :test_channel, "my-queue", "my-retry-exchange", [routing_key: "my-routing-key"] ->
          :ok

        :test_channel, "my-retry-queue", "my-delay-exchange", [routing_key: "my-routing-key"] ->
          :ok
      end)

      assert {:error, "can't declare exchange"} = result.(:test_channel)
    end
  end
end
