defmodule Lepus.Consumer.OptionsTest do
  use ExUnit.Case, async: true

  alias Lepus.Consumer.Options
  alias Lepus.TestConsumer
  import Mox

  describe ".build/2" do
    setup :verify_on_exit!

    setup do
      default_opts = [
        connection: "my-connection",
        exchange: "my-exchange",
        routing_key: "my-routing-key"
      ]

      {:ok, default_opts: default_opts}
    end

    test "default `broadway_producer_module`", %{default_opts: default_opts} do
      TestConsumer |> expect(:options, fn -> [] end)
      assert {:ok, opts} = TestConsumer |> Options.build(default_opts)
      assert BroadwayRabbitMQ.Producer = opts |> Keyword.fetch!(:broadway_producer_module)
    end

    test "global `broadway_producer_module`", %{default_opts: default_opts} do
      global_opts = default_opts |> Keyword.put(:broadway_producer_module, GlobalModule)

      TestConsumer |> expect(:options, fn -> [] end)
      assert {:ok, opts} = TestConsumer |> Options.build(global_opts)
      assert GlobalModule = opts |> Keyword.fetch!(:broadway_producer_module)
    end

    test "local `broadway_producer_module`", %{default_opts: default_opts} do
      global_opts = default_opts |> Keyword.put(:broadway_producer_module, GlobalModule)
      TestConsumer |> expect(:options, fn -> [broadway_producer_module: LocalModule] end)

      assert {:ok, opts} = TestConsumer |> Options.build(global_opts)
      assert LocalModule = opts |> Keyword.fetch!(:broadway_producer_module)
    end

    test "default `rabbit_client`", %{default_opts: default_opts} do
      TestConsumer |> expect(:options, fn -> [] end)

      assert {:ok, opts} = TestConsumer |> Options.build(default_opts)
      assert Lepus.Rabbit.BasicClient = opts |> Keyword.fetch!(:rabbit_client)
    end

    test "global `rabbit_client`", %{default_opts: default_opts} do
      global_opts = default_opts |> Keyword.put(:rabbit_client, GlobalModule)
      TestConsumer |> expect(:options, fn -> [] end)

      assert {:ok, opts} = TestConsumer |> Options.build(global_opts)
      assert GlobalModule = opts |> Keyword.fetch!(:rabbit_client)
    end

    test "local `rabbit_client`", %{default_opts: default_opts} do
      global_opts = default_opts |> Keyword.put(:rabbit_client, GlobalModule)
      TestConsumer |> expect(:options, fn -> [rabbit_client: LocalModule] end)

      assert {:ok, opts} = TestConsumer |> Options.build(global_opts)
      assert LocalModule = opts |> Keyword.fetch!(:rabbit_client)
    end

    test "global `connection`", %{default_opts: default_opts} do
      TestConsumer |> expect(:options, fn -> [] end)

      assert {:ok, opts} = TestConsumer |> Options.build(default_opts)
      assert "my-connection" = opts |> Keyword.fetch!(:connection)
    end

    test "local `connection`", %{default_opts: default_opts} do
      TestConsumer |> expect(:options, fn -> [connection: "my-local-connection"] end)

      assert {:ok, opts} = TestConsumer |> Options.build(default_opts)
      assert "my-local-connection" = opts |> Keyword.fetch!(:connection)
    end

    test "keyword `connection`", %{default_opts: default_opts} do
      TestConsumer |> expect(:options, fn -> [connection: [a: 1, b: "b"]] end)

      assert {:ok, opts} = TestConsumer |> Options.build(default_opts)
      assert [a: 1, b: "b"] = opts |> Keyword.fetch!(:connection)
    end

    test "atom `connection`", %{default_opts: default_opts} do
      TestConsumer |> expect(:options, fn -> [connection: :atom] end)
      assert {:error, _reason} = TestConsumer |> Options.build(default_opts)
    end

    test "global `exchange`", %{default_opts: default_opts} do
      TestConsumer |> expect(:options, fn -> [] end)

      assert {:ok, opts} = TestConsumer |> Options.build(default_opts)
      assert "my-exchange" = opts |> Keyword.fetch!(:exchange)
    end

    test "local `exchange`", %{default_opts: default_opts} do
      TestConsumer |> expect(:options, fn -> [exchange: "my-local-exchange"] end)

      assert {:ok, opts} = TestConsumer |> Options.build(default_opts)
      assert "my-local-exchange" = opts |> Keyword.fetch!(:exchange)
    end

    test "global `routing_key`", %{default_opts: default_opts} do
      TestConsumer |> expect(:options, fn -> [] end)

      assert {:ok, opts} = TestConsumer |> Options.build(default_opts)
      assert "my-routing-key" = opts |> Keyword.fetch!(:routing_key)
    end

    test "local `routing_key`", %{default_opts: default_opts} do
      TestConsumer |> expect(:options, fn -> [routing_key: "my-local-routing-key"] end)

      assert {:ok, opts} = TestConsumer |> Options.build(default_opts)
      assert "my-local-routing-key" = opts |> Keyword.fetch!(:routing_key)
    end

    test "default `delay_exchange`", %{default_opts: default_opts} do
      TestConsumer |> expect(:options, fn -> [] end)

      assert {:ok, opts} = TestConsumer |> Options.build(default_opts)
      assert "my-exchange.delay" = opts |> Keyword.fetch!(:delay_exchange)
    end

    test "default `delay_exchange` for empty `exchange`", %{default_opts: default_opts} do
      TestConsumer |> expect(:options, fn -> [exchange: ""] end)

      assert {:ok, opts} = TestConsumer |> Options.build(default_opts)
      assert "delay" = opts |> Keyword.fetch!(:delay_exchange)
    end

    test "global `delay_exchange`", %{default_opts: default_opts} do
      global_opts = default_opts |> Keyword.put(:delay_exchange, "global-delay-exchange")
      TestConsumer |> expect(:options, fn -> [] end)

      assert {:ok, opts} = TestConsumer |> Options.build(global_opts)
      assert "global-delay-exchange" = opts |> Keyword.fetch!(:delay_exchange)
    end

    test "local `delay_exchange`", %{default_opts: default_opts} do
      global_opts = default_opts |> Keyword.put(:delay_exchange, "global-delay-exchange")
      TestConsumer |> expect(:options, fn -> [delay_exchange: "local-delay-exchange"] end)

      assert {:ok, opts} = TestConsumer |> Options.build(global_opts)
      assert "local-delay-exchange" = opts |> Keyword.fetch!(:delay_exchange)
    end

    test "default `retry_exchange`", %{default_opts: default_opts} do
      TestConsumer |> expect(:options, fn -> [] end)

      assert {:ok, opts} = TestConsumer |> Options.build(default_opts)
      assert "my-exchange.retry" = opts |> Keyword.fetch!(:retry_exchange)
    end

    test "default `retry_exchange` for empty `exchange`", %{default_opts: default_opts} do
      TestConsumer |> expect(:options, fn -> [exchange: ""] end)

      assert {:ok, opts} = TestConsumer |> Options.build(default_opts)
      assert "retry" = opts |> Keyword.fetch!(:retry_exchange)
    end

    test "global `retry_exchange`", %{default_opts: default_opts} do
      global_opts = default_opts |> Keyword.put(:retry_exchange, "global-retry-exchange")
      TestConsumer |> expect(:options, fn -> [] end)

      assert {:ok, opts} = TestConsumer |> Options.build(global_opts)
      assert "global-retry-exchange" = opts |> Keyword.fetch!(:retry_exchange)
    end

    test "local `retry_exchange`", %{default_opts: default_opts} do
      global_opts = default_opts |> Keyword.put(:retry_exchange, "global-retry-exchange")
      TestConsumer |> expect(:options, fn -> [retry_exchange: "local-retry-exchange"] end)

      assert {:ok, opts} = TestConsumer |> Options.build(global_opts)
      assert "local-retry-exchange" = opts |> Keyword.fetch!(:retry_exchange)
    end

    test "default `failed_exchange`", %{default_opts: default_opts} do
      TestConsumer |> expect(:options, fn -> [] end)

      assert {:ok, opts} = TestConsumer |> Options.build(default_opts)
      assert "my-exchange.failed" = opts |> Keyword.fetch!(:failed_exchange)
    end

    test "default `failed_exchange` for empty `exchange`", %{default_opts: default_opts} do
      TestConsumer |> expect(:options, fn -> [exchange: ""] end)

      assert {:ok, opts} = TestConsumer |> Options.build(default_opts)
      assert "failed" = opts |> Keyword.fetch!(:failed_exchange)
    end

    test "global `failed_exchange`", %{default_opts: default_opts} do
      global_opts = default_opts |> Keyword.put(:failed_exchange, "global-failed-exchange")
      TestConsumer |> expect(:options, fn -> [] end)

      assert {:ok, opts} = TestConsumer |> Options.build(global_opts)
      assert "global-failed-exchange" = opts |> Keyword.fetch!(:failed_exchange)
    end

    test "local `failed_exchange`", %{default_opts: default_opts} do
      global_opts = default_opts |> Keyword.put(:failed_exchange, "global-failed-exchange")
      TestConsumer |> expect(:options, fn -> [failed_exchange: "local-failed-exchange"] end)

      assert {:ok, opts} = TestConsumer |> Options.build(global_opts)
      assert "local-failed-exchange" = opts |> Keyword.fetch!(:failed_exchange)
    end

    test "default `queue`", %{default_opts: default_opts} do
      TestConsumer |> expect(:options, fn -> [] end)

      assert {:ok, opts} = TestConsumer |> Options.build(default_opts)
      assert "my-exchange.my-routing-key" = opts |> Keyword.fetch!(:queue)
    end

    test "default `queue` for empty `exchange`", %{default_opts: default_opts} do
      TestConsumer |> expect(:options, fn -> [exchange: ""] end)

      assert {:ok, opts} = TestConsumer |> Options.build(default_opts)
      assert "my-routing-key" = opts |> Keyword.fetch!(:queue)
    end

    test "global `queue`", %{default_opts: default_opts} do
      global_opts = default_opts |> Keyword.put(:queue, "global-queue")
      TestConsumer |> expect(:options, fn -> [] end)

      assert {:ok, opts} = TestConsumer |> Options.build(global_opts)
      assert "global-queue" = opts |> Keyword.fetch!(:queue)
    end

    test "local `queue`", %{default_opts: default_opts} do
      global_opts = default_opts |> Keyword.put(:queue, "global-queue")
      TestConsumer |> expect(:options, fn -> [queue: "local-queue"] end)

      assert {:ok, opts} = TestConsumer |> Options.build(global_opts)
      assert "local-queue" = opts |> Keyword.fetch!(:queue)
    end

    test "default `retry_queue`", %{default_opts: default_opts} do
      TestConsumer |> expect(:options, fn -> [] end)

      assert {:ok, opts} = TestConsumer |> Options.build(default_opts)
      assert "my-exchange.my-routing-key.retry" = opts |> Keyword.fetch!(:retry_queue)
    end

    test "default `retry_queue` for empty `exchange`", %{default_opts: default_opts} do
      TestConsumer |> expect(:options, fn -> [exchange: ""] end)

      assert {:ok, opts} = TestConsumer |> Options.build(default_opts)
      assert "my-routing-key.retry" = opts |> Keyword.fetch!(:retry_queue)
    end

    test "default `retry_queue` for overwritten `queue`", %{default_opts: default_opts} do
      TestConsumer |> expect(:options, fn -> [queue: "my-queue"] end)

      assert {:ok, opts} = TestConsumer |> Options.build(default_opts)
      assert "my-queue.retry" = opts |> Keyword.fetch!(:retry_queue)
    end

    test "global `retry_queue`", %{default_opts: default_opts} do
      global_opts = default_opts |> Keyword.put(:retry_queue, "global-retry-queue")
      TestConsumer |> expect(:options, fn -> [] end)

      assert {:ok, opts} = TestConsumer |> Options.build(global_opts)
      assert "global-retry-queue" = opts |> Keyword.fetch!(:retry_queue)
    end

    test "local `retry_queue`", %{default_opts: default_opts} do
      global_opts = default_opts |> Keyword.put(:retry_queue, "global-retry-queue")
      TestConsumer |> expect(:options, fn -> [retry_queue: "local-retry-queue"] end)

      assert {:ok, opts} = TestConsumer |> Options.build(global_opts)
      assert "local-retry-queue" = opts |> Keyword.fetch!(:retry_queue)
    end

    test "default `failed_queue`", %{default_opts: default_opts} do
      TestConsumer |> expect(:options, fn -> [] end)

      assert {:ok, opts} = TestConsumer |> Options.build(default_opts)
      assert "my-exchange.my-routing-key.failed" = opts |> Keyword.fetch!(:failed_queue)
    end

    test "default `failed_queue` for empty `exchange`", %{default_opts: default_opts} do
      TestConsumer |> expect(:options, fn -> [exchange: ""] end)

      assert {:ok, opts} = TestConsumer |> Options.build(default_opts)
      assert "my-routing-key.failed" = opts |> Keyword.fetch!(:failed_queue)
    end

    test "default `failed_queue` for overwritten `queue`", %{default_opts: default_opts} do
      TestConsumer |> expect(:options, fn -> [queue: "my-queue"] end)

      assert {:ok, opts} = TestConsumer |> Options.build(default_opts)
      assert "my-queue.failed" = opts |> Keyword.fetch!(:failed_queue)
    end

    test "global `failed_queue`", %{default_opts: default_opts} do
      global_opts = default_opts |> Keyword.put(:failed_queue, "global-failed-queue")
      TestConsumer |> expect(:options, fn -> [] end)

      assert {:ok, opts} = TestConsumer |> Options.build(global_opts)
      assert "global-failed-queue" = opts |> Keyword.fetch!(:failed_queue)
    end

    test "local `failed_queue`", %{default_opts: default_opts} do
      global_opts = default_opts |> Keyword.put(:failed_queue, "global-failed-queue")
      TestConsumer |> expect(:options, fn -> [failed_queue: "local-failed-queue"] end)

      assert {:ok, opts} = TestConsumer |> Options.build(global_opts)
      assert "local-failed-queue" = opts |> Keyword.fetch!(:failed_queue)
    end

    test "without `processor`", %{default_opts: default_opts} do
      TestConsumer |> expect(:options, fn -> [] end)

      assert {:ok, opts} = TestConsumer |> Options.build(default_opts)
      assert [] = opts |> Keyword.fetch!(:processor)
    end

    test "with wrong `processor`", %{default_opts: default_opts} do
      TestConsumer |> expect(:options, fn -> [processor: "processor"] end)
      assert {:error, _reason} = TestConsumer |> Options.build(default_opts)
    end

    test "with global `processor`", %{default_opts: default_opts} do
      global_opts = default_opts |> Keyword.put(:processor, a: 1, b: 2)
      TestConsumer |> expect(:options, fn -> [] end)

      assert {:ok, opts} = TestConsumer |> Options.build(global_opts)
      assert [a: 1, b: 2] = opts |> Keyword.fetch!(:processor)
    end

    test "with local `processor`", %{default_opts: default_opts} do
      global_opts = default_opts |> Keyword.put(:processor, a: 1, b: 2)
      TestConsumer |> expect(:options, fn -> [processor: [c: 1, d: 2]] end)

      assert {:ok, opts} = TestConsumer |> Options.build(global_opts)
      assert [c: 1, d: 2] = opts |> Keyword.fetch!(:processor)
    end
  end
end
