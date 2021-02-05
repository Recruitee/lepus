defmodule Lepus.BasicClient.ServerTest do
  alias Lepus.BasicClient
  alias Phoenix.PubSub

  use ExUnit.Case, async: true

  defmodule RabbitClientMock do
    def open_connection(%{test_pid: test_pid} = connection_opts) do
      {:ok, conn_pid} = Agent.start_link(fn -> :connection end)
      test_pid |> notify(:open_connection, [connection_opts])

      {:ok, %{type: :connection, pid: conn_pid, test_pid: test_pid}}
    end

    def open_channel(%{type: :connection, test_pid: test_pid} = conn) do
      {:ok, channel_pid} = Agent.start_link(fn -> :channel end)
      test_pid |> notify(:open_channel, [conn])

      {:ok, %{type: :channel, pid: channel_pid, conn: conn}}
    end

    def declare_direct_exchange(
          %{type: :channel, conn: %{test_pid: test_pid}} = channel,
          exchange,
          opts
        ) do
      test_pid |> notify(:declare_direct_exchange, [channel, exchange, opts])
    end

    def publish(
          %{type: :channel, conn: %{test_pid: test_pid}} = channel,
          exchange,
          routing_key,
          payload,
          opts
        ) do
      test_pid |> notify(:publish, [channel, exchange, routing_key, payload, opts])
    end

    defp notify(test_pid, fun, args) do
      test_pid |> Process.send({fun, args}, [])
    end
  end

  setup do
    {:ok, connection: %{type: :connection_opts, test_pid: self()}}
  end

  describe ".start_link/1" do
    test "starts connection, channels and declares exchanges", %{connection: connection} do
      assert {:ok, _server} =
               BasicClient.start_link(
                 name: new_server_name(),
                 connection: connection,
                 rabbit_client: RabbitClientMock,
                 exchanges: ["exchange1", "exchange2"]
               )

      assert_receive {:open_connection, [%{type: :connection_opts}]}

      assert_receive {:open_channel, [%{type: :connection} = conn]}
      assert_receive {:open_channel, [%{type: :connection} = ^conn]}

      assert_receive {:declare_direct_exchange,
                      [%{type: :channel} = channel1, "exchange1", [durable: true]]}

      assert_receive {:declare_direct_exchange,
                      [%{type: :channel} = channel2, "exchange2", [durable: true]]}

      refute Map.equal?(channel1, channel2)
    end

    test "starts 2 similar supervisors without errors", %{connection: connection} do
      assert {:ok, _server1} =
               BasicClient.start_link(
                 name: new_server_name(),
                 connection: connection,
                 rabbit_client: RabbitClientMock,
                 exchanges: ["exchange1", "exchange2", ""]
               )

      assert {:ok, _server2} =
               BasicClient.start_link(
                 name: new_server_name(),
                 connection: connection,
                 rabbit_client: RabbitClientMock,
                 exchanges: ["exchange1", "exchange2", ""]
               )
    end
  end

  describe ".publish & .publish_json without sync_opts" do
    setup(%{connection: connection}) do
      client_name = new_server_name()

      {:ok, _} =
        BasicClient.start_link(
          name: client_name,
          connection: connection,
          rabbit_client: RabbitClientMock,
          exchanges: ["exchange1", "exchange2"]
        )

      {:ok, client_name: client_name}
    end

    test ".publish publishes to rabbit and adds timestamp to opts", %{client_name: client_name} do
      client_name
      |> BasicClient.publish("exchange1", "my_routing_key", "my_payload",
        amqp_opts: [my_key: "my_value"]
      )

      assert_receive {:publish,
                      [%{type: :channel}, "exchange1", "my_routing_key", "my_payload", opts]}

      assert Keyword.fetch!(opts, :my_key) == "my_value"
      assert Keyword.has_key?(opts, :timestamp)
    end

    test ".publish_json publishes to rabbit and adds timestamp and content_type to opts", %{
      client_name: client_name
    } do
      client_name
      |> BasicClient.publish_json("exchange1", "my_routing_key", %{data: "my_payload"},
        amqp_opts: [my_key: "my_value"]
      )

      assert_receive {:publish,
                      [
                        %{type: :channel},
                        "exchange1",
                        "my_routing_key",
                        ~s({"data":"my_payload"}),
                        opts
                      ]}

      assert Keyword.fetch!(opts, :my_key) == "my_value"
      assert Keyword.has_key?(opts, :timestamp)
      assert Keyword.fetch!(opts, :content_type) == "application/json"
    end

    test ".publish uses 1 channel per exchange", %{client_name: client_name} do
      client_name |> BasicClient.publish("exchange1", "my_routing_key", "my_payload1", [])
      assert_receive {:publish, [channel1, "exchange1", "my_routing_key", "my_payload1", _]}

      client_name |> BasicClient.publish("exchange2", "my_routing_key", "my_payload1", [])
      assert_receive {:publish, [channel2, "exchange2", "my_routing_key", "my_payload1", _]}

      client_name |> BasicClient.publish("", "my_routing_key", "my_payload1", [])
      assert_receive {:publish, [channel3, "", "my_routing_key", "my_payload1", _]}

      client_name |> BasicClient.publish("exchange1", "my_routing_key", "my_payload2", [])
      assert_receive {:publish, [^channel1, "exchange1", "my_routing_key", "my_payload2", _]}

      client_name |> BasicClient.publish("exchange2", "my_routing_key", "my_payload2", [])
      assert_receive {:publish, [^channel2, "exchange2", "my_routing_key", "my_payload2", _]}

      client_name |> BasicClient.publish("", "my_routing_key", "my_payload1", [])
      assert_receive {:publish, [^channel3, "", "my_routing_key", "my_payload1", _]}

      refute Map.equal?(channel1, channel2)
      refute Map.equal?(channel1, channel3)
      refute Map.equal?(channel2, channel3)
    end

    test ".publish_json uses 1 channel per exchange", %{client_name: client_name} do
      client_name
      |> BasicClient.publish_json("exchange1", "my_routing_key", %{data: "my_payload1"}, [])

      assert_receive {:publish,
                      [channel1, "exchange1", "my_routing_key", ~s({"data":"my_payload1"}), _]}

      client_name
      |> BasicClient.publish_json("exchange2", "my_routing_key", %{data: "my_payload1"}, [])

      assert_receive {:publish,
                      [channel2, "exchange2", "my_routing_key", ~s({"data":"my_payload1"}), _]}

      client_name
      |> BasicClient.publish_json("exchange1", "my_routing_key", %{data: "my_payload2"}, [])

      assert_receive {:publish,
                      [^channel1, "exchange1", "my_routing_key", ~s({"data":"my_payload2"}), _]}

      client_name
      |> BasicClient.publish_json("exchange2", "my_routing_key", %{data: "my_payload2"}, [])

      assert_receive {:publish,
                      [^channel2, "exchange2", "my_routing_key", ~s({"data":"my_payload2"}), _]}

      refute Map.equal?(channel1, channel2)
    end

    test ".publish doesn't publish with sync opt", %{client_name: client_name} do
      assert_raise RuntimeError,
                   "Add `sync_opts` to the `Lepus.BasicClient` configuration for using `sync` option",
                   fn ->
                     client_name
                     |> BasicClient.publish("exchange1", "my_routing_key", "my_payload1",
                       sync: true
                     )
                   end
    end

    test ".publish_json doesn't publish with sync opt", %{client_name: client_name} do
      assert_raise RuntimeError,
                   "Add `sync_opts` to the `Lepus.BasicClient` configuration for using `sync` option",
                   fn ->
                     client_name
                     |> BasicClient.publish_json(
                       "exchange1",
                       "my_routing_key",
                       %{data: "my_payload1"},
                       sync: true
                     )
                   end
    end
  end

  describe ".publish & .publish_json with sync_opts" do
    setup(%{connection: connection}) do
      pubsub_name = new_server_name()
      reply_to_queue = "my_app.reply_to_queue"
      client_name = new_server_name()
      broadway_name = client_name |> BasicClient.ServerNames.replies_broadway()

      {:ok, _} =
        [
          {PubSub, name: pubsub_name},
          {BasicClient,
           name: client_name,
           connection: connection,
           rabbit_client: RabbitClientMock,
           exchanges: ["exchange1", "exchange2"],
           sync_opts: [pubsub: pubsub_name, reply_to_queue: reply_to_queue],
           producer_module: Broadway.DummyProducer}
        ]
        |> Supervisor.start_link(strategy: :one_for_one)

      {:ok,
       client_name: client_name,
       reply_to_queue: reply_to_queue,
       pubsub_name: pubsub_name,
       broadway_name: broadway_name}
    end

    test ".publish publishes to rabbit and receives response", %{
      client_name: client_name,
      reply_to_queue: reply_to_queue,
      broadway_name: broadway_name
    } do
      reply = "My Reply"

      task =
        Task.async(fn ->
          assert {:ok, ^reply} =
                   client_name
                   |> BasicClient.publish("exchange1", "my_routing_key", "my_payload",
                     amqp_opts: [my_key: "my_value"],
                     sync: true
                   )
        end)

      assert_receive {:publish,
                      [%{type: :channel}, "exchange1", "my_routing_key", "my_payload", opts]}

      assert "my_value" = Keyword.fetch!(opts, :my_key)
      assert _ = Keyword.fetch!(opts, :timestamp)
      assert ^reply_to_queue = Keyword.fetch!(opts, :reply_to)
      assert correlation_id = Keyword.fetch!(opts, :correlation_id)
      assert [{"x-client", :binary, "lepus"}] = opts |> Keyword.fetch!(:headers)

      broadway_name
      |> Broadway.test_message(reply,
        metadata: [
          correlation_id: correlation_id,
          headers: [{"x-reply-status", :binary, "ok"}]
        ]
      )

      Task.await(task)
    end

    test ".publish_json publishes to rabbit and receives response", %{
      client_name: client_name,
      reply_to_queue: reply_to_queue,
      broadway_name: broadway_name
    } do
      task =
        Task.async(fn ->
          assert {:ok, %{"my_reply" => "my_data"}} =
                   client_name
                   |> BasicClient.publish(
                     "exchange1",
                     "my_routing_key",
                     %{my_payload: "my_payload"},
                     amqp_opts: [my_key: "my_value"],
                     sync: true
                   )
        end)

      assert_receive {:publish,
                      [
                        %{type: :channel},
                        "exchange1",
                        "my_routing_key",
                        %{my_payload: "my_payload"},
                        opts
                      ]}

      assert "my_value" = Keyword.fetch!(opts, :my_key)
      assert _ = Keyword.fetch!(opts, :timestamp)
      assert ^reply_to_queue = Keyword.fetch!(opts, :reply_to)
      assert correlation_id = Keyword.fetch!(opts, :correlation_id)
      assert [{"x-client", :binary, "lepus"}] = opts |> Keyword.fetch!(:headers)

      broadway_name
      |> Broadway.test_message(~s({"my_reply":"my_data"}),
        metadata: [
          correlation_id: correlation_id,
          content_type: "application/json",
          headers: [{"x-reply-status", :binary, "ok"}]
        ]
      )

      Task.await(task)
    end

    test ".publish publishes to rabbit and receives an error", %{
      client_name: client_name,
      reply_to_queue: reply_to_queue,
      broadway_name: broadway_name
    } do
      reply = "My Reply"

      task =
        Task.async(fn ->
          assert {:error, ^reply} =
                   client_name
                   |> BasicClient.publish("exchange1", "my_routing_key", "my_payload",
                     amqp_opts: [my_key: "my_value"],
                     sync: true
                   )
        end)

      assert_receive {:publish,
                      [%{type: :channel}, "exchange1", "my_routing_key", "my_payload", opts]}

      assert "my_value" = Keyword.fetch!(opts, :my_key)
      assert _ = Keyword.fetch!(opts, :timestamp)
      assert ^reply_to_queue = Keyword.fetch!(opts, :reply_to)
      assert correlation_id = Keyword.fetch!(opts, :correlation_id)
      assert [{"x-client", :binary, "lepus"}] = opts |> Keyword.fetch!(:headers)

      broadway_name
      |> Broadway.test_message(reply, metadata: [correlation_id: correlation_id])

      Task.await(task)
    end

    test ".publish_json publishes to rabbit and receives an error", %{
      client_name: client_name,
      reply_to_queue: reply_to_queue,
      broadway_name: broadway_name
    } do
      task =
        Task.async(fn ->
          assert {:error, %{"my_reply" => "my_data"}} =
                   client_name
                   |> BasicClient.publish(
                     "exchange1",
                     "my_routing_key",
                     %{my_payload: "my_payload"},
                     amqp_opts: [my_key: "my_value"],
                     sync: true
                   )
        end)

      assert_receive {:publish,
                      [
                        %{type: :channel},
                        "exchange1",
                        "my_routing_key",
                        %{my_payload: "my_payload"},
                        opts
                      ]}

      assert "my_value" = Keyword.fetch!(opts, :my_key)
      assert _ = Keyword.fetch!(opts, :timestamp)
      assert ^reply_to_queue = Keyword.fetch!(opts, :reply_to)
      assert correlation_id = Keyword.fetch!(opts, :correlation_id)
      assert [{"x-client", :binary, "lepus"}] = opts |> Keyword.fetch!(:headers)

      broadway_name
      |> Broadway.test_message(~s({"my_reply":"my_data"}),
        metadata: [
          correlation_id: correlation_id,
          content_type: "application/json"
        ]
      )

      Task.await(task)
    end

    test ".publish publishes to rabbit and receives timeout error", %{
      client_name: client_name,
      reply_to_queue: reply_to_queue
    } do
      task =
        Task.async(fn ->
          assert {:error, :timeout} =
                   client_name
                   |> BasicClient.publish("exchange1", "my_routing_key", "my_payload",
                     amqp_opts: [my_key: "my_value"],
                     sync: true,
                     timeout: 1000
                   )
        end)

      assert_receive {:publish,
                      [%{type: :channel}, "exchange1", "my_routing_key", "my_payload", opts]}

      assert "my_value" = Keyword.fetch!(opts, :my_key)
      assert _ = Keyword.fetch!(opts, :timestamp)
      assert ^reply_to_queue = Keyword.fetch!(opts, :reply_to)
      assert Keyword.fetch!(opts, :correlation_id)

      assert [{"x-client", :binary, "lepus"}, {"x-reply-timeout", :long, 1000}] =
               opts |> Keyword.fetch!(:headers) |> Enum.sort()

      Task.await(task)
    end

    test ".publish_json publishes to rabbit and receives timeout error", %{
      client_name: client_name,
      reply_to_queue: reply_to_queue
    } do
      task =
        Task.async(fn ->
          assert {:error, :timeout} =
                   client_name
                   |> BasicClient.publish_json(
                     "exchange1",
                     "my_routing_key",
                     %{my_payload: "my_payload"},
                     amqp_opts: [my_key: "my_value"],
                     sync: true,
                     timeout: 1000
                   )
        end)

      assert_receive {:publish,
                      [
                        %{type: :channel},
                        "exchange1",
                        "my_routing_key",
                        ~s({"my_payload":"my_payload"}),
                        opts
                      ]}

      assert "my_value" = Keyword.fetch!(opts, :my_key)
      assert _ = Keyword.fetch!(opts, :timestamp)
      assert ^reply_to_queue = Keyword.fetch!(opts, :reply_to)
      assert Keyword.fetch!(opts, :correlation_id)

      assert [{"x-client", :binary, "lepus"}, {"x-reply-timeout", :long, 1000}] =
               opts |> Keyword.fetch!(:headers) |> Enum.sort()

      Task.await(task)
    end
  end

  defp new_server_name do
    :"server-#{System.unique_integer([:positive, :monotonic])}"
  end
end
