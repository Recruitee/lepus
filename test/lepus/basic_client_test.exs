defmodule Lepus.BasicClient.ServerTest do
  alias Lepus.BasicClient

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
      assert {:ok, server} =
               BasicClient.start_link(
                 name: new_server_name(),
                 connection: connection,
                 rabbit_client: RabbitClientMock,
                 exchanges: ["exchange1", "exchange2", ""]
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

  describe ".publish" do
    setup(%{connection: connection}) do
      server_name = new_server_name()

      {:ok, _} =
        BasicClient.start_link(
          name: server_name,
          connection: connection,
          rabbit_client: RabbitClientMock,
          exchanges: ["exchange1", "exchange2", ""]
        )

      {:ok, server_name: server_name}
    end

    test "publishes to rabbit and adds timestamp to opts", %{server_name: server_name} do
      server_name
      |> BasicClient.publish("exchange1", "my_routing_key", "my_payload",
        amqp_opts: [my_key: "my_value"]
      )

      assert_receive {:publish,
                      [%{type: :channel}, "exchange1", "my_routing_key", "my_payload", opts]}

      assert Keyword.fetch!(opts, :my_key) == "my_value"
      assert Keyword.has_key?(opts, :timestamp)
    end

    test "uses 1 channel per exchange", %{server_name: server_name} do
      server_name |> BasicClient.publish("exchange1", "my_routing_key", "my_payload1", [])
      assert_receive {:publish, [channel1, "exchange1", "my_routing_key", "my_payload1", _]}

      server_name |> BasicClient.publish("exchange2", "my_routing_key", "my_payload1", [])
      assert_receive {:publish, [channel2, "exchange2", "my_routing_key", "my_payload1", _]}

      server_name |> BasicClient.publish("", "my_routing_key", "my_payload1", [])
      assert_receive {:publish, [channel3, "", "my_routing_key", "my_payload1", _]}

      server_name |> BasicClient.publish("exchange1", "my_routing_key", "my_payload2", [])
      assert_receive {:publish, [^channel1, "exchange1", "my_routing_key", "my_payload2", _]}

      server_name |> BasicClient.publish("exchange2", "my_routing_key", "my_payload2", [])
      assert_receive {:publish, [^channel2, "exchange2", "my_routing_key", "my_payload2", _]}

      server_name |> BasicClient.publish("", "my_routing_key", "my_payload1", [])
      assert_receive {:publish, [^channel3, "", "my_routing_key", "my_payload1", _]}

      refute Map.equal?(channel1, channel2)
      refute Map.equal?(channel1, channel3)
      refute Map.equal?(channel2, channel3)
    end
  end

  describe ".publish_json" do
    setup(%{connection: connection}) do
      server_name = new_server_name()

      {:ok, _} =
        BasicClient.start_link(
          name: server_name,
          connection: connection,
          rabbit_client: RabbitClientMock,
          exchanges: ["exchange1", "exchange2"]
        )

      {:ok, server_name: server_name}
    end

    test "publishes to rabbit and adds timestamp to opts", %{server_name: server_name} do
      server_name
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
    end

    test "uses 1 channel per exchange", %{server_name: server_name} do
      server_name
      |> BasicClient.publish_json("exchange1", "my_routing_key", %{data: "my_payload1"}, [])

      assert_receive {:publish,
                      [channel1, "exchange1", "my_routing_key", ~s({"data":"my_payload1"}), _]}

      server_name
      |> BasicClient.publish_json("exchange2", "my_routing_key", %{data: "my_payload1"}, [])

      assert_receive {:publish,
                      [channel2, "exchange2", "my_routing_key", ~s({"data":"my_payload1"}), _]}

      server_name
      |> BasicClient.publish_json("exchange1", "my_routing_key", %{data: "my_payload2"}, [])

      assert_receive {:publish,
                      [^channel1, "exchange1", "my_routing_key", ~s({"data":"my_payload2"}), _]}

      server_name
      |> BasicClient.publish_json("exchange2", "my_routing_key", %{data: "my_payload2"}, [])

      assert_receive {:publish,
                      [^channel2, "exchange2", "my_routing_key", ~s({"data":"my_payload2"}), _]}

      refute Map.equal?(channel1, channel2)
    end
  end

  defp new_server_name do
    :"server-#{System.unique_integer([:positive, :monotonic])}"
  end
end
