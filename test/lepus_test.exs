defmodule LepusTest do
  use ExUnit.Case, async: true

  defmodule Client do
    def start_link(init_arg) do
      {:start_link, [init_arg]}
    end

    def publish(name, exchange, routing_key, payload, options) do
      {:publish, [name, exchange, routing_key, payload, options]}
    end

    def publish_json(name, exchange, routing_key, payload, options) do
      {:publish_json, [name, exchange, routing_key, payload, options]}
    end
  end

  defmodule CustomClient do
    use Lepus, client: LepusTest.Client, exchanges: ["exchange1", "exchange2"]
  end

  describe ".start_link" do
    test "without init_arg" do
      assert {:start_link, [init_arg]} = CustomClient.start_link([])
      assert init_arg |> Keyword.fetch!(:exchanges) == ["exchange1", "exchange2"]
      assert init_arg |> Keyword.fetch!(:name) == LepusTest.CustomClient
      assert init_arg |> Keyword.fetch!(:connection) == nil
    end

    test "with init_arg" do
      assert {:start_link, [init_arg]} =
               CustomClient.start_link(
                 exchanges: ["exchange3"],
                 name: :other_name,
                 connection: "connection",
                 extra: "extra"
               )

      assert init_arg |> Keyword.fetch!(:exchanges) == ["exchange1", "exchange2"]
      assert init_arg |> Keyword.fetch!(:name) == LepusTest.CustomClient
      assert init_arg |> Keyword.fetch!(:connection) == "connection"
      refute init_arg |> Keyword.has_key?(:extra)
    end
  end

  describe ".publish" do
    test "with options and exchange from list" do
      result = CustomClient.publish("exchange1", "routing_key", "payload", a: 1, b: 2)

      assert {:publish, [CustomClient, "exchange1", "routing_key", "payload", [a: 1, b: 2]]} =
               result
    end

    test "with options and exchange not from list" do
      assert_raise ArgumentError,
                   ~s("exchange3" is not allowed. Only one of ["exchange1", "exchange2"] is allowed),
                   fn ->
                     CustomClient.publish("exchange3", "routing_key", "payload", a: 1, b: 2)
                   end
    end

    test "without options and exchange from list" do
      result = CustomClient.publish("exchange1", "routing_key", "payload")
      assert {:publish, [CustomClient, "exchange1", "routing_key", "payload", []]} = result
    end

    test "without options and exchange not from list" do
      assert_raise ArgumentError,
                   ~s("exchange3" is not allowed. Only one of ["exchange1", "exchange2"] is allowed),
                   fn -> CustomClient.publish("exchange3", "routing_key", "payload") end
    end
  end

  describe ".publish_json" do
    test "with options and exchange from list" do
      result =
        CustomClient.publish_json("exchange1", "routing_key", %{payload: "payload"},
          a: 1,
          b: 2
        )

      assert {:publish_json,
              [
                CustomClient,
                "exchange1",
                "routing_key",
                %{payload: "payload"},
                [a: 1, b: 2]
              ]} = result
    end

    test "with options and exchange not from list" do
      assert_raise ArgumentError,
                   ~s("exchange3" is not allowed. Only one of ["exchange1", "exchange2"] is allowed),
                   fn ->
                     CustomClient.publish_json(
                       "exchange3",
                       "routing_key",
                       %{payload: "payload"},
                       a: 1,
                       b: 2
                     )
                   end
    end

    test "without options and exchange from list" do
      result = CustomClient.publish_json("exchange1", "routing_key", %{payload: "payload"})

      assert {:publish_json,
              [CustomClient, "exchange1", "routing_key", %{payload: "payload"}, []]} = result
    end

    test "without options and exchange not from list" do
      assert_raise ArgumentError,
                   ~s("exchange3" is not allowed. Only one of ["exchange1", "exchange2"] is allowed),
                   fn ->
                     CustomClient.publish_json("exchange3", "routing_key", %{
                       payload: "payload"
                     })
                   end
    end
  end
end
