defmodule LepusTest do
  use ExUnit.Case, async: true

  defmodule Client do
    def publish(name, exchange, routing_key, payload, options) do
      {:publish, [name, exchange, routing_key, payload, options]}
    end

    def publish_json(name, exchange, routing_key, payload, options) do
      {:publish_json, [name, exchange, routing_key, payload, options]}
    end
  end

  defmodule ServerClient do
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

  defmodule CustomServerClient do
    use Lepus, client: LepusTest.ServerClient, exchanges: ["exchange1", "exchange2"]
  end

  describe ".start_link" do
    test "by CustomClient" do
      assert :ignore = CustomClient.start_link([])
    end

    test "by ServerClient without init_arg" do
      assert {:start_link, [init_arg]} = CustomServerClient.start_link([])
      assert init_arg |> Keyword.fetch!(:exchanges) == ["exchange1", "exchange2"]
      assert init_arg |> Keyword.fetch!(:name) == LepusTest.CustomServerClient
      assert init_arg |> Keyword.fetch!(:connection) == nil
    end

    test "by ServerClient with init_arg" do
      assert {:start_link, [init_arg]} =
               CustomServerClient.start_link(
                 exchanges: ["exchange3"],
                 name: :other_name,
                 connection: "connection",
                 extra: "extra"
               )

      assert init_arg |> Keyword.fetch!(:exchanges) == ["exchange1", "exchange2"]
      assert init_arg |> Keyword.fetch!(:name) == LepusTest.CustomServerClient
      assert init_arg |> Keyword.fetch!(:connection) == "connection"
      refute init_arg |> Keyword.has_key?(:extra)
    end
  end

  describe ".publish" do
    test "by CustomClient with options and exchange from list" do
      result = CustomClient.publish("exchange1", "routing_key", "payload", a: 1, b: 2)

      assert {:publish, [CustomClient, "exchange1", "routing_key", "payload", [a: 1, b: 2]]} =
               result
    end

    test "by CustomClient with options and exchange not from list" do
      assert_raise ArgumentError,
                   ~s("exchange3" is not allowed. Only one of ["exchange1", "exchange2"] is allowed),
                   fn ->
                     CustomClient.publish("exchange3", "routing_key", "payload", a: 1, b: 2)
                   end
    end

    test "by CustomClient without options and exchange from list" do
      result = CustomClient.publish("exchange1", "routing_key", "payload")
      assert {:publish, [CustomClient, "exchange1", "routing_key", "payload", []]} = result
    end

    test "by CustomClient without options and exchange not from list" do
      assert_raise ArgumentError,
                   ~s("exchange3" is not allowed. Only one of ["exchange1", "exchange2"] is allowed),
                   fn -> CustomClient.publish("exchange3", "routing_key", "payload") end
    end

    test "by CustomServerClient with options and exchange from list" do
      result = CustomServerClient.publish("exchange1", "routing_key", "payload", a: 1, b: 2)

      assert {:publish, [CustomServerClient, "exchange1", "routing_key", "payload", [a: 1, b: 2]]} =
               result
    end

    test "by CustomServerClient with options and exchange not from list" do
      assert_raise ArgumentError,
                   ~s("exchange3" is not allowed. Only one of ["exchange1", "exchange2"] is allowed),
                   fn ->
                     CustomServerClient.publish("exchange3", "routing_key", "payload", a: 1, b: 2)
                   end
    end

    test "by CustomServerClient without options and exchange from list" do
      result = CustomServerClient.publish("exchange1", "routing_key", "payload")
      assert {:publish, [CustomServerClient, "exchange1", "routing_key", "payload", []]} = result
    end

    test "by CustomServerClient without options and exchange not from list" do
      assert_raise ArgumentError,
                   ~s("exchange3" is not allowed. Only one of ["exchange1", "exchange2"] is allowed),
                   fn -> CustomServerClient.publish("exchange3", "routing_key", "payload") end
    end
  end

  describe ".publish_json" do
    test "by CustomClient with options and exchange from list" do
      result =
        CustomClient.publish_json("exchange1", "routing_key", %{payload: "payload"},
          a: 1,
          b: 2
        )

      assert {:publish_json,
              [CustomClient, "exchange1", "routing_key", %{payload: "payload"}, [a: 1, b: 2]]} =
               result
    end

    test "by CustomClient with options and exchange not from list" do
      assert_raise ArgumentError,
                   ~s("exchange3" is not allowed. Only one of ["exchange1", "exchange2"] is allowed),
                   fn ->
                     CustomClient.publish_json("exchange3", "routing_key", %{payload: "payload"},
                       a: 1,
                       b: 2
                     )
                   end
    end

    test "by CustomClient without options and exchange from list" do
      result = CustomClient.publish_json("exchange1", "routing_key", %{payload: "payload"})

      assert {:publish_json,
              [CustomClient, "exchange1", "routing_key", %{payload: "payload"}, []]} = result
    end

    test "by CustomClient without options and exchange not from list" do
      assert_raise ArgumentError,
                   ~s("exchange3" is not allowed. Only one of ["exchange1", "exchange2"] is allowed),
                   fn ->
                     CustomClient.publish_json("exchange3", "routing_key", %{payload: "payload"})
                   end
    end

    test "by CustomServerClient with options and exchange from list" do
      result =
        CustomServerClient.publish_json("exchange1", "routing_key", %{payload: "payload"},
          a: 1,
          b: 2
        )

      assert {:publish_json,
              [
                CustomServerClient,
                "exchange1",
                "routing_key",
                %{payload: "payload"},
                [a: 1, b: 2]
              ]} = result
    end

    test "by CustomServerClient with options and exchange not from list" do
      assert_raise ArgumentError,
                   ~s("exchange3" is not allowed. Only one of ["exchange1", "exchange2"] is allowed),
                   fn ->
                     CustomServerClient.publish_json(
                       "exchange3",
                       "routing_key",
                       %{payload: "payload"},
                       a: 1,
                       b: 2
                     )
                   end
    end

    test "by CustomServerClient without options and exchange from list" do
      result = CustomServerClient.publish_json("exchange1", "routing_key", %{payload: "payload"})

      assert {:publish_json,
              [CustomServerClient, "exchange1", "routing_key", %{payload: "payload"}, []]} =
               result
    end

    test "by CustomServerClient without options and exchange not from list" do
      assert_raise ArgumentError,
                   ~s("exchange3" is not allowed. Only one of ["exchange1", "exchange2"] is allowed),
                   fn ->
                     CustomServerClient.publish_json("exchange3", "routing_key", %{
                       payload: "payload"
                     })
                   end
    end
  end
end
