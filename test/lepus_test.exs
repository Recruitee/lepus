defmodule LepusTest do
  use ExUnit.Case, async: true

  defmodule BasicClient do
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
    use Lepus, client: LepusTest.BasicClient
  end

  describe ".publish" do
    test "with options" do
      result = CustomClient.publish("exchange1", "routing_key", "payload", a: 1, b: 2)

      assert {:publish, [CustomClient, "exchange1", "routing_key", "payload", [a: 1, b: 2]]} =
               result
    end

    test "without options" do
      result = CustomClient.publish("exchange1", "routing_key", "payload")
      assert {:publish, [CustomClient, "exchange1", "routing_key", "payload", []]} = result
    end
  end

  describe ".publish_json" do
    test "with options" do
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

    test "without options" do
      result = CustomClient.publish_json("exchange1", "routing_key", %{payload: "payload"})

      assert {:publish_json,
              [CustomClient, "exchange1", "routing_key", %{payload: "payload"}, []]} = result
    end
  end
end
