defmodule LepusTest do
  use ExUnit.Case, async: true

  alias Lepus.TestClient

  import Mox

  defmodule CustomClient do
    use Lepus, client: TestClient
  end

  describe ".publish" do
    test "with options" do
      TestClient
      |> expect(:publish, fn
        CustomClient, "exchange1", "routing_key", "payload", [a: 1, b: 2] -> :ok
      end)

      assert :ok = CustomClient.publish("exchange1", "routing_key", "payload", a: 1, b: 2)
    end

    test "without options" do
      TestClient
      |> expect(:publish, fn CustomClient, "exchange1", "routing_key", "payload", [] -> :ok end)

      assert :ok = CustomClient.publish("exchange1", "routing_key", "payload")
    end
  end

  describe ".publish_json" do
    test "with options" do
      TestClient
      |> expect(:publish_json, fn
        CustomClient, "exchange1", "routing_key", %{payload: "data"}, [a: 1, b: 2] -> :ok
      end)

      assert :ok =
               CustomClient.publish_json("exchange1", "routing_key", %{payload: "data"},
                 a: 1,
                 b: 2
               )
    end

    test "without options" do
      TestClient
      |> expect(:publish_json, fn
        CustomClient, "exchange1", "routing_key", %{payload: "data"}, [] -> :ok
      end)

      assert :ok = CustomClient.publish_json("exchange1", "routing_key", %{payload: "data"})
    end
  end
end
