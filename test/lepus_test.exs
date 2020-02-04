defmodule LepusTest do
  use ExUnit.Case
  doctest Lepus

  test "greets the world" do
    assert Lepus.hello() == :world
  end
end
