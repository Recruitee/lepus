defmodule Lepus.Rabbit.Config do
  @moduledoc false

  def allowed_queues_types, do: ["classic", "quorum"]
  def default_queues_type, do: Application.get_env(:lepus, :queues_type, "classic")
end
