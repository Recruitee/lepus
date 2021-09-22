defmodule Lepus.Consumer.Server do
  @moduledoc false

  alias Lepus.Consumer

  @spec start_link(atom, keyword) :: Broadway.on_start()
  def start_link(module, opts) do
    with {:ok, built_opts} <- module |> Consumer.Options.build(opts) do
      module |> Consumer.Broadway.start_link(built_opts)
    end
  end

  @spec child_spec(keyword) :: map
  def child_spec(opts) do
    module = opts |> Keyword.fetch!(:consumer)
    options = opts |> Keyword.fetch!(:options)
    id = opts |> Keyword.get(:name, module)

    %{
      id: id,
      shutdown: :infinity,
      start: {__MODULE__, :start_link, [module, options]}
    }
  end
end
