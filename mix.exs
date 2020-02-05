defmodule Lepus.MixProject do
  use Mix.Project

  def project do
    [
      app: :lepus,
      version: "0.1.0",
      elixir: "~> 1.10",
      start_permanent: Mix.env() == :prod,
      deps: deps()
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:amqp, :lager, :logger]
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:broadway_rabbitmq, "~> 0.5.0"},
      {:jason, "~> 1.0"}
    ]
  end
end
