defmodule Lepus.MixProject do
  use Mix.Project

  @version "0.1.6"
  @description "Opinionated library for RabbitMQ with exponential backoff retries and RPC."
  @source_url "https://github.com/Recruitee/lepus"

  def project do
    [
      name: "Lepus",
      version: @version,
      description: @description,
      source_url: @source_url,
      app: :lepus,
      elixir: "~> 1.8",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      docs: docs(),
      package: package()
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:amqp, :logger]
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:broadway_rabbitmq, "~> 0.7"},
      {:jason, "~> 1.0"},
      {:phoenix_pubsub, ">= 1.0.0"},
      {:nimble_options, ">= 0.3.7 and < 2.0.0"},

      # Dev tools
      {:credo, "~> 1.4", only: :dev, runtime: false},
      {:dialyxir, "~> 1.0", only: [:dev], runtime: false},
      {:mix_test_watch, "~> 1.0", only: :dev, runtime: false},
      {:ex_doc, "~> 0.25", only: :dev, runtime: false},
      {:mox, "~> 1.0", only: :test}
    ]
  end

  def docs do
    [
      main: "readme",
      extras: ["README.md"]
    ]
  end

  defp package do
    [
      links: %{"GitHub" => @source_url},
      licenses: ["MIT"]
    ]
  end
end
