defmodule FlezhaRabbitmq.MixProject do
  use Mix.Project

  # The RabbitMQ plugin package, packaged with `mix flezha.plugin.package` and
  # loaded by a Flezha controller at boot. `flezha` is the core this package is a
  # submodule of, two directories up, until it is published.
  def project do
    [
      app: :flezha_rabbitmq,
      version: "0.1.0",
      elixir: "~> 1.17",
      elixirc_paths: elixirc_paths(Mix.env()),
      deps: deps(),
      aliases: [test: "test --no-start"],
      # The end-to-end script is run by hand against a release.
      test_ignore_filters: [&String.starts_with?(&1, "test/e2e/")],
      package: [licenses: ["Apache-2.0"]]
    ]
  end

  # No `mod`: each provider's workers are started under its family by the
  # controller.
  def application do
    [
      env: [
        flezha_providers: %{
          "rabbitmq_consumer" => FlezhaRabbitmq.Consumer,
          "rabbitmq_producer" => FlezhaRabbitmq.Producer
        }
      ]
    ]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_env), do: ["lib"]

  defp deps do
    [
      {:flezha, path: "../.."},
      {:amqp, "~> 4.2"}
    ]
  end
end
