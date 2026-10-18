defmodule FlezhaRabbitmq.DocsTest do
  use ExUnit.Case, async: true

  alias Flezha.Plugin.ProviderBehaviour.Options

  @page Mix.Project.deps_paths()[:flezha] |> Path.join("docs/public/plugins/rabbitmq.md")

  for provider <- [FlezhaRabbitmq.Consumer, FlezhaRabbitmq.Producer] do
    test "the core's rabbitmq.md carries the option table #{inspect(provider)} renders" do
      name = unquote(provider).__provider__().name
      rendered = Options.render_options_table(unquote(provider))

      documented =
        @page
        |> File.read!()
        |> String.split("<!-- provider-options:begin #{name} -->\n")
        |> Enum.at(1, "")
        |> String.split("<!-- provider-options:end -->")
        |> hd()

      assert documented == rendered,
             "rabbitmq.md's #{name} option table has drifted from its declaration.\n" <>
               "Replace the block between its provider-options markers with:\n\n" <> rendered
    end
  end
end
