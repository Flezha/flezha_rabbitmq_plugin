defmodule FlezhaRabbitmq.CatalogueTest do
  @moduledoc """
  The refusals and failures the providers report are codes of the core's one
  catalogue. Built against a core that lacks one, this suite fails, not a
  user's enable or a health verdict.
  """
  use ExUnit.Case, async: true

  @catalogue Mix.Project.deps_paths()[:flezha]
             |> Path.join("flz_transport/errors.json")
             |> File.read!()
             |> Jason.decode!()

  test "every reason the providers give maps to its code" do
    for {reason, code} <- [
          {{:headers_invalid, "rabbitmq_producer", :headers, "flz.key"},
           "plugin.headers_invalid"},
          {{:rabbitmq_connection_failed, %{host: "h:5672", reason: "econnrefused"}},
           "rabbitmq.connection_failed"},
          {{:rabbitmq_channel_closed, %{code: 404, text: "NOT_FOUND"}}, "rabbitmq.channel_closed"}
        ] do
      assert Map.has_key?(@catalogue["codes"], code), "#{code} is not in the catalogue"
      assert %Flezha.Error{code: ^code} = Flezha.Error.from_reason(reason, nil)
    end
  end

  test "the codes a park names are in the catalogue" do
    assert %Flezha.Error{code: "rabbitmq.exhausted"} =
             Flezha.Error.new("rabbitmq.exhausted", %{reason: :nacked})

    assert %Flezha.Error{code: "rabbitmq.unroutable"} =
             Flezha.Error.new("rabbitmq.unroutable", %{exchange: "fx", routing_key: "k"})
  end
end
