defmodule AshClickhouse.Type.ChJSON do
  use Ash.Type
  require AshClickhouse.Type.Helper

  @constraints [
    nullable?: [
      type: :boolean,
      default: false,
      doc: "Whether the JSON can be null: Nullable(JSON)"
    ]
  ]

  @moduledoc """
  Represents a JSON, with configurable nullable.

  A builtin type that can be referenced via `:ch_json`

  ### Constraints

  #{Spark.Options.docs(@constraints)}
  """

  AshClickhouse.Type.Helper.graphql_type(__MODULE__, :ch_json, :json)

  @impl true
  def constraints, do: @constraints

  @impl true
  def storage_type(constraints), do: Ch.type(ch_type(constraints))

  def ch_type(constraints) do
    Ch.Types.json()
    |> maybe_nullable(constraints[:nullable?])
  end

  defp maybe_nullable(type, true), do: Ch.Types.nullable(type)
  defp maybe_nullable(type, _), do: type

  def coerce(value, constraints), do: cast_input(value, constraints)

  @impl true
  def cast_input(nil, _constraints), do: {:ok, nil}

  def cast_input(value, constraints) do
    case Jason.encode(value) do
      {:ok, json} ->
        json
        |> Jason.decode!()
        |> Ch.cast(ch_type(constraints))

      {:error, reason} ->
        {:error, "Failed to encode JSON: #{inspect(reason)}"}
    end
  end

  @impl true
  def cast_stored(nil, _constraints), do: {:ok, nil}

  def cast_stored(value, constraints), do: Ch.load(value, nil, ch_type(constraints))

  @impl true
  def dump_to_native(nil, _constraints), do: {:ok, nil}
  def dump_to_native(value, constraints), do: Ch.dump(value, nil, ch_type(constraints))
end
