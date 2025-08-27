defmodule AshClickhouse.Type.ChMultiPolygon do
  @constraints [
    nullable?: [
      type: :boolean,
      default: false,
      doc: "Whether the MultiPolygon can be null: Nullable(MultiPolygon)"
    ],
    low_cardinality?: [
      type: :boolean,
      default: false,
      doc: "Whether the MultiPolygon is a low cardinality type: LowCardinality(MultiPolygon)"
    ]
  ]

  @moduledoc """
  A ClickHouse-specific type for abstracting multipolygons into a single type.

  ### Constraints

  #{Spark.Options.docs(@constraints)}
  """

  use Ash.Type

  @impl true
  def storage_type(constraints) do
    constraints
    |> ch_type()
    |> Ch.type()
  end

  def ch_type(constraints) do
    Ch.Types.multipolygon()
    |> maybe_nullable(constraints[:nullable?])
    |> maybe_low_cardinality(constraints[:low_cardinality?])
  end

  defp maybe_nullable(type, true), do: Ch.Types.nullable(type)
  defp maybe_nullable(type, _), do: type

  defp maybe_low_cardinality(type, true), do: Ch.Types.low_cardinality(type)
  defp maybe_low_cardinality(type, _), do: type

  @impl true
  def generator(_constraints) do
    Enum.map(1..2, fn _ -> AshClickhouse.Type.ChPolygon.generator([]) end)
  end

  @impl true
  def cast_input(nil, _), do: {:ok, nil}

  def cast_input(value, constraints) do
    Ch.cast(value, ch_type(constraints))
  end

  @impl true
  def cast_stored(nil, _), do: {:ok, nil}

  def cast_stored(value, constraints) do
    Ch.load(value, nil, ch_type(constraints))
  end

  @impl true
  def dump_to_native(nil, _), do: {:ok, nil}
  def dump_to_native(value, constraints), do: Ch.dump(value, nil, ch_type(constraints))
end
