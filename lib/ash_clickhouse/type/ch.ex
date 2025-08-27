defmodule AshClickhouse.Type.Ch do
  use Ash.Type

  @impl true
  def storage_type(constraints) do
    constraints
    |> Keyword.get(:type)
    |> Ch.Types.decode()
    |> Ch.type()
  end

  def constraints() do
    [
      type: [
        type: :string,
        doc: "Clickhouse type"
      ]
    ]
  end

  @impl true
  def coerce(value, constraints) do
    cast_input(value, constraints)
  end

  @impl true
  def cast_input(value, constraints) do
    Ch.cast(value, Ch.Types.decode(constraints[:type]))
  end

  @impl true
  def cast_stored(value, constraints) do
    Ch.load(value, nil, Ch.Types.decode(constraints[:type]))
  end

  @impl true
  def dump_to_native(value, constraints) do
    Ch.dump(value, nil, Ch.Types.decode(constraints[:type]))
  end

  # defp do_graphql_type(:string), do: :string
  # defp do_graphql_type(:boolean), do: :boolean
  # defp do_graphql_type(:uuid), do: :id
  # defp do_graphql_type(:date), do: :date
  # defp do_graphql_type(:date32), do: :date
  # defp do_graphql_type(:time), do: :time
  # defp do_graphql_type({:time64, _p}), do: :time
  # defp do_graphql_type(:datetime), do: :datetime
  # defp do_graphql_type({:datetime, "UTC"}), do: :datetime
  # defp do_graphql_type({:datetime64, _p}), do: :datetime
  # defp do_graphql_type({:datetime64, _p, "UTC"}), do: :datetime
  # defp do_graphql_type({:fixed_string, _s}), do: :string
  # defp do_graphql_type(:json), do: :json

  # defp do_graphql_type(type) when type in [:i8, :i16, :i32, :i64, :u8, :u16, :u32, :u64],
  #   do: :integer

  # defp do_graphql_type(type) when type in [:f32, :f64], do: :float
  # defp do_graphql_type({:decimal, _p, _s}), do: :decimal

  # defp do_graphql_type(type) when type in [:decimal32, :decimal64, :decimal128, :decimal256],
  #   do: :decimal

  # defp do_graphql_type({:low_cardinality, type}), do: do_graphql_type(type)
  # defp do_graphql_type({:array, type}), do: list_of(do_graphql_type(type))
  # defp do_graphql_type({:nullable, type}), do: do_graphql_type(type)
  # defp do_graphql_type({:simple_aggregate_function, _name, type}), do: do_graphql_type(type)
  # defp do_graphql_type(:point), do: list_of(:float)
  # defp do_graphql_type(:ring), do: list_of(do_graphql_type(:point))
  # defp do_graphql_type(:polygon), do: list_of(do_graphql_type(:ring))
  # defp do_graphql_type(:multipolygon), do: list_of(do_graphql_type(:polygon))
  # defp do_graphql_type(:ipv4), do: list_of(:integer)
  # defp do_graphql_type(:ipv6), do: list_of(:integer)

  # defp list_of(type) do
  #   %Absinthe.Blueprint.TypeReference.List{of_type: type}
  # end
end
