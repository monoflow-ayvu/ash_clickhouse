defmodule AshClickhouse.Type.ChMultiPolygonTest do
  use ExUnit.Case, async: true
  alias AshClickhouse.Type.ChMultiPolygon
  import AshClickhouse.Test.Helper, only: [encode_ch_type: 1]

  describe "storage_type/1" do
    test "returns correct ClickHouse type without constraints" do
      assert {:parameterized, {Ch, :multipolygon}} =
               type = Ash.Type.storage_type(ChMultiPolygon, [])

      assert encode_ch_type(type) == "MultiPolygon"
    end

    test "returns nullable ClickHouse type with nullable constraint" do
      assert {:parameterized, {Ch, {:nullable, :multipolygon}}} =
               type = Ash.Type.storage_type(ChMultiPolygon, nullable?: true)

      assert encode_ch_type(type) == "Nullable(MultiPolygon)"
    end

    test "returns low cardinality ClickHouse type with low_cardinality constraint" do
      assert {:parameterized, {Ch, {:low_cardinality, :multipolygon}}} =
               type = Ash.Type.storage_type(ChMultiPolygon, low_cardinality?: true)

      assert encode_ch_type(type) == "LowCardinality(MultiPolygon)"
    end
  end

  describe "generator/1" do
    test "returns a list of 2 polygon generators" do
      result = ChMultiPolygon.generator([])
      assert is_list(result)
      assert length(result) == 2
    end
  end

  describe "cast_input/2" do
    test "returns ok with nil" do
      assert {:ok, nil} = ChMultiPolygon.cast_input(nil, [])
    end
  end

  describe "cast_stored/2" do
    test "returns ok with nil" do
      assert {:ok, nil} = ChMultiPolygon.cast_stored(nil, [])
    end
  end

  describe "dump_to_native/2" do
    test "returns ok with nil" do
      assert {:ok, nil} = ChMultiPolygon.dump_to_native(nil, [])
    end
  end
end
