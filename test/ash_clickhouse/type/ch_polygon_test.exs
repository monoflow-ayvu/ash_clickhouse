defmodule AshClickhouse.Type.ChPolygonTest do
  use ExUnit.Case, async: true
  alias AshClickhouse.Type.ChPolygon
  import AshClickhouse.Test.Helper, only: [encode_ch_type: 1]

  describe "storage_type/1" do
    test "returns correct ClickHouse type without constraints" do
      assert {:parameterized, {Ch, :polygon}} = type = Ash.Type.storage_type(ChPolygon, [])
      assert encode_ch_type(type) == "Polygon"
    end

    test "returns nullable ClickHouse type with nullable constraint" do
      assert {:parameterized, {Ch, {:nullable, :polygon}}} =
               type = Ash.Type.storage_type(ChPolygon, nullable?: true)

      assert encode_ch_type(type) == "Nullable(Polygon)"
    end

    test "returns low cardinality ClickHouse type with low_cardinality constraint" do
      assert {:parameterized, {Ch, {:low_cardinality, :polygon}}} =
               type = Ash.Type.storage_type(ChPolygon, low_cardinality?: true)

      assert encode_ch_type(type) == "LowCardinality(Polygon)"
    end
  end

  describe "generator/1" do
    test "returns a list of 3 ring generators" do
      result = ChPolygon.generator([])
      assert is_list(result)
      assert length(result) == 3
    end
  end

  describe "cast_input/2" do
    test "returns ok with nil" do
      assert {:ok, nil} = ChPolygon.cast_input(nil, [])
    end
  end

  describe "cast_stored/2" do
    test "returns ok with nil" do
      assert {:ok, nil} = ChPolygon.cast_stored(nil, [])
    end
  end

  describe "dump_to_native/2" do
    test "returns ok with nil" do
      assert {:ok, nil} = ChPolygon.dump_to_native(nil, [])
    end
  end
end
