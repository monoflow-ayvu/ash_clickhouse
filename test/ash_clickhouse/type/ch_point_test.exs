defmodule AshClickhouse.Type.ChPointTest do
  use ExUnit.Case, async: true
  alias AshClickhouse.Type.ChPoint
  import AshClickhouse.Test.Helper, only: [encode_ch_type: 1]

  describe "storage_type/1" do
    test "returns correct ClickHouse type without constraints" do
      assert {:parameterized, {Ch, :point}} = type = Ash.Type.storage_type(ChPoint, [])
      assert encode_ch_type(type) == "Point"
    end

    test "returns nullable ClickHouse type with nullable constraint" do
      assert {:parameterized, {Ch, {:nullable, :point}}} =
               type = Ash.Type.storage_type(ChPoint, nullable?: true)

      assert encode_ch_type(type) == "Nullable(Point)"
    end

    test "returns low cardinality ClickHouse type with low_cardinality constraint" do
      assert {:parameterized, {Ch, {:low_cardinality, :point}}} =
               type = Ash.Type.storage_type(ChPoint, low_cardinality?: true)

      assert encode_ch_type(type) == "LowCardinality(Point)"
    end
  end

  describe "generator/1" do
    test "returns a tuple of two StreamData generators" do
      result = ChPoint.generator([])
      assert is_tuple(result)
      assert tuple_size(result) == 2
    end
  end

  describe "cast_input/2" do
    test "returns ok with nil" do
      assert {:ok, nil} = Ash.Type.cast_input(ChPoint, nil, [])
    end
  end

  describe "cast_stored/2" do
    test "returns ok with nil" do
      assert {:ok, nil} = ChPoint.cast_stored(nil, [])
    end
  end

  describe "dump_to_native/2" do
    test "returns ok with nil" do
      assert {:ok, nil} = ChPoint.dump_to_native(nil, [])
    end
  end
end
