defmodule AshClickhouse.Type.ChRingTest do
  use ExUnit.Case, async: true
  alias AshClickhouse.Type.ChRing
  import AshClickhouse.Test.Helper, only: [encode_ch_type: 1]

  describe "storage_type/1" do
    test "returns correct ClickHouse type without constraints" do
      assert {:parameterized, {Ch, :ring}} = type = Ash.Type.storage_type(ChRing, [])
      assert encode_ch_type(type) == "Ring"
    end

    test "returns nullable ClickHouse type with nullable constraint" do
      assert {:parameterized, {Ch, {:nullable, :ring}}} =
               type = Ash.Type.storage_type(ChRing, nullable?: true)

      assert encode_ch_type(type) == "Nullable(Ring)"
    end

    test "returns low cardinality ClickHouse type with low_cardinality constraint" do
      assert {:parameterized, {Ch, {:low_cardinality, :ring}}} =
               type = Ash.Type.storage_type(ChRing, low_cardinality?: true)

      assert encode_ch_type(type) == "LowCardinality(Ring)"
    end
  end

  describe "generator/1" do
    test "returns a list of 3 point generators" do
      result = ChRing.generator([])
      assert is_list(result)
      assert length(result) == 3
    end
  end

  describe "cast_input/2" do
    test "returns ok with nil" do
      assert {:ok, nil} = ChRing.cast_input(nil, [])
    end
  end

  describe "cast_stored/2" do
    test "returns ok with nil" do
      assert {:ok, nil} = ChRing.cast_stored(nil, [])
    end
  end

  describe "dump_to_native/2" do
    test "returns ok with nil" do
      assert {:ok, nil} = ChRing.dump_to_native(nil, [])
    end
  end
end
