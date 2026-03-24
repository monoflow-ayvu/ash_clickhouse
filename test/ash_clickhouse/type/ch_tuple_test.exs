defmodule AshClickhouse.Type.ChTupleTest do
  use ExUnit.Case, async: true
  alias AshClickhouse.Type.ChTuple

  import AshClickhouse.Test.Helper, only: [encode_ch_type: 1]

  @base_constraints [types: [:ch_string, :ch_int32]]

  describe "storage_type/1" do
    test "returns Tuple ClickHouse type" do
      assert {:parameterized, {Ch, _}} = type = ChTuple.storage_type(@base_constraints)
      assert encode_ch_type(type) == "Tuple(String, Int32)"
    end

    test "supports types with nested constraints" do
      constraints = [types: [:ch_string, ch_int32: []]]
      assert {:parameterized, {Ch, _}} = ChTuple.storage_type(constraints)
    end

    test "supports bool and float types" do
      constraints = [types: [:ch_bool, :ch_float32]]
      assert {:parameterized, {Ch, _}} = type = ChTuple.storage_type(constraints)
      assert encode_ch_type(type) == "Tuple(Bool, Float32)"
    end
  end

  describe "cast_input/2" do
    test "casts nil to nil" do
      assert ChTuple.cast_input(nil, @base_constraints) == {:ok, nil}
    end
  end

  describe "cast_stored/2" do
    test "casts nil to nil" do
      assert ChTuple.cast_stored(nil, @base_constraints) == {:ok, nil}
    end
  end

  describe "dump_to_native/2" do
    test "dumps nil to nil" do
      assert ChTuple.dump_to_native(nil, @base_constraints) == {:ok, nil}
    end
  end

  describe "constraints/0" do
    test "only includes types" do
      constraints = ChTuple.constraints()
      keys = Keyword.keys(constraints)
      assert :types in keys
      refute :low_cardinality? in keys
    end

    test "types constraint is required" do
      types_constraint = ChTuple.constraints()[:types]
      assert types_constraint[:required] == true
    end
  end
end
