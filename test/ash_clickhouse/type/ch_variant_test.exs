defmodule AshClickhouse.Type.ChVariantTest do
  use ExUnit.Case, async: true
  alias AshClickhouse.Type.ChVariant

  import AshClickhouse.Test.Helper, only: [encode_ch_type: 1]

  @base_constraints [types: [:ch_string, :ch_int32]]

  describe "storage_type/1" do
    test "returns Variant ClickHouse type" do
      assert {:parameterized, {Ch, _}} = type = ChVariant.storage_type(@base_constraints)
      assert encode_ch_type(type) == "Variant(Int32, String)"
    end

    test "supports types with nested constraints" do
      constraints = [types: [:ch_string, ch_int32: []]]
      assert {:parameterized, {Ch, _}} = ChVariant.storage_type(constraints)
    end
  end

  describe "cast_input/2" do
    test "casts nil to nil" do
      assert ChVariant.cast_input(nil, @base_constraints) == {:ok, nil}
    end

    test "casts a string value" do
      assert {:ok, _} = ChVariant.cast_input("hello", @base_constraints)
    end
  end

  describe "cast_stored/2" do
    test "casts nil to nil" do
      assert ChVariant.cast_stored(nil, @base_constraints) == {:ok, nil}
    end
  end

  describe "dump_to_native/2" do
    test "dumps nil to nil" do
      assert ChVariant.dump_to_native(nil, @base_constraints) == {:ok, nil}
    end
  end

  describe "coerce/2" do
    test "delegates to cast_input" do
      assert ChVariant.coerce(nil, @base_constraints) == {:ok, nil}
    end
  end

  describe "constraints/0" do
    test "only includes types" do
      constraints = ChVariant.constraints()
      keys = Keyword.keys(constraints)
      assert :types in keys
      refute :nullable? in keys
      refute :low_cardinality? in keys
    end
  end
end
