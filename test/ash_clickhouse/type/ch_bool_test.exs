defmodule AshClickhouse.Type.ChBoolTest do
  use ExUnit.Case, async: true
  alias AshClickhouse.Type.ChBool

  import AshClickhouse.Test.Helper, only: [encode_ch_type: 1]

  describe "storage_types" do
    test "returns correct ClickHouse type without constraints" do
      assert {:parameterized, {Ch, :boolean}} = type = ChBool.storage_type([])
      assert encode_ch_type(type) == "Bool"
    end

    test "returns nullable ClickHouse type with nullable constraint" do
      assert {:parameterized, {Ch, {:nullable, :boolean}}} =
               type = ChBool.storage_type(nullable?: true)

      assert encode_ch_type(type) == "Nullable(Bool)"
    end
  end

  describe "matches_type?/2" do
    test "returns true for booleans" do
      assert ChBool.matches_type?(true, [])
      assert ChBool.matches_type?(false, [])
    end

    test "returns false for non-booleans" do
      refute ChBool.matches_type?("string", [])
      refute ChBool.matches_type?(123, [])
      refute ChBool.matches_type?(nil, [])
      refute ChBool.matches_type?([], [])
    end
  end

  describe "generator" do
    test "generates booleans" do
      generated_bools = Enum.take(ChBool.generator([]), 100) |> Enum.uniq()
      assert Enum.all?(generated_bools, fn bool -> bool in [true, false] end)
    end
  end

  describe "cast_input/2" do
    test "casts true and false correctly" do
      assert ChBool.cast_input(true, []) == {:ok, true}
      assert ChBool.cast_input(false, []) == {:ok, false}
      assert ChBool.cast_input(nil, []) == {:ok, nil}
    end

    test "returns error for non-boolean inputs" do
      assert :error = ChBool.cast_input("string", [])
      assert :error = ChBool.cast_input(123, [])
    end
  end

  describe "cast_stored/2" do
    test "loads true and false correctly" do
      assert ChBool.cast_stored(true, []) == {:ok, true}
      assert ChBool.cast_stored(false, []) == {:ok, false}
      assert ChBool.cast_stored(1, []) == {:ok, true}
      assert ChBool.cast_stored(0, []) == {:ok, false}
      assert ChBool.cast_stored("true", []) == {:ok, true}
      assert ChBool.cast_stored("false", []) == {:ok, false}
      assert ChBool.cast_stored(nil, []) == {:ok, nil}
    end

    test "returns error for non-boolean stored values" do
      assert :error = ChBool.cast_stored("string", [])
      assert :error = ChBool.cast_stored(123, [])
    end
  end
end
