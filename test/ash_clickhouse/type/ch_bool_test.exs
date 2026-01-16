defmodule AshClickhouse.Type.ChBoolTest do
  use ExUnit.Case, async: true
  alias AshClickhouse.Type.ChBool

  import AshClickhouse.Test.Helper, only: [encode_ch_type: 1]

  describe "storage_types" do
    test "returns correct ClickHouse type without constraints" do
      assert {:parameterized, {Ch, :boolean}} = type = Ash.Type.storage_type(ChBool, [])
      assert encode_ch_type(type) == "Bool"
    end

    test "returns correct ClickHouse type for array version" do
      assert {:array, {:parameterized, {Ch, :boolean}} = subtype} =
               Ash.Type.storage_type({:array, ChBool}, [])

      assert encode_ch_type({:array, subtype}) == "Array(Bool)"
    end

    test "returns nullable ClickHouse type with nullable constraint" do
      assert {:parameterized, {Ch, {:nullable, :boolean}}} =
               type = Ash.Type.storage_type(ChBool, nullable?: true)

      assert encode_ch_type(type) == "Nullable(Bool)"
    end

    test "returns nullable ClickHouse type with nullable constraint for array version" do
      assert {:array, {:parameterized, {Ch, {:nullable, :boolean}}} = subtype} =
               Ash.Type.storage_type({:array, ChBool}, nullable?: true)

      assert encode_ch_type({:array, subtype}) == "Array(Nullable(Bool))"
    end
  end

  describe "matches_type?/2" do
    test "returns true for booleans" do
      assert Ash.Type.matches_type?(ChBool, true, [])
      assert Ash.Type.matches_type?(ChBool, false, [])
    end

    test "returns false for non-booleans" do
      refute Ash.Type.matches_type?(ChBool, "string", [])
      refute Ash.Type.matches_type?(ChBool, 123, [])
      refute Ash.Type.matches_type?(ChBool, nil, [])
      refute Ash.Type.matches_type?(ChBool, [], [])
      refute Ash.Type.matches_type?({:array, ChBool}, ["string", 123], [])
    end
  end

  describe "generator" do
    test "generates booleans" do
      generated_bools = Enum.take(Ash.Type.generator(ChBool, []), 100) |> Enum.uniq()
      assert Enum.all?(generated_bools, fn bool -> bool in [true, false] end)
    end

    test "generates arrays of booleans" do
      generated_arrays = Enum.take(Ash.Type.generator({:array, ChBool}, []), 100)

      assert Enum.all?(generated_arrays, fn arr ->
               is_list(arr) and Enum.all?(arr, fn bool -> bool in [true, false] end)
             end)
    end
  end

  describe "cast_input/2" do
    test "casts true and false correctly" do
      assert Ash.Type.cast_input(ChBool, true, []) == {:ok, true}
      assert Ash.Type.cast_input(ChBool, false, []) == {:ok, false}
      assert Ash.Type.cast_input(ChBool, nil, []) == {:ok, nil}
      # Array version
      assert Ash.Type.cast_input({:array, ChBool}, [true, false, nil], []) ==
               {:ok, [true, false, nil]}
    end

    test "returns error for non-boolean inputs" do
      assert {:error, "is invalid"} = Ash.Type.cast_input(ChBool, "string", [])
      assert {:error, "is invalid"} = Ash.Type.cast_input(ChBool, 123, [])
      # Array version
      assert {:error, [[message: "is invalid", index: 0, path: [0]]]} =
               Ash.Type.cast_input({:array, ChBool}, ["string", true], [])

      assert {:error, [[message: "is invalid", index: 1, path: [1]]]} =
               Ash.Type.cast_input({:array, ChBool}, [false, 123], [])
    end
  end

  describe "cast_stored/2" do
    test "loads true and false correctly" do
      assert Ash.Type.cast_stored(ChBool, true, []) == {:ok, true}
      assert Ash.Type.cast_stored(ChBool, false, []) == {:ok, false}
      assert Ash.Type.cast_stored(ChBool, 1, []) == {:ok, true}
      assert Ash.Type.cast_stored(ChBool, 0, []) == {:ok, false}
      assert Ash.Type.cast_stored(ChBool, "true", []) == {:ok, true}
      assert Ash.Type.cast_stored(ChBool, "false", []) == {:ok, false}
      assert Ash.Type.cast_stored(ChBool, nil, []) == {:ok, nil}
      # Array version
      assert Ash.Type.cast_stored({:array, ChBool}, [true, false, 1, 0, "true", "false", nil], []) ==
               {:ok, [true, false, true, false, true, false, nil]}
    end

    test "returns error for non-boolean stored values" do
      assert :error = Ash.Type.cast_stored(ChBool, "string", [])
      assert :error = Ash.Type.cast_stored(ChBool, 123, [])
      # Array version
      assert {:error, [{:index, 0}]} =
               Ash.Type.cast_stored({:array, ChBool}, ["string", true], [])

      assert {:error, [{:index, 1}]} =
               Ash.Type.cast_stored({:array, ChBool}, [false, 123], [])
    end
  end

  describe "equal?/2" do
    test "returns true for equal booleans" do
      assert Ash.Type.equal?(ChBool, true, true)
      assert Ash.Type.equal?(ChBool, false, false)
      assert Ash.Type.equal?(ChBool, nil, nil)
    end

    test "returns false for different booleans" do
      refute Ash.Type.equal?(ChBool, true, false)
      refute Ash.Type.equal?(ChBool, true, nil)
      refute Ash.Type.equal?(ChBool, nil, false)
    end

    test "returns true for equal arrays of booleans" do
      assert Ash.Type.equal?({:array, ChBool}, [true, false], [true, false])
      assert Ash.Type.equal?({:array, ChBool}, [], [])
      assert Ash.Type.equal?({:array, ChBool}, [nil], [nil])
    end

    test "returns false for different arrays of booleans" do
      refute Ash.Type.equal?({:array, ChBool}, [true, false], [false, true])
      refute Ash.Type.equal?({:array, ChBool}, [true], [true, false])
      refute Ash.Type.equal?({:array, ChBool}, [true, nil], [true, false])
    end
  end

  describe "simple_equality?/0" do
    test "returns true for ChBool" do
      assert Ash.Type.simple_equality?(ChBool)
    end

    test "returns true for {:array, ChBool}" do
      assert Ash.Type.simple_equality?({:array, ChBool})
    end
  end
end
