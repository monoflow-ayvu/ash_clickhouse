defmodule AshClickhouse.Type.ChFixedStringTest do
  use ExUnit.Case, async: true
  alias AshClickhouse.Type.ChFixedString
  import AshClickhouse.Test.Helper, only: [encode_ch_type: 1]

  describe "storage_types" do
    test "returns correct ClickHouse type with length constraint" do
      assert {:parameterized, {Ch, {:fixed_string, 16}}} =
               type = Ash.Type.storage_type(ChFixedString, length: 16)

      assert encode_ch_type(type) == "FixedString(16)"
    end

    test "returns nullable ClickHouse type with nullable constraint" do
      assert {:parameterized, {Ch, {:nullable, {:fixed_string, 32}}}} =
               type = Ash.Type.storage_type(ChFixedString, length: 32, nullable?: true)

      assert encode_ch_type(type) == "Nullable(FixedString(32))"
    end

    test "returns low cardinality ClickHouse type with low_cardinality constraint" do
      assert {:parameterized, {Ch, {:low_cardinality, {:fixed_string, 24}}}} =
               type = Ash.Type.storage_type(ChFixedString, length: 24, low_cardinality?: true)

      assert encode_ch_type(type) == "LowCardinality(FixedString(24))"
    end

    test "returns low cardinality nullable ClickHouse type with both constraints" do
      assert {:parameterized, {Ch, {:low_cardinality, {:nullable, {:fixed_string, 48}}}}} =
               type =
               Ash.Type.storage_type(
                 ChFixedString,
                 length: 48,
                 nullable?: true,
                 low_cardinality?: true
               )

      assert encode_ch_type(type) == "LowCardinality(Nullable(FixedString(48)))"
    end

    test "returns correct ClickHouse type with length constraint for array version" do
      assert {:array, {:parameterized, {Ch, {:fixed_string, 16}}} = subtype} =
               Ash.Type.storage_type({:array, ChFixedString}, length: 16)

      assert encode_ch_type({:array, subtype}) == "Array(FixedString(16))"
    end

    test "returns nullable ClickHouse type with nullable constraint for array version" do
      assert {:array, {:parameterized, {Ch, {:nullable, {:fixed_string, 32}}}} = subtype} =
               Ash.Type.storage_type({:array, ChFixedString}, length: 32, nullable?: true)

      assert encode_ch_type({:array, subtype}) == "Array(Nullable(FixedString(32)))"
    end

    test "returns low cardinality ClickHouse type with low_cardinality constraint for array version" do
      assert {:array, {:parameterized, {Ch, {:low_cardinality, {:fixed_string, 24}}}} = subtype} =
               Ash.Type.storage_type({:array, ChFixedString}, length: 24, low_cardinality?: true)

      assert encode_ch_type({:array, subtype}) == "Array(LowCardinality(FixedString(24)))"
    end

    test "returns low cardinality nullable ClickHouse type with both constraints for array version" do
      assert {:array,
              {:parameterized, {Ch, {:low_cardinality, {:nullable, {:fixed_string, 48}}}}} =
                subtype} =
               Ash.Type.storage_type(
                 {:array, ChFixedString},
                 length: 48,
                 nullable?: true,
                 low_cardinality?: true
               )

      assert encode_ch_type({:array, subtype}) ==
               "Array(LowCardinality(Nullable(FixedString(48))))"
    end
  end

  test "raises error when length constraint is missing" do
    assert_raise RuntimeError, "`length` is required for `:ch_fixed_string`", fn ->
      Ash.Type.storage_type(ChFixedString, [])
    end
  end

  describe "matches_type?/2" do
    test "returns true for strings" do
      assert Ash.Type.matches_type?(ChFixedString, "example", [])
      assert Ash.Type.matches_type?(ChFixedString, "", [])
    end

    test "returns false for non-strings" do
      refute Ash.Type.matches_type?(ChFixedString, nil, [])
      refute Ash.Type.matches_type?(ChFixedString, 123, [])
      refute Ash.Type.matches_type?(ChFixedString, [], [])
      refute Ash.Type.matches_type?(ChFixedString, %{}, [])
    end

    test "returns true for arrays of strings" do
      assert Ash.Type.matches_type?({:array, ChFixedString}, ["example", "test"], [])
      assert Ash.Type.matches_type?({:array, ChFixedString}, ["", "value"], [])
    end

    test "returns false for non-string arrays" do
      refute Ash.Type.matches_type?({:array, ChFixedString}, [123, 456], [])
      refute Ash.Type.matches_type?({:array, ChFixedString}, [[], {}], [])
      refute Ash.Type.matches_type?({:array, ChFixedString}, [nil, "test"], [])
    end
  end

  describe "generator" do
    test "generates strings of specified length" do
      constraints = [length: 10]

      generated_strings =
        Enum.take(Ash.Type.generator(ChFixedString, constraints), 100) |> Enum.uniq()

      assert Enum.all?(generated_strings, &(String.length(&1) == 10))
    end

    test "generates arrays of strings of specified length" do
      constraints = [length: 10]

      generated_arrays =
        Enum.take(Ash.Type.generator({:array, ChFixedString}, items: constraints), 100)

      assert Enum.all?(generated_arrays, fn arr ->
               is_list(arr) and Enum.all?(arr, &(String.length(&1) == 10))
             end)
    end
  end

  describe "apply_constraints/2" do
    test "returns ok for valid string within length constraint" do
      constraints = [length: 5]

      assert Ash.Type.apply_constraints(ChFixedString, "hello", constraints) ==
               {:ok, "hello"}
    end

    test "returns error for string exceeding length constraint" do
      constraints = [length: 3]

      {:error, error} = Ash.Type.apply_constraints(ChFixedString, "exceed", constraints)
      assert error == [message: "length must be equal to %{length}", length: 3]
    end

    test "returns ok for valid array of strings within length constraint" do
      constraints = [length: 5]

      assert Ash.Type.apply_constraints({:array, ChFixedString}, ["hello", "world"],
               items: constraints
             ) ==
               {:ok, ["hello", "world"]}
    end

    test "returns error for array with string exceeding length constraint" do
      constraints = [length: 3]

      {:error, errors} =
        Ash.Type.apply_constraints({:array, ChFixedString}, ["abc", "exceed"], items: constraints)

      assert length(errors) == 1
      [error] = errors
      assert error[:message] =~ "length must be equal to %{length}"
      assert error[:length] == 3
    end
  end

  describe "cast_input/2" do
    test "casts valid string to FixedString" do
      constraints = [length: 4]
      {:ok, value} = Ash.Type.cast_input(ChFixedString, "test", constraints)
      assert value == "test"
    end

    test "returns error for non strings" do
      constraints = [length: 4]
      {:error, "is invalid"} = Ash.Type.cast_input(ChFixedString, 1234, constraints)
    end

    test "casts valid array of strings to FixedString" do
      {:ok, value} =
        Ash.Type.cast_input({:array, ChFixedString}, ["test", "demo"], items: [length: 4])

      assert value == ["test", "demo"]
    end

    test "returns error for non-string array inputs" do
      assert {:error, [[message: "is invalid", index: 0, path: [0]]]} =
               Ash.Type.cast_input({:array, ChFixedString}, [1234, "test"], items: [length: 4])
    end
  end

  describe "equal?/2" do
    test "returns true for equal strings" do
      assert Ash.Type.equal?(ChFixedString, "test", "test")
      assert Ash.Type.equal?(ChFixedString, nil, nil)
    end

    test "returns false for different strings" do
      refute Ash.Type.equal?(ChFixedString, "test", "demo")
      refute Ash.Type.equal?(ChFixedString, "test", nil)
      refute Ash.Type.equal?(ChFixedString, nil, "test")
    end

    test "returns true for equal arrays of strings" do
      assert Ash.Type.equal?({:array, ChFixedString}, ["test", "demo"], ["test", "demo"])
      assert Ash.Type.equal?({:array, ChFixedString}, [], [])
    end

    test "returns false for different arrays of strings" do
      refute Ash.Type.equal?({:array, ChFixedString}, ["test", "demo"], ["demo", "test"])
      refute Ash.Type.equal?({:array, ChFixedString}, ["test"], ["test", "demo"])
      refute Ash.Type.equal?({:array, ChFixedString}, ["test", nil], ["test", "demo"])
    end
  end

  describe "simple_equality?/0" do
    test "returns true for ChFixedString" do
      assert Ash.Type.simple_equality?(ChFixedString)
    end

    test "returns true for {:array, ChFixedString}" do
      assert Ash.Type.simple_equality?({:array, ChFixedString})
    end
  end
end
