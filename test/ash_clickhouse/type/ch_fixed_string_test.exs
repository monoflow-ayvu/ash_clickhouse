defmodule AshClickhouse.Type.ChFixedStringTest do
  use ExUnit.Case, async: true
  alias AshClickhouse.Type.ChFixedString
  import AshClickhouse.Test.Helper, only: [encode_ch_type: 1]

  describe "storage_types" do
    test "returns correct ClickHouse type with length constraint" do
      assert {:parameterized, {Ch, {:fixed_string, 16}}} =
               type = ChFixedString.storage_type(length: 16)

      assert encode_ch_type(type) == "FixedString(16)"
    end

    test "returns nullable ClickHouse type with nullable constraint" do
      assert {:parameterized, {Ch, {:nullable, {:fixed_string, 32}}}} =
               type = ChFixedString.storage_type(length: 32, nullable?: true)

      assert encode_ch_type(type) == "Nullable(FixedString(32))"
    end

    test "returns low cardinality ClickHouse type with low_cardinality constraint" do
      assert {:parameterized, {Ch, {:low_cardinality, {:fixed_string, 24}}}} =
               type = ChFixedString.storage_type(length: 24, low_cardinality?: true)

      assert encode_ch_type(type) == "LowCardinality(FixedString(24))"
    end

    test "returns low cardinality nullable ClickHouse type with both constraints" do
      assert {:parameterized, {Ch, {:low_cardinality, {:nullable, {:fixed_string, 48}}}}} =
               type =
               ChFixedString.storage_type(
                 length: 48,
                 nullable?: true,
                 low_cardinality?: true
               )

      assert encode_ch_type(type) == "LowCardinality(Nullable(FixedString(48)))"
    end
  end

  test "raises error when length constraint is missing" do
    assert_raise RuntimeError, "`length` is required for `:ch_fixed_string`", fn ->
      ChFixedString.storage_type([])
    end
  end

  describe "matches_type?/2" do
    test "returns true for strings" do
      assert ChFixedString.matches_type?("example", [])
      assert ChFixedString.matches_type?("", [])
    end

    test "returns false for non-strings" do
      refute ChFixedString.matches_type?(nil, [])
      refute ChFixedString.matches_type?(123, [])
      refute ChFixedString.matches_type?([], [])
      refute ChFixedString.matches_type?(%{}, [])
    end
  end

  describe "generator" do
    test "generates strings of specified length" do
      constraints = [length: 10]
      generated_strings = Enum.take(ChFixedString.generator(constraints), 100) |> Enum.uniq()

      assert Enum.all?(generated_strings, &(String.length(&1) == 10))
    end
  end

  describe "apply_constraints/2" do
    test "returns ok for valid string within length constraint" do
      constraints = [length: 5]

      assert ChFixedString.apply_constraints("hello", constraints) ==
               {:ok, "hello"}
    end

    test "returns error for string exceeding length constraint" do
      constraints = [length: 3]

      {:error, error} = ChFixedString.apply_constraints("exceed", constraints)
      assert error == [message: "length must be equal to %{length}", length: 3]
    end
  end

  describe "cast_input/2" do
    test "casts valid string to FixedString" do
      constraints = [length: 4]
      {:ok, value} = ChFixedString.cast_input("test", constraints)
      assert value == "test"
    end

    test "returns error for non strings" do
      constraints = [length: 4]
      :error = ChFixedString.cast_input(1234, constraints)
    end
  end
end
