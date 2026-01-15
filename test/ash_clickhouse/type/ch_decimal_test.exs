defmodule AshClickhouse.Type.ChDecimalTest do
  use ExUnit.Case, async: true
  alias AshClickhouse.Type.ChDecimal
  import AshClickhouse.Test.Helper, only: [encode_ch_type: 1]

  describe "storage_types" do
    test "returns correct ClickHouse type without constraints" do
      assert {:parameterized, {Ch, {:decimal, 10, 2}}} =
               type = ChDecimal.storage_type(precision: 10, scale: 2)

      assert encode_ch_type(type) == "Decimal(10, 2)"
    end

    test "returns correct ClickHouse type with precision and scale constraints" do
      assert {:parameterized, {Ch, {:decimal, 20, 5}}} =
               type = ChDecimal.storage_type(precision: 20, scale: 5)

      assert encode_ch_type(type) == "Decimal(20, 5)"
    end

    test "returns nullable ClickHouse type with nullable constraint" do
      assert {:parameterized, {Ch, {:nullable, {:decimal, 18, 4}}}} =
               type = ChDecimal.storage_type(precision: 18, scale: 4, nullable?: true)

      assert encode_ch_type(type) == "Nullable(Decimal(18, 4))"
    end
  end

  describe "matches_type?/2" do
    test "returns true for Decimal structs" do
      assert ChDecimal.matches_type?(Decimal.new("123.45"), [])
      assert ChDecimal.matches_type?(Decimal.new("0.001"), [])
    end

    test "returns false for non-Decimal values" do
      refute ChDecimal.matches_type?(nil, [])
      refute ChDecimal.matches_type?(123.45, [])
      refute ChDecimal.matches_type?("123.45", [])
      refute ChDecimal.matches_type?([], [])
    end
  end

  describe "generator" do
    test "generates Decimal structs within specified constraints" do
      constraints = [min: Decimal.new("10.00"), max: Decimal.new("20.00")]
      generated_decimals = Enum.take(ChDecimal.generator(constraints), 100) |> Enum.uniq()

      assert Enum.all?(generated_decimals, fn decimal ->
               Decimal.cmp(decimal, constraints[:min]) != :lt and
                 Decimal.cmp(decimal, constraints[:max]) != :gt
             end)
    end
  end

  describe "apply_constraints/2" do
    test "returns ok for valid Decimal within constraints" do
      constraints = [min: Decimal.new("5.00"), max: Decimal.new("15.00")]

      assert ChDecimal.apply_constraints(Decimal.new("10.00"), constraints) ==
               {:ok, Decimal.new("10.00")}
    end

    test "returns error for Decimal below min constraint" do
      constraints = [min: Decimal.new("5.00"), max: Decimal.new("15.00")]
      {:error, errors} = ChDecimal.apply_constraints(Decimal.new("3.00"), constraints)

      assert errors == [
               [message: "must be more than or equal to %{min}", min: Decimal.new("5.00")]
             ]
    end

    test "returns error for Decimal above max constraint" do
      constraints = [min: Decimal.new("5.00"), max: Decimal.new("15.00")]
      {:error, errors} = ChDecimal.apply_constraints(Decimal.new("20.00"), constraints)

      assert errors == [
               [message: "must be less than or equal to %{max}", max: Decimal.new("15.00")]
             ]
    end
  end

  describe "cast_input/2" do
    test "casts valid Decimal correctly" do
      assert ChDecimal.cast_input(Decimal.new("123.45"), precision: 5, scale: 2) ==
               {:ok, Decimal.new("123.45")}

      assert ChDecimal.cast_input("678.90", precision: 5, scale: 2) ==
               {:ok, Decimal.new("678.90")}

      assert ChDecimal.cast_input(123.45, precision: 5, scale: 2) == {:ok, Decimal.new("123.45")}
      assert ChDecimal.cast_input(nil, precision: 5, scale: 2) == {:ok, nil}
    end

    test "returns error for invalid inputs" do
      assert ChDecimal.cast_input("invalid", []) == :error
    end
  end

  describe "cast_stored/2" do
    test "casts stored string to Decimal" do
      assert ChDecimal.cast_stored("123.45", precision: 10, scale: 2) ==
               {:ok, Decimal.new("123.45")}

      assert ChDecimal.cast_stored(nil, []) == {:ok, nil}
    end

    test "returns error for non-string stored values" do
      assert :error = ChDecimal.cast_stored("any", precision: 10, scale: 2)
    end
  end
end
