defmodule AshClickhouse.Type.ChDecimalTypesTest do
  use ExUnit.Case, async: true

  alias AshClickhouse.Type.ChDecimal32
  alias AshClickhouse.Type.ChDecimal64
  alias AshClickhouse.Type.ChDecimal128
  alias AshClickhouse.Type.ChDecimal256

  import AshClickhouse.Test.Helper, only: [encode_ch_type: 1]

  describe "storage_types" do
    test "returns correct ClickHouse types for Decimal32" do
      assert {:parameterized, {Ch, {:decimal32, 3}}} =
               type = ChDecimal32.storage_type(scale: 3)

      assert encode_ch_type(type) == "Decimal(9, 3)"
    end

    test "returns correct ClickHouse types for Nullable(Decimal32)" do
      assert {:parameterized, {Ch, {:nullable, {:decimal32, 4}}}} =
               type = ChDecimal32.storage_type(scale: 4, nullable?: true)

      assert encode_ch_type(type) == "Nullable(Decimal(9, 4))"
    end

    test "returns correct ClickHouse types for Decimal64" do
      assert {:parameterized, {Ch, {:decimal64, 2}}} =
               type = ChDecimal64.storage_type(scale: 2)

      assert encode_ch_type(type) == "Decimal(18, 2)"
    end

    test "returns correct ClickHouse types for Nullable(Decimal64)" do
      assert {:parameterized, {Ch, {:nullable, {:decimal64, 6}}}} =
               type = ChDecimal64.storage_type(scale: 6, nullable?: true)

      assert encode_ch_type(type) == "Nullable(Decimal(18, 6))"
    end

    test "returns correct ClickHouse types for Decimal128" do
      assert {:parameterized, {Ch, {:decimal128, 5}}} =
               type = ChDecimal128.storage_type(scale: 5)

      assert encode_ch_type(type) == "Decimal(38, 5)"
    end

    test "returns correct ClickHouse types for Nullable(Decimal128)" do
      assert {:parameterized, {Ch, {:nullable, {:decimal128, 7}}}} =
               type = ChDecimal128.storage_type(scale: 7, nullable?: true)

      assert encode_ch_type(type) == "Nullable(Decimal(38, 7))"
    end

    test "returns correct ClickHouse types for Decimal256" do
      assert {:parameterized, {Ch, {:decimal256, 8}}} =
               type = ChDecimal256.storage_type(scale: 8)

      assert encode_ch_type(type) == "Decimal(76, 8)"
    end

    test "returns correct ClickHouse types for Nullable(Decimal256)" do
      assert {:parameterized, {Ch, {:nullable, {:decimal256, 9}}}} =
               type = ChDecimal256.storage_type(scale: 9, nullable?: true)

      assert encode_ch_type(type) == "Nullable(Decimal(76, 9))"
    end
  end

  describe "matches_type?/2" do
    test "returns true for Decimal structs" do
      assert ChDecimal32.matches_type?(Decimal.new("123.456"), [])
      assert ChDecimal64.matches_type?(Decimal.new("1234567890.12"), [])
      assert ChDecimal128.matches_type?(Decimal.new("12345678901234567890.12345"), [])

      assert ChDecimal256.matches_type?(
               Decimal.new("123456789012345678901234567890.12345678"),
               []
             )
    end

    test "returns false for non-Decimal values" do
      refute ChDecimal32.matches_type?(nil, [])
      refute ChDecimal64.matches_type?(123.45, [])
      refute ChDecimal128.matches_type?("123.45", [])
      refute ChDecimal256.matches_type?([], [])
    end
  end

  describe "generator" do
    test "Decimal32: generates Decimal structs within specified constraints" do
      constraints = [min: Decimal.new("1.00"), max: Decimal.new("10.00")]
      generated_decimals = Enum.take(ChDecimal32.generator(constraints), 100) |> Enum.uniq()

      assert Enum.all?(generated_decimals, fn decimal ->
               Decimal.cmp(decimal, constraints[:min]) != :lt and
                 Decimal.cmp(decimal, constraints[:max]) != :gt
             end)
    end

    test "Decimal64: generates Decimal structs within specified constraints" do
      constraints = [min: Decimal.new("100.00"), max: Decimal.new("200.00")]
      generated_decimals = Enum.take(ChDecimal64.generator(constraints), 100) |> Enum.uniq()

      assert Enum.all?(generated_decimals, fn decimal ->
               Decimal.cmp(decimal, constraints[:min]) != :lt and
                 Decimal.cmp(decimal, constraints[:max]) != :gt
             end)
    end

    test "Decimal128: generates Decimal structs within specified constraints" do
      constraints = [min: Decimal.new("1000.00"), max: Decimal.new("2000.00")]
      generated_decimals = Enum.take(ChDecimal128.generator(constraints), 100) |> Enum.uniq()

      assert Enum.all?(generated_decimals, fn decimal ->
               Decimal.cmp(decimal, constraints[:min]) != :lt and
                 Decimal.cmp(decimal, constraints[:max]) != :gt
             end)
    end

    test "Decimal256: generates Decimal structs within specified constraints" do
      constraints = [min: Decimal.new("10000.00"), max: Decimal.new("20000.00")]
      generated_decimals = Enum.take(ChDecimal256.generator(constraints), 100) |> Enum.uniq()

      assert Enum.all?(generated_decimals, fn decimal ->
               Decimal.cmp(decimal, constraints[:min]) != :lt and
                 Decimal.cmp(decimal, constraints[:max]) != :gt
             end)
    end
  end

  describe "apply_constraints/2" do
    test "returns ok for valid Decimal within constraints" do
      constraints = [min: Decimal.new("5.00"), max: Decimal.new("15.00")]

      assert ChDecimal32.apply_constraints(Decimal.new("10.00"), constraints) ==
               {:ok, Decimal.new("10.00")}

      assert ChDecimal64.apply_constraints(Decimal.new("10.00"), constraints) ==
               {:ok, Decimal.new("10.00")}

      assert ChDecimal128.apply_constraints(Decimal.new("10.00"), constraints) ==
               {:ok, Decimal.new("10.00")}

      assert ChDecimal256.apply_constraints(Decimal.new("10.00"), constraints) ==
               {:ok, Decimal.new("10.00")}
    end

    test "returns error for Decimal below min constraint" do
      constraints = [min: Decimal.new("5.00"), max: Decimal.new("15.00")]

      {:error, errors32} = ChDecimal32.apply_constraints(Decimal.new("3.00"), constraints)
      {:error, errors64} = ChDecimal64.apply_constraints(Decimal.new("3.00"), constraints)
      {:error, errors128} = ChDecimal128.apply_constraints(Decimal.new("3.00"), constraints)
      {:error, errors256} = ChDecimal256.apply_constraints(Decimal.new("3.00"), constraints)

      expected_error = [
        [message: "must be more than or equal to %{min}", min: Decimal.new("5.00")]
      ]

      assert errors32 == expected_error
      assert errors64 == expected_error
      assert errors128 == expected_error
      assert errors256 == expected_error
    end

    test "returns error for Decimal above max constraint" do
      constraints = [min: Decimal.new("5.00"), max: Decimal.new("15.00")]

      {:error, errors32} = ChDecimal32.apply_constraints(Decimal.new("20.00"), constraints)
      {:error, errors64} = ChDecimal64.apply_constraints(Decimal.new("20.00"), constraints)
      {:error, errors128} = ChDecimal128.apply_constraints(Decimal.new("20.00"), constraints)
      {:error, errors256} = ChDecimal256.apply_constraints(Decimal.new("20.00"), constraints)

      expected_error = [
        [message: "must be less than or equal to %{max}", max: Decimal.new("15.00")]
      ]

      assert errors32 == expected_error
      assert errors64 == expected_error
      assert errors128 == expected_error
      assert errors256 == expected_error
    end
  end

  describe "cast_input/2" do
    test "casts valid Decimal correctly" do
      assert ChDecimal32.cast_input(Decimal.new("123.45"), precision: 9, scale: 2) ==
               {:ok, Decimal.new("123.45")}

      assert ChDecimal64.cast_input("678.90", precision: 18, scale: 2) ==
               {:ok, Decimal.new("678.90")}

      assert ChDecimal128.cast_input(123.45, precision: 38, scale: 2) ==
               {:ok, Decimal.new("123.45")}

      assert ChDecimal256.cast_input(nil, precision: 76, scale: 2) == {:ok, nil}
    end

    test "returns error for invalid inputs" do
      assert ChDecimal32.cast_input("invalid", []) == :error
      assert ChDecimal64.cast_input("invalid", []) == :error
      assert ChDecimal128.cast_input("invalid", []) == :error
      assert ChDecimal256.cast_input("invalid", []) == :error
    end
  end

  describe "cast_stored/2" do
    test "casts stored string to Decimal" do
      assert ChDecimal32.cast_stored("123.45", precision: 9, scale: 2) ==
               {:ok, Decimal.new("123.45")}

      assert ChDecimal64.cast_stored("1234567890.12", precision: 18, scale: 2) ==
               {:ok, Decimal.new("1234567890.12")}

      assert ChDecimal128.cast_stored("12345678901234567890.12345", precision: 38, scale: 5) ==
               {:ok, Decimal.new("12345678901234567890.12345")}

      assert ChDecimal256.cast_stored(nil, []) == {:ok, nil}
    end

    test "returns error for non-string stored values" do
      assert :error = ChDecimal32.cast_stored("any", precision: 9, scale: 2)
      assert :error = ChDecimal64.cast_stored("any", precision: 18, scale: 2)
      assert :error = ChDecimal128.cast_stored("any", precision: 38, scale: 5)
      assert :error = ChDecimal256.cast_stored("any", precision: 76, scale: 8)
    end
  end
end
