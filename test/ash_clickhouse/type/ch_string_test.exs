defmodule AshClickhouse.Type.ChStringTest do
  use ExUnit.Case, async: true

  alias AshClickhouse.Type.ChString

  import AshClickhouse.Test.Helper, only: [encode_ch_type: 1]

  require Ash.Expr

  describe "storage_types" do
    test "returns correct ClickHouse type without constraints" do
      assert {:parameterized, {Ch, :string}} = type = Ash.Type.storage_type(ChString, [])
      assert encode_ch_type(type) == "String"
    end

    test "returns nullable ClickHouse type with nullable constraint" do
      assert {:parameterized, {Ch, {:nullable, :string}}} =
               type = Ash.Type.storage_type(ChString, nullable?: true)

      assert encode_ch_type(type) == "Nullable(String)"
    end

    test "returns low cardinality ClickHouse type with low_cardinality constraint" do
      assert {:parameterized, {Ch, {:low_cardinality, :string}}} =
               type = Ash.Type.storage_type(ChString, low_cardinality?: true)

      assert encode_ch_type(type) == "LowCardinality(String)"
    end

    test "returns low cardinality nullable ClickHouse type with both constraints" do
      assert {:parameterized, {Ch, {:low_cardinality, {:nullable, :string}}}} =
               type = Ash.Type.storage_type(ChString, low_cardinality?: true, nullable?: true)

      assert encode_ch_type(type) == "LowCardinality(Nullable(String))"
    end
  end

  describe "matches_type?/2" do
    test "returns true for strings" do
      assert Ash.Type.matches_type?(ChString, "string", [])
    end

    test "returns false for non-strings" do
      refute Ash.Type.matches_type?(ChString, 123, [])
    end
  end

  describe "generator/1" do
    test "generates strings" do
      assert ChString
             |> Ash.Type.generator([])
             |> Enum.take(100)
             |> Enum.uniq()
             |> Enum.all?(&is_binary/1)
    end

    test "generate string with min_length constraint" do
      assert ChString
             |> Ash.Type.generator(min_length: 10)
             |> Enum.take(100)
             |> Enum.uniq()
             |> Enum.all?(&(String.length(&1) >= 10))
    end

    test "generate string with trim and min_length constraint" do
      assert ChString
             |> Ash.Type.generator(trim?: true, min_length: 10)
             |> Enum.take(100)
             |> Enum.uniq()
             |> Enum.all?(&(String.length(&1) >= 10))
    end
  end

  describe "cast_input/2" do
    test "casts input to string" do
      assert {:ok, "string"} = Ash.Type.cast_input(ChString, "string", [])
    end

    test "casts nil to nil" do
      assert {:ok, nil} = Ash.Type.cast_input(ChString, nil, [])
    end

    test "cast array of strings" do
      assert {:ok, ["string", "string2"]} =
               Ash.Type.cast_input({:array, ChString}, ["string", "string2"], [])
    end

    test "returns error for non-string inputs" do
      assert {:error, "is invalid"} = Ash.Type.cast_input(ChString, 123, [])
      assert {:error, "is invalid"} = Ash.Type.cast_input(ChString, [], [])
    end

    test "returns error for array of non-string inputs" do
      assert {:error, [[message: "is invalid", index: 0, path: [0]]]} =
               Ash.Type.cast_input({:array, ChString}, [123, "string"], [])
    end
  end

  describe "coerce/2" do
    test "coerces string to string" do
      assert {:ok, "string"} = Ash.Type.coerce(ChString, "string", [])
    end

    test "coerces integer to string" do
      assert {:ok, "123"} = Ash.Type.coerce(ChString, 123, [])
    end

    test "coerces float to string" do
      assert {:ok, "123.45"} = Ash.Type.coerce(ChString, 123.45, [])
    end

    test "coerces nil to nil" do
      assert {:ok, nil} = Ash.Type.coerce(ChString, nil, [])
    end
  end

  describe "equal?/2" do
    test "returns true for equal strings" do
      assert Ash.Type.equal?(ChString, "string", "string")
      assert Ash.Type.equal?(ChString, nil, nil)
    end

    test "returns false for different strings" do
      refute Ash.Type.equal?(ChString, "string", "string2")
      refute Ash.Type.equal?(ChString, "string", nil)
      refute Ash.Type.equal?(ChString, nil, "string")
    end
  end

  describe "dump_to_native/2" do
    test "dumps nil to nil" do
      assert {:ok, nil} = Ash.Type.dump_to_native(ChString, nil, [])
    end

    test "dumps string to string" do
      assert {:ok, "string"} = Ash.Type.dump_to_native(ChString, "string", [])
    end
  end

  describe "dump_to_native/2 for arrays" do
    test "dumps array of strings to array of strings" do
      assert {:ok, ["string", "string2"]} =
               Ash.Type.dump_to_native({:array, ChString}, ["string", "string2"], [])
    end
  end

  describe "cast_atomic/2" do
    test "casts atomic expression to string with match constraint" do
      assert {:not_atomic,
              "cannot use the `match` string constraint atomically with an expression"} =
               Ash.Type.cast_atomic(ChString, Ash.Expr.expr(field == ^"test"),
                 match: ~r/^[a-z]+$/
               )
    end

    test "casts atomic expression to string with trim? constraint" do
      assert {:atomic, expr} =
               Ash.Type.cast_atomic(ChString, Ash.Expr.expr(field == ^"test"), trim?: true)

      assert Ash.Expr.expr?(expr)
    end

    test "casts atomic expression to string with allow_empty? constraint" do
      assert {:atomic, expr} =
               Ash.Type.cast_atomic(ChString, Ash.Expr.expr(field == ^"test"), allow_empty?: true)

      assert Ash.Expr.expr?(expr)
    end

    test "casts atomic expression to string without constraints" do
      assert {:atomic, expr} =
               Ash.Type.cast_atomic(ChString, Ash.Expr.expr(field == ^"test"), [])

      assert Ash.Expr.expr?(expr)
    end
  end

  describe "apply_atomic_constraints/2" do
    test "applies atomic constraints to expression" do
      assert {:ok, expr} =
               Ash.Type.apply_atomic_constraints(ChString, Ash.Expr.expr(field == ^"test"), [])

      assert Ash.Expr.expr?(expr)
    end

    test "applies atomic constraints to expression with match constraint" do
      assert {:ok, expr} =
               Ash.Type.apply_atomic_constraints(ChString, Ash.Expr.expr(field == ^"test"),
                 match: ~r/^[a-z]+$/
               )

      assert Ash.Expr.expr?(expr)
    end

    test "applies atomic constraints to expression with trim? constraint" do
      assert {:ok, expr} =
               Ash.Type.apply_atomic_constraints(ChString, Ash.Expr.expr(field == ^" test "),
                 trim?: true
               )

      assert Ash.Expr.expr?(expr)
    end

    test "applies atomic constraints when value is empty string and trim? is false and allow_empty? is false" do
      assert {:ok, nil} =
               Ash.Type.apply_atomic_constraints(ChString, "  ",
                 trim?: false,
                 allow_empty?: false
               )
    end

    test "applies atomic constraints to expression with allow_empty? constraint" do
      assert {:ok, expr} =
               Ash.Type.apply_atomic_constraints(ChString, Ash.Expr.expr(field == ^"test"),
                 allow_empty?: true
               )

      assert Ash.Expr.expr?(expr)
    end

    test "applies atomic constraints to expression with max_length constraint" do
      assert {:ok, expr} =
               Ash.Type.apply_atomic_constraints(ChString, Ash.Expr.expr(field == ^"test"),
                 max_length: 10
               )

      assert Ash.Expr.expr?(expr)
    end

    test "applies atomic constraints to expression with max_length and min_length constraints" do
      assert {:ok, expr} =
               Ash.Type.apply_atomic_constraints(ChString, Ash.Expr.expr(field == ^"test"),
                 max_length: 10,
                 min_length: 10
               )

      assert Ash.Expr.expr?(expr)
    end

    test "applies atomic constraints to expression with max_length and nil min_length constraints" do
      assert {:ok, expr} =
               Ash.Type.apply_atomic_constraints(ChString, Ash.Expr.expr(field == ^"test"),
                 max_length: nil,
                 min_length: 10
               )

      assert Ash.Expr.expr?(expr)
    end

    test "apply atomic constraints when expression is not an expr" do
      assert {:ok, "test"} = Ash.Type.apply_atomic_constraints(ChString, "test", [])
    end

    test "returns :ok for nil value" do
      assert :ok = ChString.apply_constraints(nil, [])
    end

    test "returns {ok, value} when value passes all constraints (default: trim and empty = nil)" do
      assert {:ok, "abc"} = ChString.apply_constraints("  abc  ", [])
      assert {:ok, nil} = ChString.apply_constraints("   ", [])
    end

    test "returns trimmed value if trim? true, allow_empty? true" do
      assert {:ok, "abc"} = ChString.apply_constraints("  abc  ", trim?: true, allow_empty?: true)
      assert {:ok, ""} = ChString.apply_constraints("    ", trim?: true, allow_empty?: true)
    end

    test "returns value as is if trim? false, allow_empty? true" do
      assert {:ok, "  abc  "} =
               ChString.apply_constraints("  abc  ", trim?: false, allow_empty?: true)

      assert {:ok, "    "} = ChString.apply_constraints("    ", trim?: false, allow_empty?: true)
    end

    test "returns nil if value is empty after trimming and allow_empty? is false" do
      assert {:ok, nil} = ChString.apply_constraints("    ", trim?: true, allow_empty?: false)
    end

    test "returns nil if value is empty (no trim) and allow_empty? is false" do
      assert {:ok, nil} = ChString.apply_constraints("    ", trim?: false, allow_empty?: false)
    end

    test "returns error for string longer than max_length" do
      assert  {:error, [{:message, "length must be less than or equal to %{max}"}, {:max, 3}]} = ChString.apply_constraints("abcd", max_length: 3)
    end

    test "returns error for string shorter than min_length" do
      assert {:error, [{:message, "length must be greater than or equal to %{min}"}, {:min, 5}]} = ChString.apply_constraints("abcd", min_length: 5)
    end

    test "returns error for value not matching :match regex" do
      assert  {:error, [{:message, "must match the pattern %{regex}"}, {:regex, "~r/^abc/"}]} =
               ChString.apply_constraints("testvalue", match: ~r/^abc/)
    end

    test "returns error for value not matching :match MFA regex" do
      mfa = {Regex, :compile!, ["^abc"]}

      assert {:error, [{:message, "must match the pattern %{regex}"}, {:regex, "~r/^abc/"}]} =
               ChString.apply_constraints("test", match: mfa)
    end

    test "returns multiple errors if multiple constraints fail" do
      assert {:error, errors} =
               ChString.apply_constraints(
                 "x",
                 min_length: 2,
                 match: ~r/^abc/
               )

      assert length(errors) == 2
      assert Enum.any?(errors, fn err -> Keyword.has_key?(err, :min) end)
      assert Enum.any?(errors, fn err -> Keyword.has_key?(err, :regex) end)
    end

    test "returns error for multiple errors with trim? false and allow_empty? true" do
      assert {:error, errors} =
               ChString.apply_constraints(
                 "a",
                 trim?: false,
                 allow_empty?: true,
                 min_length: 4,
                 match: ~r/^b/
               )

      assert length(errors) == 2
    end
  end
end
