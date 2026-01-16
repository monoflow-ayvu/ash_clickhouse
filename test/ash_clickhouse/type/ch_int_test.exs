defmodule AshClickhouse.Type.ChIntTest do
  use ExUnit.Case, async: true
  alias AshClickhouse.Type.ChInt8
  alias AshClickhouse.Type.ChInt16
  alias AshClickhouse.Type.ChInt32
  alias AshClickhouse.Type.ChInt64
  alias AshClickhouse.Type.ChInt128
  alias AshClickhouse.Type.ChInt256

  import AshClickhouse.Test.Helper, only: [encode_ch_type: 1]

  describe "storage_types" do
    test "returns correct ClickHouse type without constraints" do
      assert {:parameterized, {Ch, :i8}} = type = Ash.Type.storage_type(ChInt8, [])
      assert encode_ch_type(type) == "Int8"

      assert {:parameterized, {Ch, :i16}} = type = Ash.Type.storage_type(ChInt16, [])
      assert encode_ch_type(type) == "Int16"

      assert {:parameterized, {Ch, :i32}} = type = Ash.Type.storage_type(ChInt32, [])
      assert encode_ch_type(type) == "Int32"

      assert {:parameterized, {Ch, :i64}} = type = Ash.Type.storage_type(ChInt64, [])
      assert encode_ch_type(type) == "Int64"

      assert {:parameterized, {Ch, :i128}} = type = Ash.Type.storage_type(ChInt128, [])
      assert encode_ch_type(type) == "Int128"

      assert {:parameterized, {Ch, :i256}} = type = Ash.Type.storage_type(ChInt256, [])
      assert encode_ch_type(type) == "Int256"
    end

    test "returns correct ClickHouse type without constraints for array version" do
      assert {:array, {:parameterized, {Ch, :i8}} = subtype} =
               Ash.Type.storage_type({:array, ChInt8}, [])

      assert encode_ch_type({:array, subtype}) == "Array(Int8)"

      assert {:array, {:parameterized, {Ch, :i16}} = subtype} =
               Ash.Type.storage_type({:array, ChInt16}, [])

      assert encode_ch_type({:array, subtype}) == "Array(Int16)"

      assert {:array, {:parameterized, {Ch, :i32}} = subtype} =
               Ash.Type.storage_type({:array, ChInt32}, [])

      assert encode_ch_type({:array, subtype}) == "Array(Int32)"

      assert {:array, {:parameterized, {Ch, :i64}} = subtype} =
               Ash.Type.storage_type({:array, ChInt64}, [])

      assert encode_ch_type({:array, subtype}) == "Array(Int64)"

      assert {:array, {:parameterized, {Ch, :i128}} = subtype} =
               Ash.Type.storage_type({:array, ChInt128}, [])

      assert encode_ch_type({:array, subtype}) == "Array(Int128)"

      assert {:array, {:parameterized, {Ch, :i256}} = subtype} =
               Ash.Type.storage_type({:array, ChInt256}, [])

      assert encode_ch_type({:array, subtype}) == "Array(Int256)"
    end

    test "returns nullable ClickHouse type with nullable constraint" do
      assert {:parameterized, {Ch, {:nullable, :i8}}} =
               type =
               Ash.Type.storage_type(ChInt8, nullable?: true)

      assert encode_ch_type(type) == "Nullable(Int8)"

      assert {:parameterized, {Ch, {:nullable, :i16}}} =
               type =
               Ash.Type.storage_type(ChInt16, nullable?: true)

      assert encode_ch_type(type) == "Nullable(Int16)"

      assert {:parameterized, {Ch, {:nullable, :i32}}} =
               type =
               Ash.Type.storage_type(ChInt32, nullable?: true)

      assert encode_ch_type(type) == "Nullable(Int32)"

      assert {:parameterized, {Ch, {:nullable, :i64}}} =
               type =
               Ash.Type.storage_type(ChInt64, nullable?: true)

      assert encode_ch_type(type) == "Nullable(Int64)"

      assert {:parameterized, {Ch, {:nullable, :i128}}} =
               type =
               Ash.Type.storage_type(ChInt128, nullable?: true)

      assert encode_ch_type(type) == "Nullable(Int128)"

      assert {:parameterized, {Ch, {:nullable, :i256}}} =
               type =
               Ash.Type.storage_type(ChInt256, nullable?: true)

      assert encode_ch_type(type) == "Nullable(Int256)"
    end

    test "returns nullable ClickHouse type with nullable constraint for array version" do
      assert {:array, {:parameterized, {Ch, {:nullable, :i8}}} = subtype} =
               Ash.Type.storage_type({:array, ChInt8}, nullable?: true)

      assert encode_ch_type({:array, subtype}) == "Array(Nullable(Int8))"

      assert {:array, {:parameterized, {Ch, {:nullable, :i16}}} = subtype} =
               Ash.Type.storage_type({:array, ChInt16}, nullable?: true)

      assert encode_ch_type({:array, subtype}) == "Array(Nullable(Int16))"

      assert {:array, {:parameterized, {Ch, {:nullable, :i32}}} = subtype} =
               Ash.Type.storage_type({:array, ChInt32}, nullable?: true)

      assert encode_ch_type({:array, subtype}) == "Array(Nullable(Int32))"

      assert {:array, {:parameterized, {Ch, {:nullable, :i64}}} = subtype} =
               Ash.Type.storage_type({:array, ChInt64}, nullable?: true)

      assert encode_ch_type({:array, subtype}) == "Array(Nullable(Int64))"

      assert {:array, {:parameterized, {Ch, {:nullable, :i128}}} = subtype} =
               Ash.Type.storage_type({:array, ChInt128}, nullable?: true)

      assert encode_ch_type({:array, subtype}) == "Array(Nullable(Int128))"

      assert {:array, {:parameterized, {Ch, {:nullable, :i256}}} = subtype} =
               Ash.Type.storage_type({:array, ChInt256}, nullable?: true)

      assert encode_ch_type({:array, subtype}) == "Array(Nullable(Int256))"
    end
  end

  describe "matches_type?/2" do
    test "returns true for integers" do
      assert Ash.Type.matches_type?(ChInt8, 123, [])
      assert Ash.Type.matches_type?(ChInt16, 456, [])
      assert Ash.Type.matches_type?(ChInt32, 789, [])
      assert Ash.Type.matches_type?(ChInt64, 101_112, [])
      assert Ash.Type.matches_type?(ChInt128, 131_415, [])
      assert Ash.Type.matches_type?(ChInt256, 161_718, [])
    end

    test "returns false for non-integers" do
      refute Ash.Type.matches_type?(ChInt8, 3.14, [])
      refute Ash.Type.matches_type?(ChInt16, "string", [])
      refute Ash.Type.matches_type?(ChInt32, nil, [])
      refute Ash.Type.matches_type?(ChInt64, [], [])
      refute Ash.Type.matches_type?(ChInt128, {}, [])
      refute Ash.Type.matches_type?(ChInt256, :atom, [])
    end

    test "returns true for arrays of integers" do
      assert Ash.Type.matches_type?({:array, ChInt8}, [123, 456], [])
      assert Ash.Type.matches_type?({:array, ChInt16}, [789, -101], [])
      assert Ash.Type.matches_type?({:array, ChInt32}, [123, 456], [])
    end

    test "returns false for non-integer arrays" do
      refute Ash.Type.matches_type?({:array, ChInt8}, [3.14, 2.71], [])
      refute Ash.Type.matches_type?({:array, ChInt16}, ["string", "test"], [])
      refute Ash.Type.matches_type?({:array, ChInt32}, [[], {}], [])
    end
  end

  describe "generator" do
    test "generates integers within min and max" do
      [
        [min: -100, max: 100],
        [min: 0, max: 50],
        [min: -50, max: 0]
      ]
      |> Enum.each(fn constraints ->
        Enum.each(
          [ChInt8, ChInt16, ChInt32, ChInt64, ChInt128, ChInt256],
          fn module ->
            generated_integers = Enum.take(Ash.Type.generator(module, constraints), 100)

            assert Enum.all?(generated_integers, fn value ->
                     value >= constraints[:min] and value <= constraints[:max]
                   end)
          end
        )
      end)
    end

    test "generates arrays of integers within min and max" do
      [
        [min: -100, max: 100],
        [min: 0, max: 50],
        [min: -50, max: 0]
      ]
      |> Enum.each(fn constraints ->
        Enum.each(
          [ChInt8, ChInt16, ChInt32, ChInt64, ChInt128, ChInt256],
          fn module ->
            generated_arrays =
              Enum.take(Ash.Type.generator({:array, module}, items: constraints), 100)

            assert Enum.all?(generated_arrays, fn arr ->
                     is_list(arr) and
                       Enum.all?(arr, fn value ->
                         value >= constraints[:min] and value <= constraints[:max]
                       end)
                   end)
          end
        )
      end)
    end
  end

  describe "apply_constraints/2" do
    test "returns ok for valid integer within constraints" do
      constraints = [min: 10, max: 100]

      Enum.each(
        [ChInt8, ChInt16, ChInt32, ChInt64, ChInt128, ChInt256],
        fn module ->
          assert Ash.Type.apply_constraints(module, 50, constraints) == {:ok, 50}
        end
      )
    end

    test "returns error for integer below min constraint" do
      constraints = [min: 10]

      Enum.each(
        [ChInt8, ChInt16, ChInt32, ChInt64, ChInt128, ChInt256],
        fn module ->
          {:error, errors} = Ash.Type.apply_constraints(module, 5, constraints)
          assert length(errors) == 1

          [error] = errors
          assert error[:message] =~ "must be more than or equal to %{min}"
          assert error[:min] == 10
        end
      )
    end

    test "returns error for integer above max constraint" do
      constraints = [max: 100]

      Enum.each(
        [ChInt8, ChInt16, ChInt32, ChInt64, ChInt128, ChInt256],
        fn module ->
          {:error, errors} = Ash.Type.apply_constraints(module, 150, constraints)
          assert length(errors) == 1

          [error] = errors
          assert error[:message] =~ "must be less than or equal to %{max}"
          assert error[:max] == 100
        end
      )
    end

    test "returns ok for nil value" do
      Enum.each(
        [ChInt8, ChInt16, ChInt32, ChInt64, ChInt128, ChInt256],
        fn module ->
          assert Ash.Type.apply_constraints(module, nil, min: 10, max: 100) == {:ok, nil}
        end
      )
    end

    test "returns ok for valid array of integers within constraints" do
      constraints = [min: 10, max: 100]

      Enum.each(
        [ChInt8, ChInt16, ChInt32, ChInt64, ChInt128, ChInt256],
        fn module ->
          assert Ash.Type.apply_constraints({:array, module}, [50, 75], items: constraints) ==
                   {:ok, [50, 75]}
        end
      )
    end

    test "returns error for array with integer below min constraint" do
      constraints = [min: 10]

      Enum.each(
        [ChInt8, ChInt16, ChInt32, ChInt64, ChInt128, ChInt256],
        fn module ->
          {:error, errors} =
            Ash.Type.apply_constraints({:array, module}, [50, 5], items: constraints)

          assert length(errors) == 1

          [error] = errors
          assert error[:message] =~ "must be more than or equal to %{min}"
          assert error[:min] == 10
        end
      )
    end

    test "returns error for array with integer above max constraint" do
      constraints = [max: 100]

      Enum.each(
        [ChInt8, ChInt16, ChInt32, ChInt64, ChInt128, ChInt256],
        fn module ->
          {:error, errors} =
            Ash.Type.apply_constraints({:array, module}, [50, 150], items: constraints)

          assert length(errors) == 1

          [error] = errors
          assert error[:message] =~ "must be less than or equal to %{max}"
          assert error[:max] == 100
        end
      )
    end
  end

  describe "cast_input/2" do
    test "casts valid integer correctly" do
      Enum.each(
        [ChInt8, ChInt16, ChInt32, ChInt64, ChInt128, ChInt256],
        fn module ->
          assert Ash.Type.cast_input(module, 123, []) == {:ok, 123}
          assert Ash.Type.cast_input(module, -456, []) == {:ok, -456}
          assert Ash.Type.cast_input(module, nil, []) == {:ok, nil}
        end
      )
    end

    test "returns error for non-integer inputs" do
      Enum.each(
        [ChInt8, ChInt16, ChInt32, ChInt64, ChInt128, ChInt256],
        fn module ->
          assert {:error, "is invalid"} = Ash.Type.cast_input(module, 3.14, [])
          assert {:error, "is invalid"} = Ash.Type.cast_input(module, "string", [])
          assert {:error, "is invalid"} = Ash.Type.cast_input(module, [], [])
        end
      )
    end

    test "casts valid array of integers correctly" do
      Enum.each(
        [ChInt8, ChInt16, ChInt32, ChInt64, ChInt128, ChInt256],
        fn module ->
          assert Ash.Type.cast_input({:array, module}, [123, -456], []) == {:ok, [123, -456]}
          assert Ash.Type.cast_input({:array, module}, [nil, 789], []) == {:ok, [nil, 789]}
        end
      )
    end

    test "returns error for non-integer array inputs" do
      Enum.each(
        [ChInt8, ChInt16, ChInt32, ChInt64, ChInt128, ChInt256],
        fn module ->
          assert {:error, [[message: "is invalid", index: 0, path: [0]]]} =
                   Ash.Type.cast_input({:array, module}, [3.14, 123], [])

          assert {:error, [[message: "is invalid", index: 1, path: [1]]]} =
                   Ash.Type.cast_input({:array, module}, [456, "string"], [])

          assert {:error, [[message: "is invalid", index: 0, path: [0]]]} =
                   Ash.Type.cast_input({:array, module}, [[], 789], [])
        end
      )
    end
  end

  describe "equal?/2" do
    test "returns true for equal integers" do
      Enum.each(
        [ChInt8, ChInt16, ChInt32, ChInt64, ChInt128, ChInt256],
        fn module ->
          assert Ash.Type.equal?(module, 123, 123)
          assert Ash.Type.equal?(module, nil, nil)
        end
      )
    end

    test "returns false for different integers" do
      Enum.each(
        [ChInt8, ChInt16, ChInt32, ChInt64, ChInt128, ChInt256],
        fn module ->
          refute Ash.Type.equal?(module, 123, 456)
          refute Ash.Type.equal?(module, 123, nil)
          refute Ash.Type.equal?(module, nil, 456)
        end
      )
    end

    test "returns true for equal arrays of integers" do
      Enum.each(
        [ChInt8, ChInt16, ChInt32, ChInt64, ChInt128, ChInt256],
        fn module ->
          assert Ash.Type.equal?({:array, module}, [123, 456], [123, 456])
          assert Ash.Type.equal?({:array, module}, [], [])
        end
      )
    end

    test "returns false for different arrays of integers" do
      Enum.each(
        [ChInt8, ChInt16, ChInt32, ChInt64, ChInt128, ChInt256],
        fn module ->
          refute Ash.Type.equal?({:array, module}, [123, 456], [456, 123])
          refute Ash.Type.equal?({:array, module}, [123], [123, 456])
          refute Ash.Type.equal?({:array, module}, [123, nil], [123, 456])
        end
      )
    end
  end

  describe "simple_equality?/0" do
    test "returns true for integer types" do
      Enum.each(
        [ChInt8, ChInt16, ChInt32, ChInt64, ChInt128, ChInt256],
        fn module ->
          assert Ash.Type.simple_equality?(module)
        end
      )
    end

    test "returns true for array versions of integer types" do
      Enum.each(
        [ChInt8, ChInt16, ChInt32, ChInt64, ChInt128, ChInt256],
        fn module ->
          assert Ash.Type.simple_equality?({:array, module})
        end
      )
    end
  end
end
