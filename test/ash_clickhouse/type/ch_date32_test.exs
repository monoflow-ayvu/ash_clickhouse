defmodule AshClickhouse.Type.ChDate32Test do
  use ExUnit.Case, async: true
  alias AshClickhouse.Type.ChDate32

  import AshClickhouse.Test.Helper, only: [encode_ch_type: 1]

  describe "storage_types" do
    test "returns correct ClickHouse type without constraints" do
      assert {:parameterized, {Ch, :date32}} = type = Ash.Type.storage_type(ChDate32, [])
      assert encode_ch_type(type) == "Date32"
    end

    test "returns nullable ClickHouse type with nullable constraint" do
      assert {:parameterized, {Ch, {:nullable, :date32}}} =
               type = Ash.Type.storage_type(ChDate32, nullable?: true)

      assert encode_ch_type(type) == "Nullable(Date32)"
    end

    test "returns correct ClickHouse type without constraints for array version" do
      assert {:array, {:parameterized, {Ch, :date32}} = subtype} =
               Ash.Type.storage_type({:array, ChDate32}, [])

      assert encode_ch_type({:array, subtype}) == "Array(Date32)"
    end

    test "returns nullable ClickHouse type with nullable constraint for array version" do
      assert {:array, {:parameterized, {Ch, {:nullable, :date32}}} = subtype} =
               Ash.Type.storage_type({:array, ChDate32}, nullable?: true)

      assert encode_ch_type({:array, subtype}) == "Array(Nullable(Date32))"
    end
  end

  describe "matches_type?/2" do
    test "returns true for Date structs" do
      assert Ash.Type.matches_type?(ChDate32, ~D[2023-01-01], [])
      assert Ash.Type.matches_type?(ChDate32, "2023-01-01", [])
      assert Ash.Type.matches_type?(ChDate32, nil, [])
    end

    test "returns false for non-Date values" do
      refute Ash.Type.matches_type?(ChDate32, 123, [])
      refute Ash.Type.matches_type?(ChDate32, [], [])
    end

    test "returns true for arrays of Date structs" do
      assert Ash.Type.matches_type?({:array, ChDate32}, [~D[2023-01-01], ~D[2023-01-02]], [])
      assert Ash.Type.matches_type?({:array, ChDate32}, ["2023-01-01", "2023-01-02"], [])
      assert Ash.Type.matches_type?({:array, ChDate32}, [nil, ~D[2023-01-01]], [])
    end

    test "returns false for non-Date arrays" do
      refute Ash.Type.matches_type?({:array, ChDate32}, [123, 456], [])
      refute Ash.Type.matches_type?({:array, ChDate32}, [[], {}], [])
    end
  end

  describe "generator" do
    test "generates Date structs" do
      generated_dates = Enum.take(Ash.Type.generator(ChDate32, []), 100) |> Enum.uniq()
      assert Enum.all?(generated_dates, fn date -> is_struct(date, Date) end)
    end

    test "generates arrays of Date structs" do
      generated_arrays = Enum.take(Ash.Type.generator({:array, ChDate32}, []), 100)

      assert Enum.all?(generated_arrays, fn arr ->
               is_list(arr) and Enum.all?(arr, fn date -> is_struct(date, Date) end)
             end)
    end
  end

  describe "cast_input/2" do
    test "casts Date structs correctly" do
      date = ~D[2023-01-01]
      assert Ash.Type.cast_input(ChDate32, date, []) == {:ok, date}
      assert Ash.Type.cast_input(ChDate32, nil, []) == {:ok, nil}
    end

    test "returns error for non-Date inputs" do
      assert {:error, "is invalid"} = Ash.Type.cast_input(ChDate32, 123, [])
    end

    test "casts valid array of Date structs correctly" do
      date1 = ~D[2023-01-01]
      date2 = ~D[2023-01-02]
      assert Ash.Type.cast_input({:array, ChDate32}, [date1, date2], []) == {:ok, [date1, date2]}
      assert Ash.Type.cast_input({:array, ChDate32}, [nil, date1], []) == {:ok, [nil, date1]}
    end

    test "returns error for non-Date array inputs" do
      assert {:error, [[message: "is invalid", index: 0, path: [0]]]} =
               Ash.Type.cast_input({:array, ChDate32}, [123, ~D[2023-01-01]], [])
    end
  end

  describe "cast_stored/2" do
    test "loads Date32 values correctly" do
      assert Ash.Type.cast_stored(ChDate32, "2023-01-01", []) == {:ok, ~D[2023-01-01]}
      assert Ash.Type.cast_stored(ChDate32, nil, []) == {:ok, nil}
    end

    test "returns error for non-Date32 stored values" do
      assert :error = Ash.Type.cast_stored(ChDate32, "invalid-date", [])
    end

    test "loads arrays of Date32 values correctly" do
      assert Ash.Type.cast_stored({:array, ChDate32}, ["2023-01-01", "2023-01-02"], []) ==
               {:ok, [~D[2023-01-01], ~D[2023-01-02]]}

      assert Ash.Type.cast_stored({:array, ChDate32}, [nil, "2023-01-01"], []) ==
               {:ok, [nil, ~D[2023-01-01]]}
    end

    test "returns error for non-Date32 array stored values" do
      assert {:error, [{:index, 0}]} =
               Ash.Type.cast_stored({:array, ChDate32}, ["invalid-date", "2023-01-01"], [])
    end
  end

  describe "equal?/2" do
    test "returns true for equal Date structs" do
      date = ~D[2023-01-01]
      assert Ash.Type.equal?(ChDate32, date, date)
      assert Ash.Type.equal?(ChDate32, nil, nil)
    end

    test "returns false for different Date structs" do
      refute Ash.Type.equal?(ChDate32, ~D[2023-01-01], ~D[2023-01-02])
      refute Ash.Type.equal?(ChDate32, ~D[2023-01-01], nil)
      refute Ash.Type.equal?(ChDate32, nil, ~D[2023-01-01])
    end

    test "returns true for equal arrays of Date structs" do
      date1 = ~D[2023-01-01]
      date2 = ~D[2023-01-02]
      assert Ash.Type.equal?({:array, ChDate32}, [date1, date2], [date1, date2])
      assert Ash.Type.equal?({:array, ChDate32}, [], [])
    end

    test "returns false for different arrays of Date structs" do
      date1 = ~D[2023-01-01]
      date2 = ~D[2023-01-02]
      refute Ash.Type.equal?({:array, ChDate32}, [date1, date2], [date2, date1])
      refute Ash.Type.equal?({:array, ChDate32}, [date1], [date1, date2])
      refute Ash.Type.equal?({:array, ChDate32}, [date1, nil], [date1, date2])
    end
  end

  describe "simple_equality?/0" do
    test "returns true for ChDate32" do
      assert Ash.Type.simple_equality?(ChDate32)
    end

    test "returns true for {:array, ChDate32}" do
      assert Ash.Type.simple_equality?({:array, ChDate32})
    end
  end
end
