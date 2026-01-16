defmodule AshClickhouse.Type.ChDateTest do
  use ExUnit.Case, async: true
  alias AshClickhouse.Type.ChDate

  import AshClickhouse.Test.Helper, only: [encode_ch_type: 1]

  describe "storage_types" do
    test "returns correct ClickHouse type without constraints" do
      assert {:parameterized, {Ch, :date}} = type = Ash.Type.storage_type(ChDate, [])
      assert encode_ch_type(type) == "Date"
    end

    test "returns nullable ClickHouse type with nullable constraint" do
      assert {:parameterized, {Ch, {:nullable, :date}}} =
               type = Ash.Type.storage_type(ChDate, nullable?: true)

      assert encode_ch_type(type) == "Nullable(Date)"
    end

    test "returns correct ClickHouse type without constraints for array version" do
      assert {:array, {:parameterized, {Ch, :date}} = subtype} =
               Ash.Type.storage_type({:array, ChDate}, [])

      assert encode_ch_type({:array, subtype}) == "Array(Date)"
    end

    test "returns nullable ClickHouse type with nullable constraint for array version" do
      assert {:array, {:parameterized, {Ch, {:nullable, :date}}} = subtype} =
               Ash.Type.storage_type({:array, ChDate}, nullable?: true)

      assert encode_ch_type({:array, subtype}) == "Array(Nullable(Date))"
    end
  end

  describe "matches_type?/2" do
    test "returns true for Date structs" do
      assert Ash.Type.matches_type?(ChDate, ~D[2023-01-01], [])
      assert Ash.Type.matches_type?(ChDate, "2023-01-01", [])
    end

    test "returns false for non-Date values" do
      refute Ash.Type.matches_type?(ChDate, nil, [])
      refute Ash.Type.matches_type?(ChDate, 123, [])
      refute Ash.Type.matches_type?(ChDate, [], [])
    end

    test "returns true for arrays of Date structs" do
      assert Ash.Type.matches_type?({:array, ChDate}, [~D[2023-01-01], ~D[2023-01-02]], [])
      assert Ash.Type.matches_type?({:array, ChDate}, ["2023-01-01", "2023-01-02"], [])
    end

    test "returns false for non-Date arrays" do
      refute Ash.Type.matches_type?({:array, ChDate}, [123, 456], [])
      refute Ash.Type.matches_type?({:array, ChDate}, [[], {}], [])
    end
  end

  describe "generator" do
    test "generates Date structs" do
      generated_dates = Enum.take(Ash.Type.generator(ChDate, []), 100) |> Enum.uniq()
      assert Enum.all?(generated_dates, fn date -> is_struct(date, Date) end)
    end

    test "generates arrays of Date structs" do
      generated_arrays = Enum.take(Ash.Type.generator({:array, ChDate}, []), 100)

      assert Enum.all?(generated_arrays, fn arr ->
               is_list(arr) and Enum.all?(arr, fn date -> is_struct(date, Date) end)
             end)
    end
  end

  describe "cast_input/2" do
    test "casts Date structs correctly" do
      date = ~D[2023-01-01]
      assert Ash.Type.cast_input(ChDate, date, []) == {:ok, date}
      assert Ash.Type.cast_input(ChDate, nil, []) == {:ok, nil}
    end

    test "returns error for non-Date inputs" do
      assert {:error, "is invalid"} = Ash.Type.cast_input(ChDate, 123, [])
    end

    test "casts valid array of Date structs correctly" do
      date1 = ~D[2023-01-01]
      date2 = ~D[2023-01-02]
      assert Ash.Type.cast_input({:array, ChDate}, [date1, date2], []) == {:ok, [date1, date2]}
      assert Ash.Type.cast_input({:array, ChDate}, [nil, date1], []) == {:ok, [nil, date1]}
    end

    test "returns error for non-Date array inputs" do
      assert {:error, [[message: "is invalid", index: 0, path: [0]]]} =
               Ash.Type.cast_input({:array, ChDate}, [123, ~D[2023-01-01]], [])
    end
  end

  describe "equal?/2" do
    test "returns true for equal Date structs" do
      date = ~D[2023-01-01]
      assert Ash.Type.equal?(ChDate, date, date)
      assert Ash.Type.equal?(ChDate, nil, nil)
    end

    test "returns false for different Date structs" do
      refute Ash.Type.equal?(ChDate, ~D[2023-01-01], ~D[2023-01-02])
      refute Ash.Type.equal?(ChDate, ~D[2023-01-01], nil)
      refute Ash.Type.equal?(ChDate, nil, ~D[2023-01-01])
    end

    test "returns true for equal arrays of Date structs" do
      date1 = ~D[2023-01-01]
      date2 = ~D[2023-01-02]
      assert Ash.Type.equal?({:array, ChDate}, [date1, date2], [date1, date2])
      assert Ash.Type.equal?({:array, ChDate}, [], [])
    end

    test "returns false for different arrays of Date structs" do
      date1 = ~D[2023-01-01]
      date2 = ~D[2023-01-02]
      refute Ash.Type.equal?({:array, ChDate}, [date1, date2], [date2, date1])
      refute Ash.Type.equal?({:array, ChDate}, [date1], [date1, date2])
      refute Ash.Type.equal?({:array, ChDate}, [date1, nil], [date1, date2])
    end
  end

  describe "simple_equality?/0" do
    test "returns true for ChDate" do
      assert Ash.Type.simple_equality?(ChDate)
    end

    test "returns true for {:array, ChDate}" do
      assert Ash.Type.simple_equality?({:array, ChDate})
    end
  end
end
