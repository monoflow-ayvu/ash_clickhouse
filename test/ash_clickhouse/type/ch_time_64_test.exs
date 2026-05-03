defmodule AshClickhouse.Type.ChTime64Test do
  use ExUnit.Case, async: true
  alias AshClickhouse.Type.ChTime64

  import AshClickhouse.Test.Helper, only: [encode_ch_type: 1]

  describe "storage_type/1" do
    test "returns time_usec for microsecond precision" do
      assert ChTime64.storage_type(precision: :microsecond) == :time_usec
    end

    test "returns time_usec ignoring other constraints after microsecond" do
      assert ChTime64.storage_type([{:precision, :microsecond}, {:nullable?, true}]) == :time_usec
    end

    test "returns correct ClickHouse type for integer precision" do
      assert {:parameterized, {Ch, _}} = type = ChTime64.storage_type(precision: 3)
      assert encode_ch_type(type) == "Time64(3)"
    end

    test "returns correct ClickHouse type for precision 6" do
      assert {:parameterized, {Ch, _}} = type = ChTime64.storage_type(precision: 6)
      assert encode_ch_type(type) == "Time64(6)"
    end

    test "returns nullable type with nullable? constraint" do
      assert {:parameterized, {Ch, _}} =
               type = ChTime64.storage_type(precision: 3, nullable?: true)

      assert encode_ch_type(type) == "Nullable(Time64(3))"
    end

    test "returns correct type for array" do
      assert {:array, {:parameterized, {Ch, _}} = subtype} =
               Ash.Type.storage_type({:array, ChTime64}, items: [precision: 3])

      assert encode_ch_type({:array, subtype}) == "Array(Time64(3))"
    end
  end

  describe "matches_type?/2" do
    test "returns true for Time structs" do
      assert ChTime64.matches_type?(~T[12:00:00], [])
      assert ChTime64.matches_type?(~T[00:00:00.000000], [])
    end

    test "returns false for non-Time values" do
      refute ChTime64.matches_type?(nil, [])
      refute ChTime64.matches_type?("12:00:00", [])
      refute ChTime64.matches_type?(123, [])
      refute ChTime64.matches_type?([], [])
    end
  end

  describe "cast_input/2" do
    test "casts nil to nil" do
      assert ChTime64.cast_input(nil, precision: 6) == {:ok, nil}
    end

    test "normalizes Time with non-zero microseconds" do
      time_with_us = %Time{hour: 12, minute: 30, second: 0, microsecond: {123_456, 6}}
      {:ok, result} = ChTime64.cast_input(time_with_us, precision: 6)
      assert result.microsecond == {0, 0}
    end

    test "casts a regular Time struct" do
      time = ~T[12:30:00]
      assert {:ok, _} = ChTime64.cast_input(time, precision: 3)
    end
  end

  describe "cast_stored/2" do
    test "casts nil to nil" do
      assert ChTime64.cast_stored(nil, precision: 6) == {:ok, nil}
    end

    test "casts a binary time string" do
      assert {:ok, _} = ChTime64.cast_stored("12:30:00", precision: 3)
    end
  end

  describe "dump_to_native/2" do
    test "dumps nil to nil" do
      assert ChTime64.dump_to_native(nil, precision: 6) == {:ok, nil}
    end

    test "dumps a Time struct" do
      assert {:ok, _} = ChTime64.dump_to_native(~T[12:30:00], precision: 3)
    end
  end

  describe "generator/1" do
    test "generates Time structs" do
      results = Enum.take(ChTime64.generator(precision: 6), 10)
      assert Enum.all?(results, &match?(%Time{}, &1))
    end
  end
end
