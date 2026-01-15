defmodule AshClickhouse.Type.ChDateTest do
  use ExUnit.Case, async: true
  alias AshClickhouse.Type.ChDate

  import AshClickhouse.Test.Helper, only: [encode_ch_type: 1]

  describe "storage_types" do
    test "returns correct ClickHouse type without constraints" do
      assert {:parameterized, {Ch, :date}} = type = ChDate.storage_type([])
      assert encode_ch_type(type) == "Date"
    end

    test "returns nullable ClickHouse type with nullable constraint" do
      assert {:parameterized, {Ch, {:nullable, :date}}} =
               type = ChDate.storage_type(nullable?: true)

      assert encode_ch_type(type) == "Nullable(Date)"
    end
  end

  describe "matches_type?/2" do
    test "returns true for Date structs" do
      assert ChDate.matches_type?(~D[2023-01-01], [])
      assert ChDate.matches_type?("2023-01-01", [])
    end

    test "returns false for non-Date values" do
      refute ChDate.matches_type?(nil, [])
      refute ChDate.matches_type?(123, [])
      refute ChDate.matches_type?([], [])
    end
  end

  describe "generator" do
    test "generates Date structs" do
      generated_dates = Enum.take(ChDate.generator([]), 100) |> Enum.uniq()
      assert Enum.all?(generated_dates, fn date -> is_struct(date, Date) end)
    end
  end

  describe "cast_input/2" do
    test "casts Date structs correctly" do
      date = ~D[2023-01-01]
      assert ChDate.cast_input(date, []) == {:ok, date}
      assert ChDate.cast_input(nil, []) == {:ok, nil}
    end

    test "returns error for non-Date inputs" do
      assert :error = ChDate.cast_input(123, [])
    end
  end
end
