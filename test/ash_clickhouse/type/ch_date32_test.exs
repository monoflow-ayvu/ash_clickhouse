defmodule AshClickhouse.Type.ChDate32Test do
  use ExUnit.Case, async: true
  alias AshClickhouse.Type.ChDate32

  import AshClickhouse.Test.Helper, only: [encode_ch_type: 1]

  describe "storage_types" do
    test "returns correct ClickHouse type without constraints" do
      assert {:parameterized, {Ch, :date32}} = type = ChDate32.storage_type([])
      assert encode_ch_type(type) == "Date32"
    end

    test "returns nullable ClickHouse type with nullable constraint" do
      assert {:parameterized, {Ch, {:nullable, :date32}}} =
               type = ChDate32.storage_type(nullable?: true)

      assert encode_ch_type(type) == "Nullable(Date32)"
    end
  end

  describe "matches_type?/2" do
    test "returns true for Date structs" do
      assert ChDate32.matches_type?(~D[2023-01-01], [])
      assert ChDate32.matches_type?("2023-01-01", [])
      assert ChDate32.matches_type?(nil, [])
    end

    test "returns false for non-Date values" do
      refute ChDate32.matches_type?(123, [])
      refute ChDate32.matches_type?([], [])
    end
  end

  describe "generator" do
    test "generates Date structs" do
      generated_dates = Enum.take(ChDate32.generator([]), 100) |> Enum.uniq()
      assert Enum.all?(generated_dates, fn date -> is_struct(date, Date) end)
    end
  end

  describe "cast_input/2" do
    test "casts Date structs correctly" do
      date = ~D[2023-01-01]
      assert ChDate32.cast_input(date, []) == {:ok, date}
      assert ChDate32.cast_input(nil, []) == {:ok, nil}
    end

    test "returns error for non-Date inputs" do
      assert :error = ChDate32.cast_input(123, [])
    end
  end

  describe "cast_stored/2" do
    test "loads Date32 values correctly" do
      assert ChDate32.cast_stored("2023-01-01", []) == {:ok, ~D[2023-01-01]}
      assert ChDate32.cast_stored(nil, []) == {:ok, nil}
    end

    test "returns error for non-Date32 stored values" do
      assert :error = ChDate32.cast_stored("invalid-date", [])
    end
  end
end
