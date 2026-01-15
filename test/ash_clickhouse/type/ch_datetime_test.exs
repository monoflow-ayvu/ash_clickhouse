defmodule AshClickhouse.Type.ChDateTimeTest do
  use ExUnit.Case, async: true
  alias AshClickhouse.Type.ChDateTime

  import AshClickhouse.Test.Helper, only: [encode_ch_type: 1]

  describe "storage_types" do
    test "returns correct ClickHouse type without constraints" do
      assert {:parameterized, {Ch, :datetime}} = type = ChDateTime.storage_type([])
      assert encode_ch_type(type) == "DateTime"
    end

    test "returns nullable ClickHouse type with nullable constraint" do
      assert {:parameterized, {Ch, {:nullable, :datetime}}} =
               type = ChDateTime.storage_type(nullable?: true)

      assert encode_ch_type(type) == "Nullable(DateTime)"
    end

    test "returns correct ClickHouse type with timezone" do
      assert {:parameterized, {Ch, {:datetime, "UTC"}}} =
               type =
               ChDateTime.storage_type(timezone: "UTC")

      assert encode_ch_type(type) == "DateTime('UTC')"
    end

    test "returns nullable ClickHouse type with timezone and nullable constraint" do
      assert {:parameterized, {Ch, {:nullable, {:datetime, "UTC"}}}} =
               type =
               ChDateTime.storage_type(timezone: "UTC", nullable?: true)

      assert encode_ch_type(type) == "Nullable(DateTime('UTC'))"
    end
  end

  describe "matches_type?/2" do
    test "returns true for DateTime structs" do
      assert ChDateTime.matches_type?(~U[2026-01-01 00:00:00Z], [])
      assert ChDateTime.matches_type?("2026-01-01 00:00:00", [])
    end

    test "returns false for non-DateTime values" do
      refute ChDateTime.matches_type?(nil, [])
      refute ChDateTime.matches_type?(123, [])
      refute ChDateTime.matches_type?([], [])
    end
  end

  describe "generator" do
    test "generates DateTime structs" do
      generated_datetimes = Enum.take(ChDateTime.generator([]), 100) |> Enum.uniq()
      assert Enum.all?(generated_datetimes, fn dt -> is_struct(dt, DateTime) end)
    end
  end

  describe "cast_input/2" do
    test "casts DateTime structs correctly" do
      datetime = ~U[2026-01-15 12:00:00Z]
      assert ChDateTime.cast_input(datetime, []) == {:ok, ~N[2026-01-15 12:00:00]}
      assert ChDateTime.cast_input(datetime, timezone: "UTC") == {:ok, datetime}

      assert ChDateTime.cast_input(~D[2026-01-15], timezone: "UTC", cast_dates_as: :start_of_day) ==
               {:ok, ~U[2026-01-15 00:00:00Z]}

      assert ChDateTime.cast_input(nil, []) == {:ok, nil}
    end

    test "returns error for non-DateTime inputs" do
      assert {:error, "Could not cast input to datetime"} = ChDateTime.cast_input(123, [])
    end
  end

  describe "cast_stored/2" do
    test "loads DateTime values correctly" do
      assert ChDateTime.cast_stored("2026-01-01 00:00:00", []) ==
               {:ok, ~N[2026-01-01 00:00:00]}

      assert ChDateTime.cast_stored("2026-01-01 00:00:00", timezone: "UTC") ==
               {:ok, ~U[2026-01-01 00:00:00Z]}

      assert ChDateTime.cast_stored(~U[2026-01-01 00:00:00Z], []) ==
               {:ok, ~U[2026-01-01 00:00:00Z]}

      assert ChDateTime.cast_stored(nil, []) == {:ok, nil}
    end

    test "returns error for non-DateTime stored values" do
      assert {:error, "Could not cast input to datetime"} =
               ChDateTime.cast_stored("invalid-datetime", [])
    end
  end
end
