defmodule AshClickhouse.Type.ChDateTime64Test do
  use ExUnit.Case, async: true
  alias AshClickhouse.Type.ChDateTime64

  import AshClickhouse.Test.Helper, only: [encode_ch_type: 1]

  describe "storage_types" do
    test "returns correct ClickHouse type without constraints" do
      assert {:parameterized, {Ch, {:datetime64, 6}}} =
               type = ChDateTime64.storage_type(precision: 6)

      assert encode_ch_type(type) == "DateTime64(6)"
    end

    test "returns nullable ClickHouse type with nullable constraint" do
      assert {:parameterized, {Ch, {:nullable, {:datetime64, 6}}}} =
               type = ChDateTime64.storage_type(precision: 6, nullable?: true)

      assert encode_ch_type(type) == "Nullable(DateTime64(6))"
    end

    test "returns correct ClickHouse type with timezone" do
      assert {:parameterized, {Ch, {:datetime64, 6, "UTC"}}} =
               type =
               ChDateTime64.storage_type(precision: 6, timezone: "UTC")

      assert encode_ch_type(type) == "DateTime64(6, 'UTC')"
    end

    test "returns nullable ClickHouse type with timezone and nullable constraint" do
      assert {:parameterized, {Ch, {:nullable, {:datetime64, 3, "UTC"}}}} =
               type =
               ChDateTime64.storage_type(precision: 3, timezone: "UTC", nullable?: true)

      assert encode_ch_type(type) == "Nullable(DateTime64(3, 'UTC'))"

      assert {:parameterized, {Ch, {:nullable, {:datetime64, 3, "UTC"}}}} =
               type =
               ChDateTime64.storage_type(precision: 3, timezone: "UTC", nullable?: true)

      assert encode_ch_type(type) == "Nullable(DateTime64(3, 'UTC'))"
    end
  end

  describe "matches_type?/2" do
    test "returns true for DateTime structs" do
      assert ChDateTime64.matches_type?(~U[2026-01-01 00:00:00Z], precision: 6)
      assert ChDateTime64.matches_type?("2026-01-01 00:00:00", precision: 6)
    end

    test "returns false for non-DateTime values" do
      refute ChDateTime64.matches_type?(nil, precision: 6)
      refute ChDateTime64.matches_type?(123, precision: 6)
      refute ChDateTime64.matches_type?([], precision: 6)
    end
  end

  describe "generator" do
    test "generates DateTime structs" do
      generated_datetimes = Enum.take(ChDateTime64.generator([]), 100) |> Enum.uniq()
      assert Enum.all?(generated_datetimes, fn dt -> is_struct(dt, DateTime) end)
    end
  end

  describe "cast_input/2" do
    test "casts DateTime structs correctly" do
      datetime = ~U[2026-01-15 12:00:00.000000Z]

      assert ChDateTime64.cast_input(datetime, precision: 6) ==
               {:ok, ~N[2026-01-15 12:00:00.000000]}

      assert ChDateTime64.cast_input(datetime, precision: 6, timezone: "UTC") == {:ok, datetime}

      assert ChDateTime64.cast_input(~D[2026-01-15],
               precision: 6,
               timezone: "UTC",
               cast_dates_as: :start_of_day
             ) ==
               {:ok, ~U[2026-01-15 00:00:00.000000Z]}

      assert ChDateTime64.cast_input(nil, precision: 6) == {:ok, nil}
    end

    test "returns error for non-DateTime inputs" do
      assert {:error, "Could not cast input to datetime"} =
               ChDateTime64.cast_input(123, precision: 6)
    end
  end

  describe "cast_stored/2" do
    test "loads DateTime values correctly" do
      assert ChDateTime64.cast_stored("2026-01-01 00:00:00", precision: 6) ==
               {:ok, ~N[2026-01-01 00:00:00.000000]}

      assert ChDateTime64.cast_stored("2026-01-01 00:00:00", precision: 6, timezone: "UTC") ==
               {:ok, ~U[2026-01-01 00:00:00.000000Z]}

      assert ChDateTime64.cast_stored(~U[2026-01-01 00:00:00Z], precision: 6) ==
               {:ok, ~U[2026-01-01 00:00:00Z]}

      assert ChDateTime64.cast_stored(nil, []) == {:ok, nil}
    end

    test "returns error for non-DateTime stored values" do
      assert {:error, "Could not cast input to datetime"} =
               ChDateTime64.cast_stored("invalid-datetime", precision: 6)
    end
  end
end
