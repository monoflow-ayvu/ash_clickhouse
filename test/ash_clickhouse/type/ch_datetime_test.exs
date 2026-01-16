defmodule AshClickhouse.Type.ChDateTimeTest do
  use ExUnit.Case, async: true
  alias AshClickhouse.Type.ChDateTime

  import AshClickhouse.Test.Helper, only: [encode_ch_type: 1]

  describe "storage_types" do
    test "returns correct ClickHouse type without constraints" do
      assert {:parameterized, {Ch, :datetime}} = type = Ash.Type.storage_type(ChDateTime, [])
      assert encode_ch_type(type) == "DateTime"
    end

    test "returns nullable ClickHouse type with nullable constraint" do
      assert {:parameterized, {Ch, {:nullable, :datetime}}} =
               type = Ash.Type.storage_type(ChDateTime, nullable?: true)

      assert encode_ch_type(type) == "Nullable(DateTime)"
    end

    test "returns correct ClickHouse type with timezone" do
      assert {:parameterized, {Ch, {:datetime, "UTC"}}} =
               type =
               Ash.Type.storage_type(ChDateTime, timezone: "UTC")

      assert encode_ch_type(type) == "DateTime('UTC')"
    end

    test "returns nullable ClickHouse type with timezone and nullable constraint" do
      assert {:parameterized, {Ch, {:nullable, {:datetime, "UTC"}}}} =
               type =
               Ash.Type.storage_type(ChDateTime, timezone: "UTC", nullable?: true)

      assert encode_ch_type(type) == "Nullable(DateTime('UTC'))"
    end

    test "returns correct ClickHouse type without constraints for array version" do
      assert {:array, {:parameterized, {Ch, :datetime}} = subtype} =
               Ash.Type.storage_type({:array, ChDateTime}, [])

      assert encode_ch_type({:array, subtype}) == "Array(DateTime)"
    end

    test "returns nullable ClickHouse type with nullable constraint for array version" do
      assert {:array, {:parameterized, {Ch, {:nullable, :datetime}}} = subtype} =
               Ash.Type.storage_type({:array, ChDateTime}, nullable?: true)

      assert encode_ch_type({:array, subtype}) == "Array(Nullable(DateTime))"
    end

    test "returns correct ClickHouse type with timezone for array version" do
      assert {:array, {:parameterized, {Ch, {:datetime, "UTC"}}} = subtype} =
               Ash.Type.storage_type({:array, ChDateTime}, timezone: "UTC")

      assert encode_ch_type({:array, subtype}) == "Array(DateTime('UTC'))"
    end

    test "returns nullable ClickHouse type with timezone and nullable constraint for array version" do
      assert {:array, {:parameterized, {Ch, {:nullable, {:datetime, "UTC"}}}} = subtype} =
               Ash.Type.storage_type({:array, ChDateTime}, timezone: "UTC", nullable?: true)

      assert encode_ch_type({:array, subtype}) == "Array(Nullable(DateTime('UTC')))"
    end
  end

  describe "matches_type?/2" do
    test "returns true for DateTime structs" do
      assert Ash.Type.matches_type?(ChDateTime, ~U[2026-01-01 00:00:00Z], [])
      assert Ash.Type.matches_type?(ChDateTime, "2026-01-01 00:00:00", [])
    end

    test "returns false for non-DateTime values" do
      refute Ash.Type.matches_type?(ChDateTime, nil, [])
      refute Ash.Type.matches_type?(ChDateTime, 123, [])
      refute Ash.Type.matches_type?(ChDateTime, [], [])
    end

    test "returns true for arrays of DateTime structs" do
      assert Ash.Type.matches_type?(
               {:array, ChDateTime},
               [~U[2026-01-01 00:00:00Z], ~U[2026-01-02 00:00:00Z]],
               []
             )

      assert Ash.Type.matches_type?(
               {:array, ChDateTime},
               ["2026-01-01 00:00:00", "2026-01-02 00:00:00"],
               []
             )
    end

    test "returns false for non-DateTime arrays" do
      refute Ash.Type.matches_type?({:array, ChDateTime}, [123, 456], [])
      refute Ash.Type.matches_type?({:array, ChDateTime}, [[], {}], [])
    end
  end

  describe "generator" do
    test "generates DateTime structs" do
      generated_datetimes = Enum.take(Ash.Type.generator(ChDateTime, []), 100) |> Enum.uniq()
      assert Enum.all?(generated_datetimes, fn dt -> is_struct(dt, DateTime) end)
    end

    test "generates arrays of DateTime structs" do
      generated_arrays = Enum.take(Ash.Type.generator({:array, ChDateTime}, []), 100)

      assert Enum.all?(generated_arrays, fn arr ->
               is_list(arr) and Enum.all?(arr, fn dt -> is_struct(dt, DateTime) end)
             end)
    end
  end

  describe "cast_input/2" do
    test "casts DateTime structs correctly" do
      datetime = ~U[2026-01-15 12:00:00Z]
      assert Ash.Type.cast_input(ChDateTime, datetime, []) == {:ok, ~N[2026-01-15 12:00:00]}
      assert Ash.Type.cast_input(ChDateTime, datetime, timezone: "UTC") == {:ok, datetime}

      assert Ash.Type.cast_input(ChDateTime, ~D[2026-01-15],
               timezone: "UTC",
               cast_dates_as: :start_of_day
             ) ==
               {:ok, ~U[2026-01-15 00:00:00Z]}

      assert Ash.Type.cast_input(ChDateTime, nil, []) == {:ok, nil}
    end

    test "returns error for non-DateTime inputs" do
      assert {:error, "Could not cast input to datetime"} =
               Ash.Type.cast_input(ChDateTime, 123, [])
    end

    test "casts valid array of DateTime structs correctly" do
      datetime1 = ~U[2026-01-15 12:00:00Z]
      datetime2 = ~U[2026-01-16 12:00:00Z]

      assert Ash.Type.cast_input({:array, ChDateTime}, [datetime1, datetime2], []) ==
               {:ok, [~N[2026-01-15 12:00:00], ~N[2026-01-16 12:00:00]]}

      assert Ash.Type.cast_input({:array, ChDateTime}, [datetime1, datetime2],
               items: [timezone: "UTC"]
             ) ==
               {:ok, [datetime1, datetime2]}

      assert Ash.Type.cast_input({:array, ChDateTime}, [nil, datetime1], []) ==
               {:ok, [nil, ~N[2026-01-15 12:00:00]]}
    end

    test "returns error for non-DateTime array inputs" do
      assert {:error, [[message: "Could not cast input to datetime", index: 0, path: [0]]]} =
               Ash.Type.cast_input({:array, ChDateTime}, [123, ~U[2026-01-15 12:00:00Z]], [])
    end
  end

  describe "cast_stored/2" do
    test "loads DateTime values correctly" do
      assert Ash.Type.cast_stored(ChDateTime, "2026-01-01 00:00:00", []) ==
               {:ok, ~N[2026-01-01 00:00:00]}

      assert Ash.Type.cast_stored(ChDateTime, "2026-01-01 00:00:00", timezone: "UTC") ==
               {:ok, ~U[2026-01-01 00:00:00Z]}

      assert Ash.Type.cast_stored(ChDateTime, ~U[2026-01-01 00:00:00Z], []) ==
               {:ok, ~U[2026-01-01 00:00:00Z]}

      assert Ash.Type.cast_stored(ChDateTime, nil, []) == {:ok, nil}
    end

    test "returns error for non-DateTime stored values" do
      assert {:error, "Could not cast input to datetime"} =
               Ash.Type.cast_stored(ChDateTime, "invalid-datetime", [])
    end

    test "loads arrays of DateTime values correctly" do
      assert Ash.Type.cast_stored(
               {:array, ChDateTime},
               ["2026-01-01 00:00:00", "2026-01-02 00:00:00"],
               []
             ) ==
               {:ok, [~N[2026-01-01 00:00:00], ~N[2026-01-02 00:00:00]]}

      assert Ash.Type.cast_stored(
               {:array, ChDateTime},
               ["2026-01-01 00:00:00", "2026-01-02 00:00:00"],
               items: [timezone: "UTC"]
             ) ==
               {:ok, [~U[2026-01-01 00:00:00Z], ~U[2026-01-02 00:00:00Z]]}

      assert Ash.Type.cast_stored({:array, ChDateTime}, [nil, "2026-01-01 00:00:00"], []) ==
               {:ok, [nil, ~N[2026-01-01 00:00:00]]}
    end

    test "returns error for non-DateTime array stored values" do
      assert {:error, [[message: "Could not cast input to datetime", index: 0]]} =
               Ash.Type.cast_stored(
                 {:array, ChDateTime},
                 ["invalid-datetime", "2026-01-01 00:00:00"],
                 []
               )
    end
  end

  describe "equal?/2" do
    test "returns true for equal DateTime structs" do
      datetime = ~N[2026-01-01 00:00:00]
      assert Ash.Type.equal?(ChDateTime, datetime, datetime)
      assert Ash.Type.equal?(ChDateTime, nil, nil)
    end

    test "returns false for different DateTime structs" do
      refute Ash.Type.equal?(ChDateTime, ~N[2026-01-01 00:00:00], ~N[2026-01-02 00:00:00])
      refute Ash.Type.equal?(ChDateTime, ~N[2026-01-01 00:00:00], nil)
      refute Ash.Type.equal?(ChDateTime, nil, ~N[2026-01-01 00:00:00])
    end

    test "returns true for equal arrays of DateTime structs" do
      datetime1 = ~N[2026-01-01 00:00:00]
      datetime2 = ~N[2026-01-02 00:00:00]
      assert Ash.Type.equal?({:array, ChDateTime}, [datetime1, datetime2], [datetime1, datetime2])
      assert Ash.Type.equal?({:array, ChDateTime}, [], [])
    end

    test "returns false for different arrays of DateTime structs" do
      datetime1 = ~N[2026-01-01 00:00:00]
      datetime2 = ~N[2026-01-02 00:00:00]
      refute Ash.Type.equal?({:array, ChDateTime}, [datetime1, datetime2], [datetime2, datetime1])
      refute Ash.Type.equal?({:array, ChDateTime}, [datetime1], [datetime1, datetime2])
      refute Ash.Type.equal?({:array, ChDateTime}, [datetime1, nil], [datetime1, datetime2])
    end
  end

  describe "simple_equality?/0" do
    test "returns true for ChDateTime" do
      assert Ash.Type.simple_equality?(ChDateTime)
    end

    test "returns true for {:array, ChDateTime}" do
      assert Ash.Type.simple_equality?({:array, ChDateTime})
    end
  end
end
