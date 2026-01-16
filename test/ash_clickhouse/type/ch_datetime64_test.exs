defmodule AshClickhouse.Type.ChDateTime64Test do
  use ExUnit.Case, async: true
  alias AshClickhouse.Type.ChDateTime64

  import AshClickhouse.Test.Helper, only: [encode_ch_type: 1]

  describe "storage_types" do
    test "returns correct ClickHouse type without constraints" do
      assert {:parameterized, {Ch, {:datetime64, 6}}} =
               type = Ash.Type.storage_type(ChDateTime64, precision: 6)

      assert encode_ch_type(type) == "DateTime64(6)"
    end

    test "returns nullable ClickHouse type with nullable constraint" do
      assert {:parameterized, {Ch, {:nullable, {:datetime64, 6}}}} =
               type = Ash.Type.storage_type(ChDateTime64, precision: 6, nullable?: true)

      assert encode_ch_type(type) == "Nullable(DateTime64(6))"
    end

    test "returns correct ClickHouse type with timezone" do
      assert {:parameterized, {Ch, {:datetime64, 6, "UTC"}}} =
               type =
               Ash.Type.storage_type(ChDateTime64, precision: 6, timezone: "UTC")

      assert encode_ch_type(type) == "DateTime64(6, 'UTC')"
    end

    test "returns nullable ClickHouse type with timezone and nullable constraint" do
      assert {:parameterized, {Ch, {:nullable, {:datetime64, 3, "UTC"}}}} =
               type =
               Ash.Type.storage_type(ChDateTime64, precision: 3, timezone: "UTC", nullable?: true)

      assert encode_ch_type(type) == "Nullable(DateTime64(3, 'UTC'))"

      assert {:parameterized, {Ch, {:nullable, {:datetime64, 3, "UTC"}}}} =
               type =
               Ash.Type.storage_type(ChDateTime64, precision: 3, timezone: "UTC", nullable?: true)

      assert encode_ch_type(type) == "Nullable(DateTime64(3, 'UTC'))"
    end

    test "returns correct ClickHouse type without constraints for array version" do
      assert {:array, {:parameterized, {Ch, {:datetime64, 6}}} = subtype} =
               Ash.Type.storage_type({:array, ChDateTime64}, precision: 6)

      assert encode_ch_type({:array, subtype}) == "Array(DateTime64(6))"
    end

    test "returns nullable ClickHouse type with nullable constraint for array version" do
      assert {:array, {:parameterized, {Ch, {:nullable, {:datetime64, 6}}}} = subtype} =
               Ash.Type.storage_type({:array, ChDateTime64}, precision: 6, nullable?: true)

      assert encode_ch_type({:array, subtype}) == "Array(Nullable(DateTime64(6)))"
    end

    test "returns correct ClickHouse type with timezone for array version" do
      assert {:array, {:parameterized, {Ch, {:datetime64, 6, "UTC"}}} = subtype} =
               Ash.Type.storage_type({:array, ChDateTime64}, precision: 6, timezone: "UTC")

      assert encode_ch_type({:array, subtype}) == "Array(DateTime64(6, 'UTC'))"
    end

    test "returns nullable ClickHouse type with timezone and nullable constraint for array version" do
      assert {:array, {:parameterized, {Ch, {:nullable, {:datetime64, 3, "UTC"}}}} = subtype} =
               Ash.Type.storage_type({:array, ChDateTime64},
                 precision: 3,
                 timezone: "UTC",
                 nullable?: true
               )

      assert encode_ch_type({:array, subtype}) == "Array(Nullable(DateTime64(3, 'UTC')))"
    end
  end

  describe "matches_type?/2" do
    test "returns true for DateTime structs" do
      assert Ash.Type.matches_type?(ChDateTime64, ~U[2026-01-01 00:00:00Z], precision: 6)
      assert Ash.Type.matches_type?(ChDateTime64, "2026-01-01 00:00:00", precision: 6)
    end

    test "returns false for non-DateTime values" do
      refute Ash.Type.matches_type?(ChDateTime64, nil, precision: 6)
      refute Ash.Type.matches_type?(ChDateTime64, 123, precision: 6)
      refute Ash.Type.matches_type?(ChDateTime64, [], precision: 6)
    end

    test "returns true for arrays of DateTime structs" do
      assert Ash.Type.matches_type?(
               {:array, ChDateTime64},
               [~U[2026-01-01 00:00:00Z], ~U[2026-01-02 00:00:00Z]],
               items: [precision: 6]
             )

      assert Ash.Type.matches_type?(
               {:array, ChDateTime64},
               ["2026-01-01 00:00:00", "2026-01-02 00:00:00"],
               items: [precision: 6]
             )
    end

    test "returns false for non-DateTime arrays" do
      refute Ash.Type.matches_type?({:array, ChDateTime64}, [123, 456], items: [precision: 6])
      refute Ash.Type.matches_type?({:array, ChDateTime64}, [[], {}], items: [precision: 6])
    end
  end

  describe "generator" do
    test "generates DateTime structs" do
      generated_datetimes = Enum.take(Ash.Type.generator(ChDateTime64, []), 100) |> Enum.uniq()
      assert Enum.all?(generated_datetimes, fn dt -> is_struct(dt, DateTime) end)
    end

    test "generates arrays of DateTime structs" do
      generated_arrays =
        Enum.take(Ash.Type.generator({:array, ChDateTime64}, items: [precision: 6]), 100)

      assert Enum.all?(generated_arrays, fn arr ->
               is_list(arr) and Enum.all?(arr, fn dt -> is_struct(dt, DateTime) end)
             end)
    end
  end

  describe "cast_input/2" do
    test "casts DateTime structs correctly" do
      datetime = ~U[2026-01-15 12:00:00.000000Z]

      assert Ash.Type.cast_input(ChDateTime64, datetime, precision: 6) ==
               {:ok, ~N[2026-01-15 12:00:00.000000]}

      assert Ash.Type.cast_input(ChDateTime64, datetime, precision: 6, timezone: "UTC") ==
               {:ok, datetime}

      assert Ash.Type.cast_input(ChDateTime64, ~D[2026-01-15],
               precision: 6,
               timezone: "UTC",
               cast_dates_as: :start_of_day
             ) ==
               {:ok, ~U[2026-01-15 00:00:00.000000Z]}

      assert Ash.Type.cast_input(ChDateTime64, nil, precision: 6) == {:ok, nil}
    end

    test "returns error for non-DateTime inputs" do
      assert {:error, "Could not cast input to datetime"} =
               Ash.Type.cast_input(ChDateTime64, 123, precision: 6)
    end

    test "casts valid array of DateTime structs correctly" do
      datetime1 = ~U[2026-01-15 12:00:00.000000Z]
      datetime2 = ~U[2026-01-16 12:00:00.000000Z]

      assert Ash.Type.cast_input({:array, ChDateTime64}, [datetime1, datetime2],
               items: [precision: 6]
             ) ==
               {:ok, [~N[2026-01-15 12:00:00.000000], ~N[2026-01-16 12:00:00.000000]]}

      assert Ash.Type.cast_input({:array, ChDateTime64}, [datetime1, datetime2],
               items: [precision: 6, timezone: "UTC"]
             ) ==
               {:ok, [datetime1, datetime2]}

      assert Ash.Type.cast_input({:array, ChDateTime64}, [nil, datetime1], items: [precision: 6]) ==
               {:ok, [nil, ~N[2026-01-15 12:00:00.000000]]}
    end

    test "returns error for non-DateTime array inputs" do
      assert {:error, [[message: "Could not cast input to datetime", index: 0, path: [0]]]} =
               Ash.Type.cast_input({:array, ChDateTime64}, [123, ~U[2026-01-15 12:00:00.000000Z]],
                 items: [precision: 6]
               )
    end
  end

  describe "cast_stored/2" do
    test "loads DateTime values correctly" do
      assert Ash.Type.cast_stored(ChDateTime64, "2026-01-01 00:00:00", precision: 6) ==
               {:ok, ~N[2026-01-01 00:00:00.000000]}

      assert Ash.Type.cast_stored(ChDateTime64, "2026-01-01 00:00:00",
               precision: 6,
               timezone: "UTC"
             ) ==
               {:ok, ~U[2026-01-01 00:00:00.000000Z]}

      assert Ash.Type.cast_stored(ChDateTime64, ~U[2026-01-01 00:00:00Z], precision: 6) ==
               {:ok, ~U[2026-01-01 00:00:00Z]}

      assert Ash.Type.cast_stored(ChDateTime64, nil, []) == {:ok, nil}
    end

    test "returns error for non-DateTime stored values" do
      assert {:error, "Could not cast input to datetime"} =
               Ash.Type.cast_stored(ChDateTime64, "invalid-datetime", precision: 6)
    end

    test "loads arrays of DateTime values correctly" do
      assert Ash.Type.cast_stored(
               {:array, ChDateTime64},
               ["2026-01-01 00:00:00", "2026-01-02 00:00:00"],
               items: [precision: 6]
             ) ==
               {:ok, [~N[2026-01-01 00:00:00.000000], ~N[2026-01-02 00:00:00.000000]]}

      assert Ash.Type.cast_stored(
               {:array, ChDateTime64},
               ["2026-01-01 00:00:00", "2026-01-02 00:00:00"],
               items: [precision: 6, timezone: "UTC"]
             ) ==
               {:ok, [~U[2026-01-01 00:00:00.000000Z], ~U[2026-01-02 00:00:00.000000Z]]}

      assert Ash.Type.cast_stored({:array, ChDateTime64}, [nil, "2026-01-01 00:00:00"],
               items: [precision: 6]
             ) ==
               {:ok, [nil, ~N[2026-01-01 00:00:00.000000]]}
    end

    test "returns error for non-DateTime array stored values" do
      assert {:error, [[message: "Could not cast input to datetime", index: 0]]} =
               Ash.Type.cast_stored(
                 {:array, ChDateTime64},
                 ["invalid-datetime", "2026-01-01 00:00:00"],
                 items: [precision: 6]
               )
    end
  end

  describe "equal?/2" do
    test "returns true for equal DateTime structs" do
      datetime = ~N[2026-01-01 00:00:00.000000]
      assert Ash.Type.equal?(ChDateTime64, datetime, datetime)
      assert Ash.Type.equal?(ChDateTime64, nil, nil)
    end

    test "returns false for different DateTime structs" do
      refute Ash.Type.equal?(
               ChDateTime64,
               ~N[2026-01-01 00:00:00.000000],
               ~N[2026-01-02 00:00:00.000000]
             )

      refute Ash.Type.equal?(ChDateTime64, ~N[2026-01-01 00:00:00.000000], nil)
      refute Ash.Type.equal?(ChDateTime64, nil, ~N[2026-01-01 00:00:00.000000])
    end

    test "returns true for equal arrays of DateTime structs" do
      datetime1 = ~N[2026-01-01 00:00:00.000000]
      datetime2 = ~N[2026-01-02 00:00:00.000000]

      assert Ash.Type.equal?({:array, ChDateTime64}, [datetime1, datetime2], [
               datetime1,
               datetime2
             ])

      assert Ash.Type.equal?({:array, ChDateTime64}, [], [])
    end

    test "returns false for different arrays of DateTime structs" do
      datetime1 = ~N[2026-01-01 00:00:00.000000]
      datetime2 = ~N[2026-01-02 00:00:00.000000]

      refute Ash.Type.equal?({:array, ChDateTime64}, [datetime1, datetime2], [
               datetime2,
               datetime1
             ])

      refute Ash.Type.equal?({:array, ChDateTime64}, [datetime1], [datetime1, datetime2])
      refute Ash.Type.equal?({:array, ChDateTime64}, [datetime1, nil], [datetime1, datetime2])
    end
  end

  describe "simple_equality?/0" do
    test "returns true for ChDateTime64" do
      assert Ash.Type.simple_equality?(ChDateTime64)
    end

    test "returns true for {:array, ChDateTime64}" do
      assert Ash.Type.simple_equality?({:array, ChDateTime64})
    end
  end
end
