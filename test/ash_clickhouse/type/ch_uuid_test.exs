defmodule AshClickhouse.Type.ChUuidTest do
  use ExUnit.Case, async: true
  alias AshClickhouse.Type.ChUUID

  import AshClickhouse.Test.Helper, only: [encode_ch_type: 1]

  describe "storage_types" do
    test "returns correct ClickHouse type without constraints" do
      assert {:parameterized, {Ch, :uuid}} = type = Ash.Type.storage_type(ChUUID, [])
      assert encode_ch_type(type) == "UUID"
    end

    test "returns nullable ClickHouse type with nullable constraint" do
      assert {:parameterized, {Ch, {:nullable, :uuid}}} =
               type = Ash.Type.storage_type(ChUUID, nullable?: true)

      assert encode_ch_type(type) == "Nullable(UUID)"
    end

    test "returns low cardinality ClickHouse type with low cardinality constraint" do
      assert {:parameterized, {Ch, {:low_cardinality, :uuid}}} =
               type = Ash.Type.storage_type(ChUUID, low_cardinality?: true)

      assert encode_ch_type(type) == "LowCardinality(UUID)"
    end

    test "returns nullable low cardinality ClickHouse type with nullable and low cardinality constraint" do
      assert {:parameterized, {Ch, {:low_cardinality, {:nullable, :uuid}}}} =
               type = Ash.Type.storage_type(ChUUID, nullable?: true, low_cardinality?: true)

      assert encode_ch_type(type) == "LowCardinality(Nullable(UUID))"
    end
  end
  describe "matches_type?/2" do
    test "returns true for UUIDs" do
      assert Ash.Type.matches_type?(ChUUID, "123e4567-e89b-12d3-a456-426614174000", [])
      assert Ash.Type.matches_type?(ChUUID, nil, [])
    end

    test "returns false for non-UUIDs" do
      refute Ash.Type.matches_type?(ChUUID, "123e4567-e89b-12d3-a456-4266141740000", [])
      refute Ash.Type.matches_type?(ChUUID, 123, [])
      refute Ash.Type.matches_type?(ChUUID, [], [])
    end
  end

  describe "cast_input/2" do
    test "casts UUIDs" do
      assert Ash.Type.cast_input(ChUUID, "123e4567-e89b-12d3-a456-426614174000", []) == {:ok, "123e4567-e89b-12d3-a456-426614174000"}
    end

    test "casts nil to nil" do
      assert Ash.Type.cast_input(ChUUID, nil, []) == {:ok, nil}
    end

    test "returns error for non-UUID inputs" do
      assert Ash.Type.cast_input(ChUUID, 123, []) == {:error, "is invalid"}
    end
  end

  describe "generator/1" do
    test "generates UUIDs" do
      assert Ash.Type.generator(ChUUID, [])
             |> Enum.take(100)
             |> Enum.uniq()
             |> Enum.all?(&Ash.Type.matches_type?(ChUUID, &1, []))
    end
  end
end
