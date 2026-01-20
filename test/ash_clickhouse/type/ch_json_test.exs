defmodule AshClickhouse.Type.ChJsonTest do
  use ExUnit.Case, async: true

  alias AshClickhouse.Type.ChJSON

  import AshClickhouse.Test.Helper, only: [encode_ch_type: 1]

  describe "storage_types" do
    test "returns correct ClickHouse type without constraints" do
      assert {:parameterized, {Ch, :json}} = type = Ash.Type.storage_type(ChJSON, [])
      assert encode_ch_type(type) == "JSON"
    end

    test "returns correct ClickHouse type with nullable constraint" do
      assert {:parameterized, {Ch, {:nullable, :json}}} =
               type = Ash.Type.storage_type(ChJSON, nullable?: true)

      assert encode_ch_type(type) == "Nullable(JSON)"
    end

    test "returns correct ClickHouse type for array version" do
      assert {:array, {:parameterized, {Ch, :json}} = subtype} =
               Ash.Type.storage_type({:array, ChJSON}, [])

      assert encode_ch_type({:array, subtype}) == "Array(JSON)"
    end

    test "returns correct ClickHouse type for array version with nullable constraint" do
      assert {:array, {:parameterized, {Ch, {:nullable, :json}}} = subtype} =
               Ash.Type.storage_type({:array, ChJSON}, nullable?: true)

      assert encode_ch_type({:array, subtype}) == "Array(Nullable(JSON))"
    end
  end

  describe "cast_input/2" do
    test "cast atom key map to internal representation" do
      assert {:ok, %{"name" => "John", "age" => 30}} =
               Ash.Type.cast_input(ChJSON, %{name: "John", age: 30}, [])
    end

    test "cast string key map to internal representation" do
      assert {:ok, %{"name" => "John", "age" => 30}} =
               Ash.Type.cast_input(ChJSON, %{"name" => "John", "age" => 30}, [])
    end

    test "cast string json to internal representation" do
      assert {:ok, %{"name" => "John", "age" => 30}} =
               Ash.Type.cast_input(ChJSON, ~s({"name": "John", "age": 30}), [])
    end

    test "returns error for invalid JSON string" do
      assert {:error, "is invalid"} = Ash.Type.cast_input(ChJSON, "invalid", [])
    end

    test "casts valid JSON string to internal representation for array version" do
      assert {:ok,
              [
                %{"age" => 30, "name" => "John"},
                %{"age" => 30, "name" => "John"},
                %{"name" => "John", "age" => 30}
              ]} =
               Ash.Type.cast_input(
                 {:array, ChJSON},
                 [
                   %{name: "John", age: 30},
                   %{"name" => "John", "age" => 30},
                   ~s({"name": "John", "age": 30})
                 ],
                 []
               )
    end
  end

  describe "matches_type?/2" do
    test "returns true for valid JSON" do
      assert Ash.Type.matches_type?(ChJSON, ~s({"name": "John", "age": 30}), [])
    end

    test "returns false for invalid JSON" do
      refute Ash.Type.matches_type?(ChJSON, "invalid", [])
      refute Ash.Type.matches_type?(ChJSON, nil, [])
    end
  end

  describe "cast_stored/2" do
    test "casts stored JSON to internal representation" do
      assert {:ok, %{"name" => "John", "age" => 30}} =
               Ash.Type.cast_stored(ChJSON, ~s({"name": "John", "age": 30}), [])
    end
  end
end
