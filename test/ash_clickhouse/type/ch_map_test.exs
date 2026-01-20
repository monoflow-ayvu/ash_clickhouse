defmodule AshClickhouse.Type.ChMapTest do
  use ExUnit.Case, async: true

  alias AshClickhouse.Type.ChMap

  import AshClickhouse.Test.Helper, only: [encode_ch_type: 1]

  describe "storage_types" do
    test "returns correct ClickHouse type without constraints" do
      assert {:parameterized, {Ch, {:map, :string, :string}}} =
               type = Ash.Type.storage_type(ChMap, key_type: :ch_string, value_type: :ch_string)

      assert encode_ch_type(type) == "Map(String, String)"
    end

    test "returns Map(String, Nullable(String))" do
      constraints = [key_type: :ch_string, value_type: [ch_string: [nullable?: true]]]

      assert {:parameterized, {Ch, {:map, :string, {:nullable, :string}}}} =
               type = Ash.Type.storage_type(ChMap, constraints)

      assert encode_ch_type(type) == "Map(String, Nullable(String))"
    end

    test "returns nullable map" do
      assert {:parameterized, {Ch, {:nullable, {:map, :string, :string}}}} =
               type =
               Ash.Type.storage_type(ChMap,
                 nullable?: true,
                 key_type: :ch_string,
                 value_type: :ch_string
               )

      assert encode_ch_type(type) == "Nullable(Map(String, String))"
    end
  end

  describe "cast_input/2" do
    test "casts input to map" do
      assert {:ok, %{"name" => "John", "age" => 30}} =
               Ash.Type.cast_input(ChMap, %{"name" => "John", "age" => 30}, [])
    end

    test "casts binary to map" do
      assert {:ok, %{"name" => "John", "age" => 30}} =
               Ash.Type.cast_input(ChMap, ~s({"name": "John", "age": 30}), [])
    end

    test "returns error for invalid binary input" do
      assert {:error, "is invalid"} =
               Ash.Type.cast_input(ChMap, "invalid", [])
    end

    test "returns error for invalid map input" do
      assert {:error, "is invalid"} = Ash.Type.cast_input(ChMap, [name: "John", age: 30], [])
    end

    test "returns nil for empty input" do
      assert {:ok, nil} = Ash.Type.cast_input(ChMap, "", [])
    end

    test "accept nil input" do
      assert {:ok, nil} = Ash.Type.cast_input(ChMap, nil, [])
    end
  end

  describe "dump_to_native/2" do
    test "dumps internal representation to ClickHouse type" do
      assert {:ok, %{"name" => "John", "age" => 30}} =
               Ash.Type.dump_to_native(ChMap, %{"name" => "John", "age" => 30}, [])
    end
  end

  describe "matches_type?/2" do
    test "returns true for valid map" do
      assert Ash.Type.matches_type?(ChMap, %{"name" => "John", "age" => 30}, [])
    end

    test "returns false for invalid map" do
      refute Ash.Type.matches_type?(ChMap, "invalid", [])
    end
  end

  describe "generator" do
    test "generates maps with the given fields" do
      constraints = [fields: [:name, :age], key_type: :ch_string, value_type: :ch_string]
      generated_maps = Enum.take(Ash.Type.generator(ChMap, constraints), 100) |> Enum.uniq()

      assert Enum.all?(generated_maps, fn map ->
               Map.keys(map) == [:name, :age]
             end)
    end

    test "generates maps with the given fields and values" do
      constraints = [fields: [:name, :age], key_type: :ch_string, value_type: :ch_string]
      generated_maps = Enum.take(Ash.Type.generator(ChMap, constraints), 100) |> Enum.uniq()

      assert Enum.all?(generated_maps, fn map ->
               Map.keys(map) == [:name, :age]
             end)
    end

    test "generates maps when no fields are provided" do
      constraints = [key_type: :ch_atom, value_type: :ch_string]
      generated_maps = Enum.take(Ash.Type.generator(ChMap, constraints), 100) |> Enum.uniq()

      assert Enum.all?(generated_maps, fn map ->
               Enum.all?(map, fn {key, value} ->
                 is_atom(key) and is_binary(value)
               end)
             end)
    end
  end

  describe "apply_constraints/2" do
    test "applies constraints to map" do
      assert {:ok, %{age: "30", name: "John"}} =
               Ash.Type.apply_constraints(ChMap, %{name: "John", age: "30"},
                 fields: [:name, :age],
                 key_type: :ch_atom,
                 value_type: :ch_string
               )

      assert {:ok, %{"age" => "30", "name" => "John"}} =
               Ash.Type.apply_constraints(ChMap, %{"name" => "John", "age" => "30"},
                 fields: [:name, :age],
                 key_type: :ch_string,
                 value_type: :ch_string
               )
    end

    test "returns error for missing fields" do
      assert {:error, [[message: "field must be present", field: "age"]]} =
               Ash.Type.apply_constraints(ChMap, %{"name" => "John"},
                 fields: ["name", "age"],
                 key_type: :ch_string,
                 value_type: :ch_string
               )
    end

    test "returns error for invalid fields" do
      assert {:error, [[message: "is invalid", field: "age"]]} =
               Ash.Type.apply_constraints(ChMap, %{"name" => "John", "age" => 30},
                 fields: ["name", "age"],
                 key_type: :ch_string,
                 value_type: :ch_string
               )
    end
  end
end
