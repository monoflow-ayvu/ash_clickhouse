defmodule AshClickhouse.Type.ChEnum16Test do
  use ExUnit.Case, async: true

  defmodule SimpleEnum do
    use AshClickhouse.Type.ChEnum16,
      values: [
        low: 100,
        medium: 200,
        high: 300
      ]
  end

  defmodule LabeledEnum do
    use AshClickhouse.Type.ChEnum16,
      values: [
        low: [value: 100, label: "Low priority", description: "Low priority task"],
        medium: [value: 200, label: "Medium priority"],
        high: [value: 300, description: "High priority task"]
      ]
  end

  alias AshClickhouse.Type.ChEnum16
  alias AshClickhouse.Test.Resource.Types.TestEnum16

  import AshClickhouse.Test.Helper, only: [encode_ch_type: 1]

  describe "storage_type/1" do
    test "returns Enum16 ClickHouse type" do
      assert {:parameterized, {Ch, _}} = type = SimpleEnum.storage_type([])
      type_str = encode_ch_type(type)
      assert type_str =~ "Enum16"
      assert type_str =~ "low"
      assert type_str =~ "100"
    end

    test "includes boundary values from TestEnum16" do
      assert {:parameterized, {Ch, _}} = type = TestEnum16.storage_type([])
      type_str = encode_ch_type(type)
      assert type_str =~ "Enum16"
      assert type_str =~ "-32768"
      assert type_str =~ "32767"
    end
  end

  describe "values/0" do
    test "returns the list of valid atom values" do
      assert SimpleEnum.values() == [:low, :medium, :high]
    end
  end

  describe "label/1" do
    test "returns humanized label when no explicit label is defined" do
      assert SimpleEnum.label(:low) == "Low"
      assert SimpleEnum.label(:medium) == "Medium"
      assert SimpleEnum.label(:high) == "High"
    end

    test "returns explicit label when defined" do
      assert LabeledEnum.label(:low) == "Low priority"
      assert LabeledEnum.label(:medium) == "Medium priority"
    end

    test "returns humanized fallback when only description is defined" do
      assert LabeledEnum.label(:high) == "High"
    end
  end

  describe "description/1" do
    test "returns nil when no description is set" do
      assert SimpleEnum.description(:low) == nil
      assert LabeledEnum.description(:medium) == nil
    end

    test "returns description when defined" do
      assert LabeledEnum.description(:low) == "Low priority task"
      assert LabeledEnum.description(:high) == "High priority task"
    end
  end

  describe "details/1" do
    test "returns a map with label and description keys" do
      details = SimpleEnum.details(:low)
      assert is_map(details)
      assert Map.has_key?(details, :label)
      assert Map.has_key?(details, :description)
    end

    test "returns correct label and description for enum with explicit values" do
      details = LabeledEnum.details(:low)
      assert details[:label] == "Low priority"
      assert details[:description] == "Low priority task"
    end
  end

  describe "matches_type?/2" do
    test "returns true for valid enum values" do
      assert SimpleEnum.matches_type?(:low, [])
      assert SimpleEnum.matches_type?(:medium, [])
      assert SimpleEnum.matches_type?(:high, [])
    end

    test "returns false for invalid values" do
      refute SimpleEnum.matches_type?(:other, [])
      refute SimpleEnum.matches_type?("low", [])
      refute SimpleEnum.matches_type?(nil, [])
    end
  end

  describe "match/1" do
    test "matches atom values directly" do
      assert SimpleEnum.match(:low) == {:ok, :low}
      assert SimpleEnum.match(:medium) == {:ok, :medium}
      assert SimpleEnum.match(:high) == {:ok, :high}
    end

    test "matches string equivalents of atoms" do
      assert SimpleEnum.match("low") == {:ok, :low}
      assert SimpleEnum.match("high") == {:ok, :high}
    end

    test "matches by integer value" do
      assert SimpleEnum.match(100) == {:ok, :low}
      assert SimpleEnum.match(200) == {:ok, :medium}
      assert SimpleEnum.match(300) == {:ok, :high}
    end

    test "matches boundary values from TestEnum16" do
      assert TestEnum16.match(-32768) == {:ok, :enum16_min}
      assert TestEnum16.match(0) == {:ok, :enum16_zero}
      assert TestEnum16.match(32767) == {:ok, :enum16_max}
    end

    test "matches case-insensitively" do
      assert SimpleEnum.match("LOW") == {:ok, :low}
      assert SimpleEnum.match("High") == {:ok, :high}
    end

    test "returns :error for unrecognized values" do
      assert SimpleEnum.match(:unknown) == :error
      assert SimpleEnum.match("unknown") == :error
      assert SimpleEnum.match(99_999) == :error
    end
  end

  describe "match?/1" do
    test "returns true for valid values" do
      assert SimpleEnum.match?(:low)
      assert SimpleEnum.match?("medium")
      assert SimpleEnum.match?(300)
    end

    test "returns false for invalid values" do
      refute SimpleEnum.match?(:invalid)
      refute SimpleEnum.match?("nope")
      refute SimpleEnum.match?(99_999)
    end
  end

  describe "cast_input/2" do
    test "casts nil to nil" do
      assert SimpleEnum.cast_input(nil, []) == {:ok, nil}
    end

    test "casts atom values" do
      assert SimpleEnum.cast_input(:low, []) == {:ok, :low}
      assert SimpleEnum.cast_input(:high, []) == {:ok, :high}
    end

    test "casts string values" do
      assert SimpleEnum.cast_input("medium", []) == {:ok, :medium}
    end

    test "casts integer values" do
      assert SimpleEnum.cast_input(100, []) == {:ok, :low}
      assert SimpleEnum.cast_input(300, []) == {:ok, :high}
    end

    test "returns error for invalid inputs" do
      assert SimpleEnum.cast_input(:invalid, []) == :error
      assert SimpleEnum.cast_input(99_999, []) == :error
    end
  end

  describe "cast_stored/2" do
    test "casts nil to nil" do
      assert SimpleEnum.cast_stored(nil, []) == {:ok, nil}
    end

    test "casts stored string values" do
      assert SimpleEnum.cast_stored("low", []) == {:ok, :low}
    end

    test "casts stored integer values" do
      assert SimpleEnum.cast_stored(200, []) == {:ok, :medium}
    end
  end

  describe "dump_to_native/2" do
    test "dumps nil to nil" do
      assert SimpleEnum.dump_to_native(nil, []) == {:ok, nil}
    end

    test "dumps atom values as strings" do
      assert SimpleEnum.dump_to_native(:low, []) == {:ok, "low"}
      assert SimpleEnum.dump_to_native(:high, []) == {:ok, "high"}
    end
  end

  describe "generator/1" do
    test "generates valid enum values" do
      results = Enum.take(SimpleEnum.generator([]), 30) |> Enum.uniq()
      assert Enum.all?(results, &SimpleEnum.match?/1)
    end
  end

  describe "build_values_map/1" do
    test "builds a map from simple key-integer pairs" do
      map = ChEnum16.build_values_map(a: 1000, b: 2000)
      assert map[:a][:label] == "A"
      assert map[:b][:label] == "B"
      assert map[:a][:description] == nil
    end

    test "preserves explicit labels" do
      map = ChEnum16.build_values_map(my_key: [value: 1000, label: "Custom"])
      assert map[:my_key][:label] == "Custom"
    end

    test "generates humanized label when only description is provided" do
      map = ChEnum16.build_values_map(my_key: [value: 1000, description: "A description"])
      assert map[:my_key][:label] == "My key"
      assert map[:my_key][:description] == "A description"
    end
  end

  describe "build_values_list/1" do
    test "extracts keys from key-integer pairs" do
      list = ChEnum16.build_values_list(a: 100, b: 200, c: 300)
      assert list == [:a, :b, :c]
    end

    test "extracts keys from key-details pairs" do
      list = ChEnum16.build_values_list(a: [value: 100], b: [value: 200])
      assert list == [:a, :b]
    end
  end

  describe "verify_values!/1" do
    test "raises when values is nil" do
      assert_raise RuntimeError, ~r/Must provide `values`/, fn ->
        ChEnum16.verify_values!(nil)
      end
    end

    test "raises when values is not a list" do
      assert_raise RuntimeError, ~r/Must provide a list/, fn ->
        ChEnum16.verify_values!(:not_a_list)
      end
    end

    test "raises when a value has non-integer value" do
      assert_raise RuntimeError, ~r/must be a list of/, fn ->
        ChEnum16.verify_values!(a: "not_an_integer")
      end
    end

    test "accepts valid key-integer tuples" do
      assert ChEnum16.verify_values!(a: 1000, b: -32768) == [a: 1000, b: -32768]
    end

    test "accepts valid key-details keyword lists" do
      values = [a: [value: 1000, label: "A", description: "desc"]]
      assert ChEnum16.verify_values!(values) == values
    end
  end
end
