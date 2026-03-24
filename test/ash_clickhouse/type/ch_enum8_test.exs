defmodule AshClickhouse.Type.ChEnum8Test do
  use ExUnit.Case, async: true

  defmodule SimpleEnum do
    use AshClickhouse.Type.ChEnum8,
      values: [
        pending: 1,
        open: 2,
        closed: 3
      ]
  end

  defmodule LabeledEnum do
    use AshClickhouse.Type.ChEnum8,
      values: [
        pending: [value: 1, label: "Pending ticket", description: "A pending ticket"],
        open: [value: 2, label: "Open ticket"],
        closed: [value: 3, description: "A closed ticket"]
      ]
  end

  alias AshClickhouse.Type.ChEnum8
  alias AshClickhouse.Test.Resource.Types.TestEnum8

  import AshClickhouse.Test.Helper, only: [encode_ch_type: 1]

  describe "storage_type/1" do
    test "returns Enum8 ClickHouse type" do
      assert {:parameterized, {Ch, _}} = type = SimpleEnum.storage_type([])
      type_str = encode_ch_type(type)
      assert type_str =~ "Enum8"
      assert type_str =~ "pending"
      assert type_str =~ "open"
      assert type_str =~ "closed"
    end

    test "includes boundary values from TestEnum8" do
      assert {:parameterized, {Ch, _}} = type = TestEnum8.storage_type([])
      type_str = encode_ch_type(type)
      assert type_str =~ "Enum8"
      assert type_str =~ "-128"
      assert type_str =~ "127"
    end
  end

  describe "values/0" do
    test "returns the list of valid atom values" do
      assert SimpleEnum.values() == [:pending, :open, :closed]
    end
  end

  describe "label/1" do
    test "returns humanized label when no explicit label is defined" do
      assert SimpleEnum.label(:pending) == "Pending"
      assert SimpleEnum.label(:open) == "Open"
      assert SimpleEnum.label(:closed) == "Closed"
    end

    test "returns explicit label when defined" do
      assert LabeledEnum.label(:pending) == "Pending ticket"
      assert LabeledEnum.label(:open) == "Open ticket"
    end

    test "returns humanized fallback when only description is defined" do
      assert LabeledEnum.label(:closed) == "Closed"
    end
  end

  describe "description/1" do
    test "returns nil when no description is set" do
      assert SimpleEnum.description(:pending) == nil
      assert LabeledEnum.description(:open) == nil
    end

    test "returns description when defined" do
      assert LabeledEnum.description(:pending) == "A pending ticket"
      assert LabeledEnum.description(:closed) == "A closed ticket"
    end
  end

  describe "details/1" do
    test "returns a map with label and description keys" do
      details = SimpleEnum.details(:pending)
      assert is_map(details)
      assert Map.has_key?(details, :label)
      assert Map.has_key?(details, :description)
    end

    test "returns correct label and description for enum with explicit values" do
      details = LabeledEnum.details(:pending)
      assert details[:label] == "Pending ticket"
      assert details[:description] == "A pending ticket"
    end
  end

  describe "matches_type?/2" do
    test "returns true for valid enum values" do
      assert SimpleEnum.matches_type?(:pending, [])
      assert SimpleEnum.matches_type?(:open, [])
      assert SimpleEnum.matches_type?(:closed, [])
    end

    test "returns false for invalid values" do
      refute SimpleEnum.matches_type?(:other, [])
      refute SimpleEnum.matches_type?("pending", [])
      refute SimpleEnum.matches_type?(nil, [])
    end
  end

  describe "match/1" do
    test "matches atom values directly" do
      assert SimpleEnum.match(:pending) == {:ok, :pending}
      assert SimpleEnum.match(:open) == {:ok, :open}
      assert SimpleEnum.match(:closed) == {:ok, :closed}
    end

    test "matches string equivalents of atoms" do
      assert SimpleEnum.match("pending") == {:ok, :pending}
      assert SimpleEnum.match("closed") == {:ok, :closed}
    end

    test "matches by integer value" do
      assert SimpleEnum.match(1) == {:ok, :pending}
      assert SimpleEnum.match(2) == {:ok, :open}
      assert SimpleEnum.match(3) == {:ok, :closed}
    end

    test "matches boundary values from TestEnum8" do
      assert TestEnum8.match(-128) == {:ok, :enum8_min}
      assert TestEnum8.match(0) == {:ok, :enum8_zero}
      assert TestEnum8.match(127) == {:ok, :enum8_max}
    end

    test "matches case-insensitively" do
      assert SimpleEnum.match("PENDING") == {:ok, :pending}
      assert SimpleEnum.match("Closed") == {:ok, :closed}
    end

    test "returns :error for unrecognized values" do
      assert SimpleEnum.match(:unknown) == :error
      assert SimpleEnum.match("unknown") == :error
      assert SimpleEnum.match(999) == :error
    end
  end

  describe "match?/1" do
    test "returns true for valid values" do
      assert SimpleEnum.match?(:pending)
      assert SimpleEnum.match?("open")
      assert SimpleEnum.match?(3)
    end

    test "returns false for invalid values" do
      refute SimpleEnum.match?(:invalid)
      refute SimpleEnum.match?("nope")
      refute SimpleEnum.match?(999)
    end
  end

  describe "cast_input/2" do
    test "casts nil to nil" do
      assert SimpleEnum.cast_input(nil, []) == {:ok, nil}
    end

    test "casts atom values" do
      assert SimpleEnum.cast_input(:pending, []) == {:ok, :pending}
      assert SimpleEnum.cast_input(:closed, []) == {:ok, :closed}
    end

    test "casts string values" do
      assert SimpleEnum.cast_input("open", []) == {:ok, :open}
    end

    test "casts integer values" do
      assert SimpleEnum.cast_input(1, []) == {:ok, :pending}
      assert SimpleEnum.cast_input(3, []) == {:ok, :closed}
    end

    test "returns error for invalid inputs" do
      assert SimpleEnum.cast_input(:invalid, []) == :error
      assert SimpleEnum.cast_input(999, []) == :error
    end
  end

  describe "cast_stored/2" do
    test "casts nil to nil" do
      assert SimpleEnum.cast_stored(nil, []) == {:ok, nil}
    end

    test "casts stored string values" do
      assert SimpleEnum.cast_stored("pending", []) == {:ok, :pending}
    end

    test "casts stored integer values" do
      assert SimpleEnum.cast_stored(2, []) == {:ok, :open}
    end
  end

  describe "dump_to_native/2" do
    test "dumps nil to nil" do
      assert SimpleEnum.dump_to_native(nil, []) == {:ok, nil}
    end

    test "dumps atom values as strings" do
      assert SimpleEnum.dump_to_native(:pending, []) == {:ok, "pending"}
      assert SimpleEnum.dump_to_native(:closed, []) == {:ok, "closed"}
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
      map = ChEnum8.build_values_map(a: 1, b: 2)
      assert map[:a][:label] == "A"
      assert map[:b][:label] == "B"
      assert map[:a][:description] == nil
    end

    test "preserves explicit labels" do
      map = ChEnum8.build_values_map(my_key: [value: 1, label: "Custom Label"])
      assert map[:my_key][:label] == "Custom Label"
    end

    test "generates humanized label when only description is provided" do
      map = ChEnum8.build_values_map(my_key: [value: 1, description: "A description"])
      assert map[:my_key][:label] == "My key"
      assert map[:my_key][:description] == "A description"
    end
  end

  describe "build_values_list/1" do
    test "extracts keys from key-integer pairs" do
      list = ChEnum8.build_values_list(a: 1, b: 2, c: 3)
      assert list == [:a, :b, :c]
    end

    test "extracts keys from key-details pairs" do
      list = ChEnum8.build_values_list(a: [value: 1], b: [value: 2])
      assert list == [:a, :b]
    end
  end

  describe "verify_values!/1" do
    test "raises when values is nil" do
      assert_raise RuntimeError, ~r/Must provide `values`/, fn ->
        ChEnum8.verify_values!(nil)
      end
    end

    test "raises when values is not a list" do
      assert_raise RuntimeError, ~r/Must provide a list/, fn ->
        ChEnum8.verify_values!(:not_a_list)
      end
    end

    test "raises when a value has non-integer value" do
      assert_raise RuntimeError, ~r/must be a list of/, fn ->
        ChEnum8.verify_values!(a: "not_an_integer")
      end
    end

    test "accepts valid key-integer tuples" do
      assert ChEnum8.verify_values!(a: 1, b: -128) == [a: 1, b: -128]
    end

    test "accepts valid key-details keyword lists" do
      values = [a: [value: 1, label: "A", description: "desc"]]
      assert ChEnum8.verify_values!(values) == values
    end
  end
end
