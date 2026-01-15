defmodule AshClickhouse.Type.ChAtomTest do
  use ExUnit.Case, async: true
  alias AshClickhouse.Type.ChAtom

  import AshClickhouse.Test.Helper, only: [encode_ch_type: 1]

  describe "storage_types" do
    test "returns correct ClickHouse type without constraints" do
      assert {:parameterized, {Ch, {:low_cardinality, :string}}} = type = ChAtom.storage_type([])

      assert encode_ch_type(type) == "LowCardinality(String)"
    end

    test "returns nullable ClickHouse type with nullable constraint" do
      assert {:parameterized, {Ch, {:low_cardinality, {:nullable, :string}}}} =
               type =
               ChAtom.storage_type(nullable?: true)

      assert encode_ch_type(type) == "LowCardinality(Nullable(String))"
    end
  end

  describe "matches_type?/2" do
    test "returns true for atoms" do
      assert ChAtom.matches_type?(:example, [])
      assert ChAtom.matches_type?(:another_atom, [])
      assert ChAtom.matches_type?(nil, [])
    end

    test "returns false for non-atoms" do
      refute ChAtom.matches_type?("string", [])
      refute ChAtom.matches_type?(123, [])
      refute ChAtom.matches_type?([], [])
    end
  end

  describe "generator" do
    test "generates atoms from one_of constraint" do
      constraints = [one_of: [:foo, :bar, :baz]]
      generated_atoms = Enum.take(ChAtom.generator(constraints), 100) |> Enum.uniq()
      assert Enum.all?(generated_atoms, fn atom -> atom in [:foo, :bar, :baz] end)
    end

    test "generates default atoms when no one_of constraint is provided" do
      generated_atoms = Enum.take(ChAtom.generator([]), 100) |> Enum.uniq()
      assert Enum.all?(generated_atoms, fn atom -> atom in [:example, :atom, :value] end)
    end
  end

  describe "apply_constraints/2" do
    test "returns ok for valid atom in one_of constraint" do
      assert ChAtom.apply_constraints(:foo, one_of: [:foo, :bar, :baz]) == {:ok, :foo}
    end

    test "returns error for invalid atom not in one_of constraint" do
      {:error, errors} = ChAtom.apply_constraints(:invalid, one_of: [:foo, :bar, :baz])
      assert length(errors) == 1

      [error] = errors
      assert error[:message] =~ "atom must be one of"
      assert error[:value] == :invalid
    end

    test "returns ok for nil value" do
      assert ChAtom.apply_constraints(nil, one_of: [:foo, :bar, :baz]) == {:ok, nil}
    end
  end

  describe "cast_input/2" do
    test "casts valid atom correctly" do
      assert ChAtom.cast_input(:example, []) == {:ok, :example}
      assert ChAtom.cast_input("string", []) == {:ok, :string}
      assert ChAtom.cast_input(nil, []) == {:ok, nil}

      assert ChAtom.cast_input("non-existing-atom", unsafe_to_atom?: true) ==
               {:ok, :"non-existing-atom"}
    end

    test "returns error for non-atom inputs" do
      assert :error = ChAtom.cast_input(123, [])
      assert :error = ChAtom.cast_input([], [])
      assert :error = ChAtom.cast_input("non-existing-atom-2", [])
    end
  end

  describe "cast_stored/2" do
    test "casts stored string to atom" do
      assert ChAtom.cast_stored("example", []) == {:ok, :example}
      assert ChAtom.cast_stored(nil, []) == {:ok, nil}
    end

    test "returns error for non-string stored values" do
      assert :error = ChAtom.cast_stored(123, [])
      assert :error = ChAtom.cast_stored([], [])
    end

    test "casts stored string to atom with unsafe_to_atom? constraint" do
      assert ChAtom.cast_stored("dynamic_atom", unsafe_to_atom?: true) == {:ok, :dynamic_atom}
    end
  end

  describe "dump_to_native" do
    test "dumps atom to string correctly" do
      assert ChAtom.dump_to_native(:example, []) == {:ok, "example"}
      assert ChAtom.dump_to_native(nil, []) == {:ok, nil}
    end

    test "returns error for non-atom values" do
      assert :error = ChAtom.dump_to_native(123, [])
      assert :error = ChAtom.dump_to_native([], [])
    end
  end
end
