defmodule AshClickhouse.Type.ChAtomTest do
  use ExUnit.Case, async: true
  alias AshClickhouse.Type.ChAtom

  import AshClickhouse.Test.Helper, only: [encode_ch_type: 1]

  describe "storage_types" do
    test "returns correct ClickHouse type without constraints" do
      assert {:parameterized, {Ch, {:low_cardinality, :string}}} =
               type = Ash.Type.storage_type(ChAtom, [])

      assert encode_ch_type(type) == "LowCardinality(String)"
    end

    test "returns correct ClickHouse type without constraints for array version" do
      assert {:array, {:parameterized, {Ch, {:low_cardinality, :string}}} = subtype} =
               Ash.Type.storage_type({:array, ChAtom}, [])

      assert encode_ch_type({:array, subtype}) == "Array(LowCardinality(String))"
    end

    test "returns nullable ClickHouse type with nullable constraint" do
      assert {:parameterized, {Ch, {:low_cardinality, {:nullable, :string}}}} =
               type =
               Ash.Type.storage_type(ChAtom, nullable?: true)

      assert encode_ch_type(type) == "LowCardinality(Nullable(String))"
    end

    test "returns nullable ClickHouse type with nullable constraint for array version" do
      assert {:array, {:parameterized, {Ch, {:low_cardinality, {:nullable, :string}}}} = subtype} =
               Ash.Type.storage_type({:array, ChAtom}, nullable?: true)

      assert encode_ch_type({:array, subtype}) == "Array(LowCardinality(Nullable(String)))"
    end
  end

  describe "matches_type?/2" do
    test "returns true for atoms" do
      assert Ash.Type.matches_type?(ChAtom, :example, [])
      assert Ash.Type.matches_type?(ChAtom, :another_atom, [])
      assert Ash.Type.matches_type?(ChAtom, nil, [])
      assert Ash.Type.matches_type?({:array, ChAtom}, [:example, nil], [])
    end

    test "returns false for non-atoms" do
      refute Ash.Type.matches_type?(ChAtom, "string", [])
      refute Ash.Type.matches_type?(ChAtom, 123, [])
      refute Ash.Type.matches_type?(ChAtom, [], [])
    end
  end

  describe "generator" do
    test "generates atoms from one_of constraint" do
      constraints = [one_of: [:foo, :bar, :baz]]
      generated_atoms = Enum.take(Ash.Type.generator(ChAtom, constraints), 100) |> Enum.uniq()
      assert Enum.all?(generated_atoms, fn atom -> atom in [:foo, :bar, :baz] end)
    end

    test "generates arrays of atoms from one_of constraint for array version" do
      constraints = [one_of: [:foo, :bar, :baz]]
      generated_arrays = Enum.take(Ash.Type.generator({:array, ChAtom}, items: constraints), 100)

      assert Enum.all?(generated_arrays, fn arr ->
               is_list(arr) and Enum.all?(arr, fn atom -> atom in [:foo, :bar, :baz] end)
             end)
    end

    test "generates default atoms when no one_of constraint is provided" do
      generated_atoms = Enum.take(Ash.Type.generator(ChAtom, []), 100) |> Enum.uniq()
      assert Enum.all?(generated_atoms, fn atom -> atom in [:example, :atom, :value] end)
    end

    test "generates arrays of default atoms when no one_of constraint is provided for array version" do
      generated_arrays = Enum.take(Ash.Type.generator({:array, ChAtom}, []), 100)

      assert Enum.all?(generated_arrays, fn arr ->
               is_list(arr) and Enum.all?(arr, fn atom -> atom in [:example, :atom, :value] end)
             end)
    end
  end

  describe "equal?/2" do
    test "returns true for equal atoms" do
      assert Ash.Type.equal?(ChAtom, :foo, :foo)
      assert Ash.Type.equal?(ChAtom, nil, nil)
    end

    test "returns false for different atoms" do
      refute Ash.Type.equal?(ChAtom, :foo, :bar)
      refute Ash.Type.equal?(ChAtom, :foo, nil)
      refute Ash.Type.equal?(ChAtom, nil, :bar)
    end

    test "returns true for equal arrays of atoms" do
      assert Ash.Type.equal?({:array, ChAtom}, [:foo, :bar], [:foo, :bar])
      assert Ash.Type.equal?({:array, ChAtom}, [], [])
    end

    test "returns false for different arrays of atoms" do
      refute Ash.Type.equal?({:array, ChAtom}, [:foo, :bar], [:bar, :foo])
      refute Ash.Type.equal?({:array, ChAtom}, [:foo], [:foo, :bar])
      refute Ash.Type.equal?({:array, ChAtom}, [:foo, nil], [:foo, :bar])
    end
  end

  describe "simple_equality?/0" do
    test "returns true for ChAtom" do
      assert Ash.Type.simple_equality?(ChAtom)
    end

    test "returns true for {:array, ChAtom}" do
      assert Ash.Type.simple_equality?({:array, ChAtom})
    end
  end

  describe "apply_constraints/2" do
    test "returns ok for valid atom in one_of constraint" do
      assert Ash.Type.apply_constraints(ChAtom, :foo, one_of: [:foo, :bar, :baz]) == {:ok, :foo}
    end

    test "returns error for invalid atom not in one_of constraint" do
      {:error, errors} = Ash.Type.apply_constraints(ChAtom, :invalid, one_of: [:foo, :bar, :baz])
      assert length(errors) == 1

      [error] = errors
      assert error[:message] =~ "atom must be one of"
      assert error[:value] == :invalid
    end

    test "returns ok for nil value" do
      assert Ash.Type.apply_constraints(ChAtom, nil, one_of: [:foo, :bar, :baz]) == {:ok, nil}
    end

    test "returns ok for valid array of atoms in one_of constraint" do
      assert Ash.Type.apply_constraints({:array, ChAtom}, [:foo, :bar],
               items: [one_of: [:foo, :bar, :baz]]
             ) == {:ok, [:foo, :bar]}
    end

    test "returns error for array with invalid atom not in one_of constraint" do
      {:error, errors} =
        Ash.Type.apply_constraints({:array, ChAtom}, [:foo, :invalid],
          items: [one_of: [:foo, :bar, :baz]]
        )

      assert length(errors) == 1

      [error] = errors
      assert error[:message] =~ "atom must be one of"
      assert error[:value] == :invalid
    end
  end

  describe "cast_input/2" do
    test "casts valid atom correctly" do
      assert Ash.Type.cast_input(ChAtom, :example, []) == {:ok, :example}
      assert Ash.Type.cast_input(ChAtom, "string", []) == {:ok, :string}
      assert Ash.Type.cast_input(ChAtom, nil, []) == {:ok, nil}

      assert Ash.Type.cast_input(ChAtom, "non-existing-atom", unsafe_to_atom?: true) ==
               {:ok, :"non-existing-atom"}

      # Array version
      assert Ash.Type.cast_input({:array, ChAtom}, [:example, :atom], []) ==
               {:ok, [:example, :atom]}

      assert Ash.Type.cast_input({:array, ChAtom}, ["string", "atom"], []) ==
               {:ok, [:string, :atom]}

      assert Ash.Type.cast_input({:array, ChAtom}, [nil, :value], []) == {:ok, [nil, :value]}

      assert Ash.Type.cast_input({:array, ChAtom}, ["non-existing-atom", "another", nil],
               unsafe_to_atom?: true
             ) ==
               {:ok, [:"non-existing-atom", :another, nil]}
    end

    test "returns error for non-atom inputs" do
      assert {:error, "is invalid"} = Ash.Type.cast_input(ChAtom, 123, [])
      assert {:error, "is invalid"} = Ash.Type.cast_input(ChAtom, [], [])
      assert {:error, "is invalid"} = Ash.Type.cast_input(ChAtom, "non-existing-atom-2", [])

      # Array version
      assert {:error, [[message: "is invalid", index: 0, path: [0]]]} =
               Ash.Type.cast_input({:array, ChAtom}, [123, :foo], [])

      assert {:error, [[message: "is invalid", index: 1, path: [1]]]} =
               Ash.Type.cast_input({:array, ChAtom}, [:bar, "non-existing-atom-2"], [])

      assert {:error, [[message: "is invalid", index: 0, path: [0]]]} =
               Ash.Type.cast_input({:array, ChAtom}, [[], :baz], [])
    end
  end

  describe "cast_atomic/2" do
    test "casts valid atom for atomic update" do
      assert Ash.Type.cast_atomic(ChAtom, :example, []) == {:ok, :example}
      assert Ash.Type.cast_atomic(ChAtom, "string", []) == {:ok, :string}
      assert Ash.Type.cast_atomic(ChAtom, nil, []) == {:ok, nil}

      assert Ash.Type.cast_atomic(ChAtom, "non-existing-atom", unsafe_to_atom?: true) ==
               {:ok, :"non-existing-atom"}

      # Array version
      assert Ash.Type.cast_atomic({:array, ChAtom}, [:example, :atom], []) ==
               {:ok, [:example, :atom]}

      assert Ash.Type.cast_atomic({:array, ChAtom}, ["string", "atom"], []) ==
               {:ok, [:string, :atom]}

      assert Ash.Type.cast_atomic({:array, ChAtom}, [nil, :value], nil_items?: true) ==
               {:ok, [nil, :value]}

      assert Ash.Type.cast_atomic({:array, ChAtom}, ["non-existing-atom", "another", nil],
               nil_items?: true,
               items: [unsafe_to_atom?: true]
             ) ==
               {:ok, [:"non-existing-atom", :another, nil]}
    end

    test "returns error for non-atom inputs in atomic update" do
      assert {:error, "is invalid"} = Ash.Type.cast_atomic(ChAtom, 123, [])
      assert {:error, "is invalid"} = Ash.Type.cast_atomic(ChAtom, [], [])
      assert {:error, "is invalid"} = Ash.Type.cast_atomic(ChAtom, "non-existing-atom-2", [])

      # Array version
      assert {:error, [[message: "is invalid", index: 0, path: [0]]]} =
               Ash.Type.cast_atomic({:array, ChAtom}, [123, :foo], [])

      assert {:error, [[message: "is invalid", index: 1, path: [1]]]} =
               Ash.Type.cast_atomic({:array, ChAtom}, [:bar, "non-existing-atom-2"], [])

      assert {:error, [[message: "is invalid", index: 0, path: [0]]]} =
               Ash.Type.cast_atomic({:array, ChAtom}, [[], :baz], [])
    end
  end

  describe "cast_stored/2" do
    test "casts stored string to atom" do
      assert Ash.Type.cast_stored(ChAtom, "example", []) == {:ok, :example}
      assert Ash.Type.cast_stored(ChAtom, nil, []) == {:ok, nil}
    end

    test "returns error for non-string stored values" do
      assert :error = Ash.Type.cast_stored(ChAtom, 123, [])
      assert :error = Ash.Type.cast_stored(ChAtom, [], [])
    end

    test "casts stored string to atom with unsafe_to_atom? constraint" do
      assert Ash.Type.cast_stored(ChAtom, "dynamic_atom", unsafe_to_atom?: true) ==
               {:ok, :dynamic_atom}
    end
  end

  describe "dump_to_native" do
    test "dumps atom to string correctly" do
      assert Ash.Type.dump_to_native(ChAtom, :example, []) == {:ok, "example"}
      assert Ash.Type.dump_to_native(ChAtom, nil, []) == {:ok, nil}
    end

    test "returns error for non-atom values" do
      assert :error = Ash.Type.dump_to_native(ChAtom, 123, [])
      assert :error = Ash.Type.dump_to_native(ChAtom, [], [])
    end
  end
end
