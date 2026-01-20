defmodule AshClickhouse.Type.ChUintTest do
  use ExUnit.Case, async: true
  alias AshClickhouse.Type.ChUint8
  alias AshClickhouse.Type.ChUint16
  alias AshClickhouse.Type.ChUint32
  alias AshClickhouse.Type.ChUint64
  alias AshClickhouse.Type.ChUint128
  alias AshClickhouse.Type.ChUint256

  import AshClickhouse.Test.Helper, only: [encode_ch_type: 1]

  require Ash.Expr

  describe "storage_types" do
    test "returns correct ClickHouse type without constraints" do
      assert {:parameterized, {Ch, :u8}} = type = Ash.Type.storage_type(ChUint8, [])
      assert encode_ch_type(type) == "UInt8"

      assert {:parameterized, {Ch, :u16}} = type = Ash.Type.storage_type(ChUint16, [])
      assert encode_ch_type(type) == "UInt16"

      assert {:parameterized, {Ch, :u32}} = type = Ash.Type.storage_type(ChUint32, [])
      assert encode_ch_type(type) == "UInt32"

      assert {:parameterized, {Ch, :u64}} = type = Ash.Type.storage_type(ChUint64, [])
      assert encode_ch_type(type) == "UInt64"

      assert {:parameterized, {Ch, :u128}} = type = Ash.Type.storage_type(ChUint128, [])
      assert encode_ch_type(type) == "UInt128"

      assert {:parameterized, {Ch, :u256}} = type = Ash.Type.storage_type(ChUint256, [])
      assert encode_ch_type(type) == "UInt256"
    end

    test "returns nullable ClickHouse type with nullable constraint" do
      assert {:parameterized, {Ch, {:nullable, :u8}}} =
               type = Ash.Type.storage_type(ChUint8, nullable?: true)

      assert encode_ch_type(type) == "Nullable(UInt8)"

      assert {:parameterized, {Ch, {:nullable, :u16}}} =
               type = Ash.Type.storage_type(ChUint16, nullable?: true)

      assert encode_ch_type(type) == "Nullable(UInt16)"

      assert {:parameterized, {Ch, {:nullable, :u32}}} =
               type = Ash.Type.storage_type(ChUint32, nullable?: true)

      assert encode_ch_type(type) == "Nullable(UInt32)"

      assert {:parameterized, {Ch, {:nullable, :u64}}} =
               type = Ash.Type.storage_type(ChUint64, nullable?: true)

      assert encode_ch_type(type) == "Nullable(UInt64)"

      assert {:parameterized, {Ch, {:nullable, :u128}}} =
               type = Ash.Type.storage_type(ChUint128, nullable?: true)

      assert encode_ch_type(type) == "Nullable(UInt128)"

      assert {:parameterized, {Ch, {:nullable, :u256}}} =
               type = Ash.Type.storage_type(ChUint256, nullable?: true)

      assert encode_ch_type(type) == "Nullable(UInt256)"
    end
  end

  describe "matches_type?/2" do
    test "returns true for integers" do
      assert Ash.Type.matches_type?(ChUint8, 2, [])
      assert Ash.Type.matches_type?(ChUint16, 65535, [])
      assert Ash.Type.matches_type?(ChUint32, 4_294_967_295, [])
      assert Ash.Type.matches_type?(ChUint64, 18_446_744_073_709_551_615, [])

      assert Ash.Type.matches_type?(
               ChUint128,
               340_282_366_920_938_463_463_374_607_431_768_211_455,
               []
             )

      assert Ash.Type.matches_type?(
               ChUint256,
               115_792_089_237_316_195_423_570_985_008_687_907_853_269_984_665_640_564_039_457_584_007_913_129_639_935,
               []
             )
    end

    test "returns false for non-integers" do
      refute Ash.Type.matches_type?(ChUint8, 3.14, [])
      refute Ash.Type.matches_type?(ChUint8, -123, [])
      refute Ash.Type.matches_type?(ChUint16, "string", [])
      refute Ash.Type.matches_type?(ChUint32, nil, [])
      refute Ash.Type.matches_type?(ChUint64, [], [])
      refute Ash.Type.matches_type?(ChUint128, {}, [])
      refute Ash.Type.matches_type?(ChUint256, :atom, [])
      refute Ash.Type.matches_type?(ChUint8, 256, [])
      refute Ash.Type.matches_type?(ChUint16, 65536, [])
      refute Ash.Type.matches_type?(ChUint32, 4_294_967_296, [])
      refute Ash.Type.matches_type?(ChUint64, 18_446_744_073_709_551_616, [])

      refute Ash.Type.matches_type?(
               ChUint128,
               340_282_366_920_938_463_463_374_607_431_768_211_456,
               []
             )

      refute Ash.Type.matches_type?(
               ChUint256,
               115_792_089_237_316_195_423_570_985_008_687_907_853_269_984_665_640_564_039_457_584_007_913_129_639_936,
               []
             )
    end

    test "returns true for arrays of integers" do
      assert Ash.Type.matches_type?({:array, ChUint8}, [0, 255], [])
      assert Ash.Type.matches_type?({:array, ChUint16}, [0, 65535], [])
      assert Ash.Type.matches_type?({:array, ChUint32}, [0, 4_294_967_295], [])
      assert Ash.Type.matches_type?({:array, ChUint64}, [0, 18_446_744_073_709_551_615], [])

      assert Ash.Type.matches_type?(
               {:array, ChUint128},
               [0, 340_282_366_920_938_463_463_374_607_431_768_211_455],
               []
             )

      assert Ash.Type.matches_type?(
               {:array, ChUint256},
               [
                 0,
                 115_792_089_237_316_195_423_570_985_008_687_907_853_269_984_665_640_564_039_457_584_007_913_129_639_935
               ],
               []
             )
    end

    test "returns false for non-integer arrays" do
      refute Ash.Type.matches_type?({:array, ChUint8}, [3.14, 2.71], [])
      refute Ash.Type.matches_type?({:array, ChUint8}, [-123, 271], [])
      refute Ash.Type.matches_type?({:array, ChUint16}, ["string", "test"], [])
      refute Ash.Type.matches_type?({:array, ChUint32}, [[], {}], [])
      refute Ash.Type.matches_type?({:array, ChUint64}, [[], {}], [])
      refute Ash.Type.matches_type?({:array, ChUint128}, [[], {}], [])
      refute Ash.Type.matches_type?({:array, ChUint256}, [[], {}], [])
      refute Ash.Type.matches_type?({:array, ChUint8}, [256, 257], [])
      refute Ash.Type.matches_type?({:array, ChUint16}, [65536, 65537], [])
      refute Ash.Type.matches_type?({:array, ChUint32}, [4_294_967_296, 4_294_967_297], [])

      refute Ash.Type.matches_type?(
               {:array, ChUint64},
               [18_446_744_073_709_551_616, 18_446_744_073_709_551_617],
               []
             )

      refute Ash.Type.matches_type?(
               {:array, ChUint128},
               [
                 340_282_366_920_938_463_463_374_607_431_768_211_456,
                 340_282_366_920_938_463_463_374_607_431_768_211_457
               ],
               []
             )

      refute Ash.Type.matches_type?(
               {:array, ChUint256},
               [
                 115_792_089_237_316_195_423_570_985_008_687_907_853_269_984_665_640_564_039_457_584_007_913_129_639_936,
                 115_792_089_237_316_195_423_570_985_008_687_907_853_269_984_665_640_564_039_457_584_007_913_129_639_937
               ],
               []
             )
    end
  end

  describe "cast_input/2" do
    test "casts valid integer correctly" do
      assert Ash.Type.cast_input(ChUint8, 123, []) == {:ok, 123}
      assert Ash.Type.cast_input(ChUint16, 123, []) == {:ok, 123}
      assert Ash.Type.cast_input(ChUint32, 123, []) == {:ok, 123}
      assert Ash.Type.cast_input(ChUint64, 123, []) == {:ok, 123}
    end

    test "returns error for non-integer inputs" do
      assert {:error, "must be an integer between 0 and (2^8 - 1)"} =
               Ash.Type.cast_input(ChUint8, 3.14, [])

      assert {:error, "must be an integer between 0 and (2^16 - 1)"} =
               Ash.Type.cast_input(ChUint16, "string", [])

      assert {:error, "must be an integer between 0 and (2^32 - 1)"} =
               Ash.Type.cast_input(ChUint32, -123, [])

      assert {:error, "must be an integer between 0 and (2^64 - 1)"} =
               Ash.Type.cast_input(ChUint64, [], [])

      assert {:error, "must be an integer between 0 and (2^128 - 1)"} =
               Ash.Type.cast_input(ChUint128, {}, [])

      assert {:error, "must be an integer between 0 and (2^256 - 1)"} =
               Ash.Type.cast_input(ChUint256, :atom, [])
    end
  end


  describe "cast_stored/2" do
    test "casts valid integer correctly" do
      assert Ash.Type.cast_stored(ChUint8, 123, []) == {:ok, 123}
      assert Ash.Type.cast_stored(ChUint8, "123", []) == {:ok, 123}
      assert Ash.Type.cast_stored(ChUint16, 123, []) == {:ok, 123}
      assert Ash.Type.cast_stored(ChUint16, "123", []) == {:ok, 123}
      assert Ash.Type.cast_stored(ChUint32, 123, []) == {:ok, 123}
      assert Ash.Type.cast_stored(ChUint32, "123", []) == {:ok, 123}
      assert Ash.Type.cast_stored(ChUint64, 123, []) == {:ok, 123}
      assert Ash.Type.cast_stored(ChUint64, "123", []) == {:ok, 123}
      assert Ash.Type.cast_stored(ChUint128, 123, []) == {:ok, 123}
      assert Ash.Type.cast_stored(ChUint128, "123", []) == {:ok, 123}
      assert Ash.Type.cast_stored(ChUint256, 123, []) == {:ok, 123}
    end
  end

  describe "dump_to_native/2" do
    test "dumps valid integer correctly" do
      assert Ash.Type.dump_to_native(ChUint8, 123, []) == {:ok, 123}
      assert Ash.Type.dump_to_native(ChUint16, 123, []) == {:ok, 123}
      assert Ash.Type.dump_to_native(ChUint32, 123, []) == {:ok, 123}
      assert Ash.Type.dump_to_native(ChUint64, 123, []) == {:ok, 123}
      assert Ash.Type.dump_to_native(ChUint128, 123, []) == {:ok, 123}
      assert Ash.Type.dump_to_native(ChUint256, 123, []) == {:ok, 123}
    end
  end

  describe "generator/1" do
    test "generates valid integer correctly" do
      assert Ash.Type.generator(ChUint8, [])
             |> Enum.take(100)
             |> Enum.uniq()
             |> Enum.all?(&Ash.Type.matches_type?(ChUint8, &1, []))

      assert Ash.Type.generator(ChUint16, [])
             |> Enum.take(100)
             |> Enum.uniq()
             |> Enum.all?(&Ash.Type.matches_type?(ChUint16, &1, []))

      assert Ash.Type.generator(ChUint32, [])
             |> Enum.take(100)
             |> Enum.uniq()
             |> Enum.all?(&Ash.Type.matches_type?(ChUint32, &1, []))

      assert Ash.Type.generator(ChUint64, [])
             |> Enum.take(100)
             |> Enum.uniq()
             |> Enum.all?(&Ash.Type.matches_type?(ChUint64, &1, []))

      assert Ash.Type.generator(ChUint128, [])
             |> Enum.take(100)
             |> Enum.uniq()
             |> Enum.all?(&Ash.Type.matches_type?(ChUint128, &1, []))

      assert Ash.Type.generator(ChUint256, [])
             |> Enum.take(100)
             |> Enum.uniq()
             |> Enum.all?(&Ash.Type.matches_type?(ChUint256, &1, []))
    end
  end

  describe "apply_constraints/2" do
    test "applies constraint max correctly" do
      assert Ash.Type.apply_constraints(ChUint8, 99, max: 100) == {:ok, 99}
      assert Ash.Type.apply_constraints(ChUint8, 101, max: 100) == {:error, [[message: "must be less than or equal to %{max}", max: 100]]}

      assert Ash.Type.apply_constraints(ChUint16, 2**16-15, max: 2**16-10) == {:ok, 2**16-15}
      assert Ash.Type.apply_constraints(ChUint16, 2**16-5, max: 2**16-10) == {:error, [[message: "must be less than or equal to %{max}", max: 2**16-10]]}

      assert Ash.Type.apply_constraints(ChUint32, 2**32-15, max: 2**32-10) == {:ok, 2**32-15}
      assert Ash.Type.apply_constraints(ChUint32, 2**32-5, max: 2**32-10) == {:error, [[message: "must be less than or equal to %{max}", max: 2**32-10]]}

      assert Ash.Type.apply_constraints(ChUint64, 2**64-15, max: 2**64-10) == {:ok, 2**64-15}
      assert Ash.Type.apply_constraints(ChUint64, 2**64-5, max: 2**64-10) == {:error, [[message: "must be less than or equal to %{max}", max: 2**64-10]]}

      assert Ash.Type.apply_constraints(ChUint128, 2**128-15, max: 2**128-10) == {:ok, 2**128-15}
      assert Ash.Type.apply_constraints(ChUint128, 2**128-5, max: 2**128-10) == {:error, [[message: "must be less than or equal to %{max}", max: 2**128-10]]}

      assert Ash.Type.apply_constraints(ChUint256, 2**256-15, max: 2**256-10) == {:ok, 2**256-15}
      assert Ash.Type.apply_constraints(ChUint256, 2**256-5, max: 2**256-10) == {:error, [[message: "must be less than or equal to %{max}", max: 2**256-10]]}
    end

    test "applies constraint min correctly" do
      assert Ash.Type.apply_constraints(ChUint8, 10, min: 10) == {:ok, 10}
      assert Ash.Type.apply_constraints(ChUint8, 5, min: 10) == {:error, [[message: "must be greater than or equal to %{min}", min: 10]]}

      assert Ash.Type.apply_constraints(ChUint16, 10, min: 10) == {:ok, 10}
      assert Ash.Type.apply_constraints(ChUint16, 5, min: 10) == {:error, [[message: "must be greater than or equal to %{min}", min: 10]]}

      assert Ash.Type.apply_constraints(ChUint32, 10, min: 10) == {:ok, 10}
      assert Ash.Type.apply_constraints(ChUint32, 5, min: 10) == {:error, [[message: "must be greater than or equal to %{min}", min: 10]]}

      assert Ash.Type.apply_constraints(ChUint64, 10, min: 10) == {:ok, 10}
      assert Ash.Type.apply_constraints(ChUint64, 5, min: 10) == {:error, [[message: "must be greater than or equal to %{min}", min: 10]]}

      assert Ash.Type.apply_constraints(ChUint128, 10, min: 10) == {:ok, 10}
      assert Ash.Type.apply_constraints(ChUint128, 5, min: 10) == {:error, [[message: "must be greater than or equal to %{min}", min: 10]]}

      assert Ash.Type.apply_constraints(ChUint256, 10, min: 10) == {:ok, 10}
      assert Ash.Type.apply_constraints(ChUint256, 5, min: 10) == {:error, [[message: "must be greater than or equal to %{min}", min: 10]]}
    end

    test "applies constraint min and max correctly" do
      assert Ash.Type.apply_constraints(ChUint8, 10, min: 10, max: 100) == {:ok, 10}
      assert Ash.Type.apply_constraints(ChUint8, 101, min: 10, max: 100) == {:error, [[message: "must be less than or equal to %{max}", max: 100]]}
      assert Ash.Type.apply_constraints(ChUint8, 9, min: 10, max: 100) == {:error, [[message: "must be greater than or equal to %{min}", min: 10]]}

      assert Ash.Type.apply_constraints(ChUint16, 10, min: 10, max: 100) == {:ok, 10}
      assert Ash.Type.apply_constraints(ChUint16, 101, min: 10, max: 100) == {:error, [[message: "must be less than or equal to %{max}", max: 100]]}
      assert Ash.Type.apply_constraints(ChUint16, 9, min: 10, max: 100) == {:error, [[message: "must be greater than or equal to %{min}", min: 10]]}

      assert Ash.Type.apply_constraints(ChUint32, 10, min: 10, max: 100) == {:ok, 10}
      assert Ash.Type.apply_constraints(ChUint32, 101, min: 10, max: 100) == {:error, [[message: "must be less than or equal to %{max}", max: 100]]}
      assert Ash.Type.apply_constraints(ChUint32, 9, min: 10, max: 100) == {:error, [[message: "must be greater than or equal to %{min}", min: 10]]}

      assert Ash.Type.apply_constraints(ChUint64, 10, min: 10, max: 100) == {:ok, 10}
      assert Ash.Type.apply_constraints(ChUint64, 101, min: 10, max: 100) == {:error, [[message: "must be less than or equal to %{max}", max: 100]]}
      assert Ash.Type.apply_constraints(ChUint64, 9, min: 10, max: 100) == {:error, [[message: "must be greater than or equal to %{min}", min: 10]]}

      assert Ash.Type.apply_constraints(ChUint128, 10, min: 10, max: 100) == {:ok, 10}
      assert Ash.Type.apply_constraints(ChUint128, 101, min: 10, max: 100) == {:error, [[message: "must be less than or equal to %{max}", max: 100]]}
      assert Ash.Type.apply_constraints(ChUint128, 9, min: 10, max: 100) == {:error, [[message: "must be greater than or equal to %{min}", min: 10]]}

      assert Ash.Type.apply_constraints(ChUint256, 10, min: 10, max: 100) == {:ok, 10}
      assert Ash.Type.apply_constraints(ChUint256, 101, min: 10, max: 100) == {:error, [[message: "must be less than or equal to %{max}", max: 100]]}
      assert Ash.Type.apply_constraints(ChUint256, 9, min: 10, max: 100) == {:error, [[message: "must be greater than or equal to %{min}", min: 10]]}
    end
  end
end
