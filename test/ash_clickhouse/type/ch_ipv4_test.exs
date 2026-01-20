defmodule AshClickhouse.Type.ChIpv4Test do
  use ExUnit.Case, async: true
  alias AshClickhouse.Type.ChIPv4
  import AshClickhouse.Test.Helper, only: [encode_ch_type: 1]

  describe "storage_types" do
    test "returns correct ClickHouse type without constraints" do
      assert {:parameterized, {Ch, :ipv4}} = type = Ash.Type.storage_type(ChIPv4, [])
      assert encode_ch_type(type) == "IPv4"
    end

    test "returns nullable ClickHouse type with nullable constraint" do
      assert {:parameterized, {Ch, {:nullable, :ipv4}}} =
               type = Ash.Type.storage_type(ChIPv4, nullable?: true)

      assert encode_ch_type(type) == "Nullable(IPv4)"
    end

    test "returns correct ClickHouse type without constraints for array version" do
      assert {:array, {:parameterized, {Ch, :ipv4}} = subtype} =
               Ash.Type.storage_type({:array, ChIPv4}, [])

      assert encode_ch_type({:array, subtype}) == "Array(IPv4)"
    end

    test "returns nullable ClickHouse type with nullable constraint for array version" do
      assert {:array, {:parameterized, {Ch, {:nullable, :ipv4}}} = subtype} =
               Ash.Type.storage_type({:array, ChIPv4}, nullable?: true)

      assert encode_ch_type({:array, subtype}) == "Array(Nullable(IPv4))"
    end
  end

  describe "matches_type?/2" do
    test "returns true for valid IPv4 strings" do
      assert Ash.Type.matches_type?(ChIPv4, "192.168.1.1")
      assert Ash.Type.matches_type?(ChIPv4, {192, 168, 0, 1})
    end

    test "returns false for invalid IPv4 values" do
      refute Ash.Type.matches_type?(ChIPv4, nil)
      refute Ash.Type.matches_type?(ChIPv4, "invalid_ip")
      refute Ash.Type.matches_type?(ChIPv4, 12345)
      refute Ash.Type.matches_type?(ChIPv4, [])
      refute Ash.Type.matches_type?(ChIPv4, "2001:0db8:85a3:0000:0000:8a2e:0370:7334")
    end
  end

  describe "generator" do
    test "generates valid IPv4 addresses" do
      generated_ips = Enum.take(Ash.Type.generator(ChIPv4, []), 100) |> Enum.uniq()

      assert Enum.all?(generated_ips, fn ip ->
               case Ash.Type.matches_type?(ChIPv4, ip) do
                 true -> true
                 false -> false
               end
             end)
    end

    test "generates arrays of valid IPv4 addresses" do
      generated_arrays = Enum.take(Ash.Type.generator({:array, ChIPv4}, []), 100)

      assert Enum.all?(generated_arrays, fn arr ->
               Enum.all?(arr, fn ip ->
                 case Ash.Type.matches_type?(ChIPv4, ip) do
                   true -> true
                   false -> false
                 end
               end)
             end)
    end
  end

  describe "cast_input/2" do
    test "casts valid IPv4 string to internal representation" do
      assert {:ok, {192, 168, 1, 1}} = Ash.Type.cast_input(ChIPv4, "192.168.1.1", [])
      assert {:ok, {10, 0, 0, 1}} = Ash.Type.cast_input(ChIPv4, {10, 0, 0, 1}, [])
      assert {:ok, nil} = Ash.Type.cast_input(ChIPv4, nil, [])
    end

    test "returns error for invalid IPv4 inputs" do
      assert {:error, "is invalid"} = Ash.Type.cast_input(ChIPv4, "invalid_ip", [])
      assert {:error, "is invalid"} = Ash.Type.cast_input(ChIPv4, 12345, [])
    end
  end

  describe "cast_input/2 for arrays" do
    test "casts valid array of IPv4 strings to internal representation" do
      ips = ["192.168.1.1", {10, 0, 0, 1}, "172.16.0.5"]
      expected = [{192, 168, 1, 1}, {10, 0, 0, 1}, {172, 16, 0, 5}]
      assert {:ok, ^expected} = Ash.Type.cast_input({:array, ChIPv4}, ips, [])
    end

    test "returns error for invalid array values" do
      invalid_ips = ["192.168.1.1", "invalid_ip", {172, 16, 0, 5}, nil]

      assert {:error, [[message: "is invalid", index: 1, path: [1]]]} =
               Ash.Type.cast_input({:array, ChIPv4}, invalid_ips, [])
    end
  end

  describe "equal?/2" do
    test "returns true for equal IPv4 tuples" do
      assert Ash.Type.equal?(ChIPv4, {192, 168, 1, 1}, {192, 168, 1, 1})
      assert Ash.Type.equal?(ChIPv4, "192.168.1.1", "192.168.1.1")
    end

    test "returns true for equivalent IPv4 string and tuple" do
      assert Ash.Type.equal?(ChIPv4, {10, 0, 0, 1}, "10.0.0.1")
      assert Ash.Type.equal?(ChIPv4, "172.16.0.5", {172, 16, 0, 5})
    end

    test "returns false for different IPv4 addresses" do
      refute Ash.Type.equal?(ChIPv4, {192, 168, 1, 1}, {192, 168, 1, 2})
      refute Ash.Type.equal?(ChIPv4, "10.0.0.1", "10.0.0.2")
      refute Ash.Type.equal?(ChIPv4, {10, 0, 0, 1}, "10.0.0.2")
    end

    test "returns false for invalid or mismatched types" do
      refute Ash.Type.equal?(ChIPv4, nil, {192, 168, 1, 1})
      refute Ash.Type.equal?(ChIPv4, "invalid_ip", {192, 168, 1, 1})
      refute Ash.Type.equal?(ChIPv4, 12345, "192.168.1.1")
    end

    test "returns true for nil vs nil" do
      assert Ash.Type.equal?(ChIPv4, nil, nil)
    end
  end
end
