defmodule AshClickhouse.Type.ChIpv6Test do
  use ExUnit.Case, async: true
  alias AshClickhouse.Type.ChIPv6
  import AshClickhouse.Test.Helper, only: [encode_ch_type: 1]

  describe "storage_types" do
    test "returns correct ClickHouse type without constraints" do
      assert {:parameterized, {Ch, :ipv6}} = type = Ash.Type.storage_type(ChIPv6, [])
      assert encode_ch_type(type) == "IPv6"
    end

    test "returns nullable ClickHouse type with nullable constraint" do
      assert {:parameterized, {Ch, {:nullable, :ipv6}}} =
               type = Ash.Type.storage_type(ChIPv6, nullable?: true)

      assert encode_ch_type(type) == "Nullable(IPv6)"
    end

    test "returns correct ClickHouse type without constraints for array version" do
      assert {:array, {:parameterized, {Ch, :ipv6}} = subtype} =
               Ash.Type.storage_type({:array, ChIPv6}, [])

      assert encode_ch_type({:array, subtype}) == "Array(IPv6)"
    end

    test "returns nullable ClickHouse type with nullable constraint for array version" do
      assert {:array, {:parameterized, {Ch, {:nullable, :ipv6}}} = subtype} =
               Ash.Type.storage_type({:array, ChIPv6}, nullable?: true)

      assert encode_ch_type({:array, subtype}) == "Array(Nullable(IPv6))"
    end
  end

  describe "matches_type?/2" do
    test "returns true for valid IPv6 strings" do
      assert Ash.Type.matches_type?(ChIPv6, "2001:0db8:85a3:0000:0000:8a2e:0370:7334")
      assert Ash.Type.matches_type?(ChIPv6, "fe80::1ff:fe23:4567:890a")
    end

    test "returns false for invalid IPv6 values" do
      refute Ash.Type.matches_type?(ChIPv6, nil)
      refute Ash.Type.matches_type?(ChIPv6, "invalid_ip")
      refute Ash.Type.matches_type?(ChIPv6, 12345)
      refute Ash.Type.matches_type?(ChIPv6, [])
    end
  end

  describe "generator" do
    test "generates valid IPv6 addresses" do
      generated_ips = Enum.take(Ash.Type.generator(ChIPv6, []), 100) |> Enum.uniq()

      assert Enum.all?(generated_ips, fn ip ->
               case Ash.Type.matches_type?(ChIPv6, ip) do
                 true -> true
                 false -> false
               end
             end)
    end
  end

  describe "equal?/2" do
    test "returns true for equal IPv6 addresses" do
      assert Ash.Type.equal?(
               ChIPv6,
               {8193, 3512, 34211, 0, 0, 35374, 880, 29492},
               {8193, 3512, 34211, 0, 0, 35374, 880, 29492}
             )

      assert Ash.Type.equal?(
               ChIPv6,
               {8193, 3512, 34211, 0, 0, 35374, 880, 29492},
               "2001:0db8:85a3:0000:0000:8a2e:0370:7334"
             )

      assert Ash.Type.equal?(ChIPv6, {0, 0, 0, 0, 0, 0, 0, 0}, {0, 0, 0, 0, 0, 0, 0, 0})
      assert Ash.Type.equal?(ChIPv6, nil, nil)
    end

    test "returns false for different IPv6 addresses" do
      refute Ash.Type.equal?(
               ChIPv6,
               {8193, 3512, 34211, 0, 0, 35374, 880, 29492},
               {8193, 3512, 34211, 0, 0, 35374, 880, 29493}
             )

      refute Ash.Type.equal?(ChIPv6, {0, 0, 0, 0, 0, 0, 0, 0}, {0, 0, 0, 0, 0, 0, 0, 1})
      refute Ash.Type.equal?(ChIPv6, nil, {0, 0, 0, 0, 0, 0, 0, 0})
    end

    test "returns false for invalid or mismatched types" do
      refute Ash.Type.equal?(ChIPv6, nil, {8193, 3512, 34211, 0, 0, 35374, 880, 29492})
      refute Ash.Type.equal?(ChIPv6, "invalid_ip", {8193, 3512, 34211, 0, 0, 35374, 880, 29492})
      refute Ash.Type.equal?(ChIPv6, 12345, {8193, 3512, 34211, 0, 0, 35374, 880, 29492})
    end

    test "returns true for nil vs nil" do
      assert Ash.Type.equal?(ChIPv6, nil, nil)
    end
  end

  describe "cast_input/2" do
    test "casts valid IPv6 string to internal representation" do
      assert {:ok, {8193, 3512, 34211, 0, 0, 35374, 880, 29492}} =
               Ash.Type.cast_input(ChIPv6, "2001:0db8:85a3:0000:0000:8a2e:0370:7334", [])

      assert {:ok, {8193, 3512, 34211, 0, 0, 35374, 880, 29492}} =
               Ash.Type.cast_input(ChIPv6, {8193, 3512, 34211, 0, 0, 35374, 880, 29492}, [])

      assert {:ok, {65152, 0, 0, 0, 511, 65059, 17767, 35082}} =
               Ash.Type.cast_input(ChIPv6, "fe80::1ff:fe23:4567:890a", [])

      assert {:ok, {0, 0, 0, 0, 0, 0, 0, 0}} = Ash.Type.cast_input(ChIPv6, "::", [])
      assert {:ok, nil} = Ash.Type.cast_input(ChIPv6, nil, [])
    end

    test "returns error for invalid IPv6 inputs" do
      assert {:error, "is invalid"} = Ash.Type.cast_input(ChIPv6, "invalid_ip", [])
      assert {:error, "is invalid"} = Ash.Type.cast_input(ChIPv6, 12345, [])
    end
  end

  describe "cast_input/2 for arrays" do
    test "casts valid array of IPv6 strings to internal representation" do
      ips = ["2001:0db8:85a3:0000:0000:8a2e:0370:7334", "fe80::1ff:fe23:4567:890a", "::"]

      expected = [
        {8193, 3512, 34211, 0, 0, 35374, 880, 29492},
        {65152, 0, 0, 0, 511, 65059, 17767, 35082},
        {0, 0, 0, 0, 0, 0, 0, 0}
      ]

      assert {:ok, ^expected} = Ash.Type.cast_input({:array, ChIPv6}, ips, [])
    end
  end

  test "returns error for invalid array values" do
    invalid_ips = ["2001:0db8:85a3:0000:0000:8a2e:0370:7334", "invalid_ip", "::", nil]

    assert {:error, [[message: "is invalid", index: 1, path: [1]]]} =
             Ash.Type.cast_input({:array, ChIPv6}, invalid_ips, [])
  end
end
