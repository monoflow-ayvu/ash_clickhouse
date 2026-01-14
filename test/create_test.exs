defmodule AshClickhouse.CreateTest do
  use AshClickhouse.RepoCase, async: true
  alias AshClickhouse.Test.Resource.User
  alias AshClickhouse.Test.Resource.Organization
  alias AshClickhouse.Test.Resource.OrganizationUser
  alias TestRepo

  require Ash.Query

  setup do
    TestRepo.query("TRUNCATE TABLE users")
    TestRepo.query("TRUNCATE TABLE organizations")
    TestRepo.query("TRUNCATE TABLE users_organizations")
    TestRepo.query("TRUNCATE TABLE all_types")
    :ok
  end

  describe "User resource create tests" do
    test "seeding data works" do
      Ash.Seed.seed!(%User{name: "Fred"})
    end

    test "creates insert" do
      params = %{
        name: "Fred",
        email: "fred@example.com",
        age: 30,
        score: :rand.uniform(),
        is_active: true
      }

      assert {:ok, %User{}} =
               User
               |> Ash.Changeset.for_create(:create, params)
               |> Ash.create()

      assert [%{name: "Fred"}] =
               User
               |> Ash.Query.sort(:name)
               |> Ash.read!()
    end

    test "creates user with organizations using manage_relationship" do
      org_params = %{name: "Org 1", industry: "Tech", employee_count: 100, founded_year: 2020}

      {:ok, org} =
        Organization
        |> Ash.Changeset.for_create(:create, org_params)
        |> Ash.create()

      user_params = %{
        name: "Alice",
        email: "alice@example.com",
        age: 28,
        score: 99.9,
        is_active: true
      }

      changeset =
        User
        |> Ash.Changeset.for_create(:create, user_params)
        |> Ash.Changeset.manage_relationship(:organizations, [org], type: :append_and_remove)

      assert {:ok, %User{} = user} = Ash.create(changeset)
      assert [%{name: "Org 1"}] = user.organizations

      user_id = user.id
      org_id = org.id

      assert [%{user_id: ^user_id, organization_id: ^org_id, role: :member}] =
               OrganizationUser
               |> Ash.Query.filter(user_id == ^user.id and organization_id == ^org.id)
               |> Ash.read!()
    end
  end

  describe "AllTypes resource create tests" do
    alias AshClickhouse.Test.Resource.AllTypes

    test "creates insert with all types" do
      params = %{
        string_attr: "Test String",
        nullable_string_attr: nil,
        low_cardinality_string_attr: "LowCard",
        low_cardinality_nullable_string_attr: nil,
        fixed_string_attr: String.duplicate("A", 16),
        int8_attr: 127,
        nullable_int8_attr: nil,
        int16_attr: 32_767,
        nullable_int16_attr: nil,
        int32_attr: 2_147_483_647,
        nullable_int32_attr: nil,
        int64_attr: 9_223_372_036_854_775_807,
        nullable_int64_attr: nil,
        int128_attr: 170_141_183_460_469_231_731_687_303_715_884_105_727,
        nullable_int128_attr: nil,
        int256_attr: 1,
        nullable_int256_attr: nil,
        uint8_attr: 255,
        nullable_uint8_attr: nil,
        uint16_attr: 65_535,
        nullable_uint16_attr: nil,
        uint32_attr: 4_294_967_295,
        nullable_uint32_attr: nil,
        uint64_attr: 18_446_744_073_709_551_615,
        nullable_uint64_attr: nil,
        uint128_attr: 340_282_366_920_938_463_463_374_607_431_768_211_455,
        nullable_uint128_attr: nil,
        uint256_attr: 1,
        nullable_uint256_attr: nil,
        float32_attr: 3.14,
        nullable_float32_attr: nil,
        float64_attr: 2.71828,
        nullable_float64_attr: nil,
        bool_attr: true,
        atom_attr: :some_atom,
        date_attr: ~D[2024-01-01],
        nullable_date_attr: nil,
        date32_attr: ~D[2024-01-01],
        datetime_attr: ~U[2024-01-01 12:00:00Z],
        datetime64_attr: ~U[2024-01-01 12:00:00Z],
        decimal_attr: Decimal.new("0.67"),
        decimal32_attr: Decimal.new("0.123456789"),
        nullable_decimal32_attr: nil,
        decimal64_attr: Decimal.new("0.123456789123456789"),
        nullable_decimal64_attr: nil,
        decimal128_attr: Decimal.new("0.12345678912345678912345678912345678912"),
        decimal256_attr:
          Decimal.new(
            "0.1234567891234567891234567891234567891234567891234567891234567891234567891234"
          ),
        nullable_decimal256_attr: nil,
        json_attr: %{foo: "bar"},
        nullable_json_attr: nil,
        map_attr: %{"key1" => "value1", "key2" => "value2"},
        ipv4_attr: "192.168.1.1",
        nullable_ipv4_attr: nil,
        ipv6_attr: "2001:0db8:85a3:0000:0000:8a2e:0370:7334",
        nullable_ipv6_attr: nil,
        uuid_attr: "550e8400-e29b-41d4-a716-446655440000",
        nullable_uuid_attr: nil,
        tuple_attr: {"foo", 42, true},
        point_attr: {12.34, 56.78},
        ring_attr: [
          {1.0, 2.0},
          {3.0, 4.0},
          {5.0, 6.0}
        ],
        polygon_attr: [
          [
            {1.0, 2.0},
            {3.0, 4.0},
            {5.0, 6.0}
          ]
        ],
        multipolygon_attr: [
          [
            [
              {1.0, 2.0},
              {3.0, 4.0},
              {5.0, 6.0}
            ]
          ],
          [
            [
              {7.0, 8.0},
              {9.0, 10.0},
              {11.0, 12.0}
            ]
          ]
        ],
        simple_agg_func_attr: 123,
        variant_attr: "string ou 42 ou true"
      }

      assert {:ok,
              %AllTypes{
                string_attr: "Test String",
                nullable_string_attr: nil,
                low_cardinality_string_attr: "LowCard",
                low_cardinality_nullable_string_attr: nil,
                fixed_string_attr: "AAAAAAAAAAAAAAAA",
                int8_attr: 127,
                nullable_int8_attr: nil,
                int16_attr: 32_767,
                nullable_int16_attr: nil,
                int32_attr: 2_147_483_647,
                nullable_int32_attr: nil,
                int64_attr: 9_223_372_036_854_775_807,
                nullable_int64_attr: nil,
                int128_attr: 170_141_183_460_469_231_731_687_303_715_884_105_727,
                nullable_int128_attr: nil,
                int256_attr: 1,
                nullable_int256_attr: nil,
                uint8_attr: 255,
                nullable_uint8_attr: nil,
                uint16_attr: 65_535,
                nullable_uint16_attr: nil,
                uint32_attr: 4_294_967_295,
                nullable_uint32_attr: nil,
                uint64_attr: 18_446_744_073_709_551_615,
                nullable_uint64_attr: nil,
                uint128_attr: 340_282_366_920_938_463_463_374_607_431_768_211_455,
                nullable_uint128_attr: nil,
                uint256_attr: 1,
                nullable_uint256_attr: nil,
                float32_attr: 3.14,
                nullable_float32_attr: nil,
                float64_attr: 2.71828,
                nullable_float64_attr: nil,
                bool_attr: true,
                atom_attr: :some_atom,
                date_attr: ~D[2024-01-01],
                nullable_date_attr: nil,
                date32_attr: ~D[2024-01-01],
                datetime_attr: ~U[2024-01-01 12:00:00Z],
                datetime64_attr: ~U[2024-01-01 12:00:00.000000Z],
                decimal_attr: %Decimal{coef: 67, exp: -2, sign: 1},
                decimal32_attr: %Decimal{coef: 123_456_789, exp: -9, sign: 1},
                nullable_decimal32_attr: nil,
                decimal64_attr: %Decimal{coef: 123_456_789_123_456_789, exp: -18, sign: 1},
                nullable_decimal64_attr: nil,
                decimal128_attr: %Decimal{
                  coef: 12_345_678_912_345_678_912_345_678_912_345_678_912,
                  exp: -38,
                  sign: 1
                },
                decimal256_attr: %Decimal{
                  coef:
                    1_234_567_891_234_567_891_234_567_891_234_567_891_234_567_891_234_567_891_234_567_891_234_567_891_234,
                  exp: -76,
                  sign: 1
                },
                nullable_decimal256_attr: nil,
                json_attr: %{foo: "bar"},
                nullable_json_attr: nil,
                map_attr: %{"key1" => "value1", "key2" => "value2"},
                point_attr: {12.34, 56.78},
                ring_attr: [
                  {1.0, 2.0},
                  {3.0, 4.0},
                  {5.0, 6.0}
                ],
                polygon_attr: [
                  [
                    {1.0, 2.0},
                    {3.0, 4.0},
                    {5.0, 6.0}
                  ]
                ],
                multipolygon_attr: [
                  [
                    [
                      {1.0, 2.0},
                      {3.0, 4.0},
                      {5.0, 6.0}
                    ]
                  ],
                  [
                    [
                      {7.0, 8.0},
                      {9.0, 10.0},
                      {11.0, 12.0}
                    ]
                  ]
                ],
                simple_agg_func_attr: 123,
                variant_attr: "string ou 42 ou true"
              }} =
               AllTypes
               |> Ash.Changeset.for_create(:create, params)
               |> Ash.create()

      assert [
               %AllTypes{
                 string_attr: "Test String",
                 nullable_string_attr: nil,
                 low_cardinality_string_attr: "LowCard",
                 low_cardinality_nullable_string_attr: nil,
                 fixed_string_attr: "AAAAAAAAAAAAAAAA",
                 int8_attr: 127,
                 nullable_int8_attr: nil,
                 int16_attr: 32_767,
                 nullable_int16_attr: nil,
                 int32_attr: 2_147_483_647,
                 nullable_int32_attr: nil,
                 int64_attr: 9_223_372_036_854_775_807,
                 nullable_int64_attr: nil,
                 int128_attr: 170_141_183_460_469_231_731_687_303_715_884_105_727,
                 nullable_int128_attr: nil,
                 int256_attr: 1,
                 nullable_int256_attr: nil,
                 uint8_attr: 255,
                 nullable_uint8_attr: nil,
                 uint16_attr: 65_535,
                 nullable_uint16_attr: nil,
                 uint32_attr: 4_294_967_295,
                 nullable_uint32_attr: nil,
                 uint64_attr: 18_446_744_073_709_551_615,
                 nullable_uint64_attr: nil,
                 uint128_attr: 340_282_366_920_938_463_463_374_607_431_768_211_455,
                 nullable_uint128_attr: nil,
                 uint256_attr: 1,
                 nullable_uint256_attr: nil,
                 float32_attr: 3.140000104904175,
                 nullable_float32_attr: nil,
                 float64_attr: 2.71828,
                 nullable_float64_attr: nil,
                 bool_attr: true,
                 atom_attr: :some_atom,
                 date_attr: ~D[2024-01-01],
                 nullable_date_attr: nil,
                 date32_attr: ~D[2024-01-01],
                 datetime_attr: ~U[2024-01-01 12:00:00Z],
                 datetime64_attr: ~U[2024-01-01 12:00:00.000000Z],
                 decimal_attr: %Decimal{coef: 67, exp: -2, sign: 1},
                 decimal32_attr: %Decimal{coef: 123_456_789, exp: -9, sign: 1},
                 nullable_decimal32_attr: nil,
                 decimal64_attr: %Decimal{coef: 123_456_789_123_456_789, exp: -18, sign: 1},
                 nullable_decimal64_attr: nil,
                 decimal128_attr: %Decimal{
                   coef: 12_345_678_912_345_678_912_345_678_912_345_678_912,
                   exp: -38,
                   sign: 1
                 },
                 decimal256_attr: %Decimal{
                   coef:
                     1_234_567_891_234_567_891_234_567_891_234_567_891_234_567_891_234_567_891_234_567_891_234_567_891_234,
                   exp: -76,
                   sign: 1
                 },
                 nullable_decimal256_attr: nil,
                 json_attr: %{"foo" => "bar"},
                 nullable_json_attr: nil,
                 map_attr: %{"key1" => "value1", "key2" => "value2"},
                 point_attr: {12.34, 56.78},
                 ring_attr: [
                   {1.0, 2.0},
                   {3.0, 4.0},
                   {5.0, 6.0}
                 ],
                 polygon_attr: [
                   [
                     {1.0, 2.0},
                     {3.0, 4.0},
                     {5.0, 6.0}
                   ]
                 ],
                 multipolygon_attr: [
                   [
                     [
                       {1.0, 2.0},
                       {3.0, 4.0},
                       {5.0, 6.0}
                     ]
                   ],
                   [
                     [
                       {7.0, 8.0},
                       {9.0, 10.0},
                       {11.0, 12.0}
                     ]
                   ]
                 ],
                 simple_agg_func_attr: 123,
                 variant_attr: "string ou 42 ou true"
               }
             ] =
               AllTypes
               |> Ash.Query.filter(string_attr == "Test String")
               |> Ash.read!()
    end
  end
end
