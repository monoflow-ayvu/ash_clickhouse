defmodule AshClickhouse.Test.Resource.AllTypes do
  use Ash.Resource,
    domain: AshClickhouse.Test.Domain,
    data_layer: AshClickhouse.DataLayer

  actions do
    defaults([:create, :read, :update, :destroy])
    default_accept(:*)
  end

  clickhouse do
    table("all_types")
    repo(AshClickhouse.TestRepo)
    engine("MergeTree()")
    options("order by id")
  end

  attributes do
    uuid_primary_key(:id)
    attribute(:string_attr, :ch_string, public?: true)

    attribute(:nullable_string_attr, :ch_string) do
      public?(true)
      constraints(nullable?: true)
    end

    attribute(:low_cardinality_string_attr, :ch_string) do
      public?(true)
      constraints(low_cardinality?: true)
    end

    attribute(:low_cardinality_nullable_string_attr, :ch_string) do
      public?(true)
      constraints(nullable?: true, low_cardinality?: true)
    end

    attribute(:fixed_string_attr, :ch_fixed_string,
      public?: true,
      constraints: [length: 16]
    )

    for size <- [8, 16, 32, 64, 128, 256] do
      attribute(:"int#{size}_attr", :"ch_int#{size}") do
        public?(true)
      end

      attribute(:"nullable_int#{size}_attr", :"ch_int#{size}") do
        public?(true)
        constraints(nullable?: true)
      end
    end

    for size <- [8, 16, 32, 64, 128, 256] do
      attribute(:"uint#{size}_attr", String.to_atom("ch_uint#{size}")) do
        public?(true)
      end

      attribute(:"nullable_uint#{size}_attr", String.to_atom("ch_uint#{size}")) do
        public?(true)
        constraints(nullable?: true)
      end
    end

    for size <- [32, 64] do
      attribute(:"float#{size}_attr", String.to_atom("ch_float#{size}")) do
        public?(true)
      end

      attribute(:"nullable_float#{size}_attr", String.to_atom("ch_float#{size}")) do
        public?(true)
        constraints(nullable?: true)
      end
    end

    attribute(:bool_attr, :ch_bool, public?: true)

    attribute(:atom_attr, :ch_atom, public?: true)

    attribute(:date_attr, :ch_date, public?: true)

    attribute(:nullable_date_attr, :ch_date) do
      public?(true)
      constraints(nullable?: true)
    end

    attribute(:date32_attr, :ch_date32, public?: true)

    attribute(:datetime_attr, :ch_datetime,
      public?: true,
      constraints: [timezone: "UTC"]
    )

    attribute(:datetime64_attr, :ch_datetime64,
      public?: true,
      constraints: [precision: 6, timezone: "UTC"]
    )

    attribute(:decimal_attr, :ch_decimal,
      public?: true,
      constraints: [precision: 10, scale: 2]
    )

    for {precision, max_scale} <- [{32, 9}, {64, 18}, {128, 38}, {256, 76}] do
      attribute(:"decimal#{precision}_attr", :"ch_decimal#{precision}") do
        public?(true)
        constraints(scale: max_scale)
      end

      attribute(:"nullable_decimal#{precision}_attr", :"ch_decimal#{precision}") do
        public?(true)
        constraints(scale: max_scale, nullable?: true)
      end
    end

    attribute(:json_attr, :ch_json, public?: true)

    attribute(:nullable_json_attr, :ch_json) do
      public?(true)
      constraints(nullable?: true)
    end

    attribute(:map_attr, :ch_map) do
      public?(true)
      constraints(key_type: :ch_string, value_type: :ch_string, fields: ["foo", "bar", "baz"])
    end

    attribute(:map_attr_with_nullable_str_values, :ch_map) do
      public?(true)

      constraints(
        key_type: :ch_string,
        value_type: [ch_string: [nullable?: true]],
        fields: ["foo", "bar", "baz"]
      )
    end

    attribute(:ipv4_attr, :ch_ipv4, public?: true)

    attribute(:nullable_ipv4_attr, :ch_ipv4) do
      public?(true)
      constraints(nullable?: true)
    end

    attribute(:ipv6_attr, :ch_ipv6, public?: true)

    attribute(:nullable_ipv6_attr, :ch_ipv6) do
      public?(true)
      constraints(nullable?: true)
    end

    attribute(:uuid_attr, :ch_uuid, public?: true)

    attribute(:nullable_uuid_attr, :ch_uuid) do
      public?(true)
      constraints(nullable?: true)
    end

    # Time64 type is not alowed by default in ClickHouse
    # attribute(:time64_attr, :ch_time64) do
    #   public?(true)
    #   constraints(precision: 6)
    # end

    # attribute(:nullable_time64_attr, :ch_time64) do
    #   public?(true)
    #   constraints(precision: 6, nullable?: true)
    # end

    attribute(:tuple_attr, :ch_tuple) do
      public?(true)
      constraints(types: [:ch_string, :ch_int32, :ch_bool])
    end

    attribute(:point_attr, :ch_point, public?: true)
    attribute(:ring_attr, :ch_ring, public?: true)
    attribute(:polygon_attr, :ch_polygon, public?: true)
    attribute(:multipolygon_attr, :ch_multipolygon, public?: true)

    attribute(:simple_agg_func_attr, :ch_simple_aggregate_function) do
      public?(true)
      constraints(function: "sum", type: :ch_int64)
    end

    attribute(:variant_attr, :ch_variant) do
      public?(true)
      constraints(types: [:ch_string, :ch_int32, :ch_bool])
    end

    attribute(:array_of_string_attr, {:array, :ch_string}) do
      public?(true)
    end

    attribute(:array_of_low_cardinality_string_attr, {:array, :ch_string}) do
      public?(true)
      constraints(items: [low_cardinality?: true])
    end

    attribute(:array_of_nullable_string_attr, {:array, :ch_string}) do
      public?(true)
      constraints(items: [nullable?: true])
    end

    attribute(:array_of_low_cardinality_nullable_string_attr, {:array, :ch_string}) do
      public?(true)
      constraints(items: [nullable?: true, low_cardinality?: true])
    end

    attribute(:array_of_int32_attr, {:array, :ch_int32}) do
      public?(true)
    end

    attribute(:array_of_nullable_int32_attr, {:array, :ch_int32}) do
      public?(true)
      constraints(items: [nullable?: true])
    end

    attribute(:array_of_int64_attr, {:array, :ch_int64}) do
      public?(true)
    end

    attribute(:array_of_nullable_int64_attr, {:array, :ch_int64}) do
      public?(true)
      constraints(items: [nullable?: true])
    end

    attribute(:array_of_uint32_attr, {:array, :ch_uint32}) do
      public?(true)
    end

    attribute(:array_of_nullable_uint32_attr, {:array, :ch_uint32}) do
      public?(true)
      constraints(items: [nullable?: true])
    end

    attribute(:array_of_uint64_attr, {:array, :ch_uint64}) do
      public?(true)
    end

    attribute(:array_of_nullable_uint64_attr, {:array, :ch_uint64}) do
      public?(true)
      constraints(items: [nullable?: true])
    end

    attribute(:array_of_float32_attr, {:array, :ch_float32}) do
      public?(true)
    end

    attribute(:array_of_nullable_float32_attr, {:array, :ch_float32}) do
      public?(true)
      constraints(items: [nullable?: true])
    end

    attribute(:array_of_float64_attr, {:array, :ch_float64}) do
      public?(true)
    end

    attribute(:array_of_nullable_float64_attr, {:array, :ch_float64}) do
      public?(true)
      constraints(items: [nullable?: true])
    end

    attribute(:array_of_bool_attr, {:array, :ch_bool}) do
      public?(true)
    end

    attribute(:array_of_date_attr, {:array, :ch_date}) do
      public?(true)
    end

    attribute(:array_of_nullable_date_attr, {:array, :ch_date}) do
      public?(true)
      constraints(items: [nullable?: true])
    end

    attribute(:array_of_date32_attr, {:array, :ch_date32}) do
      public?(true)
    end

    attribute(:array_of_nullable_date32_attr, {:array, :ch_date32}) do
      public?(true)
      constraints(items: [nullable?: true])
    end

    attribute(:array_of_datetime_attr, {:array, :ch_datetime}) do
      public?(true)
      constraints(items: [timezone: "UTC"])
    end

    attribute(:array_of_nullable_datetime_attr, {:array, :ch_datetime}) do
      public?(true)
      constraints(items: [nullable?: true, timezone: "UTC"])
    end

    attribute(:array_of_datetime64_attr, {:array, :ch_datetime64}) do
      public?(true)
      constraints(items: [precision: 6, timezone: "UTC"])
    end

    attribute(:array_of_nullable_datetime64_attr, {:array, :ch_datetime64}) do
      public?(true)
      constraints(items: [nullable?: true, precision: 6, timezone: "UTC"])
    end

    attribute(:array_of_decimal32_attr, {:array, :ch_decimal32}) do
      public?(true)
      constraints(items: [scale: 9])
    end

    attribute(:array_of_nullable_decimal32_attr, {:array, :ch_decimal32}) do
      public?(true)
      constraints(items: [nullable?: true, scale: 9], nil_items?: true)
    end

    attribute(:array_of_decimal64_attr, {:array, :ch_decimal64}) do
      public?(true)
      constraints(items: [scale: 18])
    end

    attribute(:array_of_nullable_decimal64_attr, {:array, :ch_decimal64}) do
      public?(true)
      constraints(items: [nullable?: true, scale: 18], nil_items?: true)
    end

    attribute(:array_of_decimal128_attr, {:array, :ch_decimal128}) do
      public?(true)
      constraints(items: [scale: 38])
    end

    attribute(:array_of_nullable_decimal128_attr, {:array, :ch_decimal128}) do
      public?(true)
      constraints(items: [nullable?: true, scale: 38], nil_items?: true)
    end

    attribute(:array_of_decimal256_attr, {:array, :ch_decimal256}) do
      public?(true)
      constraints(items: [scale: 76])
    end

    attribute(:array_of_nullable_decimal256_attr, {:array, :ch_decimal256}) do
      public?(true)
      constraints(items: [nullable?: true, scale: 76], nil_items?: true)
    end

    attribute(:array_of_json_attr, {:array, :ch_json}) do
      public?(true)
    end

    attribute(:array_of_nullable_json_attr, {:array, :ch_json}) do
      public?(true)
      constraints(items: [nullable?: true], nil_items?: true)
    end

    attribute(:array_of_map_attr, {:array, :ch_map}) do
      public?(true)
      constraints(items: [key_type: :ch_string, value_type: :ch_string])
    end

    attribute(:array_of_ipv4_attr, {:array, :ch_ipv4}) do
      public?(true)
    end

    attribute(:array_of_nullable_ipv4_attr, {:array, :ch_ipv4}) do
      public?(true)
      constraints(items: [nullable?: true], nil_items?: true)
    end

    attribute(:array_of_ipv6_attr, {:array, :ch_ipv6}) do
      public?(true)
    end

    attribute(:array_of_nullable_ipv6_attr, {:array, :ch_ipv6}) do
      public?(true)
      constraints(items: [nullable?: true], nil_items?: true)
    end

    attribute(:array_of_uuid_attr, {:array, :ch_uuid}) do
      public?(true)
    end

    attribute(:array_of_nullable_uuid_attr, {:array, :ch_uuid}) do
      public?(true)
      constraints(items: [nullable?: true], nil_items?: true)
    end

    attribute(:array_of_variant_attr, {:array, :ch_variant}) do
      public?(true)
      constraints(items: [types: [:ch_string, :ch_int32, :ch_bool]])
    end

    attribute(:array_of_tuple_attr, {:array, :ch_tuple}) do
      public?(true)
      constraints(items: [types: [:ch_string, :ch_int32, :ch_bool]])
    end

    attribute(:array_of_point_attr, {:array, :ch_point}) do
      public?(true)
    end

    attribute(:array_of_ring_attr, {:array, :ch_ring}) do
      public?(true)
    end

    attribute(:array_of_polygon_attr, {:array, :ch_polygon}) do
      public?(true)
    end

    attribute(:array_of_multipolygon_attr, {:array, :ch_multipolygon}) do
      public?(true)
    end

    attribute(:enum8_attr, AshClickhouse.Test.Resource.Types.TestEnum8, public?: true)
    attribute(:enum16_attr, AshClickhouse.Test.Resource.Types.TestEnum16, public?: true)
  end
end
