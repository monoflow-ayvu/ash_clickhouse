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
      constraints(key_type: :ch_string, value_type: :ch_string)
    end

    attribute(:map_attr_with_nullable_str_values, :ch_map) do
      public?(true)

      constraints(
        key_type: :ch_string,
        value_type: [ch_string: [nullable?: true]]
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
  end
end
