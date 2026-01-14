defmodule AshClickhouse.Test.Resource.User do
  use Ash.Resource,
    domain: AshClickhouse.Test.Domain,
    data_layer: AshClickhouse.DataLayer

  actions do
    defaults([:create, :read, :update, :destroy])
    default_accept(:*)
  end

  clickhouse do
    table("users")
    repo(AshClickhouse.TestRepo)
    engine("MergeTree()")
    options("order by (id, email)")
  end

  attributes do
    uuid_primary_key(:id)
    attribute(:name, :ch_string, public?: true)
    attribute(:email, :ch_string, public?: true)
    attribute(:age, :ch_uint8, public?: true)
    attribute(:score, :ch_float64, public?: true)
    attribute(:is_active, :ch_bool, public?: true)

    attribute(:inserted_at, :ch_datetime64,
      public?: true,
      constraints: [precision: 6, timezone: "UTC"],
      default: &DateTime.utc_now/0
    )

    attribute :updated_at, :ch_datetime64 do
      public?(true)
      constraints(precision: 6, timezone: "UTC")
      writable?(false)
      default(&DateTime.utc_now/0)
      match_other_defaults?(true)
      update_default(&DateTime.utc_now/0)
      allow_nil?(false)
    end
  end

  relationships do
    many_to_many :organizations, AshClickhouse.Test.Resource.Organization do
      through(AshClickhouse.Test.Resource.OrganizationUser)
      public?(true)
    end
  end
end
