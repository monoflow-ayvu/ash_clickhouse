defmodule AshClickhouse.Test.Resource.Organization do
  use Ash.Resource,
    domain: AshClickhouse.Test.Domain,
    data_layer: AshClickhouse.DataLayer

  actions do
    defaults([:create, :read, :update, :destroy])
    default_accept(:*)
  end

  clickhouse do
    table("organizations")
    repo(AshClickhouse.TestRepo)
    engine("MergeTree()")
    options("order by id")
  end

  attributes do
    uuid_primary_key(:id)
    attribute(:name, :ch_string, public?: true)
    attribute(:industry, :ch_string, public?: true)
    attribute(:employee_count, :ch_uint32, public?: true)
    attribute(:founded_year, :ch_uint16, public?: true)

    attribute(:inserted_at, :ch_datetime64,
      public?: true,
      constraints: [precision: 6, timezone: "UTC"],
      default: &DateTime.utc_now/0,
      allow_nil?: false
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
    many_to_many :users, AshClickhouse.Test.Resource.User do
      through(AshClickhouse.Test.Resource.OrganizationUser)
      public?(true)
    end
  end
end
