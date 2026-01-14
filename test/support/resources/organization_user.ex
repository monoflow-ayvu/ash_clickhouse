defmodule AshClickhouse.Test.Resource.OrganizationUser do
  use Ash.Resource,
    domain: AshClickhouse.Test.Domain,
    data_layer: AshClickhouse.DataLayer

  actions do
    defaults([:create, :read, :update, :destroy])
    default_accept(:*)
  end

  resource do
    require_primary_key?(false)
  end

  clickhouse do
    table("organization_users")
    repo(AshClickhouse.TestRepo)
    engine("MergeTree()")
    options("order by (organization_id, user_id)")
  end

  attributes do
    attribute(:role, :ch_atom) do
      public?(true)
      constraints(one_of: [:admin, :member])
      default(:member)
    end

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
    belongs_to(:organization, AshClickhouse.Test.Resource.Organization,
      public?: true,
      attribute_type: :ch_uuid
    )

    belongs_to(:user, AshClickhouse.Test.Resource.User, public?: true, attribute_type: :ch_uuid)
  end
end
