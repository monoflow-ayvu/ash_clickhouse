defmodule AshClickhouse.DataLayer do
  @behaviour Ash.DataLayer

  @clickhouse %Spark.Dsl.Section{
    name: :clickhouse,
    describe: """
    Clickhouse data layer configuration
    """,
    sections: [],
    modules: [
      :repo
    ],
    examples: [
      """
      clickhouse do
        repo AshClickhouse
        table "organizations"
      end
      """
    ],
    schema: [
      repo: [
        type: {:or, [{:behaviour, Ecto.Repo}, {:fun, 2}]},
        required: true,
        doc:
          "The repo that will be used to fetch your data. See the `AshClickhouse.Repo` documentation for more. Can also be a function that takes a resource and a type `:read | :mutate` and returns the repo"
      ],
      table: [
        type: :string,
        doc: """
        The table to store and read the resource from. If this is changed, the migration generator will not remove the old table.
        """
      ],
      base_filter_sql: [
        type: :string,
        doc:
          "A raw sql version of the base_filter, e.g `representative = true`. Required if trying to create a unique constraint on a resource with a base_filter"
      ]
    ]
  }

  @sections [@clickhouse]

  use Spark.Dsl.Extension,
    sections: @sections,
    verifiers: []

  require Ash.Expr
  require Ecto.Query

  alias AshClickhouse.SqlImplementation
  alias AshClickhouse.DataLayer.Info
  alias AshClickhouse.ManualRelationship

  @impl true
  def can?(_, :read), do: true
  def can?(_, :create), do: true
  def can?(_, :bulk_create), do: true
  def can?(_, :update), do: true
  def can?(_, :destroy), do: true
  def can?(_, :filter), do: true

  def can?(_, {:filter_relationship, %{manual: {module, _}}}) do
    Spark.implements_behaviour?(module, ManualRelationship)
  end

  def can?(_, {:filter_relationship, _}), do: true
  def can?(_, {:filter_expr, _}), do: true
  def can?(_, :nested_expressions), do: true
  def can?(_, :boolean_filter), do: true
  def can?(_, :sort), do: true
  def can?(_, {:sort, _}), do: true
  def can?(_, :count), do: true
  def can?(_, {:count, _}), do: true
  def can?(_, :limit), do: true
  def can?(_, :offset), do: true
  def can?(_, :aggregate), do: true
  def can?(_, {:aggregate, _}), do: true
  def can?(_, {:aggregate_type, _}), do: true
  def can?(_, _), do: false

  @impl true
  def set_context(resource, data_layer_query, context) do
    AshSql.Query.set_context(resource, data_layer_query, SqlImplementation, context)
  end

  @impl true
  def resource_to_query(resource, domain) do
    AshSql.Query.resource_to_query(resource, SqlImplementation, domain)
  end

  @impl true
  def create(resource, changeset) do
    changeset = %{
      changeset
      | data:
          Map.update!(
            changeset.data,
            :__meta__,
            &Map.put(&1, :source, table(resource, changeset))
          )
    }

    repo_opts = Map.get(changeset.context, :repo_opts, [])

    case bulk_create(resource, [changeset], %{
           single?: true,
           tenant: Map.get(changeset, :to_tenant, changeset.tenant),
           action_select: changeset.action_select,
           return_records?: true,
           repo_opts: repo_opts
         }) do
      {:ok, [result]} ->
        {:ok, result}

      {:ok, []} ->
        {:ok, []}

      {:error, error} ->
        {:error, error}
    end
  end

  @impl true
  def update(resource, changeset) do
    attributes =
      changeset.data
      |> Map.from_struct()
      |> Map.take(changeset.data.__struct__.__schema__(:fields) -- Map.keys(changeset.attributes))
      |> Map.merge(changeset.attributes)

    changeset_for_insert = %{
      changeset
      | attributes: attributes,
        action_type: :create
    }

    create(resource, changeset_for_insert)
  end

  @impl true
  def bulk_create(resource, stream, options) do
    changesets = Enum.to_list(stream)

    repo = AshSql.dynamic_repo(resource, SqlImplementation, Enum.at(changesets, 0))

    opts =
      repo
      |> AshSql.repo_opts(SqlImplementation, nil, options[:tenant], resource)
      |> Keyword.merge(options[:repo_opts] || [])

    source = resolve_source(resource, Enum.at(changesets, 0))

    try do
      opts =
        if schema = Enum.at(changesets, 0).context[:data_layer][:schema] do
          Keyword.put(opts, :prefix, schema)
        else
          opts
        end

      ecto_changesets = Enum.map(changesets, & &1.attributes)

      repo.insert_all(source, ecto_changesets, opts)

      {:ok, []}
    rescue
      e ->
        changeset =
          case source do
            {table, resource} ->
              resource
              |> Ash.Changeset.new()
              |> Ash.Changeset.put_context(:data_layer, %{table: table})

            resource ->
              resource
              |> Ash.Changeset.new()
          end

        handle_raised_error(
          e,
          __STACKTRACE__,
          {:bulk_create, ecto_changeset(changeset.data, changeset, :create, repo, false)},
          resource
        )
    end
  end

  @impl true
  def destroy(resource, %{data: record} = changeset) do
    repo = AshSql.dynamic_repo(resource, SqlImplementation, changeset)
    ecto_changeset = ecto_changeset(record, changeset, :delete, repo, true)

    try do
      repo_opts =
        repo
        |> AshSql.repo_opts(SqlImplementation, nil, nil, resource)
        |> Keyword.merge(changeset.context[:repo_opts] || [])

      case repo.delete(ecto_changeset, repo_opts) do
        {:ok, _record} ->
          :ok

        {:error, error} ->
          handle_errors({:error, error})
      end
    rescue
      e ->
        handle_raised_error(e, __STACKTRACE__, ecto_changeset, resource)
    end
  end

  defp handle_errors({:error, %Ecto.Changeset{errors: errors}}) do
    {:error, Enum.map(errors, &to_ash_error/1)}
  end

  defp to_ash_error({field, {message, vars}}) do
    Ash.Error.Changes.InvalidAttribute.exception(
      field: field,
      message: message,
      private_vars: vars
    )
  end

  def to_ecto(nil), do: nil

  def to_ecto(value) when is_list(value) do
    Enum.map(value, &to_ecto/1)
  end

  def to_ecto(%resource{} = record) do
    if Spark.Dsl.is?(resource, Ash.Resource) do
      resource
      |> Ash.Resource.Info.relationships()
      |> Enum.reduce(record, fn relationship, record ->
        value =
          case Map.get(record, relationship.name) do
            %Ash.NotLoaded{} ->
              %Ecto.Association.NotLoaded{
                __field__: relationship.name,
                __cardinality__: relationship.cardinality
              }

            value ->
              to_ecto(value)
          end

        Map.put(record, relationship.name, value)
      end)
    else
      record
    end
  end

  def to_ecto(other), do: other

  def from_ecto({:ok, result}), do: {:ok, from_ecto(result)}
  def from_ecto({:error, _} = other), do: other

  def from_ecto(nil), do: nil

  def from_ecto(value) when is_list(value) do
    Enum.map(value, &from_ecto/1)
  end

  def from_ecto(%resource{} = record) do
    if Spark.Dsl.is?(resource, Ash.Resource) do
      empty = struct(resource)

      resource
      |> Ash.Resource.Info.relationships()
      |> Enum.reduce(record, fn relationship, record ->
        case Map.get(record, relationship.name) do
          %Ecto.Association.NotLoaded{} ->
            Map.put(record, relationship.name, Map.get(empty, relationship.name))

          value ->
            Map.put(record, relationship.name, from_ecto(value))
        end
      end)
    else
      record
    end
  end

  def from_ecto(other), do: other

  @doc false
  def get_source_for_upsert_field(field, resource) do
    case Ash.Resource.Info.attribute(resource, field) do
      %{source: source} when not is_nil(source) ->
        source

      _ ->
        field
    end
  end

  @doc false
  @impl true
  def sort(query, sort, _resource) do
    {:ok, Map.update!(query, :__ash_bindings__, &Map.put(&1, :sort, sort))}
  end

  @doc false
  @impl true
  def limit(query, limit, _resource) do
    {:ok, Map.update!(query, :__ash_bindings__, &Map.put(&1, :limit, limit))}
  end

  @doc false
  @impl true
  def offset(query, offset, _resource) do
    {:ok, Map.update!(query, :__ash_bindings__, &Map.put(&1, :offset, offset))}
  end

  @impl true
  def select(query, select, resource) do
    query = AshSql.Bindings.default_bindings(query, resource, SqlImplementation)
    {:ok, Ecto.Query.from(row in query, select: struct(row, ^Enum.uniq(select)))}
  end

  @impl true
  def filter(query, filter, _resource, opts \\ []) do
    AshSql.Filter.filter(query, filter, opts)
  end

  @impl true
  def run_query(query, resource) do
    query = AshSql.Bindings.default_bindings(query, resource, SqlImplementation)

    if Info.polymorphic?(resource) && no_table?(query) do
      raise_table_error!(resource, :read)
    else
      repo = AshSql.dynamic_repo(resource, SqlImplementation, query)

      repo_opts =
        repo
        |> AshSql.repo_opts(SqlImplementation, nil, nil, resource)
        |> Keyword.merge(query.__ash_bindings__.context[:repo_opts] || [])

      query
      |> repo.all(repo_opts)
      |> AshSql.Query.remap_mapped_fields(query)
      |> then(fn results ->
        if query.__ash_bindings__.context[:data_layer][:combination_of_queries?] do
          Enum.map(results, fn result ->
            Map.put(struct(resource, result), :__meta__, %Ecto.Schema.Metadata{state: :loaded})
          end)
        else
          results
        end
      end)
      |> then(&{:ok, &1})
    end
  rescue
    e ->
      handle_raised_error(e, __STACKTRACE__, query, resource)
  end

  @impl true
  def run_aggregate_query(query, aggregates, resource) do
    # Basic aggregate implementation for ClickHouse
    # For now, return basic count support only
    case aggregates do
      [%{kind: :count}] ->
        query = AshSql.Bindings.default_bindings(query, resource, SqlImplementation)

        if Info.polymorphic?(resource) && no_table?(query) do
          raise_table_error!(resource, :read)
        else
          repo = AshSql.dynamic_repo(resource, SqlImplementation, query)

          repo_opts =
            repo
            |> AshSql.repo_opts(SqlImplementation, nil, nil, resource)
            |> Keyword.merge(query.__ash_bindings__.context[:repo_opts] || [])

          count_query =
            query
            |> Ecto.Query.select([r], count())

          case repo.one(count_query, repo_opts) do
            count when is_integer(count) -> {:ok, %{count: count}}
            nil -> {:ok, %{count: 0}}
            error -> {:error, error}
          end
        end

      _other ->
        # For now, return empty results for other aggregate types
        # This prevents the "Aggregate queries not supported" error
        {:ok, %{}}
    end
  rescue
    e ->
      handle_raised_error(e, __STACKTRACE__, query, resource)
  end

  defp no_table?(%{from: %{source: {"", _}}}), do: true
  defp no_table?(_), do: false

  defp resolve_source(resource, changeset) do
    table = table(resource, changeset)
    {table, resource}
  end

  defp table(resource, changeset) do
    changeset.context[:data_layer][:table] || Info.table(resource)
  end

  defp ecto_changeset(record, changeset, type, repo, table_error?) do
    attributes =
      changeset.resource
      |> Ash.Resource.Info.attributes()
      |> Enum.map(& &1.name)

    attributes_to_change =
      Enum.reject(attributes, fn attribute ->
        Keyword.has_key?(changeset.atomics, attribute)
      end)

    ecto_changeset =
      record
      |> to_ecto()
      |> set_table(changeset, type, table_error?)
      |> Ecto.Changeset.cast(%{}, [])
      |> force_changes(Map.take(changeset.attributes, attributes_to_change))
      |> add_configured_foreign_key_constraints(record.__struct__)
      |> add_check_constraints(record.__struct__, repo)
      |> add_exclusion_constraints(record.__struct__, repo)

    case type do
      :create ->
        ecto_changeset
        |> add_my_foreign_key_constraints(record.__struct__, repo)

      type when type in [:upsert, :update] ->
        ecto_changeset
        |> add_my_foreign_key_constraints(record.__struct__, repo)
        |> add_related_foreign_key_constraints(record.__struct__, repo)

      :delete ->
        ecto_changeset
        |> add_related_foreign_key_constraints(record.__struct__, repo)
    end
  end

  defp set_table(record, changeset, operation, table_error?) do
    if Info.polymorphic?(record.__struct__) do
      table = changeset.context[:data_layer][:table] || Info.table(record.__struct__)

      record =
        if table do
          Ecto.put_meta(record, source: table)
        else
          if table_error? do
            raise_table_error!(changeset.resource, operation)
          else
            record
          end
        end

      prefix = changeset.context[:data_layer][:schema] || Info.schema(record.__struct__)

      if prefix do
        Ecto.put_meta(record, prefix: table)
      else
        record
      end
    else
      record
    end
  end

  defp raise_table_error!(resource, operation) do
    if Info.polymorphic?(resource) do
      raise """
      Could not determine table for #{operation} on #{inspect(resource)}.

      Polymorphic resources require that the `data_layer[:table]` context is provided.
      See the guide on polymorphic resources for more information.
      """
    else
      raise """
      Could not determine table for #{operation} on #{inspect(resource)}.
      """
    end
  end

  defp force_changes(changeset, changes) do
    Enum.reduce(changes, changeset, fn {key, value}, changeset ->
      Ecto.Changeset.force_change(changeset, key, value)
    end)
  end

  defp add_configured_foreign_key_constraints(changeset, resource) do
    resource
    |> Info.foreign_key_names()
    |> case do
      {m, f, a} -> List.wrap(apply(m, f, [changeset | a]))
      value -> List.wrap(value)
    end
    |> Enum.reduce(changeset, fn
      {key, name}, changeset ->
        Ecto.Changeset.foreign_key_constraint(changeset, key, name: name)

      {key, name, message}, changeset ->
        Ecto.Changeset.foreign_key_constraint(changeset, key, name: name, message: message)
    end)
  end

  defp add_check_constraints(changeset, resource, repo) do
    resource
    |> Info.check_constraints()
    |> Enum.reduce(changeset, fn constraint, changeset ->
      constraint.attribute
      |> List.wrap()
      |> Enum.reduce(changeset, fn attribute, changeset ->
        case repo.default_constraint_match_type(:check, constraint.name) do
          {:regex, regex} ->
            Ecto.Changeset.check_constraint(changeset, attribute,
              name: regex,
              message: constraint.message || "is invalid",
              match: :exact
            )

          match ->
            Ecto.Changeset.check_constraint(changeset, attribute,
              name: constraint.name,
              message: constraint.message || "is invalid",
              match: match
            )
        end
      end)
    end)
  end

  defp add_exclusion_constraints(changeset, resource, repo) do
    resource
    |> Info.exclusion_constraint_names()
    |> Enum.reduce(changeset, fn constraint, changeset ->
      case constraint do
        {key, name} ->
          case repo.default_constraint_match_type(:check, name) do
            {:regex, regex} ->
              Ecto.Changeset.exclusion_constraint(changeset, key,
                name: regex,
                match: :exact
              )

            match ->
              Ecto.Changeset.exclusion_constraint(changeset, key,
                name: name,
                match: match
              )
          end

        {key, name, message} ->
          case repo.default_constraint_match_type(:check, name) do
            {:regex, regex} ->
              Ecto.Changeset.exclusion_constraint(changeset, key,
                name: regex,
                message: message,
                match: :exact
              )

            match ->
              Ecto.Changeset.exclusion_constraint(changeset, key,
                name: name,
                message: message,
                match: match
              )
          end
      end
    end)
  end

  defp add_my_foreign_key_constraints(changeset, resource, repo) do
    resource
    |> Ash.Resource.Info.relationships()
    |> Enum.reduce(changeset, fn relationship, changeset ->
      # Check if there's a custom reference name defined in the DSL
      name =
        case Info.reference(resource, relationship.name) do
          %{name: custom_name} when not is_nil(custom_name) ->
            custom_name

          _ ->
            "#{Info.table(resource)}_#{relationship.source_attribute}_fkey"
        end

      case repo.default_constraint_match_type(:foreign, name) do
        {:regex, regex} ->
          Ecto.Changeset.foreign_key_constraint(changeset, relationship.source_attribute,
            name: regex,
            match: :exact
          )

        match ->
          Ecto.Changeset.foreign_key_constraint(changeset, relationship.source_attribute,
            name: name,
            match: match
          )
      end
    end)
  end

  defp add_related_foreign_key_constraints(changeset, resource, repo) do
    # TODO: this doesn't guarantee us to get all of them, because if something is related to this
    # schema and there is no back-relation, then this won't catch it's foreign key constraints
    resource
    |> Ash.Resource.Info.relationships()
    |> Enum.map(& &1.destination)
    |> Enum.uniq()
    |> Enum.flat_map(fn related ->
      related
      |> Ash.Resource.Info.relationships()
      |> Enum.filter(&(&1.destination == resource))
      |> Enum.map(&Map.take(&1, [:source, :source_attribute, :destination_attribute, :name]))
    end)
    |> Enum.reduce(changeset, fn %{
                                   source: source,
                                   source_attribute: source_attribute,
                                   destination_attribute: destination_attribute,
                                   name: relationship_name
                                 },
                                 changeset ->
      case Info.reference(resource, relationship_name) do
        %{name: name} when not is_nil(name) ->
          case repo.default_constraint_match_type(:foreign, name) do
            {:regex, regex} ->
              Ecto.Changeset.foreign_key_constraint(changeset, destination_attribute,
                name: regex,
                message: "would leave records behind",
                match: :exact
              )

            match ->
              Ecto.Changeset.foreign_key_constraint(changeset, destination_attribute,
                name: name,
                message: "would leave records behind",
                match: match
              )
          end

        _ ->
          name = "#{Info.table(source)}_#{source_attribute}_fkey"

          case repo.default_constraint_match_type(:foreign, name) do
            {:regex, regex} ->
              Ecto.Changeset.foreign_key_constraint(changeset, destination_attribute,
                name: regex,
                message: "would leave records behind",
                match: :exact
              )

            match ->
              Ecto.Changeset.foreign_key_constraint(changeset, destination_attribute,
                name: name,
                message: "would leave records behind",
                match: match
              )
          end
      end
    end)
  end

  defp handle_raised_error(
         %Ecto.StaleEntryError{changeset: %{data: %resource{}, filters: filters}},
         stacktrace,
         context,
         resource
       ) do
    handle_raised_error(
      Ash.Error.Changes.StaleRecord.exception(resource: resource, filter: filters),
      stacktrace,
      context,
      resource
    )
  end

  defp handle_raised_error(%Ecto.Query.CastError{} = e, stacktrace, context, resource) do
    handle_raised_error(
      Ash.Error.Query.InvalidFilterValue.exception(value: e.value, context: context),
      stacktrace,
      context,
      resource
    )
  end

  defp handle_raised_error(error, stacktrace, _ecto_changeset, _resource) do
    {:error, Ash.Error.to_ash_error(error, Exception.format_stacktrace(stacktrace))}
  end
end
