defmodule AshClickhouse.Type.ChEnum16 do
  @moduledoc """
  A ClickHouse-specific type for abstracting enums into a single type.

  This type is specifically designed to work with ClickHouse's Enum16 types and provides
  the same functionality as Ash.Type.Enum but with ClickHouse-specific optimizations.

  ClickHouse Enum16 requires key-value pairs where values are integers:
  - Enum16('option1' = 1, 'option2' = 2, 'option3' = 3)
  - Enum16('red' = 1000, 'green' = 2000, 'blue' = 3000)

  For example, your existing attribute might look like:
  ```elixir
  attribute :status, :atom, constraints: [one_of: [:open, :closed]]
  ```

  But as that starts to spread around your system, you may find that you want
  to centralize that logic. To do that, use this module to define an Ash type
  easily:

  ```elixir
  defmodule MyApp.TicketStatus do
    use AshClickhouse.Type.ChEnum16, values: [
      open: 1,      # key: value (integer)
      closed: 2,    # key: value (integer)
      escalated: 3  # key: value (integer)
    ]
  end
  ```

  Then, you can rewrite your original attribute as follows:

  ```elixir
  attribute :status, MyApp.TicketStatus
  ```

  ## Value Format Requirements

  **IMPORTANT**: ClickHouse Enum16 requires integer values for each key.
  - Valid: `[open: 1, closed: 2, escalated: 3]`
  - Valid: `[{"open", 1}, {"closed", 2}, {"escalated", 3}]`
  - Invalid: `[:open, :closed, :escalated]` (missing integer values)

  ## Custom input values

  If you need to accept inputs beyond those described above while still mapping them to one
  of the enum values, you can override the `match/1` callback.

  For example, if you want to map both the `:half_empty` and `:half_full` states to the same enum
  value, you could implement it as follows:

  ```elixir
  defmodule MyApp.GlassState do
    use AshClickhouse.Type.ChEnum16, values: [
      empty: 1,
      half_full: 2,
      full: 3
    ]

    def match(:half_empty), do: {:ok, :half_full}
    def match("half_empty"), do: {:ok, :half_full}
    def match(value), do: super(value)
  end
  ```

  ## Value labels and descriptions
  It's possible to associate a label and/or description for each value.

  ```elixir
  defmodule MyApp.TicketStatus do
    use AshClickhouse.Type.ChEnum16,
      values: [
        open: [value: 1, description: "An open ticket"],
        escalated: [value: 2, description: "An escalated ticket"],
        follow_up: [value: 3, label: "Follow up"],
        closed: [value: 4, description: "A closed ticket", label: "Closed"]
      ]
  end
  ```

  ## ClickHouse Specific Features

  This type automatically creates the appropriate ClickHouse Enum16 type and handles:
  - Storage as ClickHouse Enum16 with proper key-value pairs
  - Proper serialization/deserialization for ClickHouse
  - Support for nullable and low cardinality optimizations
  - Integration with ClickHouse's type system
  """
  @doc "The list of valid values (not all input types that match them)"
  @callback values() :: [atom | String.t()]
  @doc "The label of the value, if existing"
  @callback label(atom | String.t()) :: String.t() | nil
  @doc "The description of the value, if existing"
  @callback description(atom | String.t()) :: String.t() | nil
  @doc "The value detail map, if existing"
  @callback details(atom | String.t()) :: %{
              description: String.t() | nil,
              label: String.t() | nil
            }
  @doc "true if a given term matches a value"
  @callback match?(term) :: boolean
  @doc "finds the valid value that matches a given input term"
  @callback match(term) :: {:ok, atom} | :error

  defmacro __using__(opts) do
    quote location: :keep, generated: true, bind_quoted: [opts: opts] do
      use Ash.Type
      alias AshClickhouse.Type.ChEnum16

      require Ash.Expr

      @behaviour Ash.Type.Enum

      @opts opts
      @values_map ChEnum16.build_values_map(opts[:values])
      @values ChEnum16.build_values_list(opts[:values])

      atom_typespec =
        if Enum.any?(@values, &is_atom(&1)) do
          @values
          |> Enum.filter(&is_atom(&1))
          |> Enum.reduce(&{:|, [], [&1, &2]})
        end

      typespec =
        if Enum.any?(@values, &(not is_atom(&1))) do
          if atom_typespec do
            {:|, [],
             [atom_typespec, {{:., [], [{:__aliases__, [alias: false], [:String]}, :t]}, [], []}]}
          else
            {{:., [], [{:__aliases__, [alias: false], [:String]}, :t]}, [], []}
          end
        else
          if atom_typespec do
            atom_typespec
          else
            {:term, [], Elixir}
          end
        end

      @type t() :: unquote(typespec)

      @string_values Enum.map(@values, &to_string/1)

      @any_not_downcase? Enum.any?(@string_values, &(String.downcase(&1) != &1))

      @impl Ash.Type.Enum
      def values, do: @values

      @impl Ash.Type.Enum
      def label(value) when value in @values do
        value
        |> details()
        |> Map.get(:label)
      end

      @impl Ash.Type.Enum
      def description(value) when value in @values do
        value
        |> details()
        |> Map.get(:description)
      end

      @impl Ash.Type.Enum
      def details(value) when value in @values do
        Map.get(@values_map, value)
      end

      @impl Ash.Type
      def storage_type(constraints) do
        constraints
        |> ch_type()
        |> Ch.type()
      end

      def ch_type(_constraints) do
        Ch.Types.enum16(Enum.map(@opts[:values], &normalize_value/1))
      end

      defp normalize_value({k, v}) when is_atom(k) and is_integer(v), do: {to_string(k), v}
      defp normalize_value({k, v}) when is_binary(k) and is_integer(v), do: {k, v}

      defp normalize_value({k, [{:value, v} | _]}) when is_atom(k) when is_integer(v),
        do: {to_string(k), v}

      defp normalize_value({k, [{:value, v} | _]}) when is_binary(k) when is_integer(v),
        do: {k, v}

      @impl Ash.Type
      def generator(_constraints) do
        StreamData.member_of(@values)
      end

      @impl Ash.Type
      def cast_input(nil, _) do
        {:ok, nil}
      end

      def cast_input(value, _) do
        match(value)
      end

      @impl true
      def matches_type?(value, _) when value in @values, do: true
      def matches_type?(_, _), do: false

      @impl Ash.Type
      def cast_stored(nil, _), do: {:ok, nil}

      def cast_stored(value, _) do
        match(value)
      end

      @impl Ash.Type
      def dump_to_native(nil, _) do
        {:ok, nil}
      end

      def dump_to_native(value, _) do
        {:ok, to_string(value)}
      end

      @impl true
      def cast_atomic(new_value, constraints) do
        if Ash.Expr.expr?(new_value) do
          if @any_not_downcase? do
            {:atomic, new_value}
          else
            {:atomic, Ash.Expr.expr(string_downcase(^new_value))}
          end
        else
          case cast_input(new_value, constraints) do
            {:ok, value} -> {:atomic, value}
            {:error, error} -> {:error, error}
          end
        end
      end

      @impl true
      def apply_atomic_constraints(new_value, constraints) do
        if Ash.Expr.expr?(new_value) do
          if @any_not_downcase? do
            error_expr =
              Ash.Expr.expr(
                error(
                  Ash.Error.Changes.InvalidChanges,
                  message: "must be one of %{values}",
                  vars: %{values: ^Enum.join(@values, ", ")}
                )
              )

            Enum.reduce(@values, {:atomic, error_expr}, fn valid_value, {:atomic, expr} ->
              expr =
                Ash.Expr.expr(
                  if string_downcase(^new_value) == string_downcase(^valid_value) do
                    ^valid_value
                  else
                    ^expr
                  end
                )

              {:atomic, expr}
            end)
          else
            {:ok,
             Ash.Expr.expr(
               if ^new_value in ^@values do
                 ^new_value
               else
                 error(
                   Ash.Error.Changes.InvalidChanges,
                   message: "must be one of %{values}",
                   vars: %{values: ^Enum.join(@values, ", ")}
                 )
               end
             )}
          end
        else
          apply_constraints(new_value, constraints)
        end
      end

      @impl Ash.Type.Enum
      @spec match?(term) :: boolean
      def match?(term) do
        case match(term) do
          {:ok, _} -> true
          _ -> false
        end
      end

      @impl Ash.Type.Enum
      def match(value) when value in @values, do: {:ok, value}
      def match(value) when value in @string_values, do: {:ok, String.to_existing_atom(value)}

      def match(value) when is_integer(value) do
        match =
          Enum.find_value(@opts[:values], fn
            {valid_value, valid_int} when valid_int == value ->
              valid_value

            _ ->
              nil
          end)

        if match do
          {:ok, match}
        else
          :error
        end
      end

      def match(value) do
        value =
          value
          |> to_string()
          |> String.downcase()

        match =
          Enum.find_value(@values, fn valid_value ->
            sanitized_valid_value =
              valid_value
              |> to_string()
              |> String.downcase()

            if sanitized_valid_value == value do
              valid_value
            end
          end)

        if match do
          {:ok, match}
        else
          :error
        end
      rescue
        _ ->
          :error
      end

      defoverridable match: 1, storage_type: 1, cast_stored: 2, dump_to_native: 2
    end
  end

  @doc false
  def build_values_map(values) do
    values
    |> verify_values!()
    |> Enum.reduce(%{}, fn
      {value, details}, acc when is_list(details) ->
        details =
          if Keyword.has_key?(details, :label) do
            details
          else
            Keyword.put(details, :label, humanize(value))
          end

        Map.put(acc, value, Map.new(details))

      value, acc ->
        Map.put(acc, value, %{
          description: nil,
          label: humanize(value)
        })
    end)
  end

  @doc false
  def build_values_list(values) do
    values
    |> verify_values!()
    |> Enum.map(fn {key, _value} -> key end)
  end

  defp humanize(value) when is_atom(value) do
    value
    |> to_string()
    |> humanize()
  end

  defp humanize(value) when is_binary(value) do
    value
    |> String.replace(~r([^A-Za-z]), " ")
    |> String.capitalize()
  end

  defp humanize(value), do: value

  @doc false
  def verify_values!(values) when is_list(values) do
    Enum.each(values, fn
      {value, integer_value}
      when (is_atom(value) or is_binary(value)) and is_integer(integer_value) ->
        :ok

      {value, details} when (is_atom(value) or is_binary(value)) and is_list(details) ->
        case Keyword.get(details, :value) do
          integer_value when is_integer(integer_value) ->
            unsupported_opts =
              Enum.filter(details, fn {key, _} -> key not in [:description, :label, :value] end)

            if Enum.empty?(unsupported_opts) do
              :ok
            end

          other ->
            raise "Enum16 requires integer values. Got #{inspect(other)} for #{inspect(value)}."
        end

      other ->
        raise(
          "`values` must be a list of {`atom | string`, integer} or {`atom | string`, [value: integer, description: string, label: string]} tuples for Enum16. Got #{inspect(other)}"
        )
    end)

    values
  end

  def verify_values!(nil) do
    raise("Must provide `values` option for `use #{inspect(__MODULE__)}`")
  end

  def verify_values!(values) do
    raise("Must provide a list in `values`, got #{inspect(values)}")
  end
end
