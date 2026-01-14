# SPDX-FileCopyrightText: 2019 ash_postgres contributors <https://github.com/ash-project/ash_postgres/graphs.contributors>
#
# SPDX-License-Identifier: MIT

defmodule AshClickhouse.RepoCase do
  @moduledoc false
  use ExUnit.CaseTemplate

  using do
    quote do
      alias AshClickhouse.TestRepo

      import Ecto
      import Ecto.Query
      import AshClickhouse.RepoCase

      # and any other stuff
    end
  end

  setup _tags do
    :ok
  end
end
