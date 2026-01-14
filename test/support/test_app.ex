# SPDX-FileCopyrightText: 2019 ash_postgres contributors <https://github.com/ash-project/ash_postgres/graphs.contributors>
#
# SPDX-License-Identifier: MIT

defmodule AshClickhouse.TestApp do
  @moduledoc false
  def start(_type, _args) do
    children = [
      AshClickhouse.TestRepo
    ]

    # See https://hexdocs.pm/elixir/Supervisor.html
    # for other strategies and supported options
    opts = [strategy: :one_for_one, name: AshClickhouse.Supervisor]
    Supervisor.start_link(children, opts)
  end
end
