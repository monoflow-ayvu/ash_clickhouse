defmodule AshClickhouse.Test.Resource.Types.TestEnum8 do
  use AshClickhouse.Type.ChEnum8,
    values: [
      enum8_min: -128,
      enum8_zero: 0,
      enum8_max: 127
    ]
end
