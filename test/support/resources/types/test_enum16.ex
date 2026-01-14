defmodule AshClickhouse.Test.Resource.Types.TestEnum16 do
  use AshClickhouse.Type.ChEnum16,
    values: [
      enum16_min: -32768,
      enum16_zero: 0,
      enum16_max: 32767
    ]
end
