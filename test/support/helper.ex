defmodule AshClickhouse.Test.Helper do
  def encode_ch_type({:parameterized, {Ch, type}}) do
    type
    |> Ch.Types.encode()
    |> IO.iodata_to_binary()
  end

  def encode_ch_type(type) do
    type
    |> Ch.Types.encode()
    |> IO.iodata_to_binary()
  end
end
