defmodule WebullClientTest do
  use ExUnit.Case
  doctest WebullClient

  test "greets the world" do
    assert WebullClient.hello() == :world
  end
end
