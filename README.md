# WebullClient

**TODO: Add description**

## Installation

Until it's available in Hex, the package can be installed from github
by adding `candle_charts` to your list of dependencies in `mix.exs`:

```elixir
def deps do
  [
    {
      :webull_client,
      git: "https://github.com/rm-rf-etc/webull-client.git",
      ref: "<the commit hash goes here>"
    }
  ]
end
```

## Example Use

The scanner is only coded for getting stocks within a cap range:
```elixir
tickers =
  WebullClient.Assets.scanner(50_000_000, 150_000_000)
  |> elem(0)
  |> Enum.map(&WebullClient.Assets.kept_keys/1)
```
