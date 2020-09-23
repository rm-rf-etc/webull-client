defmodule WebullClient.Assets do
  @ticker_base_url "https://quotes-gw.webullbroker.com/api/search/pc/tickers"
  @screener_url 'https://userapi.webullbroker.com/api/wlas/screener/ng/query'
  @headers [
    {'accept', '*/*'},
    {'pragma', 'no-cache'},
    {'cache-control', 'no-cache'},
    {'content-type', 'application/json'},
    {'referrer', 'https://app.webull.com/screener'},
    {'referrerPolicy', 'no-referrer-when-downgrade'},
    {'app', 'desktop'},
    {'content-type', 'application/json'},
    {'locale', 'eng'},
    {'tz', 'America/New_York'}
  ]

  def fetch_ticker(symbol) when is_binary(symbol) do
    case HTTPoison.get("#{@ticker_base_url}?keyword=#{symbol}&pageIndex=1&pageSize=1") do
      {:ok, %HTTPoison.Response{body: body}} ->
        case Jason.decode!(body)["data"] do
          [ticker] ->
            if String.upcase(ticker["symbol"]) == String.upcase(symbol) do
              {:ok, ticker}
            else
              {:nomatch, nil}
            end

          _ ->
            IO.puts("No matches found")
            {:nomatch, nil}
        end
    end
  end

  defp scanner_fetch(body) do
    {@screener_url, @headers, 'application/json', body}
    |> case do
      tuple -> :httpc.request(:post, tuple, [], [])
    end
  end

  def scanner(lower, upper) when is_integer(lower) and is_integer(upper) do
    IO.puts("scan from #{lower} to #{upper}")

    scanner_fetch(
      Jason.encode!(%{
        "fetch" => 500,
        "rules" => %{
          "wlas.screener.rule.region" => "securities.region.name.6",
          "wlas.screener.rule.marketvalue" => "gte=#{lower}&lte=#{upper}"
        },
        "sort" => %{
          "rule" => "wlas.screener.rule.marketvalue",
          "desc" => false
          # ^ extremely important, do not change!
        }
      })
    )
    |> case do
      {:ok, {_status, _headers, body}} -> body
    end
    |> Jason.decode!()
    |> case do
      %{"items" => []} ->
        nil

      %{"items" => items} ->
        tickers = items |> Enum.map(& &1["ticker"])
        m_value = tickers |> Enum.at(-1) |> Map.get("marketValue") |> String.to_integer()
        # extremely important, ^ do not change!

        IO.puts("got #{Enum.count(tickers)} between #{lower} and #{m_value}")

        {tickers, m_value}
    end
  end

  # def get(symbol) when is_binary(symbol) do
  #   symbol = String.upcase(symbol)

  #   case Repo.all(from(t in Ticker, where: t.symbol == ^symbol, select: t)) do
  #     [t] ->
  #       {:ok, t}

  #     _ ->
  #       case download(symbol) do
  #         {:ok, match} -> {:ok, save_ticker(match)}
  #         {:nomatch, _} -> {:nomatch, nil}
  #       end
  #   end
  # end

  def kept_keys(t = %{"name" => name, "symbol" => symbol, "tickerId" => ticker_id}),
    do: %{
      name: name,
      symbol: symbol,
      exchange: Map.get(t, "disExchangeCode"),
      webull_id: ticker_id
    }

  def get_tickers(lower, upper) do
    {tickers, market_value} = scanner(lower, upper)

    if Enum.count(tickers) == 500 do
      Stream.unfold({market_value, tickers}, fn
        nil ->
          nil

        {val, tickers} ->
          case scanner(val + 1, upper) do
            {t, mv} -> {{val, tickers}, {mv, t}}
            nil -> {{val, tickers}, nil}
          end
      end)
      |> Stream.flat_map(&elem(&1, 1))
    end
  end
end
