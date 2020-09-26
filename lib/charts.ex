defmodule TraderToolbox.Services.Webull.Bars do
  alias Stream, as: S
  alias Task, as: T
  alias Enum, as: E
  use Retry

  @scanner_base_url "https://quoteapi.webullbroker.com/api/quote/tickerChartDatas/v5"
  @bar_models [:m1, :m5, :m10, :m30, :h1, :h2, :h3, :h4, :d1, :d7]

  defp not_null(str) when is_binary(str), do: !Regex.match?(~r/^null/i, str)

  def parse_bar(str) when is_binary(str) do
    str
    |> String.split(",")
    |> E.take(8)
    |> case do
      [ts, o, c, h, l, _ma, v, _vwap] ->
        %{
          t: if(not_null(ts), do: String.to_integer(ts)),
          o: if(not_null(o), do: String.to_float(o)),
          c: if(not_null(c), do: String.to_float(c)),
          h: if(not_null(h), do: String.to_float(h)),
          l: if(not_null(l), do: String.to_float(l)),
          v: if(not_null(v), do: String.to_integer(v))
        }
    end
  end

  def parse_bars(string_list) when is_list(string_list),
    do: E.map(string_list, &parse_bar/1)

  defp downloader(%{symbol: symbol, webull_id: tid}, type, time) do
    url = "#{@scanner_base_url}/#{tid}?count=800&timestamp=#{time}&type=#{type}"

    IO.puts(url)
    IO.puts("download #{type} bars for ticker #{symbol}, at time #{time}")

    retry with: linear_backoff(20, 2) |> cap(1_500) |> E.take(10) do
      HTTPoison.get(url)
    after
      {:ok, %HTTPoison.Response{body: body}} ->
        case Jason.decode!(body) do
          [%{"data" => items}] when is_list(items) ->
            IO.puts("downloader: got #{E.count(items)} bars")
            items
        end
    else
      error -> IO.warn(error)
    end
  end

  # def fetch(symbol, bar_type, time) when is_binary(symbol) and is_integer(time) do
  #   {:ok, ticker} = Stocks.get(symbol)
  #   downloader(ticker, bar_type, time)
  # end

  # def fetch(symbol, bar_type, stop_time, start_time)
  #     when is_binary(symbol) and
  #            is_integer(start_time) and
  #            is_integer(stop_time) do
  #   {:ok, ticker} = Stocks.get(symbol)

  #   dl_bars_stream(
  #     ticker,
  #     U.get_bar_model(bar_type),
  #     start_time,
  #     stop_time,
  #     U.get_bar_time(bar_type)
  #   )
  # end

  def dl_bars_stream(ticker, bar_model, stop_time, start_time, interval)
      when is_map(ticker) and
             bar_model in @bar_models and
             is_integer(start_time) and
             is_integer(stop_time) and
             start_time < stop_time and
             is_integer(interval) do
    S.unfold(start_time, fn n ->
      if(n < stop_time, do: {n, n - interval}, else: nil)
    end)
    |> T.async_stream(&downloader(ticker, bar_model, &1))
    |> S.flat_map(fn {:ok, list} -> E.reverse(list) end)
    |> S.uniq()
  end

  # def bulk_insert_bars(bars, bar_model) when bar_model in @bar_models and is_list(bars),
  #   do: Repo.insert_all(bar_model, bars)

  # def find_oldest(ticker = %Ticker{}, bar_model) when bar_model in @bar_models do
  #   Repo.all(from(b in bar_model, where: b.ticker_id == ^ticker.id, select: min(b.time)))
  #   |> E.at(0)
  #   |> DateTime.to_unix()
  # end
end
