defmodule TraderToolbox.Services.Webull.Bars do
  alias Stream, as: S
  use Retry

  @scanner_base_url "https://quoteapi.webullbroker.com/api/quote/tickerChartDatas/v5"

  defp not_null(str) when is_binary(str), do: !Regex.match?(~r/^null/i, str)

  def parse_bar(str) when is_binary(str) do
    [ts, o, c, h, l, _ma, v, _vwap] = String.split(str, ",") |> Enum.take(8)

    %{
      t: if(not_null(ts), do: String.to_integer(ts)),
      o: if(not_null(o), do: String.to_float(o)),
      c: if(not_null(c), do: String.to_float(c)),
      h: if(not_null(h), do: String.to_float(h)),
      l: if(not_null(l), do: String.to_float(l)),
      v: if(not_null(v), do: String.to_integer(v))
    }
  end

  def parse_bars(string_list) when is_list(string_list),
    do: Enum.map(string_list, &parse_bar/1)

  defp downloader(%{symbol: symbol, webull_id: tid}, type, time) do
    url = "#{@scanner_base_url}/#{tid}?count=800&timestamp=#{time}&type=#{type}"

    IO.puts(url)
    IO.puts("download #{type} bars for ticker #{symbol}, at time #{time}")

    retry with: linear_backoff(20, 2) |> cap(1_500) |> take(10) do
      HTTPoison.get(url)
    after
      {:ok, %HTTPoison.Response{body: body}} ->
        case Jason.decode!(body) do
          [%{"data" => items}] when is_list(items) ->
            IO.puts("downloader: got #{Enum.count(items)} bars")
            items
        end
    else
      error -> IO.warn(error)
    end
  end

  def fetch(t, bar_type, time) when is_binary(t) and is_integer(time) do
    {:ok, ticker} = Stocks.get(t)
    downloader(ticker, bar_type, time)
  end

  def fetch(symbol, bar_type, stop_time, start_time)
      when is_binary(symbol) and
             is_integer(start_time) and
             is_integer(stop_time) do
    {:ok, ticker} = Stocks.get(symbol)

    dl_bars_stream(
      ticker,
      U.get_bar_model(bar_type),
      start_time,
      stop_time,
      U.get_bar_time(bar_type)
    )
  end

  defp dl_bars_stream(ticker = %Ticker{}, bar_model, stop_time, start_time, interval)
       when is_struct(ticker) and
              bar_model in @bar_models and
              is_integer(start_time) and
              is_integer(stop_time) and
              is_integer(interval) do
    Stream.unfold(start_time, &if(&1 < stop_time, do: {&1, &1 - interval}, else: nil))
    |> Task.async_stream(&downloader(ticker, bar_model, &1))
    |> Stream.flat_map(fn {:ok, list} -> Enum.reverse(list) end)
    |> Stream.uniq()
  end

  def save_bars(list, ticker = %Ticker{}, bar_model) when bar_model in @bar_models do
    list
    |> Stream.map(&parse_bars(&1, ticker, bar_model))
    |> Stream.chunk_every(1_500)
    |> bulk_insert_bars(bar_model)
  end

  def bulk_insert_bars(bars, bar_model) when bar_model in @bar_models and is_list(bars),
    do: Repo.insert_all(bar_model, bars)

  def find_oldest(ticker = %Ticker{}, bar_model) when bar_model in @bar_models do
    Repo.all(from(b in bar_model, where: b.ticker_id == ^ticker.id, select: min(b.time)))
    |> Enum.at(0)
    |> DateTime.to_unix()
  end
end
