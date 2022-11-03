defmodule Lepus.BroadwayHelpers do
  @moduledoc false

  alias Broadway.Message

  @json_content_type "application/json"

  @spec maybe_decode_json(Message.t()) :: Message.t()
  def maybe_decode_json(message) do
    message |> Message.update_data(&maybe_decode_json(message, &1))
  end

  @spec maybe_decode_json(Message.t(), binary()) :: any()
  def maybe_decode_json(message, data) do
    maybe_transform_json(message, data, fn raw ->
      raw
      |> Jason.decode()
      |> case do
        {:ok, decoded} -> decoded
        _ -> raw
      end
    end)
  end

  @spec maybe_encode_json(Message.t()) :: Message.t()
  def maybe_encode_json(message) do
    message |> Message.update_data(&maybe_encode_json(message, &1))
  end

  @spec maybe_encode_json(Message.t(), any()) :: binary()
  def maybe_encode_json(message, data) do
    maybe_transform_json(message, data, fn raw ->
      raw
      |> Jason.encode()
      |> case do
        {:ok, encoded} -> encoded
        _ -> raw
      end
    end)
  end

  @spec get_header_value(Message.t() | map() | list(), binary(), any) :: any
  def get_header_value(%{metadata: metadata}, name, default) do
    get_header_value(metadata, name, default)
  end

  def get_header_value(%{headers: headers}, name, default) when is_list(headers) do
    get_header_value(headers, name, default)
  end

  def get_header_value(headers, name, default) when is_list(headers) do
    headers
    |> Enum.split_with(fn
      {^name, _, _} -> true
      _ -> false
    end)
    |> case do
      {[{_, _, value} | _], _} -> value
      _ -> default
    end
  end

  def get_header_value(_, _, default), do: default

  @spec update_headers(any(), list()) :: list()
  def update_headers(headers, tuples) when is_list(headers) do
    [tuples | headers] |> List.flatten() |> Enum.uniq_by(fn {k, _, _} -> k end)
  end

  def update_headers(_headers, tuples), do: tuples

  @spec set_lepus_metadata(Message.t(), map()) :: Message.t()
  def set_lepus_metadata(message, lepus_metadata) do
    message |> Map.update!(:metadata, &Map.put(&1, :lepus, lepus_metadata))
  end

  @spec put_in_lepus_metadata(Message.t(), any(), any()) :: Message.t()
  def put_in_lepus_metadata(message, key, value) do
    message
    |> Map.update!(:metadata, fn metadata ->
      metadata |> Map.update(:lepus, %{key => value}, &Map.put(&1, key, value))
    end)
  end

  defp maybe_transform_json(%{metadata: %{content_type: @json_content_type}}, data, fun) do
    fun.(data)
  end

  defp maybe_transform_json(_message, data, _fun), do: data
end
