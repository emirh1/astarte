#
# This file is part of Astarte.
#
# Copyright 2022 SECO Mind Srl
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

defmodule Astarte.TriggerEngine.AMQPConsumer.Queries do
  alias Astarte.Core.CQLUtils
  alias Astarte.TriggerEngine.Config
  alias Astarte.TriggerEngine.KvStore
  alias Astarte.TriggerEngine.Realms
  alias Astarte.TriggerEngine.Repo
  require Logger

  import Ecto.Query

  def list_policies(realm_name) do
    keyspace_name =
      CQLUtils.realm_name_to_keyspace_name(realm_name, Config.astarte_instance_id!())

    query =
      from k in KvStore,
        prefix: ^keyspace_name,
        where: k.group == "trigger_policy",
        select: k

    with policies when is_list(policies) <- Repo.all(query) do
      {:ok, Enum.map(policies, &extract_name_and_data/1)}
    else
      {:error, %Xandra.Error{} = err} ->
        _ = Logger.warning("Database error: #{inspect(err)}.", tag: "database_error")
        {:error, :database_error}

      {:error, %Xandra.ConnectionError{} = err} ->
        _ =
          Logger.warning("Database connection error: #{inspect(err)}.",
            tag: "database_connection_error"
          )

        {:error, :database_connection_error}
    end
  end

  def list_realms do
    keyspace_name =
      CQLUtils.realm_name_to_keyspace_name("astarte", Config.astarte_instance_id!())

    query =
      from r in Realms,
        prefix: ^keyspace_name,
        select: r.realm_name

    with realms <- Repo.all(query) do
      {:ok, realms}
    else
      {:error, %Xandra.Error{} = err} ->
        _ =
          Logger.warning("Database error while listing realms: #{inspect(err)}.",
            tag: "database_error"
          )

        {:error, :database_error}

      {:error, %Xandra.ConnectionError{} = err} ->
        _ =
          Logger.warning("Database connection error while listing realms: #{inspect(err)}.",
            tag: "database_connection_error"
          )

        {:error, :database_connection_error}
    end
  end

  defp extract_name_and_data(%KvStore{key: name, value: data}) do
    {name, data}
  end

  defp extract_realm_name(%{"realm_name" => name}) do
    name
  end
end
