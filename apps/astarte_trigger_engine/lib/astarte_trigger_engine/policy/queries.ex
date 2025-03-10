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

defmodule Astarte.TriggerEngine.Policy.Queries do
  alias Astarte.Core.CQLUtils
  alias Astarte.Core.Realm
  alias Astarte.TriggerEngine.Config
  alias Astarte.TriggerEngine.KvStore
  alias Astarte.TriggerEngine.Repo

  require Logger

  import Ecto.Query

  def retrieve_policy_data(realm_name, policy_name) do
    with :ok <- verify_realm_name(realm_name),
         {:ok, policy} <-
           do_retrieve_policy_data(realm_name, policy_name) do
      {:ok, policy}
    end
  end

  defp do_retrieve_policy_data(realm_name, policy_name) do
    keyspace_name =
      CQLUtils.realm_name_to_keyspace_name(realm_name, Config.astarte_instance_id!())

    query =
      from k in KvStore,
        prefix: ^keyspace_name,
        where: k.group == "trigger_policy" and k.key == ^policy_name,
        select: k.value

    with policy when policy != nil <- Repo.one!(query) do
      {:ok, policy}
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

  defp verify_realm_name(realm_name) do
    if Realm.valid_name?(realm_name) do
      :ok
    else
      _ =
        Logger.warning("Invalid realm name.",
          tag: "invalid_realm_name",
          realm: realm_name
        )

      {:error, :realm_not_allowed}
    end
  end
end
