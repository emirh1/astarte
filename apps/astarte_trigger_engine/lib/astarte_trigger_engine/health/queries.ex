#
# This file is part of Astarte.
#
# Copyright 2020 Ispirata Srl
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

defmodule Astarte.TriggerEngine.Health.Queries do
  alias Astarte.TriggerEngine.Config
  alias Astarte.Core.CQLUtils
  alias Astarte.TriggerEngine.Realms
  alias Astarte.TriggerEngine.Repo
  require Logger

  import Ecto.Query

  def get_astarte_health(consistency) do
    keyspace_name =
      CQLUtils.realm_name_to_keyspace_name("astarte", Config.astarte_instance_id!())

    query =
      from r in Realms,
        prefix: ^keyspace_name,
        select: count(r.realm_name)

    with count when count != nil <- Repo.one!(query, consistency: consistency) do
      :ok
    else
      :error ->
        _ =
          Logger.warning("Cannot retrieve count for astarte.realms table.",
            tag: "health_check_error"
          )

        {:error, :health_check_bad}

      {:error, %Xandra.Error{} = err} ->
        _ =
          Logger.warning("Database error, health is not good: #{inspect(err)}.",
            tag: "health_check_database_error"
          )

        {:error, :health_check_bad}

      {:error, %Xandra.ConnectionError{} = err} ->
        _ =
          Logger.warning("Database error, health is not good: #{inspect(err)}.",
            tag: "health_check_database_connection_error"
          )

        {:error, :database_connection_error}
    end
  end
end
