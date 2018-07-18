#
# This file is part of Astarte.
#
# Astarte is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# Astarte is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with Astarte.  If not, see <http://www.gnu.org/licenses/>.
#
# Copyright (C) 2017,2018 Ispirata Srl
#

defmodule Astarte.RealmManagement.EngineTest do
  use ExUnit.Case
  require Logger
  alias Astarte.Core.CQLUtils
  alias Astarte.DataAccess.Database
  alias Astarte.RealmManagement.DatabaseTestHelper
  alias Astarte.RealmManagement.Engine

  @test_interface_a_0 """
  {
     "interface_name": "com.ispirata.Hemera.DeviceLog.Status",
     "version_major": 1,
     "version_minor": 0,
     "type": "properties",
     "ownership": "device",
     "mappings": [
        {
          "endpoint": "/filterRules/%{ruleId}/%{filterKey}/value",
          "type": "string",
          "allow_unset": true
        }
     ]
  }
  """

  @test_interface_a_1 """
  {
     "interface_name": "com.ispirata.Hemera.DeviceLog.Status",
     "version_major": 1,
     "version_minor": 2,
     "type": "properties",
     "ownership": "device",
     "mappings": [
       {
         "endpoint": "/filterRules/%{ruleId}/%{filterKey}/value",
         "type": "string",
         "allow_unset": true
       }
     ]
  }
  """

  @test_interface_a_2 """
  {
     "interface_name": "com.ispirata.Hemera.DeviceLog.Status",
     "version_major": 2,
     "version_minor": 2,
     "type": "properties",
     "ownership": "device",
     "mappings": [
       {
         "endpoint": "/filterRules/%{ruleId}/%{filterKey}/value",
         "type": "string",
         "allow_unset": true
       }
     ]
  }
  """

  @test_interface_a_name_mismatch """
  {
     "interface_name": "com.ispirata.Hemera.devicelog.Status",
     "version_major": 2,
     "version_minor": 2,
     "type": "properties",
     "ownership": "device",
     "mappings": [
       {
         "endpoint": "/filterRules/%{ruleId}/%{filterKey}/value",
         "type": "string",
         "allow_unset": true
       }
     ]
  }
  """

  @test_interface_b_0 """
  {
    "interface_name": "com.ispirata.Hemera.DeviceLog.Configuration",
    "version_major": 1,
    "version_minor": 0,
    "type": "properties",
    "ownership": "server",
    "mappings": [
      {
        "endpoint": "/filterRules/%{ruleId}/%{filterKey}/value",
        "type": "string",
        "allow_unset": true
      }
    ]
  }
  """

  @test_draft_interface_a_0 """
  {
    "interface_name": "com.ispirata.Draft",
    "version_major": 0,
    "version_minor": 2,
    "type": "properties",
    "ownership": "server",
    "mappings": [
      {
        "endpoint": "/filterRules/%{ruleId}/%{filterKey}/value",
        "type": "string",
        "allow_unset": true
      },
      {
        "endpoint": "/filterRules/%{ruleId}/%{filterKey}/foo",
        "type": "boolean"
      }
    ]
  }
  """

  @test_draft_interface_b_0 """
  {
   "interface_name": "com.ObjectAggregation",
   "version_major": 0,
   "version_minor": 3,
   "type": "datastream",
   "ownership": "device",
   "aggregation": "object",
   "mappings": [
      {
        "endpoint": "/x",
        "type": "double"
      },
      {
        "endpoint": "/y",
        "type": "double"
      }
    ]
  }
  """

  @test_draft_interface_b_1 """
  {
   "interface_name": "com.ObjectAggregation",
   "version_major": 0,
   "version_minor": 4,
   "type": "datastream",
   "ownership": "device",
   "aggregation": "object",
   "mappings": [
      {
        "endpoint": "/x",
        "type": "double"
      },
      {
        "endpoint": "/y",
        "type": "double"
      },
      {
        "endpoint": "/z",
        "type": "double"
      },
      {
        "endpoint": "/speed",
        "type": "double"
      }

    ]
  }
  """

  @test_draft_interface_c_0 """
  {
   "interface_name": "com.ispirata.TestDatastream",
   "version_major": 0,
   "version_minor": 10,
   "type": "datastream",
   "ownership": "device",
   "mappings": [
      {
        "endpoint": "/%{sensorId}/realValues",
        "type": "double"
      },
      {
        "endpoint": "/%{sensorId}/integerValues",
        "type": "integer"
      }
    ]
  }
  """

  @test_draft_interface_c_1 """
  {
   "interface_name": "com.ispirata.TestDatastream",
   "version_major": 0,
   "version_minor": 15,
   "type": "datastream",
   "ownership": "device",
   "mappings": [
      {
        "endpoint": "/%{sensorId}/realValues",
        "type": "double"
      },
      {
        "endpoint": "/%{sensorId}/integerValues",
        "type": "integer"
      },
      {
        "endpoint": "/%{sensorId}/stringValues",
        "type": "string"
      },
      {
        "endpoint": "/testLong/something",
        "type": "longinteger"
      }
    ]
  }
  """

  @test_draft_interface_c_downgrade """
  {
   "interface_name": "com.ispirata.TestDatastream",
   "version_major": 0,
   "version_minor": 14,
   "type": "datastream",
   "ownership": "device",
   "mappings": [
      {
        "endpoint": "/%{sensorId}/realValues",
        "type": "double"
      },
      {
        "endpoint": "/%{sensorId}/integerValues",
        "type": "integer"
      },
      {
        "endpoint": "/%{sensorId}/stringValues",
        "type": "string"
      },
      {
        "endpoint": "/testLong/something/downgrade",
        "type": "longinteger"
      }
    ]
  }
  """

  @test_draft_interface_c_invalid_change """
  {
   "interface_name": "com.ispirata.TestDatastream",
   "version_major": 0,
   "version_minor": 15,
   "type": "properties",
   "ownership": "device",
   "mappings": [
      {
        "endpoint": "/%{sensorId}/realValues",
        "type": "double"
      },
      {
        "endpoint": "/%{sensorId}/integerValues",
        "type": "integer"
      },
      {
        "endpoint": "/%{sensorId}/stringValues",
        "type": "string"
      },
      {
        "endpoint": "/testLong/something",
        "type": "longinteger"
      }
    ]
  }
  """

  @test_draft_interface_c_incompatible_change """
  {
   "interface_name": "com.ispirata.TestDatastream",
   "version_major": 0,
   "version_minor": 15,
   "type": "datastream",
   "ownership": "device",
   "mappings": [
      {
        "endpoint": "/%{sensorId}/realValues",
        "type": "double"
      },
      {
        "endpoint": "/%{sensorId}/integerValues",
        "type": "double"
      },
      {
        "endpoint": "/%{sensorId}/stringValues",
        "type": "string"
      },
      {
        "endpoint": "/testLong/something",
        "type": "longinteger"
      }
    ]
  }
  """

  @test_draft_interface_c_wrong_update """
  {
   "interface_name": "com.ispirata.TestDatastream",
   "version_major": 0,
   "version_minor": 20,
   "type": "datastream",
   "ownership": "device",
   "mappings": [
      {
        "endpoint": "/%{sensorId}/realValues",
        "type": "double"
      }
    ]
  }
  """

  setup do
    with {:ok, client} <- DatabaseTestHelper.connect_to_test_database() do
      DatabaseTestHelper.seed_test_data(client)
    end
  end

  setup_all do
    with {:ok, client} <- DatabaseTestHelper.connect_to_test_database() do
      DatabaseTestHelper.create_test_keyspace(client)
    end

    on_exit(fn ->
      with {:ok, client} <- DatabaseTestHelper.connect_to_test_database() do
        DatabaseTestHelper.drop_test_keyspace(client)
      end
    end)
  end

  test "install interface" do
    assert Engine.get_interfaces_list("autotestrealm") == {:ok, []}

    assert Engine.install_interface("autotestrealm", @test_interface_a_0) == :ok
    assert Engine.install_interface("autotestrealm", @test_interface_b_0) == :ok

    assert Engine.install_interface("autotestrealm", @test_interface_a_1) ==
             {:error, :already_installed_interface}

    assert Engine.install_interface("autotestrealm", @test_interface_a_2) == :ok

    # It is not possible to delete an interface with a major version different than 0
    assert Engine.delete_interface("autotestrealm", "com.ispirata.Hemera.DeviceLog.Status", 1) ==
             {:error, :forbidden}

    assert unpack_source(
             Engine.interface_source("autotestrealm", "com.ispirata.Hemera.DeviceLog.Status", 1)
           ) == unpack_source({:ok, @test_interface_a_0})

    assert unpack_source(
             Engine.interface_source(
               "autotestrealm",
               "com.ispirata.Hemera.DeviceLog.Configuration",
               1
             )
           ) == unpack_source({:ok, @test_interface_b_0})

    assert unpack_source(
             Engine.interface_source("autotestrealm", "com.ispirata.Hemera.DeviceLog.Status", 2)
           ) == unpack_source({:ok, @test_interface_a_2})

    assert unpack_source(
             Engine.interface_source(
               "autotestrealm",
               "com.ispirata.Hemera.DeviceLog.Missing",
               1
             )
           ) == unpack_source({:error, :interface_not_found})

    assert Engine.list_interface_versions(
             "autotestrealm",
             "com.ispirata.Hemera.DeviceLog.Configuration"
           ) == {:ok, [[major_version: 1, minor_version: 0]]}

    assert Engine.list_interface_versions(
             "autotestrealm",
             "com.ispirata.Hemera.DeviceLog.Missing"
           ) == {:error, :interface_not_found}

    {:ok, interfaces_list} = Engine.get_interfaces_list("autotestrealm")

    sorted_interfaces =
      interfaces_list
      |> Enum.sort()

    assert sorted_interfaces == [
             "com.ispirata.Hemera.DeviceLog.Configuration",
             "com.ispirata.Hemera.DeviceLog.Status"
           ]
  end

  test "interface name case mismatch fail" do
    assert Engine.install_interface("autotestrealm", @test_interface_a_0) == :ok

    assert Engine.install_interface("autotestrealm", @test_interface_a_name_mismatch) ==
             {:error, :invalid_name_casing}
  end

  test "delete interface" do
    assert Engine.install_interface("autotestrealm", @test_draft_interface_a_0) == :ok

    assert Engine.get_interfaces_list("autotestrealm") == {:ok, ["com.ispirata.Draft"]}

    assert unpack_source(Engine.interface_source("autotestrealm", "com.ispirata.Draft", 0)) ==
             unpack_source({:ok, @test_draft_interface_a_0})

    assert Engine.list_interface_versions("autotestrealm", "com.ispirata.Draft") ==
             {:ok, [[major_version: 0, minor_version: 2]]}

    {:ok, client} = Database.connect("autotestrealm")
    d = :crypto.strong_rand_bytes(16)

    e1 =
      CQLUtils.endpoint_id(
        "com.ispirata.TestDatastream",
        0,
        "/filterRules/%{ruleId}/%{filterKey}/value"
      )

    p1 = "/filterRules/0/TEST/value"
    DatabaseTestHelper.seed_properties_test_value(client, d, "com.ispirata.Draft", 0, e1, p1)

    assert DatabaseTestHelper.count_interface_properties_for_device(
             client,
             d,
             "com.ispirata.Draft",
             0
           ) == 1

    assert Engine.delete_interface("autotestrealm", "com.ispirata.Draft", 0) == :ok

    assert DatabaseTestHelper.count_interface_properties_for_device(
             client,
             d,
             "com.ispirata.Draft",
             0
           ) == 0

    assert Engine.get_interfaces_list("autotestrealm") == {:ok, []}

    assert Engine.interface_source("autotestrealm", "com.ispirata.Draft", 0) ==
             {:error, :interface_not_found}

    assert Engine.list_interface_versions("autotestrealm", "com.ispirata.Draft") ==
             {:error, :interface_not_found}

    assert Engine.install_interface("autotestrealm", @test_draft_interface_a_0) == :ok

    assert Engine.get_interfaces_list("autotestrealm") == {:ok, ["com.ispirata.Draft"]}

    assert unpack_source(Engine.interface_source("autotestrealm", "com.ispirata.Draft", 0)) ==
             unpack_source({:ok, @test_draft_interface_a_0})

    assert Engine.list_interface_versions("autotestrealm", "com.ispirata.Draft") ==
             {:ok, [[major_version: 0, minor_version: 2]]}
  end

  test "install object aggregated interface" do
    assert Engine.install_interface("autotestrealm", @test_draft_interface_b_0) == :ok

    assert Engine.get_interfaces_list("autotestrealm") == {:ok, ["com.ObjectAggregation"]}

    assert Engine.list_interface_versions("autotestrealm", "com.ObjectAggregation") ==
             {:ok, [[major_version: 0, minor_version: 3]]}

    assert Engine.delete_interface("autotestrealm", "com.ObjectAggregation", 0) == :ok

    assert Engine.get_interfaces_list("autotestrealm") == {:ok, []}

    assert Engine.list_interface_versions("autotestrealm", "com.ObjectAggregation") ==
             {:error, :interface_not_found}

    # Try again so we can verify if it has been completely deleted

    assert Engine.install_interface("autotestrealm", @test_draft_interface_b_0) == :ok

    assert Engine.get_interfaces_list("autotestrealm") == {:ok, ["com.ObjectAggregation"]}

    assert Engine.list_interface_versions("autotestrealm", "com.ObjectAggregation") ==
             {:ok, [[major_version: 0, minor_version: 3]]}

    assert Engine.delete_interface("autotestrealm", "com.ObjectAggregation", 0) == :ok
  end

  test "delete datastream interface" do
    assert Engine.install_interface("autotestrealm", @test_draft_interface_c_0) == :ok

    assert Engine.get_interfaces_list("autotestrealm") == {:ok, ["com.ispirata.TestDatastream"]}

    assert Engine.list_interface_versions("autotestrealm", "com.ispirata.TestDatastream") ==
             {:ok, [[major_version: 0, minor_version: 10]]}

    {:ok, client} = Database.connect("autotestrealm")
    d = :crypto.strong_rand_bytes(16)
    e1 = CQLUtils.endpoint_id("com.ispirata.TestDatastream", 0, "/%{sensorId}/realValues")
    p1 = "/0/realValues"

    DatabaseTestHelper.seed_datastream_test_data(
      client,
      d,
      "com.ispirata.TestDatastream",
      0,
      e1,
      p1
    )

    e2 = CQLUtils.endpoint_id("com.ispirata.TestDatastream", 0, "/%{sensorId}/integerValues")
    p2 = "/0/integerValues"

    DatabaseTestHelper.seed_datastream_test_data(
      client,
      d,
      "com.ispirata.TestDatastream",
      0,
      e2,
      p2
    )

    assert Engine.delete_interface("autotestrealm", "com.ispirata.TestDatastream", 0) == :ok

    assert DatabaseTestHelper.count_rows_for_datastream(
             client,
             d,
             "com.ispirata.TestDatastream",
             0,
             e1,
             p1
           ) == 0

    assert DatabaseTestHelper.count_rows_for_datastream(
             client,
             d,
             "com.ispirata.TestDatastream",
             0,
             e2,
             p2
           ) == 0
  end

  test "update individual datastream interface" do
    assert Engine.install_interface("autotestrealm", @test_draft_interface_c_0) == :ok

    assert unpack_source(
             Engine.interface_source("autotestrealm", "com.ispirata.TestDatastream", 0)
           ) == unpack_source({:ok, @test_draft_interface_c_0})

    assert Engine.get_interfaces_list("autotestrealm") == {:ok, ["com.ispirata.TestDatastream"]}

    assert Engine.list_interface_versions("autotestrealm", "com.ispirata.TestDatastream") ==
             {:ok, [[major_version: 0, minor_version: 10]]}

    assert Engine.update_interface("autotestrealm", @test_draft_interface_c_1) == :ok

    assert unpack_source(
             Engine.interface_source("autotestrealm", "com.ispirata.TestDatastream", 0)
           ) == unpack_source({:ok, @test_draft_interface_c_1})

    assert Engine.get_interfaces_list("autotestrealm") == {:ok, ["com.ispirata.TestDatastream"]}

    assert Engine.list_interface_versions("autotestrealm", "com.ispirata.TestDatastream") ==
             {:ok, [[major_version: 0, minor_version: 15]]}
  end

  test "update object aggregated interface" do
    assert Engine.install_interface("autotestrealm", @test_draft_interface_b_0) == :ok

    assert unpack_source(Engine.interface_source("autotestrealm", "com.ObjectAggregation", 0)) ==
             unpack_source({:ok, @test_draft_interface_b_0})

    assert Engine.get_interfaces_list("autotestrealm") == {:ok, ["com.ObjectAggregation"]}

    assert Engine.list_interface_versions("autotestrealm", "com.ObjectAggregation") ==
             {:ok, [[major_version: 0, minor_version: 3]]}

    assert Engine.update_interface("autotestrealm", @test_draft_interface_b_1) == :ok

    assert unpack_source(Engine.interface_source("autotestrealm", "com.ObjectAggregation", 0)) ==
             unpack_source({:ok, @test_draft_interface_b_1})

    assert Engine.get_interfaces_list("autotestrealm") == {:ok, ["com.ObjectAggregation"]}

    assert Engine.list_interface_versions("autotestrealm", "com.ObjectAggregation") ==
             {:ok, [[major_version: 0, minor_version: 4]]}

    assert Engine.delete_interface("autotestrealm", "com.ObjectAggregation", 0) == :ok

    assert Engine.get_interfaces_list("autotestrealm") == {:ok, []}
  end

  test "fail update missing interface" do
    assert Engine.update_interface("autotestrealm", @test_draft_interface_b_1) ==
             {:error, :interface_major_version_does_not_exist}

    assert Engine.update_interface("autotestrealm", @test_draft_interface_c_1) ==
             {:error, :interface_major_version_does_not_exist}
  end

  test "fail update with less mappings" do
    assert Engine.install_interface("autotestrealm", @test_draft_interface_c_0) == :ok

    assert Engine.get_interfaces_list("autotestrealm") == {:ok, ["com.ispirata.TestDatastream"]}

    assert Engine.list_interface_versions("autotestrealm", "com.ispirata.TestDatastream") ==
             {:ok, [[major_version: 0, minor_version: 10]]}

    assert Engine.update_interface("autotestrealm", @test_draft_interface_c_wrong_update) ==
             {:error, :missing_endpoints}

    assert Engine.get_interfaces_list("autotestrealm") == {:ok, ["com.ispirata.TestDatastream"]}

    assert Engine.list_interface_versions("autotestrealm", "com.ispirata.TestDatastream") ==
             {:ok, [[major_version: 0, minor_version: 10]]}

    assert Engine.update_interface("autotestrealm", @test_draft_interface_c_1) == :ok

    assert unpack_source(
             Engine.interface_source("autotestrealm", "com.ispirata.TestDatastream", 0)
           ) == unpack_source({:ok, @test_draft_interface_c_1})

    assert Engine.install_interface("autotestrealm", @test_draft_interface_c_wrong_update) ==
             {:error, :already_installed_interface}

    assert Engine.get_interfaces_list("autotestrealm") == {:ok, ["com.ispirata.TestDatastream"]}

    assert Engine.list_interface_versions("autotestrealm", "com.ispirata.TestDatastream") ==
             {:ok, [[major_version: 0, minor_version: 15]]}
  end

  test "fail on interface type change" do
    assert Engine.install_interface("autotestrealm", @test_draft_interface_c_0) == :ok

    assert Engine.get_interfaces_list("autotestrealm") == {:ok, ["com.ispirata.TestDatastream"]}

    assert Engine.list_interface_versions("autotestrealm", "com.ispirata.TestDatastream") ==
             {:ok, [[major_version: 0, minor_version: 10]]}

    assert Engine.update_interface("autotestrealm", @test_draft_interface_c_invalid_change) ==
             {:error, :invalid_update}

    assert unpack_source(
             Engine.interface_source("autotestrealm", "com.ispirata.TestDatastream", 0)
           ) == unpack_source({:ok, @test_draft_interface_c_0})

    assert Engine.get_interfaces_list("autotestrealm") == {:ok, ["com.ispirata.TestDatastream"]}

    assert Engine.list_interface_versions("autotestrealm", "com.ispirata.TestDatastream") ==
             {:ok, [[major_version: 0, minor_version: 10]]}
  end

  test "fail on mapping incompatible change" do
    assert Engine.install_interface("autotestrealm", @test_draft_interface_c_0) == :ok

    assert Engine.get_interfaces_list("autotestrealm") == {:ok, ["com.ispirata.TestDatastream"]}

    assert Engine.list_interface_versions("autotestrealm", "com.ispirata.TestDatastream") ==
             {:ok, [[major_version: 0, minor_version: 10]]}

    assert Engine.update_interface("autotestrealm", @test_draft_interface_c_incompatible_change) ==
             {:error, :incompatible_endpoint_change}

    assert unpack_source(
             Engine.interface_source("autotestrealm", "com.ispirata.TestDatastream", 0)
           ) == unpack_source({:ok, @test_draft_interface_c_0})

    assert Engine.get_interfaces_list("autotestrealm") == {:ok, ["com.ispirata.TestDatastream"]}

    assert Engine.list_interface_versions("autotestrealm", "com.ispirata.TestDatastream") ==
             {:ok, [[major_version: 0, minor_version: 10]]}
  end

  test "fail on interface downgrade" do
    assert Engine.install_interface("autotestrealm", @test_draft_interface_c_0) == :ok

    assert unpack_source(
             Engine.interface_source("autotestrealm", "com.ispirata.TestDatastream", 0)
           ) == unpack_source({:ok, @test_draft_interface_c_0})

    assert Engine.list_interface_versions("autotestrealm", "com.ispirata.TestDatastream") ==
             {:ok, [[major_version: 0, minor_version: 10]]}

    assert Engine.update_interface("autotestrealm", @test_draft_interface_c_1) == :ok

    assert unpack_source(
             Engine.interface_source("autotestrealm", "com.ispirata.TestDatastream", 0)
           ) == unpack_source({:ok, @test_draft_interface_c_1})

    assert Engine.list_interface_versions("autotestrealm", "com.ispirata.TestDatastream") ==
             {:ok, [[major_version: 0, minor_version: 15]]}

    assert Engine.update_interface("autotestrealm", @test_draft_interface_c_downgrade) ==
             {:error, :downgrade_not_allowed}

    assert unpack_source(
             Engine.interface_source("autotestrealm", "com.ispirata.TestDatastream", 0)
           ) == unpack_source({:ok, @test_draft_interface_c_1})

    assert Engine.list_interface_versions("autotestrealm", "com.ispirata.TestDatastream") ==
             {:ok, [[major_version: 0, minor_version: 15]]}
  end

  test "get JWT public key PEM with existing realm" do
    assert Engine.get_jwt_public_key_pem("autotestrealm") ==
             {:ok, DatabaseTestHelper.jwt_public_key_pem_fixture()}
  end

  test "get JWT public key PEM with unexisting realm" do
    assert Engine.get_jwt_public_key_pem("notexisting") == {:error, :realm_not_found}
  end

  test "update JWT public key PEM" do
    new_pem = "not_exactly_a_PEM_but_will_do"
    assert Engine.update_jwt_public_key_pem("autotestrealm", new_pem) == :ok
    assert Engine.get_jwt_public_key_pem("autotestrealm") == {:ok, new_pem}

    # Put the PEM fixture back
    assert Engine.update_jwt_public_key_pem(
             "autotestrealm",
             DatabaseTestHelper.jwt_public_key_pem_fixture()
           ) == :ok

    assert Engine.get_jwt_public_key_pem("autotestrealm") ==
             {:ok, DatabaseTestHelper.jwt_public_key_pem_fixture()}
  end

  test "update JWT public key PEM with unexisting realm" do
    assert Engine.get_jwt_public_key_pem("notexisting") == {:error, :realm_not_found}
  end

  defp unpack_source({:ok, source}) when is_binary(source) do
    {:ok, Poison.decode!(source)}
  end

  defp unpack_source(any) do
    any
  end
end
