defmodule AstarteAppengineApiWeb.InterfacePlug do

  def init(options) do
    options
  end

  @doc """
  Rewrites any request to "/v1/:realm_name/devices/:device_id/interfaces/:interface/PATH" to "/v1/:realm_name/devices/:device_id/interfaces/:interface".
  Everything will be handled by phoenix router later.
  """
  def call(conn, _opts) do
      if match?(["v1", _realm_name, "devices", _device_id, "interfaces", _interface, _path_component_1 | _], conn.path_info) do
        ["v1", realm_name, "devices", device_id, "interfaces", interface | subpath] = conn.path_info

        joined_path = Enum.join(subpath, "/")
        query_encoded_path = Enum.join(subpath, "%2F")

        new_query_params = Map.put(conn.query_params, "path", joined_path)
        new_query_string = "path=#{query_encoded_path}&#{conn.query_string}"
        new_params = Map.put(conn.params, "path", joined_path)

        %{ conn |
          params: new_params,
          path_info: ["v1", realm_name, "devices", device_id, "interfaces", interface],
          query_params: new_query_params,
          query_string: new_query_string,
          request_path: "/v1/#{realm_name}/devices/#{device_id}/interfaces/#{interface}?#{new_query_string}"
        }
      else
        conn
      end
  end
end
