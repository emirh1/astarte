#
# This file is part of Astarte.
#
# Copyright 2018 Ispirata Srl
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

defmodule Astarte.Housekeeping.APIWeb.AuthTest do
  use Astarte.Housekeeping.APIWeb.ConnCase

  alias Astarte.Housekeeping.API.JWTTestHelper

  @request_path "/v1/realms"
  @valid_auth_path "^realms$"
  @valid_auth_path_no_delim "realms"
  @non_exact_match_valid_auth_path "^rea.*$"
  @non_matching_auth_path "^stats.*$"

  @expected_data %{"data" => []}

  require Logger

  setup %{conn: conn} do
    {:ok, conn: put_req_header(conn, "accept", "application/json")}
  end

  describe "JWT" do
    test "no token returns 401", %{conn: conn} do
      conn = get(conn, @request_path)
      assert json_response(conn, 401)["errors"]["detail"] == "Missing authorization token"
    end

    test "all access token returns the data", %{conn: conn} do
      conn =
        put_req_header(
          conn,
          "authorization",
          "bearer #{JWTTestHelper.gen_jwt_all_access_token()}"
        )
        |> get(@request_path)

      assert json_response(conn, 200) == @expected_data
    end

    test "valid token returns the data", %{conn: conn} do
      conn =
        put_req_header(
          conn,
          "authorization",
          "bearer #{JWTTestHelper.gen_jwt_token(["^GET$::#{@valid_auth_path}"])}"
        )
        |> get(@request_path)

      assert json_response(conn, 200) == @expected_data
    end

    test "valid token without delimiters returns the data", %{conn: conn} do
      conn =
        put_req_header(
          conn,
          "authorization",
          "bearer #{JWTTestHelper.gen_jwt_token(["GET::#{@valid_auth_path_no_delim}"])}"
        )
        |> get(@request_path)

      assert json_response(conn, 200) == @expected_data
    end

    test "token matching only prefix returns 403", %{conn: conn} do
      conn =
        put_req_header(
          conn,
          "authorization",
          "bearer #{JWTTestHelper.gen_jwt_token(["^GET$::#{@valid_auth_path}"])}"
        )
        |> get("#{@request_path}/suffix")

      assert json_response(conn, 403)["errors"]["detail"] ==
               "Unauthorized access to #{conn.assigns.method} #{conn.assigns.path}. Please verify your permissions"
    end

    test "token for another path returns 403", %{conn: conn} do
      conn =
        put_req_header(
          conn,
          "authorization",
          "bearer #{JWTTestHelper.gen_jwt_token(["^GET$::#{@non_matching_auth_path}"])}"
        )
        |> get(@request_path)

      assert json_response(conn, 403)["errors"]["detail"] ==
               "Unauthorized access to #{conn.assigns.method} #{conn.assigns.path}. Please verify your permissions"
    end

    test "token for both paths returns the data", %{conn: conn} do
      conn =
        put_req_header(
          conn,
          "authorization",
          "bearer #{JWTTestHelper.gen_jwt_token(["^GET$::#{@non_matching_auth_path}", "^GET$::#{@valid_auth_path}"])}"
        )
        |> get(@request_path)

      assert json_response(conn, 200) == @expected_data
    end

    test "token for another method returns 403", %{conn: conn} do
      conn =
        put_req_header(
          conn,
          "authorization",
          "bearer #{JWTTestHelper.gen_jwt_token(["^POST$::#{@valid_auth_path}"])}"
        )
        |> get(@request_path)

      assert json_response(conn, 403)["errors"]["detail"] ==
               "Unauthorized access to #{conn.assigns.method} #{conn.assigns.path}. Please verify your permissions"
    end

    test "token for both methods returns the data", %{conn: conn} do
      conn =
        put_req_header(
          conn,
          "authorization",
          "bearer #{JWTTestHelper.gen_jwt_token(["^POST$::#{@valid_auth_path}", "^GET$::#{@valid_auth_path}"])}"
        )
        |> get(@request_path)

      assert json_response(conn, 200) == @expected_data
    end

    test "token with generic matching regexp returns the data", %{conn: conn} do
      conn =
        put_req_header(
          conn,
          "authorization",
          "bearer #{JWTTestHelper.gen_jwt_token(["^.*$::#{@non_exact_match_valid_auth_path}"])}"
        )
        |> get(@request_path)

      assert json_response(conn, 200) == @expected_data
    end

    test "invalid JWT token returns 401", %{conn: conn} do
      conn =
        put_req_header(
          conn,
          "authorization",
          "bearer invalid_token"
        )
        |> get(@request_path)

      assert json_response(conn, 401)["errors"]["detail"] == "Invalid JWT token"
    end

    test "token with mismatched signature returns 401", %{conn: conn} do
      token = JWTTestHelper.gen_jwt_token_with_wrong_signature(["^GET$::#{@valid_auth_path}"])

      conn =
        put_req_header(conn, "authorization", "bearer #{token}")
        |> get(@request_path)

      assert json_response(conn, 401)["errors"]["detail"] == "Invalid JWT token"
    end
  end
end
