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

defmodule Astarte.RealmManagement.APIWeb.TriggerView do
  use Astarte.RealmManagement.APIWeb, :view
  alias Astarte.RealmManagement.APIWeb.TriggerView
  alias Astarte.Core.Triggers.SimpleTriggersProtobuf.SimpleTriggerContainer

  use Astarte.Core.Triggers.SimpleTriggersProtobuf

  def render("index.json", %{triggers: triggers}) do
    %{data: render_many(triggers, TriggerView, "trigger_name_only.json")}
  end

  def render("show.json", %{trigger: trigger}) do
    %{data: render_one(trigger, TriggerView, "trigger.json")}
  end

  def render("trigger.json", %{trigger: trigger}) do
    %{
      name: trigger.name,
      action: trigger.action,
      simple_triggers: trigger.simple_triggers
    }
  end

  def render("trigger_name_only.json", %{trigger: trigger}) do
    trigger
  end
end
