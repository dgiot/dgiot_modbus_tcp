%%--------------------------------------------------------------------
%% Copyright (c) 2016-2017 John liu <34489690@qq.com>.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%--------------------------------------------------------------------

%% @doc dgiot_modbus_tcp Application
-module(dgiot_modbus_tcp_app).
-behaviour(application).
-emqx_plugin(?MODULE).

%% Application callbacks
-export([start/2, stop/1]).


%%--------------------------------------------------------------------
%% Application callbacks
%%--------------------------------------------------------------------

start(_StartType, _StartArgs) ->
    {ok, Sup} = dgiot_modbus_tcp_sup:start_link(),
    Spec =  dgiot_modbus_tcp:start_http(),
    {ok, _} = supervisor:start_child(Sup, Spec),
    {ok, Sup}.

stop(_State) ->
    ok.
