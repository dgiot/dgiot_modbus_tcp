%%%-------------------------------------------------------------------
%%% @author stoneliu
%%% @copyright (C) 2020, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 20. 三月 2021 12:00
%%%-------------------------------------------------------------------
-module(dgiot_modbus_tcp).
-author("stoneliu").

-export([
    start_http/0,
    get_deviceid/2
]).

start_http() ->
    Port = application:get_env(dgiot_modbus_tcp, port, 80),
    {file, Here} = code:is_loaded(?MODULE),
    Dir = filename:dirname(filename:dirname(Here)),
    Root = shuwa_httpc:url_join([Dir, "/priv/"]),
    DocRoot = Root ++ "www",
    shuwa_http_server:start_http(?MODULE, Port, DocRoot).

get_deviceid(ProdcutId, DevAddr) ->
    #{<<"objectId">> := DeviceId} =
        shuwa_parse:get_objectid(<<"Device">>, #{<<"product">> => ProdcutId, <<"devaddr">> => DevAddr}),
    DeviceId.

