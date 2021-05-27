%%%-------------------------------------------------------------------
%%% @author stoneliu
%%% @copyright (C) 2019, shuwa
%%% @doc
%%% API 处理模块
%%% Created : 20. 三月 2021 12:00
%%% @end
%%%-------------------------------------------------------------------
-module(dgiot_modbus_tcp_handler).
-author("stoneliu").
-behavior(shuwa_rest).
-shuwa_rest(all).
-compile([{parse_transform, lager_transform}]).

%% API
-export([swagger_feeders/0]).
-export([handle/4]).

%% API描述
%% 支持二种方式导入
%% 示例:
%% 1. Metadata为map表示的JSON,
%%    shuwa_http_server:bind(<<"/pump">>, ?MODULE, [], Metadata)
%% 2. 从模块的priv/swagger/下导入
%%    shuwa_http_server:bind(<<"/swagger_feeders.json">>, ?MODULE, [], priv)
swagger_modbus_tcp() ->
    [
        shuwa_http_server:bind(<<"/swagger_modbus_tcp.json">>, ?MODULE, [], priv)
    ].


%%%===================================================================
%%% 请求处理
%%%  如果登录, Context 内有 <<"user">>, version
%%%===================================================================

-spec handle(OperationID :: atom(), Args :: map(), Context :: map(), Req :: shuwa_req:req()) ->
    {Status :: shuwa_req:http_status(), Body :: map()} |
    {Status :: shuwa_req:http_status(), Headers :: map(), Body :: map()} |
    {Status :: shuwa_req:http_status(), Headers :: map(), Body :: map(), Req :: shuwa_req:req()}.

handle(OperationID, Args, Context, Req) ->
    Headers = #{},
    case catch do_request(OperationID, Args, Context, Req) of
        {ErrType, Reason} when ErrType == 'EXIT'; ErrType == error ->
            lager:info("do request: ~p, ~p, ~p~n", [OperationID, Args, Reason]),
            Err = case is_binary(Reason) of
                      true -> Reason;
                      false ->
                          shuwa_framework:format("~p", [Reason])
                  end,
            {500, Headers, #{<<"error">> => Err}};
        ok ->
%%            lager:debug("do request: ~p, ~p ->ok ~n", [OperationID, Args]),
            {200, Headers, #{}, Req};
        {ok, Res} ->
%%            lager:info("do request: ~p, ~p ->~p~n", [OperationID, Args, Res]),
            {200, Headers, Res, Req};
        {Status, Res} ->
%%            lager:info("do request: ~p, ~p ->~p~n", [OperationID, Args, Res]),
            {Status, Headers, Res, Req};
        {Status, NewHeaders, Res} ->
%%            lager:info("do request: ~p, ~p ->~p~n", [OperationID, Args, Res]),
            {Status, maps:merge(Headers, NewHeaders), Res, Req}
    end.


%%%===================================================================
%%% 内部函数 Version:API版本
%%%===================================================================


%% PumpTemplet 概要: 新增报告模板 描述:新增报告模板
%% OperationId:post_pump_templet
%% 请求:get /iotapi/pump/templet
%%  服务器不支持的API接口
do_request(_OperationId, _Args, _Context, _Req) ->
    {error, <<"Not Allowed.">>}.