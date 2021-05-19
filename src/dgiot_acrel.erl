%%%-------------------------------------------------------------------
%%% @author stoneliu
%%% @copyright (C) 2020, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 20. 三月 2021 12:00
%%%-------------------------------------------------------------------
-module(dgiot_acrel).
-author("stoneliu").

-export([
    start_http/0,
    get_deviceid/2,
    get_sinmahe_json/0,
    create_subdev/3,
    test_fault/0,
    send_fault/4,
    send/4,
    get_fault/1,
    post_fault/0,
    update_runstate/3,
    update_powerstate/3
]).

start_http() ->
    Port = application:get_env(dgiot_acrel, port, 80),
    {file, Here} = code:is_loaded(?MODULE),
    Dir = filename:dirname(filename:dirname(Here)),
    Root = shuwa_httpc:url_join([Dir, "/priv/"]),
    DocRoot = Root ++ "www",
    shuwa_http_server:start_http(?MODULE, Port, DocRoot).

get_deviceid(ProdcutId, DevAddr) ->
    #{<<"objectId">> := DeviceId} =
        shuwa_parse:get_objectid(<<"Device">>, #{<<"product">> => ProdcutId, <<"devaddr">> => DevAddr}),
    DeviceId.


send()->

    ok.

send(DeviceId, Name, FaultRule, #{
    <<"FaultType">> := FaultType,
    <<"FaultLevel">> := FaultLevel,
    <<"FaultCode">> := FaultCode} = FaultData) ->
    maps:fold(fun(UserId, Rule, Acc) ->
        IsFaultType = lists:member(shuwa_utils:to_binary(FaultType), maps:get(<<"FaultType">>, Rule)),
        IsFaultLevel = lists:member(shuwa_utils:to_binary(FaultLevel), maps:get(<<"FaultLevel">>, Rule)),
        IsFaultCode = lists:member(shuwa_utils:to_int(FaultCode), maps:get(<<"FaultCode">>, Rule)),
        case (IsFaultType and IsFaultLevel and IsFaultCode) of
            true ->
                send_fault(DeviceId, Name, UserId, #{
                    <<"FaultType">> => maps:get(<<"FaultType">>, FaultData,1),
                    <<"FaultLevel">> => maps:get(<<"FaultLevel">>, FaultData,1),
                    <<"FaultCode">> => maps:get(<<"FaultCode">>, FaultData,1),
                    <<"FaultSrc">> => maps:get(<<"FaultSrc">>, FaultData,0)
                }),
                Acc + 1;
            false -> Acc
        end
              end, 0, FaultRule).

test_fault() ->
    UserId = <<"QOGSAQMoX4">>,
    DeviceId = <<"1e82f1c59b">>,
    Name = <<"数蛙002"/utf8>>,
    FaultData = #{
        <<"FaultType">> => 1,
        <<"FaultLevel">> => 1,
        <<"FaultCode">> => 5,
        <<"FaultSrc">> => 0
    },
    send_fault(DeviceId, Name, UserId,  FaultData).

get_fault(FaultData) ->
    FaultData#{
        <<"FaultType">> =>  get_faulttype(maps:get(<<"FaultType">>, FaultData)),
        <<"FaultLevel">> => get_faultlevel(maps:get(<<"FaultLevel">>, FaultData)),
        <<"FaultCode">> =>  get_faultcode(maps:get(<<"FaultCode">>, FaultData), maps:get(<<"FaultSrc">>, FaultData))
    }.

post_fault() ->
    Rules = shuwa_data:get({sinmahe_json,fault}),
    FaultType =
        maps:fold(fun(K, V, Acc) ->
            Acc ++ [#{<<"id">> => K, <<"text">> => V}]
                  end, [], maps:get(<<"FaultType">>, Rules)),
    FaultLevel =
        maps:fold(fun(K, V, Acc) ->
            Acc ++ [#{<<"id">> => K, <<"text">> => V}]
                  end, [], maps:get(<<"FaultLevel">>, Rules)),
    FaultCode =
        maps:fold(fun(_K, V, Acc) ->
                Acc ++ V
                  end, [], maps:get(<<"FaultCode">>, Rules)),
    #{
        <<"FaultType">> =>  FaultType,
        <<"FaultLevel">> => FaultLevel,
        <<"FaultCode">> =>  FaultCode
    }.

%%
%%#{
%%<<"FaultType">> => 1,
%%<<"FaultLevel">> => 1,
%%<<"FaultCode">> => 5,
%%<<"FaultSrc">> => 0
%%}
send_fault(DeviceId, Name, UserId, FaultData) ->
    lager:info("DeviceId ~p ,Name ~p",[DeviceId,Name]),
    FaultType = get_faulttype(maps:get(<<"FaultType">>, FaultData)),
    FaultLevel = get_faultlevel(maps:get(<<"FaultLevel">>, FaultData)),
    Data = get_faultcode(maps:get(<<"FaultCode">>, FaultData), maps:get(<<"FaultSrc">>, FaultData)),
    Payload = #{
        <<"description">> => <<"设备:"/utf8, Name/binary," ", FaultType/binary>>,
        <<"title">> => FaultLevel,
        <<"ticker">> => DeviceId,
        <<"text">> => Data
    },
    shuwa_umeng:send(UserId, Payload).

get_faulttype(FaultType) ->
    case shuwa_data:get({sinmahe_json,fault}) of
        #{<<"FaultType">> := Type} ->
            maps:get(shuwa_utils:to_binary(FaultType), Type);
        _ -> <<"正常"/utf8>>
    end.

get_faultlevel(FaultLevel) ->
    case shuwa_data:get({sinmahe_json,fault}) of
        #{<<"FaultLevel">> := Level} ->
            maps:get(shuwa_utils:to_binary(FaultLevel), Level);
        _ -> <<"正常"/utf8>>
    end.

get_faultcode(FaultCode, FaultSrc) ->
    case shuwa_data:get({sinmahe_json,fault}) of
        #{<<"FaultCode">> := Codes} ->
            BinFaultCode = shuwa_utils:to_binary(FaultCode),
            Error = <<"Err", BinFaultCode/binary>>,
            lists:filter(fun(#{<<"code">> := Code}) ->
                case Code of
                    FaultSrc -> true;
                    _ -> false
                end
                         end, maps:get(Error, Codes));
        _ -> <<"正常"/utf8>>
    end.

get_sinmahe_json() ->
    {file, Here} = code:is_loaded(?MODULE),
    Dir = filename:dirname(filename:dirname(Here)),
    Root = shuwa_httpc:url_join([Dir, "/priv/"]),
    TplPath = Root ++ "sinmahe.json",
    case file:read_file(TplPath) of
        {ok, Bin} ->
            case catch jsx:decode(Bin, [{labels, binary}, return_maps]) of
                {'EXIT', Reason} ->
                    lager:info("~p",[Reason]),
                    {error, Reason};
                Datas ->
                    shuwa_data:insert({sinmahe_json,fault}, Datas),
                    {ok, Datas}
            end;
        {error, Reason2} ->
            lager:info("~p",[Reason2]),
            {error, Reason2}
    end,
    case shuwa_parse:query_object(<<"Product">>, #{<<"where">> => #{<<"devType">> => <<"sinmahe_PeriodicInformation">>}}) of
        {ok, #{<<"results">> := [#{<<"objectId">> := PeriodProductId} | _]}} ->
            shuwa_data:insert({simahe, <<"PeriodicInformation">>}, PeriodProductId);
        _ ->
            lager:info("Product not exist")
    end,
    case shuwa_parse:query_object(<<"Product">>, #{<<"where">> => #{<<"devType">> => <<"sinmahe_FaultInformation">>}}) of
        {ok, #{<<"results">> := [#{<<"objectId">> := FaultProductId} | _]}} ->
            shuwa_data:insert({simahe, <<"FaultInformation">>}, FaultProductId);
        _ ->
            lager:info("Product not exist")
    end,
    case shuwa_parse:query_object(<<"Product">>, #{<<"where">> => #{<<"devType">> => <<"sinmahe_BasicInformation">>}}) of
        {ok, #{<<"results">> := [#{<<"objectId">> := BasicProductId} | _]}} ->
            shuwa_data:insert({simahe, <<"BasicInformation">>}, BasicProductId);
        _ ->
            lager:info("Product not exist"),
            <<"">>
    end.

create_subdev(ProductId, DevAddr, Type) ->
    SubProductId = shuwa_data:get({simahe, Type}),
    SubDevaddr = <<Type/binary, "_", DevAddr/binary>>,
    case shuwa_parse:get_object(<<"Device">>, dgiot_acrel:get_deviceid(SubProductId, SubDevaddr)) of
        {ok, #{<<"objectId">> := _ObjectId}} -> pass;
        _ ->
            Device = #{
                <<"ACL">> => #{
                    <<"*">> => #{
                        <<"read">> => true
                    }
                },
                <<"devaddr">> => SubDevaddr,
                <<"product">> => #{<<"__type">> => <<"Pointer">>,
                    <<"className">> => <<"Product">>,
                    <<"objectId">> => SubProductId},
                <<"parentId">> => #{<<"__type">> => <<"Pointer">>,
                    <<"className">> => <<"Device">>,
                    <<"objectId">> => dgiot_acrel:get_deviceid(ProductId, DevAddr)},
                <<"route">> => #{DevAddr => SubDevaddr}
            },
            shuwa_parse:create_object(<<"Device">>, Device)
    end.

update_runstate(ProductId,DevAddr,Data) ->
    case maps:get(<<"RunState">>, Data, undefine) of
        undefine -> pass;
        RunState ->
            case shuwa_data:get({sinmahe_runstate, DevAddr}) of
                RunState -> pass;
                _  ->  case shuwa_parse:get_object(<<"Device">>, dgiot_acrel:get_deviceid(ProductId, DevAddr)) of
                           {ok, #{<<"objectId">> := ObjectId, <<"basedata">> := BaseData}} ->
                               shuwa_parse:update_object(<<"Device">>, ObjectId, #{<<"isEnable">> => false,
                                   <<"basedata">> => BaseData#{<<"RunState">> => RunState}});
                           _ ->
                               pass
                       end
            end
    end.

update_powerstate(ProductId,DevAddr,Data) ->
    case maps:get(<<"PowerState">>, Data, undefine) of
        undefine -> pass;
        RunState ->
            case shuwa_data:get({sinmahe_runstate, DevAddr}) of
                RunState -> pass;
                _  ->  case shuwa_parse:get_object(<<"Device">>, dgiot_acrel:get_deviceid(ProductId, DevAddr)) of
                           {ok, #{<<"objectId">> := ObjectId, <<"basedata">> := BaseData}} ->
                               shuwa_parse:update_object(<<"Device">>, ObjectId, #{<<"isEnable">> => false,
                                   <<"basedata">> => BaseData#{<<"PowerState">> => RunState}});
                           _ ->
                               pass
                       end
            end
    end.



