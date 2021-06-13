%%%-------------------------------------------------------------------
%%% @copyright (C) 2019, <COMPANY>
%%% @doc
%%%  前置机客户端
%%% @end
%%% Created : 20. 三月 2021 12:00
%%%-------------------------------------------------------------------
-module(dgiot_modbus_tcp_channel).
-behavior(shuwa_channelx).
-author("johnliu").
-include_lib("shuwa_framework/include/shuwa_socket.hrl").
-include("dgiot_modbus_tcp.hrl").
-define(TYPE, <<"MODBUS_TCP">>).
-define(MAX_BUFF_SIZE, 1024).
-record(state, {
    id,
    devaddr = <<>>,
    heartcount = 0,
    regtype = <<>>,
    head = "xxxxxx0eee",
    len = 0,
    app = <<>>,
    product = <<>>,
    deviceId = <<>>,
    scale = 10,
    temperature = 0,
    env = <<>>
}).
%% API
-export([start/2]).

%% Channel callback
-export([init/3, handle_init/1, handle_event/3, handle_message/2, stop/3]).
%% TCP callback
-export([init/1, handle_info/2, handle_cast/2, handle_call/3, terminate/2, code_change/3]).

%% 注册通道类型
-channel(?TYPE).
-channel_type(#{
    type => 1,
    title => #{
        zh => <<"MODBUS_TCP通道"/utf8>>
    },
    description => #{
        zh => <<"MODBUS_TCP通道"/utf8>>
    }
}).
%% 注册通道参数
-params(#{
    <<"port">> => #{
        order => 1,
        type => integer,
        required => true,
        default => 20110,
        title => #{
            zh => <<"端口"/utf8>>
        },
        description => #{
            zh => <<"侦听端口"/utf8>>
        }
    },
    <<"regtype">> => #{
        order => 2,
        type => string,
        required => true,
        default => <<"上传Mac"/utf8>>,
        title => #{
            zh => <<"注册类型"/utf8>>
        },
        description => #{
            zh => <<"上传Mac"/utf8>>
        }
    },
    <<"regular">> => #{
        order => 3,
        type => string,
        required => true,
        default => <<"9C-A5-25-**-**-**">>,
        title => #{
            zh => <<"登录报文帧头"/utf8>>
        },
        description => #{
            zh => <<"填写正则表达式匹配login"/utf8>>
        }
    },
    <<"DTUTYPE">> => #{
        order => 4,
        type => string,
        required => true,
        default => <<"usr">>,
        title => #{
            zh => <<"控制器厂商"/utf8>>
        },
        description => #{
            zh => <<"控制器厂商"/utf8>>
        }
    },
    <<"heartbeat">> => #{
        order => 5,
        type => integer,
        required => true,
        default => 10,
        title => #{
            zh => <<"心跳周期"/utf8>>
        },
        description => #{
            zh => <<"心跳周期"/utf8>>
        }
    }
}).

start(ChannelId, ChannelArgs) ->
    shuwa_channelx:add(?TYPE, ChannelId, ?MODULE, ChannelArgs).

%% 通道初始化
init(?TYPE, ChannelId, #{
    <<"port">> := Port,
    <<"heartbeat">> := Heartbeat,
    <<"regtype">> := Type,
    <<"regular">> := Regular,
    <<"product">> := Products
} = _Args) ->
    [{ProdcutId, App} | _] = get_app(Products),
    {Header, Len} = get_header(Regular),
    State = #state{
        id = ChannelId,
        regtype = Type,
        head = Header,
        len = Len,
        app = App,
        product = ProdcutId
    },
    shuwa_data:insert({ChannelId, heartbeat}, {Heartbeat, Port}),
    {ok, State, shuwa_tcp_server:child_spec(?MODULE, shuwa_utils:to_int(Port), State)};

init(?TYPE, _ChannelId, _Args) ->
    {ok, #{}, #{}}.

handle_init(State) ->
    {ok, State}.

%% 通道消息处理,注意：进程池调用
handle_event(_EventId, _Event, State) ->
    {ok, State}.

handle_message(_Message, State) ->
    {ok, State}.

stop(_ChannelType, _ChannelId, _State) ->
    ok.

%% =======================
%% {ok, State} | {stop, Reason}
%%init(TCPState) ->
%%    erlang:send_after(5 * 1000, self(), login),
%%    {ok, TCPState}.

init(#tcp{state = #state{id = ChannelId}} = TCPState) ->
    lager:info("ChannelId ~p", [ChannelId]),
    case shuwa_bridge:get_products(ChannelId) of
        {ok, _TYPE, _ProductIds} ->
            {ok, TCPState};
        {error, not_find} ->
            {stop, not_find_channel}
    end.

%% 9C A5 25 CD 00 DB
%% 11 04 02 06 92 FA FE
handle_info({tcp, Buff}, #tcp{socket = Socket, state = #state{id = ChannelId, devaddr = <<>>, head = Head, len = Len, product = ProductId} = State} = TCPState) ->
    shuwa_bridge:send_log(ChannelId, "DTU revice from  ~p", [shuwa_utils:binary_to_hex(Buff)]),
    DTUIP = shuwa_evidence:get_ip(Socket),
    DtuAddr = shuwa_utils:binary_to_hex(Buff),
    List = shuwa_utils:to_list(DtuAddr),
    List1 = shuwa_utils:to_list(Buff),
    #{<<"objectId">> := DeviceId} =
        shuwa_parse:get_objectid(<<"Device">>, #{<<"product">> => ProductId, <<"devaddr">> => DtuAddr}),
    case re:run(DtuAddr, Head, [{capture, first, list}]) of
        {match, [Head]} when length(List) == Len ->
            {DevId, Devaddr} =
                case create_device(DeviceId, ProductId, DtuAddr, DTUIP) of
                    {<<>>, <<>>} ->
                        {<<>>, <<>>};
                    {DevId1, Devaddr1} ->
                        {DevId1, Devaddr1}
                end,
            {noreply, TCPState#tcp{buff = <<>>, state = State#state{devaddr = Devaddr, deviceId = DevId}}};
        _Error ->
            case re:run(Buff, Head, [{capture, first, list}]) of
                {match, [Head]} when length(List1) == Len ->
                    create_device(DeviceId, ProductId, Buff, DTUIP),
                    {noreply, TCPState#tcp{buff = <<>>, state = State#state{devaddr = Buff}}};
                Error1 ->
                    lager:info("Error1 ~p Buff ~p ", [Error1, shuwa_utils:to_list(Buff)]),
                    {noreply, TCPState#tcp{buff = <<>>}}
            end
    end;

handle_info({tcp, Buff}, #tcp{state = #state{id = ChannelId, devaddr = DtuAddr, env = #{product := ProductId, pn := Pn, di := Di}, product = DtuProductId} = State} = TCPState) ->
    shuwa_bridge:send_log(ChannelId, "revice from  ~p", [shuwa_utils:binary_to_hex(Buff)]),
    case modbus_rtu:parse_frame(Buff, [], #{
        <<"dtuproduct">> => ProductId,
        <<"channel">> => ChannelId,
        <<"dtuaddr">> => DtuAddr,
        <<"slaveId">> => shuwa_utils:to_int(Pn),
        <<"address">> => Di}) of
        {_, Things} ->
            NewTopic = <<"thing/", DtuProductId/binary, "/", DtuAddr/binary, "/post">>,
            shuwa_bridge:send_log(ChannelId, "end to_task: ~p: ~p ~n", [NewTopic, jsx:encode(Things)]),
            shuwa_mqtt:publish(DtuAddr, NewTopic, jsx:encode(Things));
        Other ->
            lager:info("Other ~p", [Other]),
            pass
    end,
    {noreply, TCPState#tcp{buff = <<>>, state = State#state{env = <<>>}}};

handle_info({deliver, Topic, Msg}, #tcp{state = #state{id = ChannelId, product = DtuProductId} = State} = TCPState) ->
    Payload = shuwa_mqtt:get_payload(Msg),
    shuwa_bridge:send_log(ChannelId, "begin from_task: ~ts: ~ts ", [unicode:characters_to_list(Topic), unicode:characters_to_list(Payload)]),
    case jsx:is_json(Payload) of
        true ->
            Data = jsx:decode(Payload, [{labels, binary}, return_maps]),
            case binary:split(Topic, <<$/>>, [global, trim]) of
                %%接收task采集指令
                [<<"thing">>, DtuProductId, DtuAddr] ->
                    Env =
                        case Data of
                            #{<<"thingdata">> := #{
                                <<"command">> := <<"r">>,
                                <<"data">> := Value,
                                <<"di">> := Di,
                                <<"pn">> := SlaveId,
                                <<"product">> := ProductId,
                                <<"protocol">> := <<"modbus">>} = Thingdata1} ->
                                Datas = modbus_rtu:to_frame(#{
                                    <<"addr">> => SlaveId,
                                    <<"value">> => Value,
                                    <<"productid">> => ProductId,
                                    <<"di">> => Di
                                }),
                                lists:map(fun(X) ->
                                    shuwa_bridge:send_log(ChannelId, "to_device: ~p ", [shuwa_utils:binary_to_hex(X)]),
                                    shuwa_tcp_server:send(TCPState, X)
                                          end, Datas),
                                #{product => ProductId, pn => SlaveId, di => Di};
                            _ ->
                                <<>>
                        end,
                    {noreply, TCPState#tcp{state = State#state{env = Env}, buff = <<>>}};
                %%接收task汇聚过来的整个dtu物模型采集的数据
                [App, DtuProductId, DtuAddr] ->
                    shuwa_pumpdtu:save_dtu(Data#{<<"devaddr">> => DtuAddr, <<"app">> => App}),
                    {noreply, TCPState};
                _Other ->
                    lager:info("_Other ~p ", [_Other]),
                    {noreply, TCPState}
            end;
        false ->
            {noreply, TCPState}
    end;

%% {stop, TCPState} | {stop, Reason} | {ok, TCPState} | ok | stop
handle_info(_Info, TCPState) ->
    lager:info("TCPState ~p", [TCPState]),
    {noreply, TCPState}.

handle_call(_Msg, _From, TCPState) ->
    {reply, ok, TCPState}.

handle_cast(_Msg, TCPState) ->
    {noreply, TCPState}.

terminate(_Reason, _TCPState) ->
    ok.

code_change(_OldVsn, TCPState, _Extra) ->
    {ok, TCPState}.

get_header(Regular) ->
    lists:foldl(fun(X, {Header, Len}) ->
        case X of
            "**" -> {Header, Len + length(X)};
            _ -> {Header ++ X, Len + length(X)}
        end
                end, {[], 0},
        re:split(shuwa_utils:to_list(Regular), "-", [{return, list}])).


get_app(Products) ->
    lists:map(fun({ProdcutId, #{<<"ACL">> := Acl}}) ->
        Predicate = fun(E) ->
            case E of
                <<"role:", _/binary>> -> true;
                _ -> false
            end
                    end,
        [<<"role:", App/binary>> | _] = lists:filter(Predicate, maps:keys(Acl)),
        {ProdcutId, App}
              end, Products).

create_device(DeviceId, ProductId, DTUMAC, DTUIP) ->
    case shuwa_parse:get_object(<<"Product">>, ProductId) of
        {ok, #{<<"ACL">> := Acl, <<"devType">> := DevType}} ->
            case shuwa_parse:get_object(<<"Device">>, DeviceId) of
                {ok, #{<<"results">> := [#{<<"devaddr">> := _GWAddr} | _] = _Result}} ->
                    shuwa_parse:update_object(<<"Device">>, DeviceId, #{<<"ip">> => DTUIP, <<"status">> => <<"ONLINE">>}),
                    shuwa_task:save_pnque(ProductId, DTUMAC, ProductId, DTUMAC),
                    create_instruct(Acl, ProductId, DeviceId),
                    {DeviceId, DTUMAC};
                _ ->
                    shuwa_shadow:create_device(#{
                        <<"devaddr">> => DTUMAC,
                        <<"name">> => <<"USRDTU", DTUMAC/binary>>,
                        <<"ip">> => DTUIP,
                        <<"isEnable">> => true,
                        <<"product">> => ProductId,
                        <<"ACL">> => Acl,
                        <<"status">> => <<"ONLINE">>,
                        <<"location">> => #{<<"__type">> => <<"GeoPoint">>, <<"longitude">> => 120.161324, <<"latitude">> => 30.262441},
                        <<"brand">> => <<"USRDTU">>,
                        <<"devModel">> => DevType
                    }),
                    shuwa_task:save_pnque(ProductId, DTUMAC, ProductId, DTUMAC),
                    create_instruct(Acl, ProductId, DeviceId),
                    {DeviceId, DTUMAC}
            end;
        Error2 ->
            lager:info("Error2 ~p ", [Error2]),
            {<<>>, <<>>}
    end.

create_instruct(ACL, DtuProductId, DtuDevId) ->
    case shuwa_shadow:lookup_prod(DtuProductId) of
        {ok, #{<<"thing">> := #{<<"properties">> := Properties}}} ->
            lists:map(fun(Y) ->
                case Y of
                    #{<<"dataForm">> := #{<<"slaveid">> := 256}} ->   %%不做指令
                        pass;
                    #{<<"dataForm">> := #{<<"slaveid">> := SlaveId}} ->
                        Pn = shuwa_utils:to_binary(SlaveId),
%%                        lager:info("DtuProductId ~p DtuDevId ~p Pn ~p ACL ~p", [DtuProductId, DtuDevId, Pn, ACL]),
%%                        lager:info("Y ~p", [Y]),
                        shuwa_instruct:create(DtuProductId, DtuDevId, Pn, ACL, <<"all">>, #{<<"properties">> => [Y]});
                    _ -> pass
                end
                      end, Properties);
        _ -> pass
    end.