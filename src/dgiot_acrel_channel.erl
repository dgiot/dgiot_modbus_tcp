%%%-------------------------------------------------------------------
%%% @copyright (C) 2019, <COMPANY>
%%% @doc
%%%  前置机客户端
%%% @end
%%% Created : 20. 三月 2021 12:00
%%%-------------------------------------------------------------------
-module(dgiot_acrel_channel).
-behavior(shuwa_channelx).
-author("johnliu").
-include_lib("shuwa_framework/include/shuwa_socket.hrl").
-include("dgiot_acrel.hrl").
-define(TYPE, <<"tcpdebg">>).
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
    temperature = 0
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
        zh => <<"TCP调试通道采集通道"/utf8>>
    },
    description => #{
        zh => <<"TCP调试通道采集通道"/utf8>>
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
        order => 2,
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
            erlang:send_after(5 * 100, self(), login),
            {ok, TCPState};
        {error, not_find} ->
            {stop, not_find_channel}
    end.


handle_info({tcp, Buff}, #tcp{socket = Socket, state = #state{id = ChannelId, devaddr = <<>>, head = Head, len = Len, product = ProductId} = State} = TCPState) ->
    shuwa_bridge:send_log(ChannelId, "DTU revice from  ~p", [shuwa_utils:binary_to_hex(Buff)]),
    DTUIP = shuwa_evidence:get_ip(Socket),
    DtuAddr = shuwa_utils:binary_to_hex(Buff),
    List = shuwa_utils:to_list(DtuAddr),
    case re:run(DtuAddr, Head, [{capture, first, list}]) of
        {match, [Head]} when length(List) == Len ->
            #{<<"objectId">> := DeviceId} =
                shuwa_parse:get_objectid(<<"Device">>, #{<<"product">> => ProductId, <<"devaddr">> => DtuAddr}),
            create_device(DeviceId, ProductId, DtuAddr, DTUIP),
            {noreply, TCPState#tcp{buff = <<>>, state = State#state{devaddr = DtuAddr, deviceId = DeviceId}}};
        _ ->
            {noreply, TCPState#tcp{buff = <<>>}}
    end;

handle_info({tcp, Buff}, #tcp{state = #state{id = ChannelId, devaddr = DevAddr, deviceId = _DeviceId, product = ProductId, scale = Scale, temperature = Temperature} = State} = TCPState) ->
    shuwa_bridge:send_log(ChannelId, "Acrel revice from  ~p", [shuwa_utils:binary_to_hex(Buff)]),
    <<SlaveId:8, _FunCode:8, Len:8, ResponseData/binary>> = Buff,
    case SlaveId of
        1 ->
            case Len of
                2 ->
                    <<Data:Len/binary, _Crc/binary>> = ResponseData,
                    <<_Type:8, Scale1:8>> = Data,
                    {noreply, TCPState#tcp{state = State#state{scale = Scale1}}};
                6 ->
                    <<Data:Len/binary, _Crc/binary>> = ResponseData,
                    <<L1:16, L2:16, L3:16>> = Data,
                    NewL1 = L1 / Scale,
                    NewL2 = L2 / Scale,
                    NewL3 = L3 / Scale,
                    Map = #{<<"l1_current">> => NewL1,
                        <<"l2_current">> => NewL2,
                        <<"l3_current">> => NewL3,
                        <<"temperature">> => Temperature},
                    shuwa_tdengine_adapter:save(ProductId, DevAddr, Map);
                _ ->
                    {noreply, TCPState}
            end,
            Topic = <<"thing/", ProductId/binary, "/", DevAddr/binary, "/post">>,
            shuwa_mqtt:publish(self(), Topic, shuwa_utils:binary_to_hex(Buff)),
            {noreply, TCPState};
        17 ->
            case Len of
                2 ->
                    <<Data:16, _Crc/binary>> = ResponseData,
                    NewData = (Data - 10000) / 100,
                    Topic = <<"thing/", ProductId/binary, "/", DevAddr/binary, "/post">>,
                    shuwa_mqtt:publish(self(), Topic, shuwa_utils:binary_to_hex(Buff)),
                    {noreply, TCPState#tcp{state = State#state{temperature = NewData}}};
                _ ->
                    {noreply, TCPState}
            end;
        _ ->
            {noreply, TCPState}
    end;

handle_info(login, #tcp{state = #state{devaddr = DevAddr, deviceId = DeviceId, product = ProductId, id = ChannelId}} = TCPState) ->
    shuwa_bridge:send_log(ChannelId, "ChannelId ~p DevAddr ~p DeviceId ~p", [ChannelId, DevAddr, DeviceId]),
    shuwa_mqtt:subscribe(<<"thing/", ProductId/binary, "/", DevAddr/binary>>),
    erlang:send_after(1000, self(), sendscale),
    {noreply, TCPState#tcp{buff = <<>>}};

%%比例因子
%%01 03 02 03 0A 38 B3
%%<<1,3,2,3,10,56,179>>
%%<<16#01, 16#03, 16#02, 16#03, 16#0a, 16#38, 16#b3>>
handle_info(sendscale, TCPState) ->
    Payload = <<"010300180001040D">>,
    shuwa_tcp_server:send(TCPState, shuwa_utils:hex_to_binary(Payload)),
    erlang:send_after(5000, self(), temperature),
    {noreply, TCPState};

%%温度
%%110400500001334B
handle_info(temperature, #tcp{state = #state{id = ChannelId}} = TCPState) ->
    Payload = <<"110400500001334B">>,
    shuwa_tcp_server:send(TCPState, shuwa_utils:hex_to_binary(Payload)),
    {Heartbeat, _Version} = shuwa_data:get({ChannelId, heartbeat}),
    lager:info("Heartbeat ~p", [Heartbeat]),
    erlang:send_after(1000, self(), sendcurrent),
    erlang:send_after(Heartbeat * 1000, self(), temperature),
    {noreply, TCPState};

%%三相电流
%% 01 03 06 00 00 00 00 00 00 21 75
%%<<1,3,6,0,0,0,0,0,0,33,117>>
%%<<1,3,6,0,60,0,61,0,0,224,188>>
handle_info(sendcurrent, TCPState) ->
    Payload = <<"01030000000305CB">>,
    shuwa_tcp_server:send(TCPState, shuwa_utils:hex_to_binary(Payload)),
    {noreply, TCPState};

handle_info({deliver, _Topic, Msg}, TCPState) ->
    Payload = shuwa_mqtt:get_payload(Msg),
    Bin = shuwa_utils:hex_to_binary(Payload),
    shuwa_tcp_server:send(TCPState, Bin),
    {noreply, TCPState};

%% {stop, TCPState} | {stop, Reason} | {ok, TCPState} | ok | stop
handle_info(_Info, TCPState) ->
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
                    shuwa_parse:update_object(<<"Device">>, DeviceId, #{<<"ip">> => DTUIP, <<"status">> => <<"ONLINE">>});
                _ ->
                    shuwa_shadow:create_device(#{
                        <<"devaddr">> => DTUMAC,
                        <<"name">> => <<"电动机保护器"/utf8, DTUMAC/binary>>,
                        <<"ip">> => DTUIP,
                        <<"isEnable">> => true,
                        <<"product">> => ProductId,
                        <<"ACL">> => Acl,
                        <<"status">> => <<"ONLINE">>,
                        <<"location">> => #{<<"__type">> => <<"GeoPoint">>, <<"longitude">> => 120.161324, <<"latitude">> => 30.262441},
                        <<"brand">> => <<"电动机保护器"/utf8>>,
                        <<"devModel">> => DevType
                    })
            end;
        Error2 -> lager:info("Error2 ~p ", [Error2])
    end.
