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
-include("dgiot_modbus_tcp.hrl").
-include_lib("shuwa_framework/include/shuwa_socket.hrl").

-define(MAX_BUFF_SIZE, 1024).

-export([
    get_deviceid/2,
    start/2
]).

%% TCP callback
-export([init/1, handle_info/2, handle_cast/2, handle_call/3, terminate/2, code_change/3]).

start(Port, State) ->
    shuwa_tcp_server:child_spec(?MODULE, shuwa_utils:to_int(Port), State).

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

handle_info({deliver, _Topic, Msg}, #tcp{state = #state{id = ChannelId, product = DtuProductId} = State} = TCPState) ->
    case binary:split(shuwa_mqtt:get_topic(Msg), <<$/>>, [global, trim]) of
        [<<"thing">>, _ProductId, _DevAddr] ->
            [#{<<"thingdata">> := ThingData} | _] = jsx:decode(shuwa_mqtt:get_payload(Msg), [{labels, binary}, return_maps]),
            #{<<"thingdata">> := #{
                <<"command">> := <<"r">>,
                <<"data">> := Value,
                <<"di">> := Di,
                <<"pn">> := SlaveId,
                <<"product">> := ProductId,
                <<"protocol">> := <<"modbus">>}
            } = ThingData,
            Datas = modbus_rtu:to_frame(#{
                <<"addr">> => SlaveId,
                <<"value">> => Value,
                <<"productid">> => ProductId,
                <<"di">> => Di}),
            lists:map(fun(X) ->
                shuwa_bridge:send_log(ChannelId, "to_device: ~p ", [shuwa_utils:binary_to_hex(X)]),
                shuwa_tcp_server:send(TCPState, X)
                      end, Datas),
            {noreply, TCPState#{#tcp{state = #state{env =#{product => ProductId, pn => SlaveId, di => Di}}}}};
        _Other ->
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

get_deviceid(ProdcutId, DevAddr) ->
    #{<<"objectId">> := DeviceId} =
        shuwa_parse:get_objectid(<<"Device">>, #{<<"product">> => ProdcutId, <<"devaddr">> => DevAddr}),
    DeviceId.

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