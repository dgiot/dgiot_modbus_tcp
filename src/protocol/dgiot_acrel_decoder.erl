%%%-------------------------------------------------------------------
%%% @author kenneth
%%% @copyright (C) 2018, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 07. 五月 2021 12:00
%%%-------------------------------------------------------------------

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-module(dgiot_acrel_decoder).
-author("stoneliu").

-define(ILLEGAL_FUNCTION, 1).
-define(ILLEGAL_DATA_ADDRESS, 2).
-define(ILLEGAL_DATA_VALUE, 3).
-define(SLAVE_DEVICE_FAILURE, 4).
-define(ACKNOWLEDGE, 5).
-define(SLAVE_DEVICE_BUSY, 6).
-define(NEGATIVE_ACKNOWLEDGE, 7).
-define(MEMORY_PARITY_ERROR, 8).
-define(GATEWAY_PATH_UNAVAILABLE, 10).
-define(GATEWAY_TARGET_DEVICE_FAILED_TO_RESPOND, 11).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-define(FC_READ_COILS, 16#01). %读取线圈状态
-define(FC_READ_INPUTS, 16#02). %读取输入状态
-define(FC_READ_HREGS, 16#03). %读取保持寄存器
-define(FC_READ_IREGS, 16#04). %读取输入寄存器

-define(FC_WRITE_COIL, 16#05).  %强置单线圈
-define(FC_WRITE_HREG, 16#06).  %预置单寄存器
-define(FC_WRITE_COILS, 16#0f).  %强置多线圈
-define(FC_WRITE_HREGS, 16#10).  %预置多寄存器

-record(rtu_req, {slaveId, funcode, address, quality}).

%%
-export([to_frame/1, modbus_encoder/1]).



parse_frame(<<>>, Acc, _State) -> {<<>>, Acc};

parse_frame(<<MbAddr:8, BadCode:8, ErrorCode:8, Crc:2/binary>> = Buff, Acc,
        #{<<"addr">> := DtuAddr} = State) ->
    CheckCrc = shuwa_utils:crc16(<<MbAddr:8, BadCode:8, ErrorCode:8>>),
    case CheckCrc =:= Crc of
        true ->
            Error = case ErrorCode of
                        ?ILLEGAL_FUNCTION -> {error, illegal_function};
                        ?ILLEGAL_DATA_ADDRESS -> {error, illegal_data_address};
                        ?ILLEGAL_DATA_VALUE -> {error, illegal_data_value};
                        ?SLAVE_DEVICE_FAILURE -> {error, slave_device_failure};
                        ?ACKNOWLEDGE -> {error, acknowledge};
                        ?SLAVE_DEVICE_BUSY -> {error, slave_device_busy};
                        ?NEGATIVE_ACKNOWLEDGE -> {error, negative_acknowledge};
                        ?MEMORY_PARITY_ERROR -> {error, memory_parity_error};
                        ?GATEWAY_PATH_UNAVAILABLE -> {error, gateway_path_unavailable};
                        ?GATEWAY_TARGET_DEVICE_FAILED_TO_RESPOND -> {error, gateway_target_device_failed_to_respond};
                        _ -> {error, unknown_response_code}
                    end,
            lager:info("DtuAddr ~p Modbus ~p, BadCode ~p, Error ~p", [DtuAddr, MbAddr, BadCode, Error]),
            {<<>>, Acc};
        false ->
            parse_frame(Buff, Acc, State)
    end;

parse_frame(Buff, Acc, #{<<"dtuproduct">> := ProductId, <<"dtuaddr">> := DtuAddr} = State) ->
    case decode_data(Buff, ProductId, Acc) of
        {Rest1, Acc1} ->
            parse_frame(Rest1, Acc1, State);
        [Buff, Acc] ->
            [Buff, Acc]
    end;

parse_frame(<<SlaveId:8, _/binary>> = Buff, Acc, #{<<"dtuaddr">> := DtuAddr} = State) ->
    case shuwa_shadow:lookup_hub(DtuAddr, shuwa_utils:to_binary(SlaveId)) of
        {error, _} ->
            [<<>>, Acc];
        [ProductId, _DevAddr] ->
            case decode_data(Buff, ProductId, Acc) of
                {Rest1, Acc1} ->
                    parse_frame(Rest1, Acc1, State);
                [Buff, Acc] ->
                    {Buff, Acc}
            end
    end.

decode_data(Buff, ProductId, Acc) ->
    <<SlaveId:8, FunCode:8, ResponseData/binary>> = Buff,
    {SizeOfData, DataBytes} =
        case FunCode of
            ?FC_READ_COILS ->
                <<Size:8, Data/binary>> = ResponseData,
                {Size, Data};
            ?FC_READ_INPUTS ->
                <<Size:8, Data/binary>> = ResponseData,
                {Size, Data};
            ?FC_READ_HREGS ->
                <<Size:8, Data/binary>> = ResponseData,
                {Size, Data};
            ?FC_READ_IREGS ->
                <<Size:8, Data/binary>> = ResponseData,
                {Size, Data};
            ?FC_WRITE_COIL -> {0, []};
            ?FC_WRITE_HREG -> {0, []};
            ?FC_WRITE_COILS -> {0, []};
            ?FC_WRITE_HREGS -> {0, []};
            _ -> {0, []}
        end,
    case SizeOfData > 0 of
        true ->
            <<UserZone:SizeOfData/bytes, Crc:2/binary, Rest1/binary>> = DataBytes,
            CheckBuf = <<SlaveId:8, FunCode:8, SizeOfData:8, UserZone/binary>>,
            CheckCrc = shuwa_utils:crc16(CheckBuf),
            case CheckCrc =:= Crc of
                true ->
                    Acc1 = Acc ++ modbus_decoder(ProductId, SlaveId, UserZone),
                    {Rest1, Acc1};
                false ->
                    {Rest1, Acc}
            end;
        false ->
            case FunCode of
                ?FC_WRITE_COIL ->
                    get_write(ResponseData, SlaveId, FunCode, ProductId, Acc);
                ?FC_WRITE_HREG ->
                    get_write(ResponseData, SlaveId, FunCode, ProductId, Acc);
                ?FC_WRITE_COILS ->
                    [Buff, Acc];
                ?FC_WRITE_HREGS ->
                    [Buff, Acc];
                _ -> [Buff, Acc]
            end
    end.

get_write(ResponseData, SlaveId, FunCode, ProductId, Acc) ->
    <<_Addr:2/binary, Rest1/binary>> = ResponseData,
    Size1 = byte_size(Rest1) - 2,
    <<UserZone:Size1/bytes, Crc:2/binary>> = Rest1,
    CheckBuf = <<SlaveId:8, FunCode:8, _Addr:2/binary, UserZone/binary>>,
    CheckCrc = shuwa_utils:crc16(CheckBuf),
    case CheckCrc =:= Crc of
        true ->
            Acc1 = Acc ++ modbus_decoder(ProductId, SlaveId, UserZone),
            {<<>>, Acc1};
        false ->
            {<<>>, Acc}
    end.


modbus_decoder(ProductId, SlaveId, Data) ->
    case shuwa_shadow:lookup_prod(ProductId) of
        {ok, #{<<"thing">> := #{<<"properties">> := Props}}} ->
            lists:foldl(fun(X, Acc) ->
                case X of
                    #{<<"identifier">> := Identifier,
                        <<"dataForm">> := #{
                            <<"slaveid">> := SlaveId,
                            <<"address">> := Address,
                            <<"protocol">> := <<"modbus">>
                        }} ->
                        case format_value(Data, X) of
                            {Value, _Rest} ->
                                Acc ++ [#{Identifier => Value}];
                            _ -> Acc
                        end;
                    _ -> Acc
                end
                        end, [], Props);
        _ -> []
    end.

%% 发送读取物模型命令
to_frame(ProductId) ->
    lists:foldl(fun({Cmd, SlaveId, OperateType, Address, Quality} = R, Acc) ->
        FunCode =
            case Cmd of
                <<"r">> ->
                    case OperateType of
                        <<"coilStatus">> -> ?FC_READ_COILS;
                        <<"inputStatus">> -> ?FC_READ_INPUTS;
                        <<"holdingRegister">> -> ?FC_READ_HREGS;
                        <<"inputRegister">> -> ?FC_READ_IREGS;
                        _ -> ?FC_READ_HREGS
                    end;
                _ ->
                    case OperateType of
                        <<"coilStatus">> -> ?FC_WRITE_COIL;
                        <<"inputStatus">> -> ?FC_WRITE_COILS; %%需要校验，写多个线圈是什么状态
                        <<"holdingRegister">> -> ?FC_WRITE_HREG;
                        <<"inputRegister">> -> ?FC_WRITE_HREGS; %%需要校验，写多个保持寄存器是什么状态
                        _ -> ?FC_WRITE_HREG
                    end
            end,
        <<H:8, L:8>> = shuwa_utils:hex_to_binary(Address),
        RtuReq = #rtu_req{
            slaveId = shuwa_utils:to_int(SlaveId),
            funcode = shuwa_utils:to_int(FunCode),
            address = H * 256 + L,
            quality = shuwa_utils:to_int(Quality)
        },
        Acc ++ [build_req_message(RtuReq)]
                end, [], modbus_encoder(ProductId)).

%% #{<<"accessMode">> => <<"r">>,
%% <<"dataForm">> => #{<<"address">> => <<"Edit-24">>,
%%                     <<"byteorder">> => <<"big">>,
%%                     <<"collection">> => <<"%s">>,
%%                     <<"control">> => <<"%q">>,
%%                     <<"data">> => <<"528590">>,
%%                     <<"offset">> => 0,
%%                     <<"protocol">> => <<"normal">>,
%%                     <<"quantity">> => <<"528590">>,
%%                     <<"rate">> => 1,
%%                     <<"strategy">> => <<"20">>},
%% <<"dataType">> => #{<<"specs">> => #{<<"max">> => 10000,
%%                                      <<"min">> => -1000,
%%                                      <<"step">> => 0.01,
%%                                      <<"unit">> => <<"Â°C">>},
%%                     <<"type">> => <<"float">>},
%% <<"identifier">> => <<"temperature">>,
%% <<"name">> => <<"æ¸©åº¦">>,
%% <<"required">> => true}
modbus_encoder(ProductId) ->
    case shuwa_shadow:lookup_prod(ProductId) of
        {ok, #{<<"thing">> := #{<<"properties">> := Props}}} ->
            lists:foldl(fun(X, Acc) ->
                case X of
                    #{<<"accessMode">> := <<"r">>, <<"dataForm">> := #{<<"slaveid">> := SlaveId, <<"address">> := Address, <<"protocol">> := <<"modbus">>,
                        <<"quantity">> := Quantity, <<"operatetype">> := Operatetype}} ->
                        Acc ++ [{<<"r">>, SlaveId, Operatetype, Address, Quantity}];
                    #{<<"accessMode">> := Cmd, <<"dataForm">> := #{<<"slaveid">> := SlaveId, <<"address">> := Address, <<"protocol">> := <<"modbus">>,
                        <<"quantity">> := Quantity, <<"operatetype">> := Operatetype}} ->
                        Acc ++ [{Cmd, SlaveId, Operatetype, Address, Quantity}];
                    _ ->
                        Acc
                end
                        end, [], Props);
        Error ->
            []
    end.

build_req_message(Req) when is_record(Req, rtu_req) ->
    % validate
    if
        (Req#rtu_req.slaveId < 0) or (Req#rtu_req.slaveId > 247) ->
            throw({argumentError, Req#rtu_req.slaveId});
        true -> ok
    end,
    if
        (Req#rtu_req.funcode < 0) or (Req#rtu_req.funcode > 255) ->
            throw({argumentError, Req#rtu_req.funcode});
        true -> ok
    end,
    if
        (Req#rtu_req.address < 0) or (Req#rtu_req.address > 65535) ->
            throw({argumentError, Req#rtu_req.address});
        true -> ok
    end,
    if
        (Req#rtu_req.quality < 1) or (Req#rtu_req.quality > 2000) ->
            throw({argumentError, Req#rtu_req.quality});
        true -> ok
    end,
    Message =
        case Req#rtu_req.funcode of
            ?FC_READ_COILS ->
                <<(Req#rtu_req.slaveId):8, (Req#rtu_req.funcode):8, (Req#rtu_req.address):16, (Req#rtu_req.quality):16>>;
            ?FC_READ_INPUTS ->
                <<(Req#rtu_req.slaveId):8, (Req#rtu_req.funcode):8, (Req#rtu_req.address):16, (Req#rtu_req.quality):16>>;
            ?FC_READ_HREGS ->
                <<(Req#rtu_req.slaveId):8, (Req#rtu_req.funcode):8, (Req#rtu_req.address):16, (Req#rtu_req.quality):16>>;
            ?FC_READ_IREGS ->
                <<(Req#rtu_req.slaveId):8, (Req#rtu_req.funcode):8, (Req#rtu_req.address):16, (Req#rtu_req.quality):16>>;
            ?FC_WRITE_COIL ->
                ValuesBin = case Req#rtu_req.quality of
                                1 ->
                                    <<16#ff, 16#00>>;
                                _ ->
                                    <<16#00, 16#00>>
                            end,
                <<(Req#rtu_req.slaveId):8, (Req#rtu_req.funcode):8, (Req#rtu_req.address):16, ValuesBin/binary>>;
            ?FC_WRITE_COILS ->
                Quantity = length(Req#rtu_req.quality),
                ValuesBin = list_bit_to_binary(Req#rtu_req.quality),
                ByteCount = length(binary_to_list(ValuesBin)),
                <<(Req#rtu_req.slaveId):8, (Req#rtu_req.funcode):8, (Req#rtu_req.address):16, Quantity:16, ByteCount:8, ValuesBin/binary>>;
            ?FC_WRITE_HREG ->
                ValueBin = list_word16_to_binary([Req#rtu_req.quality]),
                <<(Req#rtu_req.slaveId):8, (Req#rtu_req.funcode):8, (Req#rtu_req.address):16, ValueBin/binary>>;
            ?FC_WRITE_HREGS ->
                Quantity = length(Req#rtu_req.quality),
                ValuesBin = list_word16_to_binary(Req#rtu_req.quality),
                ByteCount = length(binary_to_list(ValuesBin)),
                <<(Req#rtu_req.slaveId):8, (Req#rtu_req.funcode):8, (Req#rtu_req.address):16, Quantity:16, ByteCount:8, ValuesBin/binary>>;
            _ ->
                erlang:error(function_not_implemented)
        end,
    Checksum = shuwa_utils:crc16(Message),
    <<Message/binary, Checksum/binary>>.



list_bit_to_binary(Values) when is_list(Values) ->
    L = length(Values),
    AlignedValues = case L rem 8 of
                        0 ->
                            Values;
                        Remainder ->
                            Values ++ [0 || _ <- lists:seq(1, 8 - Remainder)]
                    end,
    list_to_binary(
        bit_as_bytes(AlignedValues)
    ).

bit_as_bytes(L) when is_list(L) ->
    bit_as_bytes(L, []).

bit_as_bytes([], Res) ->
    lists:reverse(Res);
bit_as_bytes([B0, B1, B2, B3, B4, B5, B6, B7 | Rest], Res) ->
    bit_as_bytes(Rest, [<<B7:1, B6:1, B5:1, B4:1, B3:1, B2:1, B1:1, B0:1>> | Res]).

list_word16_to_binary(Values) when is_list(Values) ->
    list_to_binary(
        lists:map(
            fun(X) ->
                RoundedValue = round(X),
                <<RoundedValue:16>>
            end,
            Values
        )
    ).


format_value(Buff, #{
    <<"accessMode">> := <<"rw">>,
    <<"dataForm">> := DataForm} = X) ->
    format_value(Buff, X#{<<"accessMode">> => <<"r">>,
        <<"dataForm">> => DataForm#{<<"quantity">> => byte_size(Buff)}
    });
format_value(Buff, #{<<"dataForm">> := #{
    <<"quantity">> := Len,
    <<"byteorder">> := <<"big">>,
    <<"originaltype">> := <<"uint16">>
}}) ->
    Size = max(2, Len) * 8,
    <<Value:Size/unsigned-big-integer, Rest/binary>> = Buff,
    {Value, Rest};

format_value(Buff, #{<<"dataForm">> := #{
    <<"quantity">> := Len,
    <<"byteorder">> := <<"little">>,
    <<"originaltype">> := <<"uint16">>}}) ->
    Size = max(2, Len) * 8,
    <<Value:Size/unsigned-little-integer, Rest/binary>> = Buff,
    {Value, Rest};

format_value(Buff, #{<<"dataForm">> := #{
    <<"quantity">> := Len,
    <<"byteorder">> := <<"big">>,
    <<"originaltype">> := <<"int16">>}}) ->
    Size = max(2, Len) * 8,
    <<Value:Size/signed-big-integer, Rest/binary>> = Buff,
    {Value, Rest};

format_value(Buff, #{<<"dataForm">> := #{
    <<"quantity">> := Len,
    <<"byteorder">> := <<"little">>,
    <<"originaltype">> := <<"int16">>}}) ->
    Size = max(2, Len) * 8,
    <<Value:Size/signed-little-integer, Rest/binary>> = Buff,
    {Value, Rest};

format_value(Buff, #{<<"dataForm">> := #{
    <<"quantity">> := Len,
    <<"byteorder">> := <<"big">>,
    <<"originaltype">> := <<"uint32">>}
}) ->
    Size = max(4, Len) * 8,
    <<H:2/binary, L:2/binary, Rest/binary>> = Buff,
    <<Value:Size/unsigned-big-integer>> = <<H:2/binary, L:2/binary>>,
    {Value, Rest};

format_value(Buff, #{<<"dataForm">> := #{
    <<"quantity">> := Len,
    <<"byteorder">> := <<"little">>,
    <<"originaltype">> := <<"uint32">>}
}) ->
    Size = max(4, Len) * 8,
    <<H:2/binary, L:2/binary, Rest/binary>> = Buff,
    <<Value:Size/unsigned-big-integer>> = <<L:2/binary, H:2/binary>>,
    {Value, Rest};

format_value(Buff, #{<<"dataForm">> := #{
    <<"quantity">> := Len,
    <<"byteorder">> := <<"big">>,
    <<"originaltype">> := <<"int32">>}
}) ->
    Size = max(4, Len) * 8,
    <<H:2/binary, L:2/binary, Rest/binary>> = Buff,
    <<Value:Size/signed-big-integer>> = <<H:2/binary, L:2/binary>>,
    {Value, Rest};

format_value(Buff, #{<<"dataForm">> := #{
    <<"quantity">> := Len,
    <<"byteorder">> := <<"little">>,
    <<"originaltype">> := <<"int32">>}}) ->
    Size = max(4, Len) * 8,
    <<H:2/binary, L:2/binary, Rest/binary>> = Buff,
    <<Value:Size/signed-big-integer>> = <<L:2/binary, H:2/binary>>,
    {Value, Rest};

format_value(Buff, #{<<"dataForm">> := #{
    <<"quantity">> := Len,
    <<"byteorder">> := <<"big">>,
    <<"originaltype">> := <<"float">>}}) ->
    Size = max(4, Len) * 8,
    <<H:2/binary, L:2/binary, Rest/binary>> = Buff,
    <<Value:Size/unsigned-big-float>> = <<H:2/binary, L:2/binary>>,
    {Value, Rest};

format_value(Buff, #{<<"dataForm">> := #{
    <<"quantity">> := Len,
    <<"byteorder">> := <<"little">>,
    <<"originaltype">> := <<"float">>}}) ->
    Size = max(4, Len) * 8,
    <<H:2/binary, L:2/binary, Rest/binary>> = Buff,
    <<Value:Size/unsigned-big-float>> = <<L:2/binary, H:2/binary>>,
    {Value, Rest};

format_value(Buff, #{<<"dataForm">> := #{
    <<"quantity">> := Len,
    <<"byteorder">> := <<"big">>,
    <<"originaltype">> := <<"double">>}}) ->
    Size = max(4, Len) * 8,
    <<H:2/binary, L:2/binary, Rest/binary>> = Buff,
    <<Value:Size/unsigned-big-float>> = <<H:2/binary, L:2/binary>>,
    {Value, Rest};

format_value(Buff, #{<<"dataForm">> := #{
    <<"quantity">> := Len,
    <<"byteorder">> := <<"little">>,
    <<"originaltype">> := <<"double">>}}) ->
    Size = max(4, Len) * 8,
    <<H:2/binary, L:2/binary, Rest/binary>> = Buff,
    <<Value:Size/unsigned-big-float>> = <<L:2/binary, H:2/binary>>,
    {Value, Rest};

format_value(Buff, #{<<"dataForm">> := #{
    <<"quantity">> := Len,
    <<"byteorder">> := <<"big">>,
    <<"originaltype">> := <<"string">>}}) ->
    Size = Len * 8,
    <<H:2/binary, L:2/binary, Rest/binary>> = Buff,
    <<Value:Size/big-binary>> = <<H:2/binary, L:2/binary>>,
    {Value, Rest};

format_value(Buff, #{<<"dataForm">> := #{
    <<"quantity">> := Len,
    <<"byteorder">> := <<"little">>,
    <<"originaltype">> := <<"string">>}}) ->
    Size = Len * 8,
    <<H:2/binary, L:2/binary, Rest/binary>> = Buff,
    <<Value:Size/big-binary>> = <<L:2/binary, H:2/binary>>,
    {Value, Rest};

%% customized data（按大端顺序返回hex data）
format_value(Buff, #{<<"dataForm">> := #{
    <<"quantity">> := Len,
    <<"byteorder">> := <<"big">>}}) ->
    <<Value:Len/big-binary, Rest/binary>> = Buff,
    {Value, Rest};

format_value(Buff, #{<<"dataForm">> := #{
    <<"quantity">> := Len,
    <<"byteorder">> := <<"little">>}}) ->
    <<Value:Len/little-binary, Rest/binary>> = Buff,
    {Value, Rest};

%% @todo 其它类型处理
format_value(_, #{<<"identifier">> := Field}) ->
    lager:info("Field ~p", [Field]),
    throw({field_error, <<Field/binary, " is not validate">>}).
