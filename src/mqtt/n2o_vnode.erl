-module(n2o_vnode).
-description('N2O MQTT Backend').
-include("n2o.hrl").
-include("emqttd.hrl").
-export([proc/2]).
-export([get_vnode/1,get_vnode/2,validate/2]).
-export([load/1,unload/0]).
-export([on_message_publish/2]).

%% N2O-MQTT Topic Format

%% Client: 1. actions/:vsn/:module/:client
%% Server: 2. events/:vsn/:node/:module/:user/:client/:token

% N2O VNODE SERVER for MQTT

proc(init,#pi{name=Name}=Async) ->
    n2o:info(?MODULE,"VNode Init: ~p\r~n",[Name]),
    {ok, C} = emqttc:start_link([{host, "127.0.0.1"},
                                 {client_id, gen_name(Name)},
                                 {clean_sess, false},
                                 {logger, {console, error}},
                                 {reconnect, 5}]),
    {ok,Async#pi{state = C}};

proc({mqttc, C, connected}, State = #pi{name = Name, state = C}) ->
    Topic = n2o:to_binary([<<"events/+/">>, n2o:to_binary(Name), "/#"]),
    emqttc:subscribe(C, Topic, 2),
    {ok, State};

proc({publish,_,_}, State = #pi{state=[]}) ->
    {reply, [], State};

proc({publish, To, Request}, State = #pi{name = Name, state = C}) ->
    Bert = n2o:decode(Request),
    case emqttd_topic:words(To) of
        [ _Origin, Vsn, Node, Module, _Username, Id, Token | _ ] = Addr ->
            From = n2o:to_binary(["actions/", n2o:to_binary(Vsn), "/",
                                  n2o:to_binary(Module), "/", n2o:to_binary(Id)]),
            Sid  = token_to_sid(Token),
            Ctx  = #cx {module = fix_ctx_module(Module), session = Sid,
                        node = Node, params = Id, client_pid = C, from = From, vsn = Vsn},
            n2o:info(?MODULE, "n2o(~p): RECV PUBLISH(Topic=~s, Hash=0x~8.16.0b, Payload=~p)",
                     [Name, To, erlang:phash2(Request), Bert]),
            put(context, Ctx),
            try n2o_proto:info(Bert, [], Ctx) of
                Reply ->
                    Return = handle_proto_reply(Reply, C, Name),
                    debug(Name, To, Bert, Addr, Return),
                    {reply, Return, State}
            catch
                Err:Rea ->
                    n2o:error(?MODULE,"Catch:~p~n",[n2o:stack_trace(Err,Rea)]),
                    {reply, {error, "Internal error"}, State}
            end;
        Addr ->
            Return = {error, {"Unknown Address", Addr}},
            debug(Name, To, Bert, Addr, Return),
            {reply, Return, State}
    end;

proc(Unknown,Async) ->
    {reply,{uknown,Unknown,0},Async}.

debug(Name, Topic, BERT, Address, Return) ->
    case application:get_env(n2o, dump_loop, no) of
        yes ->
            n2o:info(?MODULE,"VNODE:~p Message on topic ~tp.\r~n", [Name, Topic]),
            n2o:info(?MODULE,"BERT: ~tp\r~nAddress: ~p\r~n",[BERT,Address]),
            n2o:info(?MODULE,"on_message_publish: ~s.\r~n", [Topic]),
            case Return of
                {error, R} ->
                    n2o:info(?MODULE,"ERROR: ~p~n",[R]);
                _ ->
                    skip
            end,
            ok;
        _ ->
            skip
    end.

gen_name(Pos) when is_integer(Pos) ->
    gen_name(integer_to_list(Pos));
gen_name(Pos) ->
    Hash = erlang:phash2(node()),
    iolist_to_binary(io_lib:format("~8.16.0b_~s", [Hash, Pos])).

fix_ctx_module('')          -> index;
fix_ctx_module(<<"index">>) -> index;
fix_ctx_module(Module)      -> list_to_atom(binary_to_list(Module)).

token_to_sid(Token) ->
    case application:get_env(n2o, token_as_sid, false) of
        true ->
            n2o:to_binary(Token);
        false ->
            case n2o:depickle(n2o:to_binary(Token)) of
                {{A,_},_} -> A;
                B -> B
            end
    end.

handle_proto_reply({reply, {_, <<>>}, _, _Ctx}, _MqttClient, _Name) ->
    skip;
handle_proto_reply({reply, {Encoder, Term}, _, #cx{from = From}}, MqttClient, Name) ->
    Reply = encode_proto_reply(Encoder, Term),
    {ok, emqttc:publish(MqttClient, From, Reply, [{qos, 2}])};
handle_proto_reply(Reply, _MqttClient, _Name) ->
    {error, {"Invalid Return", Reply}}.

encode_proto_reply(bert,    Term) -> n2o_bert:encode(Term);
encode_proto_reply(json,    Term) -> n2o_json:encode(Term);
encode_proto_reply(binary,  Term) -> Term;
encode_proto_reply(default, Term) -> n2o:encode(Term);
encode_proto_reply(Encoder, Term) -> Encoder:encode(Term).

% MQTT HOOKS

load(Env) ->
    emqttd:hook('message.publish', fun ?MODULE:on_message_publish/2, [Env]).

unload() ->
    emqttd:unhook('message.publish', fun ?MODULE:on_message_publish/2).

on_message_publish(Message = #mqtt_message{topic = <<"actions/", _/binary>>, from=_From}, _Env) ->
    {ok, Message};
on_message_publish(#mqtt_message{topic = <<"events/", _TopicTail/binary>> = Topic,
                                 qos = Qos, from = {ClientId,_},
                                 payload = Payload} = Message, _Env) ->
    {Module, ValidateFun} = application:get_env(n2o, validate, {?MODULE, validate}),
    case Module:ValidateFun(Payload, ClientId) of
        ok ->
            case emqttd_topic:words(Topic) of
                [E, V, '', M, U, _C, T] ->
                    %% The vnode is not given. Redirect it to a vnode.
                    {Mod, F} = application:get_env(n2o, vnode, {?MODULE, get_vnode}),
                    NewTopic = emqttd_topic:join([E, V, Mod:F(ClientId, Payload), M, U, ClientId, T]),
                    NewMessage = emqttd_message:make(ClientId, Qos, NewTopic, Payload),
                    {ok, NewMessage};
                [_E, _V, _N, _M, _U, ClientId, _T] ->
                    %% A vnode is given, and the ClientId is correct
                    {ok, Message};
                [E, V, N, M, U, _C, T] ->
                    %% A vnode is given, but the ClientId is not correct.
                    %% Patch it.
                    NewTopic = emqttd_topic:join([E, V, N, M, U, ClientId, T]),
                    NewMessage = emqttd_message:make(ClientId, Qos, NewTopic, Payload),
                    {ok, NewMessage};
                %% @NOTE redirect to event topic with correct ClientId
                _ ->
                    {ok, Message}
            end;
        {error, ErrMessage} ->
            {ok, ErrMessage};
        _ ->
            ok
    end;
on_message_publish(_Message, _) ->
    ok.

get_vnode(ClientId) ->
    get_vnode(ClientId, []).

get_vnode(ClientId, _) ->
    [H|_] = binary_to_list(erlang:md5(ClientId)),
    integer_to_binary(H rem (length(n2o:ring())) + 1).

validate(_Payload, _ClientId) ->
    ok.
