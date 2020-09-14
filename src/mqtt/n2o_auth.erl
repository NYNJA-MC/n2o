-module(n2o_auth).
-description('N2O MQTT Auth').
-include("emqttd.hrl").
-export([init/1, check/3, description/0]).

init([Listeners]) ->
    {ok, Listeners}.

description() ->
    "N2O Authentication Module".

check(#mqtt_client{client_id = ClientId} = Client, _Password, _Listeners) ->
    ClientId2 = case
                    ClientId of
                    <<>> -> get_client_id();
                    _ ->  ClientId
                end,
    case ClientId2 of
        <<"emqttd_", _/binary>> ->
            Pid = Client#mqtt_client.client_pid,
            PageModule = Client#mqtt_client.username,
            Topic = n2o:to_binary(["actions/1/", PageModule, "/", ClientId2]),
            emqttd_client:subscribe(Pid, [{Topic, 2}]),
            ignore;
        _ -> ignore
    end;
check(_Client, _Password, _Opts) ->
    ignore.

get_client_id() ->
    {_, NPid, _} = emqttd_guid:new(),
    n2o:to_binary(["emqttd_", integer_to_list(NPid)]).
