-module(n2o_pi).

-description('N2O Process').

-include("n2o.hrl").

-include("n2o_pi.hrl").

-behaviour(gen_server).

%% Gen server callbacks
-export([code_change/3,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         init/1,
         start_link/1,
         terminate/2
         ]).

-export([start/1,
         stop/2,
         restart/2]).

%% @doc Add a #pi{} child to a supervisor
start(#pi{table = Tab, name = Name, module = Module, sup = Sup} = Async) ->
    ChildSpec = #{id       => {Tab, Name},
                  start    => {?MODULE, start_link, [Async]},
                  restart  => transient,
                  shutdown => 5000,
                  type     => worker,
                  modules  => [Module] %% TODO: This should propably be ?MODULE.
                 },
    case supervisor:start_child(Sup,ChildSpec) of
        {ok, Pid}       -> {Pid, Async#pi.name};
        {ok, Pid, _}    -> {Pid, Async#pi.name};
        {error, Reason} -> {error, Reason}
    end.

%% @doc Remove a #pi{} child from a supervisor
stop(Tab, Name) ->
    case n2o:cache(Tab, {Tab, Name}) of
        Pid when is_pid(Pid) ->
            Async = sys:get_state(Pid),
            supervisor:terminate_child(Async#pi.sup, {Tab, Name}),
            supervisor:delete_child(Async#pi.sup, {Tab, Name}),
            n2o:cache(Tab, {Tab, Name}, undefined),
            Async;
        Data ->
            {error, {not_pid, Data}}
    end.

%% @doc Restart a #pi{} child
restart(Tab, Name) ->
    case stop(Tab, Name) of
        #pi{} = Async ->
            start(Async);
        Error -> Error
    end.

%% @doc Callback from the supervisor when starting the child.
start_link(Parameters) ->
    gen_server:start_link(?MODULE, Parameters, []).

%% Gen server callbacks

init(#pi{module = Mod, table = Tab, name = Name} = Handler) ->
    n2o:cache(Tab, {Tab, Name}, self(), infinity),
    Mod:proc(init, Handler).

terminate(_Reason, #pi{name = Name,sup = Sup,table = Tab}) ->
    spawn(fun() -> supervisor:delete_child(Sup, {Tab, Name}) end),
    io:format("n2o_pi:terminate~n"),
    n2o:cache(Tab, {Tab, Name}, undefined),
    ok.

code_change(_,State,_) ->
    {ok, State}.

handle_call(Message, _, #pi{module = Mod} = Async) ->
    Mod:proc(Message, Async).

handle_cast(Message, #pi{module = Mod} = Async) ->
    Mod:proc(Message, Async).

handle_info(timeout, #pi{module = Mod} = Async) ->
    Mod:proc(timeout, Async);
handle_info(Message, #pi{module = Mod} = Async) ->
    case Mod:proc(Message, Async) of
        {_, _, S} -> {noreply, S};
        {_, S}    -> {noreply, S};
        S         -> {noreply, S}
    end.

