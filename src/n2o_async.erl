-module(n2o_async).
-deprecated([]).
-description('N2O Process').
-include("n2o.hrl").
-include("n2o_pi.hrl").

-behaviour(gen_server).

-export([start_link/1]).

%% API
-export([start/1,
         stop/2,
         send/2,
         send/3,
         pid/2,
         restart/2
        ]).

%% Gen server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3
        ]).

%% @doc Start a handler and add it to the supervisor Group
start(#handler{class = Class, name = Name, module = Module, group = Group} = Async) ->
    ChildSpec = #{id => {Class, Name},
                  start => {?MODULE, start_link, [Async]},
                  restart => transient,
                  shutdown => 5000,
                  type => worker,
                  modules => [Module]},
    case supervisor:start_child(Group, ChildSpec) of
        {ok,Pid}       -> {Pid, Async#handler.name};
        {ok,Pid,_}     -> {Pid, Async#handler.name};
        {error,Reason} -> {error, Reason}
    end.

%% @doc Stop a handler and remove it from its supervisor.
stop(Class, Name) ->
    case n2o_async:pid(Class, Name) of
        Pid when is_pid(Pid) ->
            Async = sys:get_state(Pid),
            Group = Async#handler.group,
            supervisor:terminate_child(Group, {Class, Name}),
            supervisor:delete_child(Group, {Class, Name}),
            n2o:cache(Class, {Class,Name}, undefined),
            Async;
        Data ->
            {error, {not_pid, Data}}
    end.

%% @doc Restart a handler
restart(Class, Name) ->
    case stop(Class,Name) of
        #handler{} = Async -> start(Async);
        Error -> Error
    end.

%% @doc A misnomer for a synchronous call to a handler.
send(Pid, Message) when is_pid(Pid) ->
    gen_server:call(Pid,Message).

%% @doc A misnomer for a synchronous call to a handler.
send(Class, Name, Message) ->
    gen_server:call(n2o_async:pid(Class, Name), Message).

pid(Class, Name) ->
    n2o:cache(Class, {Class, Name}).

%% Gen server callbacks (is passed through the handler)

start_link (Parameters) ->
    gen_server:start_link(?MODULE, Parameters, []).

init(#handler{module = Mod, class = Class, name = Name} = Handler) ->
    %% Register as the process for this handler.
    n2o:cache(Class, {Class, Name}, self(), infinity),
    Mod:proc(init, Handler).

terminate(_Reason, #handler{name=Name,group=Group,class=Class}) ->
    spawn(fun() -> supervisor:delete_child(Group,{Class,Name}) end),
    n2o:cache(Class,{Class,Name},undefined), ok.

code_change(_, State, _) ->
    {ok, State}.

handle_call({get}, _From, Async) ->
    %% TODO: This should be replaced by a primitive doing
    %% sys:get_state/1 instead.
    {reply, Async, Async};
handle_call(Message, _From, #handler{module = Mod} = Async) ->
    Mod:proc(Message, Async).

handle_cast(Message, #handler{module = Mod} = Async) ->
    Mod:proc(Message, Async).

handle_info(timeout, #handler{module = Mod} = Async) ->
    Mod:proc(timeout, Async);
handle_info(Message, #handler{module=Mod}=Async) ->
    case Mod:proc(Message, Async) of
        {_,_,S} -> {noreply, S};
        {_,S}   -> {noreply, S};
        S       -> {noreply, S}
    end.
