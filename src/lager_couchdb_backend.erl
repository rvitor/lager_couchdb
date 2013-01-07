%% @doc CouchDB backend for lager.

-module(lager_couchdb_backend).

-behaviour(gen_event).

-export([init/1, 
        handle_call/2, 
        handle_event/2, 
        handle_info/2, 
        terminate/2,
        code_change/3]).


-record(state, {level, database, formatter, format_config}).

-include_lib("lager/include/lager.hrl").

-define(DEFAULT_FORMAT,["[", severity, "] ",
        {pid, ""},
        {module, [
                {pid, ["@"], ""},
                module,
                {function, [":", function], ""},
                {line, [":",line], ""}], ""},
        " ", message]).


%% @private
init([Host, Port, Database, Level]) when is_atom(Level) ->
    init([Host, Port, Database, Level, {lager_default_formatter, ?DEFAULT_FORMAT}]);
    
init([CouchDBHost, CouchDBPort, DatabaseName, Level, {Formatter, FormatterConfig}]) when is_atom(Level), is_atom(Formatter) ->
    Server = couchbeam:server_connection(CouchDBHost, CouchDBPort),
    {ok, Database} = couchbeam:open_db(Server, DatabaseName),
    {ok, #state{level = Level,
                database = Database,
                formatter = Formatter,
                format_config = FormatterConfig
        }
    }.


%% @private
handle_call(get_loglevel, #state{level=Level} = State) ->
    {ok, Level, State};
    
handle_call({set_loglevel, Level}, State) ->
    try parse_level(Level) of
        Lvl ->
            {ok, ok, State#state{level=Lvl}}
    catch
        _:_ ->
            {ok, {error, bad_log_level}, State}
    end;
handle_call(_Request, State) ->
    {ok, ok, State}.

%% @private
handle_event({log, Dest, Level, {Date, Time}, [LevelStr, Location, Message]},
    #state{level = L, database = Database} = State) when Level > L ->
    case lists:member(lager_couchdb_backend, Dest) of
        true ->
            Doc = {[
                    {<<"level">>, b(Level)},
                    {<<"level_srt">>, b(LevelStr)},
                    {<<"date">>, b(Date)}, 
                    {<<"time">>, b(Time)}, 
                    {<<"location">>, b(Location)}, 
                    {<<"message">>, b(Message)}
            ]},
            couchbeam:save_doc(Database, Doc),
            {ok, State};
        false ->
            {ok, State}
    end;
    
handle_event({log, Level, {Date, Time}, [LevelStr, Location, Message]},
  #state{level = LogLevel, database = Database} = State) when Level =< LogLevel ->
    Doc = {[
            {<<"level">>, b(Level)},
            {<<"level_srt">>, b(LevelStr)},
            {<<"date">>, b(Date)}, 
            {<<"time">>, b(Time)}, 
            {<<"location">>, b(Location)}, 
            {<<"message">>, b(Message)}
    ]},
    couchbeam:save_doc(Database, Doc),
    {ok, State};

handle_event(_Event, State) ->
    {ok, State}.

%% @private
handle_info(_Info, State) ->
    {ok, State}.

%% @private
terminate(_Reason, _State) ->
    ok.

%% @private
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

parse_level(Level) ->
    try lager_util:config_to_mask(Level) of
        Res ->
            Res
    catch
        error:undef ->
            %% must be lager < 2.0
            lager_util:level_to_num(Level)
    end.
 
b(T) when is_binary(T) ->
    T;    
b(T) when is_list(T) ->
    list_to_binary(T);
b(T) when is_atom(T) ->
    <<_:4/binary, Bin/binary>> = term_to_binary(T),
    Bin.
