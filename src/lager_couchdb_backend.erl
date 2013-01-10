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
    application:start(couchbeam),
    Server = couchbeam:server_connection(CouchDBHost, CouchDBPort),
    {ok, Database} = couchbeam:open_db(Server, DatabaseName),
    try parse_level(Level) of
        Lvl ->
            {ok, #state{level = Lvl,
                        database = Database,
                        formatter = Formatter,
                        format_config = FormatterConfig
                }
            }
        catch
            _:_ ->
                {error, bad_log_level}
        end.

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
            LMessage = binary_to_list(iolist_to_binary(Message)),
            case re:run(LMessage, "^\\[\"(.*)\"\\]\\[\"(.*)\"\\] .*") of
                {match,[{_MessageStart, MessageEnd},{TidStart,TidLen},{AuthUriStart,AuthUriLen}]} ->
                    FullTid = string:sub_string(LMessage, TidStart + 1, TidStart + TidLen),
                    case string:str(FullTid, ":") of
                        0 ->
                            Tid = FullTid,
                            ITid = null;
                        N ->
                            Tid = string:sub_string(FullTid, 1, N - 1),
                            ITid = string:sub_string(FullTid, N + 1)
                    end,
                    AuthUri = string:sub_string(LMessage, AuthUriStart + 1, AuthUriStart + AuthUriLen),
                    SubMessage = string:sub_string(LMessage, AuthUriStart + AuthUriLen + 4, MessageEnd),
                    <<Y:32/bitstring, _:8/bitstring, M:16/bitstring, _:8/bitstring, D:16/bitstring>> = iolist_to_binary(Date),
                    <<H:16/bitstring, _:8/bitstring, MN:16/bitstring, _:8/bitstring, S/bitstring>> = iolist_to_binary(Time),
                    DateTime = {{list_to_integer(binary_to_list(Y)), list_to_integer(binary_to_list(M)), list_to_integer(binary_to_list(D))}, 
                                {list_to_integer(binary_to_list(H)), list_to_integer(binary_to_list(MN)), list_to_float(binary_to_list(S))}},
                    Doc = {[
                            {<<"level">>, b(Level)},
                            {<<"level_srt">>, b(LevelStr)},
                            {<<"date">>, b(Date)}, 
                            {<<"time">>, b(Time)}, 
                            {<<"location">>, b(Location)},
                            {<<"tid">>, b(Tid)},
                            {<<"item_tid">>, b(ITid)},
                            {<<"auth_uri">>, b(AuthUri)},
                            {<<"message">>, b(SubMessage)},
                            {<<"time_stamp">>, date_time_to_unixtimestamp(DateTime)}
                    ]},
                    couchbeam:save_doc(Database, Doc);
                _ ->
                    ignore
            end,
            {ok, State};
        false ->
            {ok, State}
    end;
    
handle_event({log, Level, {Date, Time}, [LevelStr, Location, Message]},
  #state{level = LogLevel, database = Database} = State) when Level =< LogLevel ->
      LMessage = binary_to_list(iolist_to_binary(Message)),
      case re:run(LMessage, "^\\[\"(.*)\"\\]\\[\"(.*)\"\\] .*") of
          {match,[{_MessageStart, MessageEnd},{TidStart,TidLen},{AuthUriStart,AuthUriLen}]} ->
              FullTid = string:sub_string(LMessage, TidStart + 1, TidStart + TidLen),
              case string:str(FullTid, ":") of
                  0 ->
                      Tid = FullTid,
                      ITid = null;
                  N ->
                      Tid = string:sub_string(FullTid, 1, N - 1),
                      ITid = string:sub_string(FullTid, N + 1)
              end,
              AuthUri = string:sub_string(LMessage, AuthUriStart + 1, AuthUriStart + AuthUriLen),
              SubMessage = string:sub_string(LMessage, AuthUriStart + AuthUriLen + 4, MessageEnd),
              <<Y:32/bitstring, _:8/bitstring, M:16/bitstring, _:8/bitstring, D:16/bitstring>> = iolist_to_binary(Date),
              <<H:16/bitstring, _:8/bitstring, MN:16/bitstring, _:8/bitstring, S:16/bitstring, _/binary>> = iolist_to_binary(Time),
              DateTime = {{list_to_integer(binary_to_list(Y)), list_to_integer(binary_to_list(M)), list_to_integer(binary_to_list(D))}, 
                          {list_to_integer(binary_to_list(H)), list_to_integer(binary_to_list(MN)), list_to_integer(binary_to_list(S))}},
              Doc = {[
                      {<<"level">>, b(Level)},
                      {<<"level_srt">>, b(LevelStr)},
                      {<<"date">>, b(Date)}, 
                      {<<"time">>, b(Time)}, 
                      {<<"location">>, b(Location)},
                      {<<"tid">>, b(Tid)},
                      {<<"item_tid">>, b(ITid)},
                      {<<"auth_uri">>, b(AuthUri)},
                      {<<"message">>, b(SubMessage)},
                      {<<"time_stamp">>, date_time_to_unixtimestamp(DateTime)}
              ]},
              couchbeam:save_doc(Database, Doc);
          _ ->
              ignore
      end,
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

b(T) when is_integer(T) ->
    T;
b(T) when is_binary(T) ->
    T;    
b(T) when is_list(T) ->
    list_to_binary(T);
b(T) when is_atom(T) ->
    <<_:4/binary, Bin/binary>> = term_to_binary(T),
    Bin.
    
date_time_to_unixtimestamp(DateTime) ->
    calendar:datetime_to_gregorian_seconds(DateTime) -
             calendar:datetime_to_gregorian_seconds( {{1970,1,1},{0,0,0}} ).
