#!/usr/bin/env escript
%% -*- erlang -*-
%%! +sbwt none

%% The benchmarks of the Go package, for ETS, DETS and, if on the code
%% path, Bitcask, printed as Go prints its own, for benchstat:
%%
%%   KVS_BENCH_DIR=$HOME/tmp escript erlang/bench.escript > erlang.txt
%%   ERL_LIBS=path/to/bitcask/_build/default/lib escript erlang/bench.escript
%%
%% It prints no configuration lines, so that its results join those of
%% Go in one table. ETS keeps nothing on disk: its puts are sync=none.

-mode(compile).

-define(KEYS, 100000).
-define(RANDOM, 1000000).
-define(VAL, <<0:(64*8)>>).
-define(BENCHTIME, 1000000). % µs

main(_) ->
    Base = case os:getenv("KVS_BENCH_DIR") of
               false -> "/tmp";
               Dir -> Dir
           end,
    Keys = list_to_tuple([<<I:64>> || I <- lists:seq(0, ?KEYS - 1)]),
    Random = list_to_tuple([element(rand:uniform(?KEYS), Keys) || _ <- lists:seq(1, ?RANDOM)]),
    put(keys, Keys),
    put(random, Random),
    Stores = [ets(set), ets(rw), dets()] ++
        case code:which(bitcask) of
            non_existing -> [];
            _ -> [bitcask()]
        end,
    [get_bench(S, Base) || S <- Stores],
    [put_bench(S, Base, Sync) || S <- Stores, Sync <- [none, each]],
    [put_durable_bench(S, Base) || S <- Stores],
    [open_bench(S, Base) || S <- Stores],
    ok.

%% A store is a map of funs, as the store interface of the Go package:
%% open(Dir, Durable) returns a handle, which put, get and close take.
%% reader(Handle), if given, returns the handle another process reads
%% with; writer(Handle) the fun other processes put a key with.

ets(Mode) ->
    Opts = case Mode of
               set -> [set, public];
               rw -> [set, public, {read_concurrency, true}, {write_concurrency, true}]
           end,
    #{name => case Mode of set -> "ets"; rw -> "ets-rw" end,
      memory => true,
      open => fun(_Dir, _Durable) -> ets:new(t, Opts) end,
      put => fun(T, K, V) -> true = ets:insert(T, {K, V}) end,
      get => fun(T, K) -> [_] = ets:lookup(T, K) end,
      close => fun ets:delete/1}.

dets() ->
    #{name => "dets",
      open => fun(Dir, Durable) ->
                  {ok, T} = dets:open_file(make_ref(),
                                           [{file, filename:join(Dir, "t.dets")}, {type, set}]),
                  {T, Durable}
              end,
      put => fun({T, Durable}, K, V) ->
                 ok = dets:insert(T, {K, V}),
                 Durable andalso (ok = dets:sync(T))
             end,
      get => fun({T, _}, K) -> [_] = dets:lookup(T, K) end,
      close => fun({T, _}) -> ok = dets:close(T) end}.

%% Bitcask, the Riak way: a reference works in the process that opened
%% it, which writes; the others read with a reference of their own, and
%% have it put for them. Its NIFs do the I/O. A durable put syncs: the
%% sync_strategy o_sync opens no file O_SYNC, as the NIF clears it when
%% it reads the option create after it.
bitcask() ->
    Put = fun({Ref, _, Durable}, K, V) ->
              ok = bitcask:put(Ref, K, V),
              Durable andalso (ok = bitcask:sync(Ref))
          end,
    #{name => "bitcask",
      open => fun(Dir, Durable) ->
                  ok = application:set_env(bitcask, io_mode, nif),
                  {bitcask:open(Dir, [read_write]), Dir, Durable}
              end,
      put => Put,
      get => fun({Ref, _, _}, K) -> {ok, _} = bitcask:get(Ref, K) end,
      close => fun({Ref, _, _}) -> ok = bitcask:close(Ref) end,
      reader => fun({_, Dir, Durable}) -> {bitcask:open(Dir), Dir, Durable} end,
      writer => fun(H) ->
                    Owner = self(),
                    fun(K) -> call(Owner, fun() -> Put(H, K, ?VAL) end) end
                end}.

%% call(Owner, F) has F run by Owner, waiting in parallel.
call(Owner, F) ->
    Owner ! {call, self(), F},
    receive {Owner, Result} -> Result end.

%% The benchmarks.

get_bench(#{name := Name} = S, Base) ->
    with_store(S, Base, false, fun(H) ->
        fill(S, H),
        Get = maps:get(get, S),
        report("Get/store=" ++ Name ++ "/mode=serial",
               measure(fun(N) -> loop(N, fun(K) -> Get(H, K) end) end)),
        report("Get/store=" ++ Name ++ "/mode=parallel",
               measure(fun(N) ->
                           parallel(N, fun() ->
                               R = reader(S, H),
                               fun(K) -> Get(R, K) end
                           end)
                       end))
    end).

put_bench(#{memory := true}, _Base, each) ->
    ok;
put_bench(#{name := Name, put := Put} = S, Base, Sync) ->
    with_store(S, Base, Sync =:= each, fun(H) ->
        report("Put/store=" ++ Name ++ "/sync=" ++ atom_to_list(Sync),
               measure(fun(N) -> loop(N, fun(K) -> Put(H, K, ?VAL) end) end))
    end).

put_durable_bench(#{memory := true}, _Base) ->
    ok;
put_durable_bench(#{name := Name} = S, Base) ->
    with_store(S, Base, true, fun(H) ->
        Put = writer(S, H),
        report("PutDurable/store=" ++ Name,
               measure(fun(N) -> parallel(N, fun() -> Put end) end))
    end).

open_bench(#{memory := true}, _Base) ->
    ok;
open_bench(#{name := Name, open := Open, get := Get, close := Close} = S, Base) ->
    Dir = mkdir(Base),
    H = Open(Dir, false),
    fill(S, H),
    Close(H),
    Last = element(?KEYS, get(keys)),
    report("Open/store=" ++ Name,
           measure(fun(N) ->
                       loop(N, fun(_) ->
                           H1 = Open(Dir, false),
                           Get(H1, Last),
                           Close(H1)
                       end)
                   end)),
    rmdir(Dir).

reader(S, H) -> (maps:get(reader, S, fun(X) -> X end))(H).

%% writer(S, H) returns the fun other processes put with; it is called
%% in the process that opened the store.
writer(#{put := Put} = S, H) ->
    case maps:find(writer, S) of
        {ok, Writer} -> Writer(H);
        error -> fun(K) -> Put(H, K, ?VAL) end
    end.

with_store(#{open := Open, close := Close}, Base, Durable, Fun) ->
    Dir = mkdir(Base),
    H = Open(Dir, Durable),
    Fun(H),
    Close(H),
    rmdir(Dir).

fill(#{put := Put}, H) ->
    Keys = get(keys),
    [Put(H, element(I, Keys), ?VAL) || I <- lists:seq(1, ?KEYS)],
    ok.

%% loop(N, F) calls F on N random keys, from a random place in them.
loop(N, F) ->
    loop(N, rand:uniform(?RANDOM), get(random), F).

loop(0, _, _, _) -> ok;
loop(N, I, Random, F) ->
    F(element(I, Random)),
    loop(N - 1, I rem ?RANDOM + 1, Random, F).

%% parallel(N, Init) runs N calls in as many processes as schedulers:
%% each calls Init, then the fun it returns, on its share of keys. It
%% runs the funs they call it with meanwhile, see call/2.
parallel(N, Init) ->
    P = erlang:system_info(schedulers_online),
    Random = get(random),
    Self = self(),
    Pids = [spawn_link(fun() ->
                           put(random, Random),
                           F = Init(),
                           loop(N div P + 1, F),
                           Self ! {done, self()}
                       end) || _ <- lists:seq(1, P)],
    wait(Pids).

wait([]) ->
    ok;
wait([Pid | Pids] = All) ->
    receive
        {done, Pid} ->
            wait(Pids);
        {call, From, F} ->
            From ! {self(), F()},
            wait(All)
    end.

%% measure(Run) grows N as testing.B does, until Run(N) takes a second,
%% and returns {N, ns/op}.
measure(Run) ->
    measure(Run, 1).

measure(Run, N) ->
    {T, _} = timer:tc(fun() -> Run(N) end),
    if
        T >= ?BENCHTIME ->
            {N, T * 1000 / N};
        true ->
            Predict = round(?BENCHTIME * 1.2 * N / max(T, 1)),
            measure(Run, min(100 * N, max(Predict, N + 1)))
    end.

report(Name, {N, NsOp}) ->
    io:format("Benchmark~s-~b\t~b\t~.1f ns/op~n",
              [Name, erlang:system_info(schedulers_online), N, NsOp]).

mkdir(Base) ->
    Dir = filename:join(Base, lists:concat(["compare-erl-", os:getpid(), "-", erlang:unique_integer([positive])])),
    ok = file:make_dir(Dir),
    Dir.

rmdir(Dir) ->
    ok = file:del_dir_r(Dir).
