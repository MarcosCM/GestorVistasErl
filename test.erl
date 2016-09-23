%% coding: latin-1
%% FICHERO: test.erl
%% DESCRIPCIÓN: Conjunto de pruebas para verificar que
%%		el sistema distribuido funciona correctamente.

-module(test).
-export([start/0]).
-import(gestor_vistas, [init/0]).
-import(server, [init/1]).
-import(client, [vista_valida/1, primario/1, lee/2, escribe/3, escribe_hash/3]).
% Constantes
-include("network.hrl").
-include("nodes.hrl").

start() ->
	% Crear gestor de vistas
	Gv = spawn(?NODOLOCAL1, gestor_vistas, init, []),
	io:format("PID del Gestor de Vistas: ~p~n",[Gv]),
	timer:sleep(2*?LATIDO),

	% Añadir primario
	Sv1 = spawn(?NODOLOCAL2, server, init, [{?NOMBREREG_GESTOR, ?NODOLOCAL1}]),
	io:format("Nuevo servidor con PID ~p~n"
		"Probando operaciones con servidor inoperativo (aun no hay backup)~n", [Sv1]),
	timer:sleep(4*?LATIDO),

	% Operaciones con servidor inoperativo
	{_, Primario, _, _} = client:vista_valida({?NOMBREREG_GESTOR, ?NODOLOCAL1}),
	client:escribe(Primario, clave_1, valor_1),
	timer:sleep(1000),

	client:escribe_hash(Primario, clave_1, valor_1),
	timer:sleep(1000),

	client:lee(Primario, clave_1),
	timer:sleep(1000),

	% Añadir backup
	Sv2 = spawn(?NODOLOCAL3, server, init, [{?NOMBREREG_GESTOR, ?NODOLOCAL1}]),
	io:format("Nuevo servidor con PID ~p~n"
		"Probando operaciones con servidor operativo~n", [Sv2]),
	timer:sleep(4*?LATIDO),

	% Operaciones con servidor operativo
	client:escribe(Sv1, clave_1, valor_1),
	timer:sleep(1000),

	client:lee(Sv1, clave_1),
	timer:sleep(1000),

	client:escribe(Sv1, clave_1, valor_1_mod),
	timer:sleep(1000),

	client:escribe_hash(Sv1, clave_1, valor_1_mod2),
	timer:sleep(1000),

	client:lee(Sv1, clave_1),
	timer:sleep(1000),

	% Matar backup
	exit(Sv2, kill),
	io:format("Muerto el servidor con PID ~p~n"
		"Probando escritura con servidor inoperativo tras fallo de backup~n", [Sv2]),
	timer:sleep((?TIMEOUT+1)*?LATIDO),

	% Escritura tras fallo de backup
	client:escribe(Sv1, clave_1, valor_1_mod3),
	timer:sleep(1000),

	% Añadir nuevo backup
	Sv3 = spawn(?NODOLOCAL3, server, init, [{?NOMBREREG_GESTOR, ?NODOLOCAL1}]),
	io:format("Nuevo servidor con PID ~p~n", [Sv3]),
	timer:sleep(4*?LATIDO),

	% Matar primario
	exit(Sv1, kill),
	io:format("Muerto el servidor con PID ~p~n"
		"Probando escritura con servidor inoperativo tras fallo de primario~n", [Sv1]),
	timer:sleep((?TIMEOUT+1)*?LATIDO),

	% Escritura tras fallo de primario
	{_, NuevoPrimario, _, _} = client:vista_valida({?NOMBREREG_GESTOR, ?NODOLOCAL1}),
	client:escribe(NuevoPrimario, clave_2, valor_2),
	timer:sleep(1000),

	client:escribe_hash(NuevoPrimario, clave_2, valor_2),
	timer:sleep(1000),

	client:lee(NuevoPrimario, clave_2),
	timer:sleep(1000).
