%% coding: latin-1
%% FICHERO: client.erl
%% DESCRIPCIÓN: Exporta las funciones del lado del cliente para
%%		pedir al gestor de vistas tanto el primario actual
%%		como la última vista válida.

-module(client).
-export([vista_valida/1, primario/1, lee/2, escribe/3, escribe_hash/3]).

% Obtiene la vista válida en el momento
vista_valida(GestorVistas) ->
	GestorVistas ! {vista, self()},
	receive
		{Vista, Primario, Backup, Operativo} ->
			{Vista, Primario, Backup, Operativo};
		true ->
			none
	end.

% Obtiene sólo el primario
primario(GestorVistas) ->
	GestorVistas ! {primario, self()},
	receive
		P ->
			P
	end.

lee(Servidor, Clave) ->
	io:format("[Cliente]: Deseo leer ~p~n", [Clave]),
	Servidor ! {get_val, Clave, self()},
	receive
		{get_err, Clave} ->
			io:format("[Cliente]: Error en lectura de ~p~n", [Clave]);
		Valor ->
			io:format("[Cliente]: Lee ~p = ~p~n", [Clave, Valor])
	end.

escribe(Servidor, Clave, NuevoValor) ->
	io:format("[Cliente]: Escribe ~p = ~p~n", [Clave, NuevoValor]),
	Servidor ! {set_val, Clave, NuevoValor, self()},
	receive
		{set_val_ack, Clave, Valor} ->
			io:format("[Cliente]: Exito en escritura ~p = ~p~n", [Clave, Valor]);
		{set_err, Clave, Valor} ->
			io:format("[Cliente]: Error en ~p = ~p~n", [Clave, Valor]);
		true ->
			io:format("[Cliente]: None~n", [])
	end.

escribe_hash(Servidor, Clave, NuevoValor) ->
	io:format("[Cliente]: Escribe_hash ~p = ~p~n", [Clave, NuevoValor]),
	Servidor ! {set_hash_val, Clave, NuevoValor, self()},
	receive
		{set_hash_val_ack, Clave, OldVal} ->
			io:format("[Cliente]: Valor Antiguo de ~p = ~p~n", [Clave, OldVal]);
		{set_hash_err, Clave, Valor} ->
			io:format("[Cliente]: Error (hash) en ~p = ~p~n", [Clave, Valor]);
		true ->
			io:format("[Cliente]: None~n", [])
	end.
