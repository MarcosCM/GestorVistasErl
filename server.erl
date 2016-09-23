%% coding: latin-1
%% FICHERO: server.erl
%% DESCRIPCIÓN: Exporta la función de inicio de un servidor que
%%		cumplirá la función de Primario, Backup o nodo Backup
%%		según considere el gestor de vistas.

-module(server).
-export([init/1, latidos_proc/3]).
% Constantes
-include("network.hrl").

% Inicio del servidor
init(GestorVistas) ->
	GestorVistas ! {latido, 0, self()},
	receive
		{listener, ListenerAsignado} ->
			% Crea link para que cuando termine un proceso también
			% termine el proceso que envía sus latidos
			LatidosProc = spawn_link(?MODULE, latidos_proc, [ListenerAsignado, 0, self()]),
			server_loop(LatidosProc, 0, 0, 0, dict:new());
		true ->
			none
	end.

server_loop(LatidosProc, VistaMia, PrimarioMio, BackupMio, BaseDatos) ->
	receive
		% Soy primario y recibo escritura normal =>
		% forward a backup; escribir en mi BD; enviar ACK al cliente
		{set_val, Clave, Valor, Cpid} when PrimarioMio == self() ->
			if
				BackupMio /= 0 ->
					BackupMio ! {set_val_forward, Clave, Valor, self()},
					Ack = wait_for_set_ack(),
					Cpid ! {Ack, Clave, Valor},
					if
						% éxito en la escritura
						Ack == set_val_ack ->
							server_loop(LatidosProc, VistaMia,
								PrimarioMio, BackupMio, dict:store(Clave, Valor, BaseDatos));
						% error en la escritura
						true ->
							server_loop(LatidosProc, VistaMia,
								PrimarioMio, BackupMio, BaseDatos)
					end;
				% Si el servidor no está operativo =>
				% enviar ERR
				true ->
					Cpid ! {set_err, Clave, Valor},
					server_loop(LatidosProc, VistaMia,
						PrimarioMio, BackupMio, BaseDatos)
			end;

		% Soy primario y recibo escritura hash =>
		% forward a backup; escribir en mi BD; enviar ACK al cliente
		{set_hash_val, Clave, Valor, Cpid} when PrimarioMio == self() ->
			if
				BackupMio /= 0 ->
					NewVal = erlang:md5(erlang:list_to_binary(
						[erlang:atom_to_binary(dict:fetch(Clave, BaseDatos), latin1)]
						++ [erlang:atom_to_binary(Valor, latin1)])),
					BackupMio ! {set_val_forward, Clave, NewVal, self()},
					Ack = wait_for_set_ack(),
					if
						% éxito en la escritura
						Ack == set_val_ack ->
							Cpid ! {set_hash_val_ack, Clave, dict:fetch(Clave, BaseDatos)},
							server_loop(LatidosProc, VistaMia,
								PrimarioMio, BackupMio, dict:store(Clave, NewVal, BaseDatos));
						% error en la escritura
						true ->
							Cpid ! {set_hash_err, Clave, Valor},
							server_loop(LatidosProc, VistaMia,
								PrimarioMio, BackupMio, BaseDatos)
					end;
				% Si el servidor no está operativo =>
				% enviar ERR
				true ->
					Cpid ! {set_hash_err, Clave, Valor},
					server_loop(LatidosProc, VistaMia,
						PrimarioMio, BackupMio, BaseDatos)
			end;
			

		% Soy Backup y recibo escritura normal =>
		% enviar ERR a cliente
		{set_val, Clave, Valor, Cpid} ->
			Cpid ! {set_err, Clave, Valor},
			server_loop(LatidosProc, VistaMia,
				PrimarioMio, BackupMio, BaseDatos);

		% Soy Backup y recibo escritura hash =>
		% enviar ERR a cliente
		{set_hash_val, Clave, Valor, Cpid} ->
			Cpid ! {set_hash_err, Clave, Valor},
			server_loop(LatidosProc, VistaMia,
				PrimarioMio, BackupMio, BaseDatos);

		% Recibo lectura =>
		% devolver valor sea quien sea
		{get_val, Clave, Cpid} ->
			if
				BackupMio /= 0 ->
					Valor = dict:find(Clave, BaseDatos),
					if
						Valor == error ->
							Cpid ! {get_err, Clave};
						true ->
							Cpid ! erlang:element(2, Valor)
					end;
				% No está operativo =>
				% enviar ERR a cliente
				true ->
					Cpid ! {get_err, Clave}
			end,
			server_loop(LatidosProc, VistaMia,
				PrimarioMio, BackupMio, BaseDatos);

		% Soy primario y hay partición de red =>
		% enviar ERR al servidor que me forwardea la petición
		{set_val_forward, _Clave, _Valor, From} when PrimarioMio == self() ->
			From ! set_err,
			server_loop(LatidosProc, VistaMia,
				PrimarioMio, BackupMio, BaseDatos);

		% Soy backup y me redirigen una escritura
		% escribir en mi BD; devolver ACK
		{set_val_forward, Clave, Valor, From} ->
			From ! set_val_ack,
			server_loop(LatidosProc, VistaMia, PrimarioMio,
				BackupMio, dict:store(Clave, Valor, BaseDatos));

		% Mensaje común desde el gestor de vistas
		{Vista, Primario, Backup} ->
			if
				% Nueva vista donde yo soy el primario y hay un nuevo backup => transferir BD
				Vista /= VistaMia, Primario == self(), Backup /= 0 ->
					Backup ! {backup, BaseDatos},
					wait_for_db_ack(),
					LatidosProc ! Vista,
					server_loop(LatidosProc, Vista, Primario, Backup, BaseDatos);
				% Nueva vista donde yo soy el backup => sincronizar con primario
				Vista /= VistaMia, Primario /= self(), Backup == self() ->
					NuevaBaseDatos = wait_for_sync(),
					Primario ! db_ack,
					LatidosProc ! Vista,
					server_loop(LatidosProc, Vista, Primario, Backup, NuevaBaseDatos);
				% Nueva vista donde yo soy primario y no hay backup
				% o Nueva vista donde ni soy primario ni backup
				% o Misma vista que la que tenía
				true ->
					LatidosProc ! Vista,
					server_loop(LatidosProc, Vista, Primario, Backup, BaseDatos)
			end;

		true ->
			server_loop(LatidosProc, VistaMia, PrimarioMio, BackupMio, BaseDatos)
	end.

% Proceso para enviar latidos por intervalos
latidos_proc(ListenerGestorVistas, Vista, MiProc)->
	timer:sleep(?LATIDO),
	% receive no bloqueante
	receive
		% Actualiza vista y envía latido
		NuevaVista ->
			ListenerGestorVistas ! {latido, NuevaVista},
			latidos_proc(ListenerGestorVistas, NuevaVista, MiProc)
	after
		0 ->
			ListenerGestorVistas ! {latido, Vista},
			latidos_proc(ListenerGestorVistas, Vista, MiProc)
	end.

% Espera a recibir una confirmación de escritura ya sea ACK o ERR.
% Envía error a las peticiones entrantes.
wait_for_set_ack() ->
	receive
		set_val_ack ->
			set_val_ack;
		set_err ->
			set_err;
		{get_val, Clave, Cpid} ->
			Cpid ! {get_err, Clave},
			wait_for_set_ack();
		{set_val, Clave, Valor, Cpid} ->
			Cpid ! {set_err, Clave, Valor},
			wait_for_set_ack();
		{set_hash_val, Clave, Valor, Cpid} ->
			Cpid ! {set_hash_err, Clave, Valor},
			wait_for_set_ack();
		true ->
			wait_for_set_ack()
	end.
% Espera a recibir la confirmación de transferencia de base de datos.
% Envía error a las peticiones entrantes.
wait_for_db_ack() ->
	receive
		db_ack ->
			none;
		{get_val, Clave, Cpid} ->
			Cpid ! {get_err, Clave},
			wait_for_db_ack();
		{set_val, Clave, Valor, Cpid} ->
			Cpid ! {set_err, Clave, Valor},
			wait_for_db_ack();
		{set_hash_val, Clave, Valor, Cpid} ->
			Cpid ! {set_hash_err, Clave, Valor},
			wait_for_db_ack();
		true ->
			wait_for_db_ack()
	end.

% Espera a recibir la base de datos del primario.
% Envía error a las peticiones entrantes.
wait_for_sync() ->
	receive
		{backup, BaseDatos} ->
			BaseDatos;
		{get_val, Clave, Cpid} ->
			Cpid ! {get_err, Clave},
			wait_for_sync();
		{set_val, Clave, Valor, Cpid} ->
			Cpid ! {set_err, Clave, Valor},
			wait_for_sync();
		{set_hash_val, Clave, Valor, Cpid} ->
			Cpid ! {set_hash_err, Clave, Valor},
			wait_for_sync();
		true ->
			wait_for_sync()
	end.
