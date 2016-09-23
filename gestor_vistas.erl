%% coding: latin-1
%% FICHERO: gestor_vistas.erl
%% DESCRIPCIÓN: Exporta la función de inicio de un gestor de vistas
%%		que tolera a un fallo (caída) a la vez entre los
%%		servidores Primario y Backup y sin tolerancia a
%%		fallos en el propio gestor de vistas.

-module(gestor_vistas).
-export([init/0, listener/3]).
% Constantes
-include("network.hrl").

init() ->
	register(?NOMBREREG_GESTOR, self()),
	gestor_loop(false, 0, 0, 0, 0, 0, 0, []).

gestor_loop(Operativo, VistaValida, PrimarioValido, BackupValido, 
	    VistaPropuesta, PrimarioPropuesto, BackupPropuesto, SvsDisponibles) ->
	receive
		% MENSAJES POR PARTE DEL CLIENTE

		% Solicita proceso del servidor primario y el gestor de vistas está operativo =>
		% Devuelve PID del primario
		{primario, Pid} when Operativo == true ->
			Pid ! PrimarioValido,
			gestor_loop(Operativo, VistaValida, PrimarioValido, BackupValido,
				    VistaPropuesta, PrimarioPropuesto, BackupPropuesto, SvsDisponibles);

		% Solicita proceso del servidor primario pero el gestor de vistas no está operativo =>
		% Devuelve false ya que aún no hay primario válido
		{primario, Pid} ->
			Pid ! false,
			gestor_loop(Operativo, VistaValida, PrimarioValido, BackupValido,
				    VistaPropuesta, PrimarioPropuesto, BackupPropuesto, SvsDisponibles);

		% Solicita la vista propuesta y si está activa o no
		% Devuelve la tupla {Vista, Primario, Backup, Operativo}
		{vista, Pid} ->
			Pid ! {VistaPropuesta, PrimarioPropuesto, BackupPropuesto, Operativo},
			gestor_loop(Operativo, VistaValida, PrimarioValido, BackupValido,
				    VistaPropuesta, PrimarioPropuesto, BackupPropuesto, SvsDisponibles);



		% MENSAJES POR PARTE DE LOS SERVIDORES (INICIALIZACIÓN)

		% CASO INICIAL: No hay ni Primario ni Backup => Pid es nuevo Primario y actualiza vista propuesta
		{latido, 0, Pid} when PrimarioPropuesto == 0, BackupPropuesto == 0 ->
			P = spawn(?MODULE, listener, [self(), Pid, ?TIMEOUT]),
			Pid ! {listener, P},
			Pid ! {VistaPropuesta+1, Pid, BackupPropuesto},
			%io:format("[~p false]: ~p ! {~B, ~p, ~p}~n",[self(), Pid, VistaPropuesta+1, Pid, BackupPropuesto]),
			gestor_loop(false, VistaValida, PrimarioValido, BackupValido,
				    VistaPropuesta+1, Pid, BackupPropuesto, SvsDisponibles);

		% CASO INICIAL: Hay Primario propuesto pero no Backup => Pid es nuevo Backup y actualiza vista propuesta; espera ack de Primario
		{latido, 0, Pid} when BackupPropuesto == 0 ->
			P = spawn(?MODULE, listener, [self(), Pid, ?TIMEOUT]),
			Pid ! {listener, P},
			Pid ! {VistaPropuesta+1, PrimarioPropuesto, Pid},
			%io:format("[~p false]: ~p ! {~B, ~p, ~p}~n",[self(), Pid, VistaPropuesta+1, PrimarioPropuesto, Pid]),
			gestor_loop(false, VistaValida, PrimarioValido, BackupValido,
				    VistaPropuesta+1, PrimarioPropuesto, Pid, SvsDisponibles);

		% CASO GENÉRICO: Los nuevos servidores se añaden a la lista de disponibles
		{latido, 0, Pid} ->
			P = spawn(?MODULE, listener, [self(), Pid, ?TIMEOUT]),
			Pid ! {listener, P},
			Pid ! {VistaPropuesta, PrimarioPropuesto, BackupPropuesto},
			%io:format("[~p true]: ~p ! {~B, ~p, ~p}~n",[self(), Pid, VistaPropuesta, PrimarioPropuesto, BackupPropuesto]),
			gestor_loop(true, VistaValida, PrimarioValido, BackupValido,
				    VistaPropuesta, PrimarioPropuesto, BackupPropuesto, SvsDisponibles ++ [Pid]);



		% MENSAJES POR PARTE DE LOS SERVIDORES (LATIDOS COMUNES)

		% Gestor inoperativo
		{latido, Vista, Pid} when Operativo /= true ->
			Pid ! {VistaPropuesta, PrimarioPropuesto, BackupPropuesto},
			if
				% Si es el nuevo Primario con ack y existe Backup => Vista propuesta ya es válida
				Pid == PrimarioPropuesto, Vista == VistaPropuesta, BackupPropuesto > 0 ->
					%io:format("[~p true]: ~p ! {~B, ~p, ~p}~n",[self(), Pid, VistaPropuesta, PrimarioPropuesto, BackupPropuesto]),
					gestor_loop(true, VistaPropuesta, Pid, BackupPropuesto,
						    VistaPropuesta, Pid, BackupPropuesto, SvsDisponibles);
				% Si es el nuevo Primario con ack y no existe Backup (caso inicial) => Propuesta válida pero servidor inoperativo.
				Pid == PrimarioPropuesto, Vista == VistaPropuesta ->
					%io:format("[~p false]: ~p ! {~B, ~p, ~p}~n",[self(), Pid, VistaPropuesta, PrimarioPropuesto, BackupPropuesto]),
					gestor_loop(false, VistaPropuesta, Pid, BackupPropuesto,
						    VistaPropuesta, Pid, BackupPropuesto, SvsDisponibles);
				% Sino => Comunica la vista propuesta y sigue esperando
				true ->
					%io:format("[~p false]: ~p ! {~B, ~p, ~p}~n",[self(), Pid, VistaPropuesta, PrimarioPropuesto, BackupPropuesto]),
					gestor_loop(false, VistaValida, PrimarioValido, BackupValido,
						    VistaPropuesta, PrimarioPropuesto, BackupPropuesto, SvsDisponibles)
			end;

		% Gestor operativo
		{latido, _Vista, Pid} ->
			Pid ! {VistaValida, PrimarioValido, BackupValido},
			%io:format("[~p true]: ~p ! {~B, ~p, ~p}~n",[self(), Pid, VistaValida, PrimarioValido, BackupValido]),
			gestor_loop(true, VistaValida, PrimarioValido, BackupValido,
				    VistaPropuesta, PrimarioPropuesto, BackupPropuesto, SvsDisponibles);



		% MENSAJES POR PARTE DE LOS LISTENER

		% El servidor Primario ha caído => intentar reemplazarlo
		{crash, Pid} when Pid == PrimarioValido ->
			IsEmpty = length(SvsDisponibles),
			if
				% Si no hay servidores disponibles => esperar a que se inicie un nuevo servidor para ser Backup
				IsEmpty == 0 ->
					gestor_loop(false, VistaValida, PrimarioValido, BackupValido,
						    VistaPropuesta+1, BackupValido, 0, SvsDisponibles);
				% Si hay servidores disponibles => mover backup a primario y asignar nuevo backup
				true ->
					NewBackup = lists:nth(1, SvsDisponibles),
					gestor_loop(false, VistaValida, PrimarioValido, BackupValido,
						    VistaPropuesta+1, BackupValido, NewBackup, lists:delete(NewBackup, SvsDisponibles))
			end;
		% El servidor Backup ha caído => intentar reemplazarlo
		{crash, Pid} when Pid == BackupValido ->
			IsEmpty = length(SvsDisponibles),
			if
				% Si no hay servidores disponibles => esperar a que se inicie un nuevo servidor para ser Backup
				IsEmpty == 0 ->
					gestor_loop(false, VistaValida, PrimarioValido, BackupValido,
						    VistaPropuesta+1, PrimarioPropuesto, 0, SvsDisponibles);
				% Si hay servidores disponibles => asignar nuevo backup
				true ->
					NewBackup = lists:nth(1, SvsDisponibles),
					gestor_loop(false, VistaValida, PrimarioValido, BackupValido,
						    VistaPropuesta+1, PrimarioPropuesto, NewBackup, lists:delete(NewBackup, SvsDisponibles))
			end;
		% Otro servidor ha caído => gestor de vistas sigue como estaba
		{crash, Pid} ->
			gestor_loop(Operativo, VistaValida, PrimarioValido, BackupValido,
				    VistaPropuesta, PrimarioPropuesto, BackupPropuesto, lists:delete(Pid, SvsDisponibles));



		% OTROS MENSAJES (nunca)

		% No es ninguno de los mensajes anteriores => continúa el flujo normal
		true ->
			gestor_loop(Operativo, VistaValida, PrimarioValido, BackupValido,
				    VistaPropuesta, PrimarioPropuesto, BackupPropuesto, SvsDisponibles)
	end.

% Thread listener para cada servidor
listener(GestorVistas, Pid, Count) when Count > 0 ->
	receive
		% Latido común del servidor
		{latido, VistaAck} ->
			GestorVistas ! {latido, VistaAck, Pid},
			listener(GestorVistas, Pid, ?TIMEOUT);
		% Else (nunca llega)
		true ->
			none
	after
		?LATIDO ->
			NewCount = Count - 1,
			listener(GestorVistas, Pid, NewCount)
	end;

listener(GestorVistas, Pid, 0) ->
	GestorVistas ! {crash, Pid}.
