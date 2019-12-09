Deuda Tecnica
-Validar con Abrahan lo siguiente:
        1.- El insumo Clientes Particulares no tiene encabezado, siempre llega asi?
        2.- ¿El Layout de Clientes Particulares es siempre el mismo? esto para dejar fija la posicion de los campos 0 y 12 correspondientes a ID_CLIENTE y ACTIVO
         
-Integracion de tabla/calendario para deteccion automatica de 5to dia habil
-Definir como SourceReader el archivo conf
-Definir la lectura por particion con parametro de fecha 
-Modificar la funcion ctesVigentes: solo debera recibir dos DF y unirlos, el integrador debera llamarlo las veces necesarias hasta conjuntar el universo completo