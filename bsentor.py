import csv, json, os, mysql.connector, pathlib
from mysql.connector import errorcode

#constantes
HOST="host"
USER="user"
PASSWD="password"
DB="database"
PORT="port"
CHARSET="charset"
#variables
inicio=0
inicio_1=1
para_crear_db=False
#concatenadores
main_menu=""
host=""
user=""
password=""
database=""
port=""
charset=""
#dictionary vacío
configuracion={}
#funcion/es
def recorrido_rows_indices(y=False):
	"""Usa fetch-all en una variable para recorrer todas las rows y luego imprimirlas por índice"""
	while y==True:
		y=False
		print("\n\tSeleccione tipo de recorrido del registro")
		print("\t1. Sin índice")
		print("\t2. Con índice")
		tipo_index=input("\tIngrese opción numérica: ")
		rows=cursor.fetchall()#(presenta todo el resultado | FETCH ALL /fetchone presentar la primera row)	
		print("\n\t>>REGISTROS DATABASE:")
		if tipo_index.isnumeric()==True:
			if tipo_index=="1":
				for i in rows:
					print("\t",i)
			elif tipo_index=="2":
				for i in rows:
					print("\t",i[0],i[1],i[2],i[3],i[4],i[5],i[6],i[7])
			else:
				error_op(1)
				y=True
		else:
			error_op(2)
			y=True
def crear(datos):
	"""crea un archivo de salida según el directorio actual"""
	os.chdir(os.getcwd())
	#os.chdir()
	data_filename="iss_tui_registros.js"
	with open(data_filename, "w", encoding="utf-8") as file_handle:
		json.dump(datos, file_handle)
def error_op(x=None):
	if x==1:
		print("\n\t>error: opción incorrecta")
	elif x==2:
	  	print("\n\t>error: debe ingresar un número")
"""------------------------------------------------------------------------------------------------------"""
while inicio==0:
	inicio+=1
	#MENU INICIO: argumentos de conexión
	print("\n/////////////////////////////////////////////////////////")
	print("\t>>>BIENVENIDO A MYSQL-CONNECTOR-PYTHON<<<")
	print("////////////////////////////////////////////////////////")
	
	print("\n\tIngreso de datos de configuracion de conexión")
	print("\t1. Manual")
	print("\t2. Automático (se ingresará datos por default)")
	opcion_configuracion=input("\t>Ingrese opción numérica: ")

	if opcion_configuracion.isnumeric()==True:
		if opcion_configuracion=="1":
			print("\n\tIngrese los siguientes datos")
			host=input("\n\t>HOST: ")
			user=input("\t>USER: ")
			password=input("\t>PASSWORD: ")
			port=input("\t>PORT: ")
			charset=input("\t>CHARSET (ej. utf8): ")
		elif opcion_configuracion=="2":
			host="localhost"
			user="root"
			password=""
			port="3306"
			charset="utf8"
		else:
			error_op(1)
			inicio=0
	else:
		error_op(2)
		inicio=0

	while inicio_1==1:
		inicio_1+=1
		print("\n\t1. Ingresar database")
		print("\t2. Crear database y reconectar a la misma")
		print("\t3. Entrar sin database")
		op_database=input("\t>Ingrese opción numérica: ")
		if op_database.isnumeric()==True:
			if op_database=="1":
				database=input("\t>DATABASE: ")
				configuracion[DB]=database
			elif op_database=="2":
				main_menu="0"
			elif op_database=="3":
				continue
			else:
				error_op(1)
				inicio_1=1
		else:
			error_op(2)
			inicio_1=1

	#los argumentos de conexión son previamente definidos en un diccionario [con keys:values] para su mejor manipulación
	#otra opción --> configuracion=mysql.connector.connect(option_files='/etc/mysql/connectors.cnf')

	configuracion[HOST]=host
	configuracion[USER]=user
	configuracion[PASSWD]=password
	configuracion[PORT]=port
	configuracion[CHARSET]=charset

	try: #conectando a la DB
		while inicio==1:
			if para_crear_db==True:
				para_crear_tabla=True
			inicio+=1
			conexion_db=mysql.connector.connect(**configuracion)
			print("\n==========================================================")
			print("\t   C O N E C T A D O  E X I T O S A M E N T E")
			print("\t   >>>Bienvenido",user)
			print("\t   >Database ingresada:", database)
			print("==========================================================")

			#cursor (nos permite ejecutar y usar los comandos SQL en la sesión actual)
			#nos da la ventaja de tener multiples y separados ambientes de trabajo a traves de la misma conexión a la base de datos.
			cursor=conexion_db.cursor()

			#MENU PRINCIPAL
			print("\n\tSELECCIONE UNA OPCIÓN para",database)
			print("\t1. SQL: códigos")
			print("\t2. CSV: importar o extraer datos ")
			print("\t3. JSON: exportar datos")
			print("\t4. Exit")
			if main_menu=="0":
				main_menu="0"
			else:
				main_menu=input("\t>Ingrese una opción numérica: ")

			if main_menu.isnumeric()==True:
				if main_menu=="0":
					if para_crear_db==False:
						print("\n\t>>>creando database...")
						cursor.execute("CREATE DATABASE iss_tui")
						database="iss_tui"
						configuracion[DB]=database
						print("\t>>>reconectando a...", database)
						para_crear_db=True
						inicio=1
					elif para_crear_tabla==True:
						print("\n\t>>>creando tablas con sus columnas...")
						cursor.execute("CREATE TABLE registros (`id` int(11) NOT NULL AUTO_INCREMENT, `clave_profesor` int(4) DEFAULT NULL,  `fecha` date DEFAULT NULL,  `hora_de_ingreso` time DEFAULT NULL,  `hora_de_salida` time DEFAULT NULL,  `sala` int(3) DEFAULT NULL, PRIMARY KEY (`id`))")
						cursor.execute("INSERT INTO registros (id, clave_profesor, fecha, hora_de_ingreso, hora_de_salida, sala) VALUES ('1','1111','2019/07/17','12:12:12','13:13:13','505')")
						print("\t>>tablas y columnas creadas en registros")
						para_crear_db=False
						main_menu=""
						inicio=1
				elif main_menu=="1":
					x=0
					while x==0:
						x+=1
						print("\n\tOPCIONES SQL")
						print("\t0. Ingresar código manualmente")
						print("\t1. Leer tabla de",database)
						print("\t2. ID's")
						print("\t3. Clave profesor")
						print("\t4. Fecha")
						print("\t5. volver")
						op_sql=input("\t>Ingrese opcion numérica: ")
						if op_sql.isnumeric()==True:
							if op_sql=="0":
								cursor.execute(input("\n\t>Ingrese código SQL: "))
								print("\t>ejectuando código sql ingresado...")
							elif op_sql=="1":								
								#ejecutar la query
								cursor.execute("SELECT * FROM registros")
								recorrido_rows_indices(True)
							elif op_sql=="2":
								print("\n\tIngrese ID's BETWEEN (puede seleccionar el mismo número para una ID)")
								id1=input("\t> ID 1: ")
								id2=input("\t> ID 2: ")
								cursor.execute("SELECT * FROM registros WHERE id BETWEEN %s AND %s",(id1,id2,))
								recorrido_rows_indices(True)
							elif op_sql=="3":
								clave=input("\n\t> clave: ")
								cursor.execute("SELECT * FROM registros WHERE clave_profesor=%s",(clave,))
								recorrido_rows_indices(True)		
							elif op_sql=="4":
								print("Ingrese en el siguiente formato: AAAA/MM/DD")
								fecha=input("\t>Fecha: ")
								cursor.execute("SELECT * FROM registros WHERE fecha=%s",(fecha,))
								recorrido_rows_indices(True)		
							elif op_sql=="5":
								inicio=1
							else:
								error_op(1)
								x=0
						else:
							error_op(2)
							x=0

				elif main_menu=="2":
					archivo=input("\n\tIngrese nombre del archivo.csv o path (ej. C:/users/user/desktop/carpeta/archivo.csv): ")
					if os.path.exists(archivo):	
						with open(archivo,"r") as csv_file:
							csv_data=csv.reader(csv_file, delimiter=",", lineterminator='\n')
							print("\n\t>>Leyendo",archivo,"...")
							#separar el statement sql entre query y values 
							#e implementarlo en un loop (for) a cada índice de cada fila (row) del archivo csv que ha sido abierto y leído.
							query=("INSERT INTO registros (clave_profesor, fecha, hora_de_ingreso, hora_de_salida, sala) VALUES (%s,%s,%s,%s,%s)")
							values=("clave_profesor","fecha","hora_de_ingreso","hora_de_salida","sala")
							for row in csv_data:
								str(row[0])
								str(row[1])
								str(row[2])
								str(row[3])
								str(row[4])
								print("\timportando:",row) #imprime una lista de todos los valores por cada linea 
								cursor.execute(query, row)
								cursor.execute("DELETE FROM registros where sala=0 OR clave_profesor=0;")
							print("\t>>Datos innecesarios eliminados correctamente.")
							print("\n\t>>>Ha importado",archivo," EXITOSAMENTE a",database,"!!!<<<")
							conexion_db.commit()		
					else:
						print("\n\t>>El archivo no existe")
						inicio=1
				
				elif main_menu=="3":
					#Importar datos Json
					print("\n\tExportando datos a json...")
					#row_headers=[x[0] for x in cursor.description] #this will extract row headers
					cursor.execute("SELECT * FROM registros")
					data=cursor.fetchall()						
					for e in data:
						datos_json=(json.dumps(data, sort_keys=True, indent=4, separators=(',', ': '), default=str))
						print("exportando",datos_json)
						crear(datos_json)
					print("\tExportado con éxito")

				elif main_menu=="4": #exit
					continue

	#para error de conexión
	except mysql.connector.Error as err:
	  if err.errno==errorcode.ER_ACCESS_DENIED_ERROR:
	    print("\n\t>>USUARIO o PASSWORD incorrectos")
	    inicio=0
	  elif err.errno==errorcode.ER_BAD_DB_ERROR:
	    print("\n\t>>La DATABASE no existe")
	    inicio=0
	  else:
	    print("\n>>>ERROR DE MYSQL (conexión o parámetros):")
	    print("\t>>No es posible conectar MySQL server")
	    print("\t>>error:",err)
	    print("\n\t>Ejecute el programa y realice la conexión nuevamente.")
	    break
    
	else:
	  while inicio==2:
	  	inicio+=1
	  	print("\n¿Desea realizar otra operación?")
	  	print("\t1. Sí")
	  	print("\t2. No")
	  	operacion=input("\t>Ingrese opción numérica: ")

	  	if operacion.isnumeric()==True:
	  		if operacion=="1":
	  			while inicio==3:
	  				print("\n\t1. MENU INICIO (configuracion de conexión)")
	  				print("\t2. volver")
	  				menus=input("\t>Ingrese una opción: ")
	  				if menus.isnumeric()==True:
	  					if menus=="1":
	  						inicio=0
	  					elif menus=="2":
	  						inicio=2
	  					else:
	  						error_op(1)
	  						inicio=3
	  				else:
	  					error_op(2)
	  					inicio=3
	  		elif operacion=="2":
	  			#cerrando cursor
	  			print("\n\t>>Cerrando el cursor...")
	  			cursor.close()
	  			#cerrando conexión
	  			print("\t>>Desconectando de la DATABASE",database,"...")
	  			conexion_db.close()
	  			print("\t>>Desconectado de MySQL.")
	  			enter=input("\n\t>>ENTER PARA TERMINAR")
	  			os.system("cls" if os.name == "nt" else "clear")
	  			break
	  		else:
	  			error_op(1)
	  			inicio=2
	  	else:
	  		error_op(2)
	  		incio=2
