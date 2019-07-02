import csv, json, os, mysql.connector
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
			database=input("\t>DATABASE: ")
			port=input("\t>PORT: ")
			charset=input("\t>CHARSET: ")
		elif opcion_configuracion=="2":
			database=input("\t>DATABASE: ")
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
	
	#los argumentos de conexión definidos en un diccionario [con keys:values] para su mejor manipulación
	#otra opción --> configuracion=mysql.connector.connect(option_files='/etc/mysql/connectors.cnf')
	configuracion[HOST]=host
	configuracion[USER]=user
	configuracion[PASSWD]=password
	configuracion[DB]=database
	configuracion[PORT]=port
	configuracion[CHARSET]=charset	
	#conectando a la DB
	try:
		conexion_db=mysql.connector.connect(**configuracion)
		print("\n\t>>>CONECTADO EXITOSAMENTE A LA DATABASE:",database)
		print("\t>>>Bienvenido",user)

		#cursor (nos permite ejecutar y usar los comandos SQL en la sesión actual)
		#nos da la ventaja de tener multiples y separados ambientes de trabajo a traves de la misma conexión a la base de datos.
		cursor=conexion_db.cursor()

		while inicio==1:
			inicio+=1
			#MENU PRINCIPAL
			print("\n\tSELECCIONE UNA OPCIÓN para",database)
			print("\t1. Extraccion de datos 'registros' en sql")
			print("\t2. CSV: importar o extraer datos ")
			print("\t3. JSON: importar o extraer datos")
			print("\t4. Exit")
			main_menu=input("\t>Ingrese una opción numérica: ")
			if main_menu.isnumeric()==True:
				if main_menu=="1":
					x=0	
					while x==0:
						x+=1
						print("\n\tOPCIONES SQL")
						print("\t1. Leer tabla de",database)
						print("\t2. ID's")
						print("\t3. Clave profesor")
						print("\t4. Nombre y apellido")
						print("\t5. Fecha")
						print("\t6. volver")
						op_sql=input("\tIngrese opcion numérica: ")
						if op_sql.isnumeric()==True:
							if op_sql=="1":								
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
								print("\n\tPara ingresar acentos o caracteres en español, es necesario que su database esté con UTF-8")
								print("\tDe lo contrario no se imprimirán los resultados buscados")
								nombre=input("\t\n> nombre: ")
								apellido=input("\t> apellido: ")
								cursor.execute("SELECT * FROM registros WHERE nombre=%s AND apellido=%s",(nombre,apellido,))
								recorrido_rows_indices(True)							
							elif op_sql=="5":
								print("Ingrese en el siguiente formato: AAAA/MM/DD")
								fecha=input("\t>Fecha: ")
								cursor.execute("SELECT * FROM registros WHERE fecha=%s",(fecha,))
								recorrido_rows_indices(True)		
							elif op_sql=="6":
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
							query=("INSERT INTO registros (id,clave_profesor, nombre, apellido, fecha, hora_de_ingreso, hora_de_salida, sala) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)")
							values=("id","clave_profesor","nombre","apellido","fecha","hora_de_ingreso","hora_de_salida","sala")
							for row in csv_data:
								str(row[0])
								str(row[1])
								str(row[2])
								str(row[3])
								str(row[4])
								str(row[5])
								str(row[6])
								str(row[7])
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
					z=True
					while z==True:
						z=False
						print("\n\tSeleccione una opción de datos")
						print("\t1. Insertar")
						print("\t2. Extraer")
						op_json=input("\tIngrese opción numérica: ")
						if op_json.isnumeric()==True:
							if op_json=="1":
								"""
								add_reg={}
								ID="id"
								CV_P="clave_profesor"
								NAME="nombre"
								LAST_N="apellido"
								DATE="fecha"
								T_IN="hora_de_entrada"
								T_OUT="hora_de_salida"
								ROOM="sala"
								print("Ingresar datos")
								id_=input("id_: ")
								clave_p=input("clave_p: ")
								name_=input("name_: ")
								last_n=input("last_n: ")
								date_=input("date_")
								t_in=input("t_in: ")
								t_out=input("t_out: ")
								room=input("room: ")
								add_reg[ID]=id_
								add_reg[CV_P]=clave_p
								add_reg[NAME]=name_
								add_reg[LAST_N]=last_n
								add_reg[DATE]=date_
								add_reg[T_IN]=t_in
								add_reg[T_OUT]=t_out
								add_reg[ROOM]=room
								
								add_reg={
									"id":"12",
									"clave_profesor":"1234",
									"nombre":"Vergamin",
									"apellido":"Perra",
									"fecha":"2019/12/21",
									"hora_de_ingreso":"09:34:12",
									"hora_de_salida":"13:23:03",
									"sala":"203"
									}
								print(add_reg)
								query_1=("INSERT INTO registros (id,clave_profesor, nombre, apellido, fecha, hora_de_ingreso, hora_de_salida, sala) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)")
								values_1=("id","clave_profesor","nombre","apellido","fecha","hora_de_ingreso","hora_de_salida","sala")
								cursor.execute(query_1, values_1, (json.dumps(add_reg),))
								print("insertados")
							elif op_json=="2":
								print(None)
							else:
								error_op(1)
								z=True
						else:
							error_op(2)
							z=True
							"""

#				elif main_menu=="4":

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
	    inicio=0
    
	else:
	  while inicio==2:
	  	inicio+=1
	  	print("\n¿Desea realizar otra operación?")
	  	print("\t1. Sí")
	  	print("\t2. No")
	  	operacion=input("Ingrese opción numérica: ")

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
	  			
	  		elif operacion=="2" or operacion=="exit":
	  			#cerrando cursor
	  			print("\n>>Cerrando el cursor...")
	  			cursor.close()
	  			#cerrando conexión
	  			print(">>Desconectando de la DATABASE",database,"...")
	  			conexion_db.close()
	  			print(">>Desconectado de MySQL.")
	  			break
	  		else:
	  			error_op(1)
	  			inicio=2
	  	else:
	  		error_op(2)
	  		incio=2
