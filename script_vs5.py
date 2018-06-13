from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from bs4 import BeautifulSoup
import re
import pandas as pd
import os
import time
from datetime import datetime
import urllib2
import requests
import psycopg2
import json
import sys
#from dicttoxml import dicttoxml
import xml.etree.ElementTree as ET
from selenium.common.exceptions import TimeoutException, NoSuchElementException, ElementNotVisibleException


def update_pct(w_str):
    w_str = str(w_str)
    sys.stdout.write("\b" * len(w_str))
    sys.stdout.write(" " * len(w_str))
    sys.stdout.write("\b" * len(w_str))
    sys.stdout.write(w_str)
    sys.stdout.flush()

#launch url
url = "https://comprar.gob.ar/Compras.aspx?qs=W1HXHGHtH10="

#crear conexion con postgresql
try:
    conn = psycopg2.connect("dbname='comprar' user='postgres' host='localhost' password='Scrap$2018'")
except:
    print "No se puede conectar a la base de datos"
    sys.exit()

cur = conn.cursor()

driver = webdriver.PhantomJS()
driver.implicitly_wait(30)
try:
    driver.get(url)
except (NoSuchElementException, ElementNotVisibleException):
    log = {
        'respuesta' : 'Error de conexion.',
        'error' : 'La url no responde.',
        'url' : url
    }
    statement = 'INSERT INTO historial (resultado) VALUES (\'' + json.dumps(log) + '\')'
    cur.execute(statement)
    conn.commit()
    print "La url a consultar no esta disponible."
    driver.close()
    sys.exit()
#obtenemos paginacion
try:
    t_paginas = driver.find_element_by_id(id_='ctl00_CPH1_lblCantidadListaPliegosAperturaProxima')
except NoSuchElementException:
    log = {
        'respuesta' : 'No se encontro el elemento para generar el paginador del script',
        'error' : 'No se encontro la etiqueta ctl00_CPH1_lblCantidadListaPliegosAperturaProxima',
        'url' : url
    }
    statement = 'INSERT INTO historial (resultado) VALUES (\'' + json.dumps(log) + '\')'
    cur.execute(statement)
    conn.commit()
    print "La url a consultar no esta disponible."
    driver.close()
    sys.exit()

statement = 'SELECT licitaciones.numero_proceso,licitaciones.url FROM licitaciones;'
try:
    cur.execute(statement)
except psycopg2.IntegrityError:
    conn.rollback()
    sys.exit()
else:
    conn.commit()
rows = cur.fetchall()
num_registros_licitaciones = len(rows)
    
t_paginas_temp = t_paginas.text.split("(")
t_paginas_temp = t_paginas_temp[1].split(")")

n_registros_temp = int(t_paginas_temp[0])
n_paginas_resto = n_registros_temp % 10
n_paginas = n_registros_temp / 10

if n_paginas_resto > 0:
    n_paginas = n_paginas + 2

print "Finalizo el proceso de calculo del paginador."
print "Numero de registros en Base de Datos: %d"  % len(rows)
print "Numero de paginas a consultar son:" + str(n_paginas)
print "Numero de registros a consultar son:" + str(n_registros_temp)

num_registros_nuevos = 0
num_registros_existentes = 0
num_registros_actualizados = 0
num_registros_fallidos = 0

pct = 0

#recorrido del resto de las paginas
paginador_inicial = 1
for i in range(paginador_inicial,n_paginas):
    print "\n++++++++++++++++++ pagina: " + str(i) 
    if i > 1:
        scr = driver.execute_script("__doPostBack('ctl00$CPH1$GridListaPliegosAperturaProxima', 'Page$"+ str(i) +"');")
        time.sleep(6)
    #print scr
    #driver.implicitly_wait(30)
    soup = BeautifulSoup(driver.page_source, 'html.parser')
    table = soup.find_all('table', class_='table')
    tr = table[0].find_all('tr')

    #se inicializan contadores y banderas
    bandera1 = 10 #numero de filas
    bandera2 = 7  #numero de columnas
    contador1 = 0 
    contador2 = 0
    numero_proceso = ""

    for i in tr:
        #print str(n_paginas_resto) + " == " + str(contador1)
        contador2 = 0
        contador1 = contador1 + 1;
        #print str(n_paginas_resto) + " = " + str(contador1)
        saltar_registro = False
        agregar_url = False
        #vuelta para armar registro de licitaciones
        if contador1 >= 2 and contador1 <= 11: 
            print "+++++++ vuelta: " + str(contador1)
            for a in i.find_all('td'):
                contador2 = contador2 + 1;
                if contador2 == 1:
                    try:
                        numero_proceso = a.text
                    except:
                        print "numero_proceso no posee datos"
                        break
                    numero_proceso = numero_proceso.rstrip('\n\r')
                    print "numero de proceso: " + numero_proceso[1:]
                    numero_proceso_t = numero_proceso[1:]
                    for row in rows:
                        if row[0] == numero_proceso_t:
                            if row[1] == "about:blank":
                                agregar_url = True
                            else:
                                saltar_registro = True
                    #print numero_proceso
                if contador2 == 2:
                    try:
                        nombre_proceso = a.text
                    except:
                        break
                    nombre_proceso = nombre_proceso.rstrip('\n')
                if contador2 == 3:
                    try:
                        tipo_proceso = a.text
                    except:
                        break
                    tipo_proceso = tipo_proceso.rstrip('\n')
                if contador2 == 4:
                    try:
                        fecha_apertura_temp = a.text
                    except:
                        break
                    fecha_apertura_temp = fecha_apertura_temp.rstrip('\n')
                    print "fecha apertura" + fecha_apertura_temp[1:]
                    fecha_apertura_temp = fecha_apertura_temp.split(" ")
                    try:
                        fecha_apertura = fecha_apertura_temp[0]+" "+fecha_apertura_temp[1]
                        fecha_apertura = fecha_apertura + ':00'
                        
                    except:
                        break
                if contador2 == 5:
                    try:
                        estado = a.text
                    except:
                        break
                    estado = estado.rstrip('\n')
                if contador2 == 6:
                    try:
                        unidad_ejecutora = a.text
                    except:
                        break
                    unidad_ejecutora = unidad_ejecutora.rstrip('\n')
                if contador2 == 7:
                    try:
                        servicio_financiero = a.text
                    except:
                        break
                    servicio_financiero = servicio_financiero.rstrip('\n')
                #driver_clone = driver
                #btn_num_proceso = driver_clone.find_element_by_link_text(numero_proceso)
                #btn_num_proceso.click()
            if saltar_registro:
                num_registros_existentes = num_registros_existentes + 1
                print "omitido."
                #update_pct("Numero de registros procesados {n}%".format(n=str(pct)))
            else:
                if contador1 < 10:
                    cont = '0'+str(contador1)
                else:
                    cont = str(contador1)
                t = "theForm.__EVENTTARGET.value = 'ctl00$CPH1$GridListaPliegosAperturaProxima$ctl"+cont+"$lnkNumeroProceso';theForm.setAttribute('target', 'myActionWin');window.open('','myActionWin','width=500,height=300,toolbar=0');theForm.submit();"
                try:
                    driver.execute_script(t)
                except:
                    error = "no se ejecuto el script"
                time.sleep(8)
                driver.switch_to_window(driver.window_handles[1])
                url_licitacion = driver.current_url
                driver.close()
                driver.switch_to_window(driver.window_handles[0])
                if agregar_url:
                    statement = 'update licitaciones set url=\''+str(url_licitacion)+'\' where numero_proceso=\''+numero_proceso[1:]+'\''
                    try:
                        cur.execute(statement)
                    except psycopg2.DatabaseError:
                        conn.rollback()
                        num_registros_fallidos = num_registros_fallidos + 1
                        print "fallido"
                    else:
                        try:
                            conn.commit()
                        except psycopg2.DatabaseError:
                            num_registros_fallidos = num_registros_fallidos + 1
                            print "fallido"
                            break
                        num_registros_actualizados = num_registros_actualizados + 1
                        print "actualizado"
                else:
                    statement = 'INSERT INTO licitaciones (numero_proceso,nombre_proceso,tipo_proceso,fecha_apertura,estado,unidad_ejecutora,servicio_financiero,url) VALUES (\'' + numero_proceso[1:] + '\',\'' + nombre_proceso + '\',\'' + tipo_proceso[1:] + '\',to_timestamp(\''+fecha_apertura+'\', \'DD/MM/YYYY HH24:MI:SS \'),\'' + estado[1:] + '\',\'' + unidad_ejecutora[1:] + '\',\'' + servicio_financiero[1:] + '\',\'' + str(url_licitacion) + '\')'
                    try:
                        cur.execute(statement)
                    except psycopg2.DatabaseError:
                        conn.rollback()
                        num_registros_fallidos = num_registros_fallidos + 1
                        print "fallido"
                    else:
                        try:
                            conn.commit()
                        except psycopg2.DatabaseError:
                            num_registros_fallidos = num_registros_fallidos + 1
                            print "fallido"
                            break
                        num_registros_nuevos = num_registros_nuevos + 1
                        print "agregado"
                            
            pct = pct + 1
            #update_pct("Numero de registros procesados {n}".format(n=str(pct)))
print "========================================================================"
print " +++++++++++++++++++++++++ RESULTADOS ++++++++++++++++++++++++++++++++++"
print "REGISTROS NUEVOS: " + str(num_registros_nuevos)
print "REGISTROS EXISTENTES: " + str(num_registros_existentes)
print "REGISTROS ACTUALIZADOS: " + str(num_registros_actualizados)
print "REGISTROS FALLIDOS: " + str(num_registros_fallidos)
log = {
        'ACCION' : 'CULMINACION DEL PROCESO DE SCRAPING',
        'REGISTROS NUEVOS' : str(num_registros_nuevos),
    'REGISTROS EXISTENTES' : str(num_registros_existentes),
    'REGISTROS ACTUALIZADOS' : str(num_registros_actualizados),
    'REGISTROS FALLIDOS' : str(num_registros_fallidos),
        'url' : url
    }
statement = 'INSERT INTO historial (resultado) VALUES (\'' + json.dumps(log) + '\')'
cur.execute(statement)
conn.commit()

cur.close()

