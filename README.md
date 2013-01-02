models-log-processor
====================

Lista de archivos y directorios:

* items/
 * Tiene los logs filtrados donde supuestamente estan los hits a items. Estos
   logs son usados por el procesador.py para general la base en sqlite.
 * bajarlos desde http://10.0.5.76/~gabriel/items
* supergrep
 * Script que filtra los logs originales de los webs tratando de dejar solo los
   hits a item pages.
* lista.txt
 * Lista de los archivos originales para bajarlos con 'wget -i lista.txt'
 * Elegí el 26 de Noviembre porque fue el dia con mas trafico en los ultimos
   dos o tres meses.
* procesador.py
 * Procesa los logs de items y guarda la info básica en un archivo sqlite3.
