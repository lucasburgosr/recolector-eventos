# Recolector de eventos de turismo de reuniones

Este sistema combina la API de búsqueda de Google con las APIs de Groq y Gemini para procesar la información con IA.

## Pipeline de datos
### Búsqueda con Custom Search API

La API de Google Custom Search nos permite realizar búsquedas con términos, ubicación y períodos determinados. También nos permite excluir resultados no deseados con algunos filtros. Para usar esta API de búsqueda, primero hay que configurar un motor de búsqueda personalizado en https://programmablesearchengine.google.com/. Esto nos permite ingresar las páginas en las que queremos que se busque y en cuales no. También podemos indicar si queremos incluir resultados de toda la web o únicamente de las páginas seleccionadas.

Para la búsqueda tomamos la lista de sedes registradas en el Observatorio Económico de Turismo de Reuniones (OETR) y armamos combinaciones de sedes + tipo de evento. Por cada ejecución se realizan 100 búsquedas que pueden traer **hasta** 10 resultados cada una, dándonos un máximo posible de 1000 resultados por búsqueda.

El resultado de este script es un archivo CSV con los links encontrados y el título de cada sitio web.

El código relacionado con esta parte se encuentra en el archivo ```search.py```.

---

### Revisión de links

