### Descripción general de los archivos

#### 1. **Archivo `ingestar_datos.py`**
   Este script está diseñado para procesar e ingresar datos en un sistema de destino (Snowflake) utilizando la librería `dlt`. Sus características principales incluyen:
   - **Carga de Credenciales:** Lee las credenciales desde un archivo `secrets.toml` para configurar la conexión a Snowflake.
   - **Procesamiento de Archivos CSV:** Escanea una carpeta en busca de archivos CSV y los asocia a tablas específicas basadas en un mapeo definido.
   - **Configuración de Pipelines:** Configura dinámicamente pipelines para diferentes datasets según el tipo de tabla, utilizando los esquemas `google_sheets` o `sql_server_dbo`.
   - **Ingesta de Datos:** Carga los datos de los archivos CSV en tablas dentro del destino configurado.
   - **Registro de Logs:** Proporciona mensajes sobre el progreso y errores durante la ingesta.

#### 2. **Archivo `crear_datos.py`**
   Este script genera datos de ejemplo de manera programática y los exporta como archivos CSV. Sus principales funcionalidades son:
   - **Generación de Datos Aleatorios:** 
     - Direcciones con combinaciones únicas de país, estado, dirección y código postal.
     - Usuarios con datos únicos como correos electrónicos, números de teléfono, y direcciones asignadas.
     - Productos organizados por categorías con precios e inventarios aleatorios.
     - Pedidos y detalles relacionados, incluyendo costos, promociones y servicios de envío.
     - Eventos relacionados con productos y dominios específicos.
     - Presupuestos asignados a productos por categorías.
     - Reseñas de productos con calificaciones y comentarios.
     - Devoluciones de pedidos con razones variadas.
   - **Cálculos Derivados:**
     - Costos totales de pedidos en base a los detalles de productos y costos de envío.
   - **Exportación de Datos:** Guarda los datos generados en archivos CSV listos para ser usados por otros scripts como `ingestar_datos.py`.

Ambos archivos están diseñados para trabajar de manera conjunta: `crear_datos.py` genera los datos, mientras que `ingestar_datos.py` los procesa y los carga en el sistema de destino.
