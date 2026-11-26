# Dashboard de Power BI (Entrega 2)

Cuatro consultas de Power Query que leen la **API desplegada** (no la base de datos): cada clic en
**Inicio → Actualizar** vuelve a llamar a la API y el tablero muestra lo que el streaming acaba de escribir.

1. Crear el parámetro `BaseURL` (Inicio → Transformar datos → Administrar parámetros → Nuevo; tipo Texto;
   valor: la URL pública, p. ej. `https://curtsy-endearing-scribing.ngrok-free.dev`).
2. Para cada archivo `.pq`: Inicio → Obtener datos → Consulta en blanco → Editor avanzado → pegar el contenido
   → nombrar la consulta igual que el archivo (Hotspots, Alertas, Supervivencia, Evolucion).
3. Si Power BI pregunta por credenciales: **Anónimo**. Si pregunta por niveles de privacidad: **Público**.

Todas las llamadas envían la cabecera `ngrok-skip-browser-warning` para saltar la página de aviso de ngrok.
