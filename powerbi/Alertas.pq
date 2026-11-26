// Visual 2: tabla de alertas activas de riesgo de cierre (las escribe el streaming)
let
    Respuesta = Json.Document(Web.Contents(BaseURL, [
        RelativePath = "alerts",
        Query = [activas = "1", limite = "500"],
        Headers = [#"ngrok-skip-browser-warning" = "1"]])),
    Columnas = {"business_id", "name", "categoria_principal", "probabilidad", "umbral", "fecha_evento",
                "procesado_en", "primera_alerta", "veces", "geohash", "lat", "lon"},
    Tabla = if List.IsEmpty(Respuesta[resultados]) then #table(Columnas, {})
            else Table.FromRecords(Respuesta[resultados], Columnas, MissingField.UseNull),
    Tipos = Table.TransformColumnTypes(Tabla, {
        {"probabilidad", type number}, {"umbral", type number}, {"fecha_evento", type datetime},
        {"procesado_en", type datetime}, {"primera_alerta", type datetime}, {"veces", Int64.Type},
        {"lat", type number}, {"lon", type number}}, "en-US")
in
    Tipos
