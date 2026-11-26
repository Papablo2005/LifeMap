// Visual 4: evolución del % de reseñas negativas por ventana de 30 días (histórico hasta T + streaming)
let
    Respuesta = Json.Document(Web.Contents(BaseURL, [
        RelativePath = "live/evolucion",
        Headers = [#"ngrok-skip-browser-warning" = "1"]])),
    Columnas = {"inicio", "resenas", "negativas", "pct_negativo", "celdas", "hotspots", "fuente"},
    Tabla = if List.IsEmpty(Respuesta[resultados]) then #table(Columnas, {})
            else Table.FromRecords(Respuesta[resultados], Columnas, MissingField.UseNull),
    Tipos = Table.TransformColumnTypes(Tabla, {
        {"inicio", type datetime}, {"resenas", Int64.Type}, {"negativas", Int64.Type},
        {"pct_negativo", type number}, {"celdas", Int64.Type}, {"hotspots", Int64.Type}}, "en-US")
in
    Tipos
