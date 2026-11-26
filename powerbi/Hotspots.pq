// Visual 1: mapa de hotspots (última ventana de 30 días por celda de geohash)
let
    Respuesta = Json.Document(Web.Contents(BaseURL, [
        RelativePath = "live/hotspots",
        Query = [lat = "38.627", lon = "-90.199", radio = "60000", ventana = "ultima"],
        Headers = [#"ngrok-skip-browser-warning" = "1"]])),
    Columnas = {"geohash", "inicio", "fin", "resenas", "negativas", "pct_negativo", "pct_negativo_anterior",
                "delta", "hotspot", "lat", "lon"},
    Tabla = if List.IsEmpty(Respuesta[resultados]) then #table(Columnas, {})
            else Table.FromRecords(Respuesta[resultados], Columnas, MissingField.UseNull),
    Tipos = Table.TransformColumnTypes(Tabla, {
        {"inicio", type datetime}, {"fin", type datetime}, {"resenas", Int64.Type}, {"negativas", Int64.Type},
        {"pct_negativo", type number}, {"pct_negativo_anterior", type number}, {"delta", type number},
        {"hotspot", type logical}, {"lat", type number}, {"lon", type number}}, "en-US"),
    Estado = Table.AddColumn(Tipos, "estado", each if [hotspot] then "Hotspot" else "Normal", type text)
in
    Estado
