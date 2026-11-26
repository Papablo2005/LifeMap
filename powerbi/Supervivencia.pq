// Visual 3: curvas de Kaplan-Meier (por categoría y por zona; solo grupos con muestra suficiente)
let
    Respuesta = Json.Document(Web.Contents(BaseURL, [
        RelativePath = "survival",
        Headers = [#"ngrok-skip-browser-warning" = "1"]])),
    Columnas = {"tipo", "grupo", "nombre", "t_anios", "supervivencia", "ic_inferior", "ic_superior", "en_riesgo"},
    Tabla = if List.IsEmpty(Respuesta[resultados]) then #table(Columnas, {})
            else Table.FromRecords(Respuesta[resultados], Columnas, MissingField.UseNull),
    Tipos = Table.TransformColumnTypes(Tabla, {
        {"t_anios", type number}, {"supervivencia", type number}, {"ic_inferior", type number},
        {"ic_superior", type number}, {"en_riesgo", Int64.Type}}, "en-US")
in
    Tipos
