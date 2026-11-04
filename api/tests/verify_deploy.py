# archivo: api/tests/verify_deploy.py
"""Tras desplegar: comprueba que /health informa el commit que Jenkins acaba de construir.
Uso (dentro del contenedor api):  ESPERADO=<hash> python tests/verify_deploy.py"""
import json
import os
import sys
import time
import urllib.error
import urllib.request

esperado = os.environ["ESPERADO"]
for _ in range(30):
    try:
        try:
            r = urllib.request.urlopen("http://localhost:5000/health", timeout=10)
            cuerpo = json.loads(r.read())
        except urllib.error.HTTPError as e:           # 503 también trae el JSON con el commit
            cuerpo = json.loads(e.read())
        break
    except Exception:
        time.sleep(2)
else:
    sys.exit("La API no respondió tras el despliegue")
print(json.dumps(cuerpo, indent=2, ensure_ascii=False))
if cuerpo.get("commit") != esperado:
    sys.exit(f"El commit desplegado ({cuerpo.get('commit')}) no coincide con el esperado ({esperado})")
print("OK: el commit desplegado coincide con el de Jenkins")
