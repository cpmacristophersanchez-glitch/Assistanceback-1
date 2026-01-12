from flask import Flask, request, jsonify
from flask_cors import CORS
from asociar import asociar_empleados, subir_a_mongo
from leerAlumnos import leer_alumnos
from login import verificar_usuario
from subirPromedio import subir_promedio_mongo
app= Flask(__name__)
app.config['JSON_AS_ASCII'] = False
CORS(app, origins=["https://localhost:3000"])
@app.route("/", methods=["GET"])
def index():
    return jsonify({"message": "Hello World"})
@app.route("/login", methods=["POST"])
def login():
    data = request.get_json()
    username = data.get("username")
    password = data.get("password")
    if verificar_usuario(username, password):
        return jsonify({"message": "Login exitoso"}), 200
    else:
        return jsonify({"message": "Credenciales inválidas"}), 401
    
@app.route("/subir", methods=["POST"])
def subir():
    file = request.files.get("file")
    if not file:
        return jsonify({"error": "No se envió ningún archivo"}), 400
    data, mes_ano = asociar_empleados(file, file.filename)
    
    subir_a_mongo(data, mes_ano)
    return jsonify({"message": f"Datos subidos para {mes_ano}"}), 200

@app.route("/leeralumnos", methods=["GET"])
def leer():
    alumnos = leer_alumnos()
    return jsonify(alumnos)
@app.route("/subirpromedio", methods=["POST"])
def subir_promedio():
    data = request.get_json()
    persona= data.get("persona")
    promedio= data.get("promedio")
    subir_promedio_mongo(persona, promedio)
    return jsonify({"message": f"Promedio de {persona} subido: {promedio}"}), 200

if __name__ == "__main__":
    app.run(debug=True)
