from datetime import datetime, timezone
from flask import Flask, Response, jsonify, request
from flask_sqlalchemy import SQLAlchemy
import json 
import paho.mqtt.client as mqtt

# pip install paho-mqtt flask -> Conexão com os sensores

# CONEXÃO COM O BANCO DE DADOS

# Nome da Aplicação
app = Flask("registro")

# Configura o SQLALCHEMY para rastrear modificações
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

# Configuração do banco de dados
app.config['SQL_ALCHEMY_DATABASE_URI'] = 'mysql://root:senai%40134@127.0.0.1/bd_medidor'

#Cria uma instância do SQLALCHEMY, passando a aplicação FLask como parâmetro
mybd = SQLAlchemy(app)

# CONEXÃO DOS SENSORES
mqtt_dados = {}

def conexao_sensor(cliente, rc):
    cliente.subscribe("projeto_integrado/SENAI134/Cienciadedados/GrupoX")

def msg_sensor(msg):
    global mqtt_dados

    # Decodificar a mensagem recebida de bytes para string
    valor = msg.payload.decode('utf-8')

    # Decodificar de string para JSON
    mqtt_dados = json.loads(valor)

    print(f"Mensagem Recebida: {mqtt_dados}")

    # Correlaçao Banco de Dados com Sensores
    with app.app_context():
        try:
            temperatura = mqtt_dados.get('temperature')
            pressao = mqtt_dados.get('pressure')
            altitude = mqtt_dados.get('altitude')
            umidade = mqtt_dados.get('humidity')
            co2 = mqtt_dados.get('co2')
            poeria = 0
            tempo_registro = mqtt_dados.get('timestamps')

            if tempo_registro is None:
                print("TimeStamp não encontrado")
                return 
            
            try:
                tempo_oficial = datetime.fromtimestamp(int
                (tempo_registro), tz=timezone.utc)

            except (ValueError, TypeError) as e:
                print(f"Erro ao converter timestamp: {str(e)}")
                return
            
            # Criar o objeto que vai simular a tabela do banco
            novos_dados = Registro(
                temperaturaV = temperatura,
                pressaoV = pressao,
                altitudeV = altitude,
                umidadeV = umidade,
                co2V = co2,
                poeiraV = poeria,
                tempo_registroV = tempo_oficial
            )
            
            # Adicionar novo registro ao banco

            mybd.session.add(novos_dados)
            mybd.session.commit()
            print("Dados foram inseridos com sucesso no banco de dados !")

        except Exception as e:
            print(f"Erro ao processar os dados do MQTT: {str(e)}")
            mybd.session.rollback()

mqtt_client = mqtt.Client()
mqtt_client.on_connect = conexao_sensor
mqtt_client.on_message = msg_sensor
mqtt_client.connect("test.mosquitto.org", 1883, 60)

def start_mqtt():
    mqtt_client.loop_start()