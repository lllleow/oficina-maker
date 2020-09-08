# -*- coding: utf-8 -*-

from datetime import date, datetime
import threading
from threading import Thread
import Adafruit_DHT
import time
import random
import paho.mqtt.client as mqtt

#Ordem dos campos
# - id
# - dia
# - mes
# - ano
# - hora
# - minuto
# - latitude
# - longitude
# - temperatura

#Configuração MQTT
MQTT_BROKER = "broker.hivemq.com"

#Configurações de IO
DHT_SENSOR = Adafruit_DHT.DHT11
DHT_PIN = 4

#Id da equipe
id = "98B694A2FE8D7D01C05C30CAE5E4DC16"

#Lat e long do endereço Mal Deodoro, 2921, Alto da XV (Casa do integrante da equipe Leonardo)
lat = -25.4280223
long = -49.2441736

#Mantem a informação sobre a data da ultima atualização de localização (+- 0.05)
lastLocationUpdate = date.today()

#Mantem a informação sobre a data do ultimo envio do summary
lastSummaryUpdate = datetime.now()

#Inicia o client mqtt como nulo, inicialmente
mqttClient = None

#Mantem as informações das medições da ultima hora
lastMesuredTemps = []
lastMesuredHours = []
lastMesuredMinutes = []
lastMesuredLats = []
lastMesuredLongs = []

def start_client():
    start_mqtt_client()
    handle_data_gathering()

#Inicia o client com o id da equipe
def start_mqtt_client():
    global id, mqttClient, MQTT_BROKER

    print("Starting server")

    mqttClient = mqtt.Client("", True, None, mqtt.MQTTv31)
    mqttClient.on_connect = on_connect
    mqttClient.on_message = on_message
    mqttClient.connect(MQTT_BROKER, 1883, 120)

    thread = Thread(target = loop_mqtt)
    thread.start()
    # thread.join()

#Gerencia a repetição do gather_data
def handle_data_gathering():
    gather_data()
    #Aguarda 60 segundos e chama a função novamente
    threading.Timer(60.0, handle_data_gathering).start()

#Faz a medição e envia os dados
def gather_data():
    global lastMesuredTemps, lastMesuredHours, lastMesuredMinutes, lastMesuredLats, lastMesuredLongs, lastLocationUpdate, lastSummaryUpdate, mqttClient

    #Pega os dados do DHT11
    _, temp = Adafruit_DHT.read(DHT_SENSOR, DHT_PIN)

    #Verifica se é um dado válido
    if temp is not None:
        #Envia os dados para o servidor
        submit_minute_data(mqttClient, temp)
        
        #Se fizer mais de uma hora que enviamos o ultimo sumário, enviamos uma mensagem com o sumário dos ultimos ciclos
        if(((datetime.now() - lastSummaryUpdate).seconds / 3600) >= 1):
            submit_summary_data(mqttClient)

            lastMesuredTemps = []
            lastMesuredHours = []
            lastMesuredMinutes = []
            lastMesuredLats = []
            lastMesuredLongs = []

            lastSummaryUpdate = datetime.now()
        
        #Se o dia mudar (passar da meia noite) adicionamos uma variação na localização
        if(lastLocationUpdate != date.today()):
            lastLocationUpdate = date.today
            update_location()

        mqttClient.loop()
    else:
        print("Failed to fetch temperature")

#Adiciona a variação na localização toda vez que chegar 0h (meia noite)
def update_location():
    global lat, long
    lat += random.uniform(-0.05, 0.05)
    long += random.uniform(-0.05, 0.05)

#Envia os dados das medições minuto-por-minuto
def submit_minute_data(client, temp):
    global long, lat

    lastMesuredTemps.append(temp)
    lastMesuredHours.append(datetime.now().hour)
    lastMesuredMinutes.append(datetime.now().minute)
    lastMesuredLats.append(long)
    lastMesuredLongs.append(lat)

    body = generate_line_body(temp)

    print(body)
    mqttClient.publish(id + "/valores_intantaneos", body)
    ### Fazer a submissão MQTT

#Envia o sumário das ultimas medições
def submit_summary_data(client):
    global lastMesuredTemps, lastMesuredHours, lastMesuredMinutes, lastMesuredLats, lastMesuredLongs

    maxTemp = max(lastMesuredTemps)
    minTemp = min(lastMesuredTemps)
    avgMinute = 0 if len(lastMesuredMinutes) == 0 else sum(lastMesuredMinutes)/len(lastMesuredMinutes)
    avgHours = 0 if len(lastMesuredHours) == 0 else sum(lastMesuredHours)/len(lastMesuredHours)
    avgLat = 0 if len(lastMesuredLats) == 0 else sum(lastMesuredLats)/len(lastMesuredLats)
    avgLong = 0 if len(lastMesuredLongs) == 0 else sum(lastMesuredLongs)/len(lastMesuredLongs)

    body = str(avgHours) + "," + str(avgMinute) + "," + str(minTemp) + "," +  str(maxTemp) + "," + str(avgLat) + "," + str(avgLong)

    print(body)
    mqttClient.publish(id + "/valores_medios", body)


#Gera uma string formatada em CSV com os dados de dia e horário atual no formato e ordem acordado pelos integrantes da equipe
def generate_current_time_csv_string():
    today = datetime.now()
    return today.strftime("%d,%m,%Y,%H,%M,%S")

#Concatena as informações da leitura com os outros dados do payload
def generate_line_body(temp):
    global long, lat
    return id + "," + generate_current_time_csv_string() + "," + str(lat) + "," + str(long) + "," + str(temp)

#Função que gerencia o update do MQTT
def loop_mqtt():
    while(True):
        mqttClient.loop()

### CLIENT MQTT

def on_connect(client, userdata, flags, rc):
    print("Connect: " + " - " + userdata + " - " + flags + " - " + rc)
    client.subscribe("alerta")

def on_message(client, userdata, msg):
    print(str(msg.payload))

### INICIO DO PROGRAMA

start_client()