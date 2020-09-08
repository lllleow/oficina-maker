# -*- coding: utf-8 -*-
import paho.mqtt.client as mqtt
import time

#Configuração MQTT
id = "98B694A2FE8D7D01C05C30CAE5E4DC16"
def start_client():
    print(generate_csv_header())
    start_mqtt_client()

#Inicia o client com o id da equipe
def start_mqtt_client():
    mqttClient = mqtt.Client("", True, None, mqtt.MQTTv31)
    mqttClient.on_connect = on_connect
    mqttClient.on_message = on_message
    mqttClient.connect("broker.hivemq.com", 1883, 120)
    
    while(True):
        mqttClient.loop()

#Gera o header do CSV
def generate_csv_header():
    return "id,dia,mes,ano,hora,minuto,latitude,longitude,temperatura"

### CLIENT MQTT

def on_connect(client, userdata, flags, rc):
    print("Connected")
    client.subscribe(id + "/valores_intantaneos")
    client.subscribe(id + "/valores_medios")

def on_message(client, userdata, msg):
    payload = str(msg.payload.decode("utf-8"))

    if(msg.topic == id + "/valores_intantaneos"):
        with open('instantaneos.csv', 'a') as file:
            file.write(payload + "\n")
    elif(msg.topic == id + "/valores_medios"):
        with open('medios.csv', 'a') as file:
            file.write(payload + "\n")
    else:
        print(msg.topic)
        with open('others.txt', 'a') as file:
            file.write("["+msg.topic+"] -> " + payload + "\n")

    print(payload)

### INICIO DO PROGRAMA

start_client()