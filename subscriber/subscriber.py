import paho.mqtt.client as mqtt
import mysql.connector
import json
import requests

db = mysql.connector.connect(
    host="localhost",
    user="root",
    password="root",
    database="iot_project"
)
cursor = db.cursor()

API_KEY = "UFSWFN21ZQDRM76S"

def on_connect(client, userdata, flags, reason_code, properties):
    print("Connected to MQTT Broker")
    client.subscribe("env/data")
    print("Subscribed to topic: env/data")

def on_message(client, userdata, msg):
    data = json.loads(msg.payload.decode())

    temperature = data["temperature"]
    humidity = data["humidity"]
    gas = data["gas"]

    print("Data Received:")
    print(f"Temperature : {temperature} °C")
    print(f"Humidity    : {humidity} %")
    print(f"Gas Level   : {gas}")
    print("-" * 30)

    cursor.execute(
        "INSERT INTO sensor_data (temperature, humidity, gas) VALUES (%s, %s, %s)",
        (temperature, humidity, gas)
    )
    db.commit()

    requests.get(
        "https://api.thingspeak.com/update",
        params={
            "api_key": API_KEY,
            "field1": temperature,
            "field2": humidity,
            "field3": gas
        }
    )

client = mqtt.Client(
    callback_api_version=mqtt.CallbackAPIVersion.VERSION2
)

client.on_connect = on_connect
client.on_message = on_message

client.connect("test.mosquitto.org", 1883, 60)
client.loop_forever()