#include <WiFi.h>
#include <ArduinoMqttClient.h>
#include <DHT.h>

const char* ssid = "SUNBEAM";
const char* password = "1234567890";

const char* broker = "test.mosquitto.org";
int port = 1883;
const char* topic = "sensor_data";

WiFiClient wifiClient;
MqttClient mqttClient(wifiClient);

#define DHT_PIN 4
#define DHT_TYPE DHT11
#define MQ2_PIN 34

DHT dht(DHT_PIN, DHT_TYPE);

void setup() {
  Serial.begin(115200);
  dht.begin();

  WiFi.begin(ssid, password);
  while (WiFi.status() != WL_CONNECTED) {
    delay(500);
    Serial.print(".");
  }
  Serial.println("\nWiFi connected");

  mqttClient.connect(broker, port);
  Serial.println("MQTT connected");
}

void loop() {
  float temperature = dht.readTemperature();
  float humidity = dht.readHumidity();
  int gas = analogRead(MQ2_PIN);

  if (isnan(temperature) || isnan(humidity)) {
    Serial.println("Failed to read DHT sensor");
    return;
  }

  String payload = "{";
  payload += "\"temperature\":" + String(temperature) + ",";
  payload += "\"humidity\":" + String(humidity) + ",";
  payload += "\"gas\":" + String(gas);
  payload += "}";

  mqttClient.beginMessage(topic);
  mqttClient.print(payload);
  mqttClient.endMessage();

  Serial.println(payload);
  delay(5000);
}