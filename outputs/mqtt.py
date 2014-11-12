import output
import json
import paho.mqtt.client as mqtt

class Mqtt(output.Output):
	requiredData = ["deviceid","accesstoken","hostname","port"]
	optionalData = []
	def __init__(self,data):
		self.deviceId=data["deviceid"]
		self.accessToken=data["accesstoken"]
		self.mqttServer=data["hostname"]
		self.mqttPort=data["port"]
		self.client = mqtt.Client(client_id=self.deviceId)
		self.client.username_pw_set(self.deviceId, self.accessToken)
		self.client.connect(self.mqttServer, self.mqttPort, 60)

	def outputData(self,dataPoints):
		print "Called MQTT Output."
		arr = {
			'to' : self.deviceId,
			'type' : '_command',
			'body' : dict()
		}
		for i in dataPoints:
			arr["body"][i["name"]] = i["value"]
		self.client.publish("{\"to\":\""+self.deviceId+"\"}", json.dumps(arr))
		return True
