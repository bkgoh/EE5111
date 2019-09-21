import datetime #for timestamp
import time     #for sleep
import json     #to construct message
from AWSIoTPythonSDK.MQTTLib import AWSIoTMQTTShadowClient
            
# A random programmatic shadow client ID.
SHADOW_CLIENT = "myShadowClient"

# The unique hostname that &IoT; generated for 
# this device.
HOST_NAME = "a1jdbkpe0sdaq2-ats.iot.ap-southeast-1.amazonaws.com"

# The relative path to the correct root CA file for &IoT;, 
# which you have already saved onto this device.
ROOT_CA = "AmazonRootCA1.pem"

# The relative path to your private key file that 
# &IoT; generated for this device, which you 
# have already saved onto this device.
PRIVATE_KEY = "a80693860e-private.pem.key"

# The relative path to your certificate file that 
# &IoT; generated for this device, which you 
# have already saved onto this device.
CERT_FILE = "a80693860e-certificate.pem.crt"

# A programmatic shadow handler name prefix.
SHADOW_HANDLER = "A0169335X_FD001"

# Automatically called whenever the shadow is updated.
def myShadowUpdateCallback(payload, responseStatus, token):
  print()
  print('UPDATE: $aws/things/' + SHADOW_HANDLER + 
    '/shadow/update/#')
  print("payload = " + payload)
  print("responseStatus = " + responseStatus)
  print("token = " + token)

# Create, configure, and connect a shadow client.
myShadowClient = AWSIoTMQTTShadowClient(SHADOW_CLIENT)
myShadowClient.configureEndpoint(HOST_NAME, 8883)
myShadowClient.configureCredentials(ROOT_CA, PRIVATE_KEY,
  CERT_FILE)
myShadowClient.configureConnectDisconnectTimeout(10)
myShadowClient.configureMQTTOperationTimeout(5)
myShadowClient.connect()

# Create a programmatic representation of the shadow.
myDeviceShadow = myShadowClient.createShadowHandlerWithName(
  SHADOW_HANDLER, True)

# =============================================================================

#open file handler
file = open("train_FD001.txt", "r") #open file handler

# Create a list of headers describing the data in file
oslist = list()
for x in range(3):
    oslist.append("os" + str(x+1))
    
sensorlist = list()
for x in range(21):
    sensorlist.append("sensor" + str(x+1))
    
headerlist = ["id","timestamp","matric_no.","cycle"]
headerlist.extend(oslist)
headerlist.extend(sensorlist)

for line in file:           #Loop for every line in file
    data = line.split()     #split string data in line into list of elements
    data[0] = "FD001_" + data[0]    #Overwrite 'id' as 'FD001_'+'id'
    data.insert(1,str(datetime.datetime.utcnow()))  #Add timestamp
    data.insert(2,"A0169335X")                      #Add Matriculation number    
    
    #create dictionary using headerlist and data
    DataDict = dict(zip(headerlist, data))  
    
    #convert DataDict into json format
    JsonMsg = json.dumps(DataDict)

    #Add 'state' and 'reported' nodes         
    AWSMsg = str('{"state":{"reported":') + JsonMsg + str("}}")
    
    #Perform shadowUpdate
    myDeviceShadow.shadowUpdate(AWSMsg,myShadowUpdateCallback, 5)
    
    #Delay before next line is run
    time.sleep(10)

file.close()



