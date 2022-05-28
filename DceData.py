import configparser
from requests import Session
from requests.auth import HTTPBasicAuth
from zeep import Client
from zeep.transports import Transport
from pandas import DataFrame

config = configparser.ConfigParser()
config.read('config.ini')

deviceWSDL = config.get('DCE','DeviceWSDL')
sensorWSDL = config.get('DCE','SensorWSDL')
dceUsername = config.get('DCE','Username')
dcePassword = config.get('DCE','Password')
dceTimeout = int(config.get('DCE','Timeout'))
outputFile = config.get('DEFAULT','OutputFile')

devices = []
devIds = []
sensors = {}
points = []

session = Session()
session.auth = HTTPBasicAuth(dceUsername, dcePassword)
devicesClient = Client(deviceWSDL,
                       transport=Transport(session=session, timeout=dceTimeout))
sensorsClient = Client(sensorWSDL,
                       transport=Transport(session=session, timeout=dceTimeout))

devicesFactory = devicesClient.type_factory('ns1')

sensorsFactory = sensorsClient.type_factory('ns2')
sensorType = sensorsFactory.ISXCSensor()
dataType = sensorsFactory.ISXCSensorData()

allDevicesResponse = devicesClient.service.getAllDevices()
for device in allDevicesResponse:
    tmpDev = devicesFactory.ISXCNamedElement(device)
    deviceId = tmpDev.ISXCElement.ISXCNamedElement.ISXCElement.id
    deviceName = tmpDev.ISXCElement.ISXCNamedElement.name
    deviceHostname = tmpDev.ISXCElement.hostName
    tmpDevices = {
        'deviceId': deviceId,
        'deviceName': deviceName,
        'deviceHostname': deviceHostname
    }
    devices.append(tmpDevices)

for dev in devices:
    sensorsOfDevicesResponse = sensorsClient.service.getSensorsForDevice(dev['deviceId'])
    if sensorsOfDevicesResponse is not None:
        sensorIdListArray = []
        for sens in sensorsOfDevicesResponse:
            tmpSens = sensorsFactory.ISXCNamedElement(sens)
            sensorId = tmpSens.ISXCElement.ISXCNamedElement.ISXCElement.id
            sensorName = tmpSens.ISXCElement.ISXCNamedElement.name
            sensorType = tmpSens.ISXCElement.ISXCSensorType
            sensorIdListArray.append(str(sensorId))
            tmpSensor = {
                'deviceId': dev['deviceId'],
                'deviceName': dev['deviceName'],
                'deviceHostname': dev['deviceHostname'],
                'sensorId': sensorId,
                'sensorName': sensorName,
                'sensorType': sensorType
            }
            sensors[sensorId] = tmpSensor
    sensorDataResponse = sensorsClient.service.getMultipleSensorData(ArrayOfISXCElementID=sensorIdListArray)

    for data in sensorDataResponse:
        tmpData = sensorsFactory.ISXCNamedElement(data)
        sensors[tmpData.ISXCElement.ISXCElementID]['sensorDataTimestamp'] = tmpData.ISXCElement.ISXCSensorData.timeStamp
        sensors[tmpData.ISXCElement.ISXCElementID]['sensorDataValue'] = tmpData.ISXCElement.ISXCSensorData.value
        sensors[tmpData.ISXCElement.ISXCElementID]['sensorDataUnit'] = tmpData.ISXCElement.ISXCSensorData.units
        sensors[tmpData.ISXCElement.ISXCElementID]['sensorDataType'] = tmpData.ISXCElement.ISXCSensorData.ISXCValueType

DataFrame(sensors).to_csv(outputFile, index_label= 'index')