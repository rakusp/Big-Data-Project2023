import json
import java.io
from org.apache.commons.io import IOUtils
from java.nio.charset import StandardCharsets
from org.apache.nifi.processor.io import StreamCallback

class ModJSON(StreamCallback):
    def __init__(self):
	pass
    def process(self, inputStream, outputStream):
	text = IOUtils.toString(inputStream, StandardCharsets.UTF_8)
	obj = json.loads(text)
	coords = obj['geometry.coordinates']
	if type(coords[0]) is list:
	    coord = coords[int(len(coords) // 2)]
	else:
	    coord = coords
	newObj = {
            "id": obj['properties.id'],
            "iconCat": obj['properties.iconCategory'],
	    "magnitudeOfDelay": obj['properties.magnitudeOfDelay'],
	    "start": obj['properties.startTime'],
	    "end": obj['properties.endTime'],
	    "lon": coord[0],
	    "lat": coord[1],
	    "city": obj['city']
        }
   	outputStream.write(bytearray(json.dumps(newObj, indent=4).encode('utf-8')))

flowFile = session.get()
if (flowFile != None):
    flowFile = session.write(flowFile, ModJSON())
session.transfer(flowFile, REL_SUCCESS)
session.commit()
