
Technical Overview

As per the given secenirio below are the requirments 

1- In IOT project to processing the sensores data (10 a minut) 
2- Calculating the angle as per the input
3- Storing the data in storage service

To handle real time data and process it to the GCP I have used below architecture and required services

a- Clound IOT core: To enforce structured handling of sensor devices' security keys and metadata, and secured delivery of measurement data between sensors and cloud.
b- Cloud Pubsub: For sending messages to the cloud dataflow once data is available in IOT core.
c- Clound Data Flow: For processing the input time and calculating angle corresponding to input time.
d- Google Big Query: To store staginging/processing data
e- Data Studio: Further for reporting purpose.

Prerequisites and setup 

1- Setup the GCP project 
2- Enable the APIs (IOTcore, Dataflow)
3- Create a pubsub topic for messaging between IOT core and dataflow.
4- Configure the IOT core, pass the pubsub topic name to IOT core device registry.
5- Deploy your dataflow pipeline.
