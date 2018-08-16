import datetime, time
from influxdb import InfluxDBClient
import xml.etree.ElementTree as et
from influxdb import SeriesHelper
from pykml.parser import Schema
from pykml import parser
import json,re

myclient = InfluxDBClient('127.0.0.1', 8086, 'root', 'root', 'kml1')
myclient.create_database('kml1')
class MySeriesHelper(SeriesHelper):
    
    #"""Instantiate SeriesHelper to write points to the backend."""

    class Meta:
        #"""Meta class stores time series helper configuration."""

        # The client should be an instance of InfluxDBClient.
        client = myclient

        # The series name must be a string. Add dependent fields/tags
        # in curly brackets.
        series_name = 'kml_data'

        # Defines all the fields in this time series.
        fields = ['Time','Type','ObjectId','HashValue','SourceId', 'DestId','TTLValue','Text','File', 'Latitude','Longitude']

        # Defines all the tags for the series.
        tags = []

        # Defines the number of data points to store prior to writing
        # on the wire.
        bulk_size = 0

        # autocommit must be set to True when using bulk_size
        autocommit = True

def parse(path):
    i=0       
    showtime = datetime.datetime.utcnow()
    if path.endswith('.kml'):
        content = open(path,'r')
        doc=parser.parse(content)
        #print(doc)
        root=doc.getroot()
##		pl_data=[]

##		for plmark in root.iter('{http://www.opengis.net/kml/2.2}Placemark'):
##			pl_data.append(plmark.find('{http://www.opengis.net/kml/2.2}name').text)

        for extdata in root.iter('{http://www.opengis.net/kml/2.2}Data'):    
            if extdata.get('name')!='total':
                src_data=extdata.find('{http://www.opengis.net/kml/2.2}value').text
                src_data=re.split('-|_',src_data)
                #print(len(src_data))
                if len(src_data)>6 and src_data[1]=='text':
                    i=4
                    while i<=(len(src_data)-3):
                        src_data[3]=src_data[3] + "-" + src_data[i]
                        del src_data[i]
                elif len(src_data)>10:
                    i=8
                    while i<=(len(src_data)-3):
                        #print(i)
                        src_data[7]=src_data[7] + "-" + src_data[i]
                        del src_data[i]                
                if len(src_data)==10:
                    MySeriesHelper(Time=src_data[0],Type=src_data[1],ObjectId=src_data[2],HashValue=src_data[3],
                                                   SourceId=src_data[4],DestId=src_data[5],TTLValue=src_data[6],Text='',File=src_data[-3],Latitude=src_data[-2],Longitude=src_data[-1])
                elif len(src_data)==6:
                    MySeriesHelper(Time=src_data[0],Type=src_data[1],ObjectId=src_data[2],HashValue='',
                                                   SourceId='',DestId='',TTLValue='',Text=src_data[-3],File='',Latitude=src_data[-2],Longitude=src_data[-1])

                else:
                    print("KML dosen't follow schema")
                    
##        result = myclient.query('select * from kml_data;')
##        print(result)
            
    else:
        print("Not a KML file")

##path='1234.kml'
##parse(path)
