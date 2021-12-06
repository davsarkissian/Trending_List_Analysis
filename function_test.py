import pandas as pd
import datetime
import json

def weekday(date):
    date = date.split('T')
    date = date[0].split('-')
    date = list(map(lambda x : int(x), date))
    x = datetime.datetime(date[0], date[1], date[2])
    return x.strftime("%w")

def oneDay(date):
    date = date.split('T')
    return date[0]

def listOfCategory():
    with open('FR_category_id.json') as json_data:
        data_dict = json.load(json_data)
    listCategory = list()
    for i in range(len(data_dict["items"])):
        id = data_dict["items"][i]["id"]
        name = data_dict["items"][i]['snippet']['title']
        listCategory += [[int(id), name]]
    return listCategory

def nameCategory(id):
    try :
        listCategory = listOfCategory()
        filter_object = list(filter(lambda a: id in a, listCategory))
        filter_object = filter_object[0][1]
    except:
        filter_object = None
    return filter_object


