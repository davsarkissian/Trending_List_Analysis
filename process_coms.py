import pandas as pd
from datetime import datetime
from datetime import date
from pyyoutube import Api
import numpy as np
import threading
import json
import os 

data_path = "Data/FR_youtube_trending_data_copie.csv"
yt_data = pd.read_csv(data_path)



def iterateOverFiles():
    directory = 'Data/comByVideo/'
 
# iterate over files in
# that directory
    for filename in os.listdir(directory):
        f = os.path.join(directory, filename)
        # checking if it is a file
        if os.path.isfile(f):
            print(filename)

def importJsonFile():
    
    with open('Data/comByVideo/__VnP2s-Hnc.json') as f:
        d = json.load(f)
    return d


def get_only_date(date):
    only_date = date[:10]
    date_time_obj = datetime.strptime(only_date, '%Y-%m-%d')
    return str(date_time_obj)


def get_all_comments_from_video(video_id):
    
    try:
        com_list=[]
        
        ct_by_video = api.get_comment_threads(video_id=video_id,count=None)
        comment_dict = ct_by_video.to_dict()

        for item in comment_dict['items']:
            comment = item['snippet']['topLevelComment']['snippet']['textDisplay']
            date = item['snippet']['topLevelComment']['snippet']['publishedAt']
            likes = item['snippet']['topLevelComment']['snippet']['likeCount'] 
            author = item['snippet']['topLevelComment']['snippet']['authorDisplayName']
            vid = yt_data.loc[yt_data.video_id == video_id]
            trending_entry = vid['trending_date_only'][1]
            if get_only_date(date) < trending_entry:
                keyList = {
                    "comment":comment,
                    "date":date,
                    "likes":likes

                }
                com_list.append(keyList)
    except:
        com_list = []
    """
    try: 
        
        commentThread_json = api.get_comment_threads(video_id=video_id,count=None)
        com_list = commentThread_json.to_dict()
    except:
        com_list = {}

    """
    return com_list

def comsToJsonFile(video_id):

    try: 
        
        commentThread_json = api.get_comment_threads(video_id=video_id,count=1500)

        com_list = commentThread_json.to_dict()

        json_file = open("./Data/comByVideo/%s.json" % video_id, "w",encoding='utf-8')
        json.dump(com_list, json_file,ensure_ascii=False)
        json_file.close()

    except:
        com_list = {}
        json_file = open("./Data/comByVideo/%s.json" % video_id, "w",encoding='utf-8')
        json.dump(com_list, json_file ,ensure_ascii=False)
        json_file.close()


def process1(df_1):
    df_1['com_list'] = df_1['video_id'].apply(lambda x: get_all_comments_from_video(x))
    df_1.to_csv('df_first_part.csv')
    
def process2(df_2):
    df_2['com_list'] = df_2['video_id'].apply(lambda x: get_all_comments_from_video(x))
    df_2.to_csv('df_second_part.csv')

def process3(df_3):
    df_3['video_id'].apply(lambda x: comsToJsonFile(x))

def process4(df_4):
    df_4['video_id'].apply(lambda x: comsToJsonFile(x))



if __name__ == '__main__':
    '''
    yt_data['trending_date_only'] = yt_data['trending_date'].apply(lambda x : get_only_date(x))
    yt_data.drop_duplicates(subset ="video_id", keep = 'first', inplace=True)

    df_1,df_2,df_3,df_4,df_5,df_6,df_7,df_8 = np.array_split(yt_data,8)
#mettre chaque fichier dans un csv

#enregistrer les com thread dans des json localement (nom du json = id video) et esuite integrer au csv

    '''    
    iterateOverFiles()
    