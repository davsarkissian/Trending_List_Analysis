import pandas as pd
from datetime import datetime
from datetime import date
from pyyoutube import Api
import numpy as np
import threading
import json
import os 


counter =0


def iterateOverFiles():
    directory = 'Data/comByVideo/'
 
# iterate over files in
# that directory
    for filename in os.listdir(directory):
        f = os.path.join(directory, filename)
        # checking if it is a file
        if os.path.isfile(f):
            print(filename)

def get_only_date(date):
    only_date = date[:10]
    date_time_obj = datetime.strptime(only_date, '%Y-%m-%d')
    print(str(date_time_obj))
    return str(date_time_obj)

def importJsonFile(video_id):
    global counter
    print(video_id)
    
    try:
        counter +=1
        com_list =[]
        with open('Data/comByVideo/{}.json'.format(video_id)) as f:
            dict_list = json.load(f)
            for js in dict_list:
                for item in js['items']:
                    comment = item['snippet']['topLevelComment']['snippet']['textDisplay']
                    date = item['snippet']['topLevelComment']['snippet']['publishedAt']
                    likes = item['snippet']['topLevelComment']['snippet']['likeCount'] 
                    author = item['snippet']['topLevelComment']['snippet']['authorDisplayName']
                    vid = yt_data.loc[yt_data.video_id == video_id]
                    """ trending_entry = vid['trending_date_only'].item()[:10]
                    if get_only_date(date) < trending_entry:
                        print('ok') """
                    keyList = {
                        "comment":comment,
                        "date":get_only_date(date),
                        "likes":likes,
                        "author":author

                    }
                    com_list.append(keyList)
        print('1')
    except:
        com_list = []
        print('2')
    print(counter)
    return com_list

    


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


    data_path = "Data/FR_youtube_trending_data.csv"
    yt_data = pd.read_csv(data_path)

    yt_data['trending_date_only'] = yt_data['trending_date'].apply(lambda x : get_only_date(x))
    yt_data.drop_duplicates(subset ="video_id", keep = 'first', inplace=True)

    """df_1,df_2,df_3,df_4,df_5,df_6,df_7,df_8 = np.array_split(yt_data,8)
    
    df_1["comment_list"] = df_1["video_id"].apply(lambda x : importJsonFile(x))
    df_1.to_csv('Data/full-yt-data1.csv')
    df_2["comment_list"] = df_2["video_id"].apply(lambda x : importJsonFile(x))
    df_2.to_csv('Data/full-yt-data2.csv')
    df_3["comment_list"] = df_3["video_id"].apply(lambda x : importJsonFile(x))
    df_3.to_csv('Data/full-yt-data3.csv')
    df_4["comment_list"] = df_4["video_id"].apply(lambda x : importJsonFile(x))
    df_4.to_csv('Data/full-yt-data4.csv')
    df_5["comment_list"] = df_5["video_id"].apply(lambda x : importJsonFile(x))
    df_5.to_csv('Data/full-yt-data5.csv')
    df_6["comment_list"] = df_6["video_id"].apply(lambda x : importJsonFile(x))
    df_6.to_csv('Data/full-yt-data6.csv')
    df_7["comment_list"] = df_7["video_id"].apply(lambda x : importJsonFile(x))
    df_7.to_csv('Data/full-yt-data7.csv')
    df_8["comment_list"] = df_8["video_id"].apply(lambda x : importJsonFile(x))
    df_8.to_csv('Data/full-yt-data8.csv') """

    """ part1 = "Data/full-yt-data1.csv"
    df_part1 = pd.read_csv(part1,low_memory=False,lineterminator='\n')
    print('ok')
    part2 = "Data/full-yt-data2.csv"
    df_part2 = pd.read_csv(part2,low_memory=False,lineterminator='\n')
    print('ok')
    part3 = "Data/full-yt-data3.csv"
    df_part3 = pd.read_csv(part3,low_memory=False,lineterminator='\n')
    print('ok')
    part4 = "Data/full-yt-data4.csv"
    df_part4 = pd.read_csv(part4,low_memory=False,lineterminator='\n')
    print('ok') """
    """ part5 = "Data/full-yt-data5.csv"
    df_part5 = pd.read_csv(part5,low_memory=False,lineterminator='\n')
    print('ok')
    part6 = "Data/full-yt-data6.csv"
    df_part6 = pd.read_csv(part6,low_memory=False,lineterminator='\n')
    print('ok') """
    """ part1 = "Data/full_yt_data.csv"
    df1 = pd.read_csv(part1,low_memory=False,lineterminator='\n')
    print('ok')
    part2 = "Data/full_yt_data1.csv"
    df2 = pd.read_csv(part2,low_memory=False,lineterminator='\n')
    print('ok')

    frame = [df1,df2]
    print('1')
    full_YT_data = pd.concat(frame)

    print('2')

    full_YT_data.to_csv('Data/FULL_YT_DATA.csv') """

    