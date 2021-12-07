import pandas as pd
from datetime import datetime
from datetime import date
from pyyoutube import Api
import numpy as np
import threading


data_path = "Data/FR_youtube_trending_data_copie.csv"
yt_data = pd.read_csv(data_path)





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
    return com_list


def process1(df_1):
    df_1['com_list'] = df_1['video_id'].apply(lambda x: get_all_comments_from_video(x))
    df_1.to_csv(index=False)
    
def process2(df_2):
    df_2['com_list'] = df_2['video_id'].apply(lambda x: get_all_comments_from_video(x))
    df_2.to_csv(index=False)

def process3(df_3):
    df_3['com_list'] = df_3['video_id'].apply(lambda x: get_all_comments_from_video(x))
    df_3.to_csv(index=False)

def process4(df_4):
    df_4['com_list'] = df_4['video_id'].apply(lambda x: get_all_comments_from_video(x))
    df_4.to_csv(index=False)



if __name__ == '__main__':
    
    yt_data['trending_date_only'] = yt_data['trending_date'].apply(lambda x : get_only_date(x))
    yt_data.drop_duplicates(subset ="video_id", keep = 'first', inplace=True)

    df_1,df_2,df_3,df_4 = np.array_split(yt_data,4)



    api = Api(api_key="AIzaSyB1yzx6LfVXAfCEcpT8IexkHKIt1s7mfRg")
    print('debut de process')
    """
    video_id = 'AcBd_RH9JSw'
    vid_no_com = 'SExxIJcLk-Y'
    res = get_all_comments_from_video(video_id,df_1)
    print(res)
    print(len(res))
    """
    th1 = threading.Thread(target=process1(df_1))
    th2 = threading.Thread(target=process2(df_2))
    th3 = threading.Thread(target=process3(df_3))
    th4 = threading.Thread(target=process4(df_4))


    th1.start()
    th2.start()
    th3.start()
    th4.start()

    th1.join()
    th2.join()
    th3.join()
    th4.join()

    
    
    
    
    print('done')