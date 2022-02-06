from googleapiclient.discovery import build
import json
import pandas as pd
import numpy as np
import threading
from datetime import datetime
from datetime import date

data_path = "Data/FR_youtube_trending_data_copie.csv"
yt_data = pd.read_csv(data_path)


api_key="AIzaSyCfoWStv5meUynmjpqNjwDUBMH9NTY7FSY"
video_id = 'JVm8P6kKgD0'
#video_id="P3IkBxra3a8"
youtube = build('youtube','v3',developerKey=api_key)

def get_only_date(date):
    only_date = date[:10]
    date_time_obj = datetime.strptime(only_date, '%Y-%m-%d')
    return str(date_time_obj)

def get_comment_threads(youtube,**kwargs):
    
    try:


        token_list = []
        pages_com = []
        result = youtube.commentThreads().list(**kwargs).execute()
        pages_com.append(result)

        while result:

            if 'nextPageToken' in result:
                print(result['nextPageToken'])
                token_list.append(result['nextPageToken'])
                kwargs['pageToken'] = result['nextPageToken']
                result = youtube.commentThreads().list(**kwargs).execute()
                pages_com.append(result)
                
                if len(token_list) == 14
                :
                    break
                    
            else:
                break 

        
        json_file = open("./Data/comByVideo/%s.json" % video_id, "w",encoding='utf-8')
        json.dump(pages_com, json_file,ensure_ascii=False)
        json_file.close()

    except:
        pages_com=[]
        json_file = open("./Data/comByVideo/%s.json" % video_id, "w",encoding='utf-8')
        json.dump(pages_com, json_file ,ensure_ascii=False)
        json_file.close()


if __name__ == '__main__':

    api_key="AIzaSyCfoWStv5meUynmjpqNjwDUBMH9NTY7FSY"
    video_id = 'JVm8P6kKgD0'
    #video_id="P3IkBxra3a8"


    #get_comment_threads(youtube,part='snippet',videoId=video_id,maxResults=100,order="time")

    yt_data.drop_duplicates(subset ="video_id", keep = 'first', inplace=True)
    yt_data['trending_date_only'] = yt_data['trending_date'].apply(lambda x : get_only_date(x))

    df_1,df_2,df_3,df_4,df_5,df_6,df_7,df_8 = np.array_split(yt_data,8)

    print(df_1.info)





