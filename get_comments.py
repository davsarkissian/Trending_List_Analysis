from googleapiclient.discovery import build
import json
import pandas as pd
import numpy as np
import threading
from datetime import datetime
from datetime import date

data_path = "Data/FR_youtube_trending_data_copie.csv"
yt_data = pd.read_csv(data_path)

def drop_duplicates(df):
    df.drop_duplicates(subset ="video_id", keep = 'first', inplace=True)
    return df

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
                #print(result['nextPageToken'])
                token_list.append(result['nextPageToken'])
                kwargs['pageToken'] = result['nextPageToken']
                result = youtube.commentThreads().list(**kwargs).execute()
                pages_com.append(result)
                
                if len(token_list) == 14:
                    break
                    
            else:
                break 

        
        json_file = open("./Data/comByVideo/%s.json" % kwargs['videoId'], "w",encoding='utf-8')
        json.dump(pages_com, json_file,ensure_ascii=False)
        json_file.close()

    except:
        pages_com=[]
        json_file = open("./Data/comByVideo/%s.json" % kwargs['videoId'], "w",encoding='utf-8')
        json.dump(pages_com, json_file ,ensure_ascii=False)
        json_file.close()

def process1(df,apiKey):
    youtube = build('youtube','v3',developerKey=apiKey)
    df['video_id'].apply(lambda x : get_comment_threads(youtube,part='snippet',videoId=x,maxResults=100,order="time"))

def process2(df,apiKey):
    youtube = build('youtube','v3',developerKey=apiKey)
    df['video_id'].apply(lambda x : get_comment_threads(youtube,part='snippet',videoId=x,maxResults=100,order="time"))

def process3(df,apiKey):
    youtube = build('youtube','v3',developerKey=apiKey)
    df['video_id'].apply(lambda x : get_comment_threads(youtube,part='snippet',videoId=x,maxResults=100,order="time"))



if __name__ == '__main__':

    apiKeyList = ['AIzaSyCfoWStv5meUynmjpqNjwDUBMH9NTY7FSY',
    'AIzaSyAW2z0U9ThLuPIFMOvaPW599sT4_iI3Bhc',
    'AIzaSyBdA2vjCTTDgKqd6g4-yiFz0rYkPn-nt0Q',
    'AIzaSyA7lnlOS2BRknpfPhm7pez5N7vHyQRFmuI',
    'AIzaSyB1yzx6LfVXAfCEcpT8IexkHKIt1s7mfRg',
    'AIzaSyDtKh18qPOAQSrhJRHX5H3dH9_C1PAVBDY',
    'AIzaSyC91f5S1U0zxL1JhyxwlU-J-uLnz7cUono',
    'AIzaSyCgKuOL808Jm1XCIL9aV5KUtRLnPWs95oo',
    'AIzaSyDQzm5SA20wgYZlTuMYfDwQmgPeqai4vsI',
    'AIzaSyCHTNerORYz0ObrOu4LfmZEOKYPRJ0YDy8',
    ]

    #youtube = build('youtube','v3',developerKey=apiKeyList[6])

    yt_data = drop_duplicates(yt_data)
    
    #api_key="AIzaSyCfoWStv5meUynmjpqNjwDUBMH9NTY7FSY"
    #video_id = '56HGTFZabxA'
    #video_id="P3IkBxra3a8"

    '''
    get_comment_threads(youtube,part='snippet',videoId=video_id,maxResults=100,order="time")
    
    yt_data.drop_duplicates(subset ="video_id", keep = 'first', inplace=True)
    yt_data['trending_date_only'] = yt_data['trending_date'].apply(lambda x : get_only_date(x))
    '''
    df_1,df_2,df_3,df_4,df_5,df_6,df_7,df_8,df_9,df_10,df_11,df_12,df_13,df_14,df_15,df_16,df_17,df_18,df_19,df_20,df_21,df_22,df_23,df_24,df_25,df_26,df_27,df_28,df_29,df_30 = np.array_split(yt_data,30)
    
    #df_7['video_id'].apply(lambda x : get_comment_threads(youtube,part='snippet',videoId=x,maxResults=100,order="time"))

    th1 = threading.Thread(target=process1(df_8,apiKeyList[7]))
    th2 = threading.Thread(target=process2(df_9,apiKeyList[8]))
    th3 = threading.Thread(target=process3(df_10,apiKeyList[9]))

    th1.start()
    th2.start()
    th3.start()

    th1.join()
    th2.join()
    th3.join()

# sur chaque projet je peux traiter 600 video par jour
#sur 10 projet on peut faire une recup sur 6000 videos sur 20 projet 12000 videos
#il faudrait faire 30 sous data set de 600 videos chacun

