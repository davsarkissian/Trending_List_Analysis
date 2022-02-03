from googleapiclient.discovery import build

api_key="AIzaSyAW2z0U9ThLuPIFMOvaPW599sT4_iI3Bhc"
video_id = 'JVm8P6kKgD0'
youtube = build('youtube','v3',developerKey=api_key)

def get_comment_threads(youtube,**kwargs):
    
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
        else:
            break 
    print(len(token_list))
    return pages_com

res = get_comment_threads(youtube,part='snippet',videoId=video_id,maxResults=100)
print(res)







#part='snippet',videoId=video_id,maxResults=100