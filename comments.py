#import os

import googleapiclient.discovery

def main(video_id):
    # Disable OAuthlib's HTTPS verification when running locally.
    # *DO NOT* leave this option enabled in production.
    
    #os.environ["OAUTHLIB_INSECURE_TRANSPORT"] = "1"

    api_service_name = "youtube"
    api_version = "v3"
    DEVELOPER_KEY = "AIzaSyAW2z0U9ThLuPIFMOvaPW599sT4_iI3Bhc"

    youtube = googleapiclient.discovery.build(
        api_service_name, api_version, developerKey = DEVELOPER_KEY)

    request = youtube.commentThreads().list(
        part="snippet",
        maxResults=100,
        moderationStatus="published",
        order="time",
        textFormat="plainText",
        videoId=video_id
    )
    response = request.execute()

    return response
if __name__ == "__main__":

    video_id = "JVm8P6kKgD0"
    res = main(video_id)