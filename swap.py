import googleapiclient.discovery
from pymongo import MongoClient
import pandas as pd
import datetime
import mysql.connector
import streamlit as st

api_key = "AIzaSyCon7LvBWb2Eheo8gpLlA2nh_e4gtfqGbI"
ch_id = "UC0c9xcXYGuZBfcP5LOtdOCw"
import googleapiclient.discovery
api_service_name = "youtube" 
api_version = "v3"

youtube = googleapiclient.discovery.build(
        api_service_name, api_version, developerKey=api_key)

request = youtube.channels().list(
        part="snippet,contentDetails,statistics",
        id="UC0c9xcXYGuZBfcP5LOtdOCw"
    )
channal_data = request.execute()

def get_channel_info(channel_id):
     request = youtube.channels().list(
        part="snippet,contentDetails,statistics",
        id=channel_id
    )
     response = request.execute()

     for i in response['items']:
         channel_informations =dict(channel_name= i['snippet']['title'],
                                    channel_id=i['id'],
                                    channel_description=i['snippet']['description'],
                                    views=i['statistics']['viewCount'],
                                    playlists = i['contentDetails']['relatedPlaylists']['uploads'],
                                    subscription_count=i['statistics']['subscriberCount'],
                                    total_videos=i['statistics']['videoCount'])
     return channel_informations

def get_videos_ids(channel_id):
    video_ids=[]
    response=youtube.channels().list(id=channel_id,
                                      part='contentDetails').execute()
    playlist_id= response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    
    next_page = None
                    
    while True:
        request = youtube.playlistItems().list(part = 'snippet',playlistId=playlist_id,
                                                                        maxResults=50,
                                                                        pageToken=next_page).execute()
        for i in range (len(request['items'])):
            video_ids.append(request['items'][i]['snippet']['resourceId']['videoId'])
        next_page = request.get('nextPageToken')
        if next_page is None:
            break
    return(video_ids)

def get_video_info(video_Ids):
    video_data=[]
    for i in video_Ids:

        video_response = youtube.videos().list(
                    part='snippet,statistics,contentDetails',
                    id=i
                )
        response=video_response.execute()

        #pprint.pprint(response['items'][0].keys())
        
        for item in response['items']:
            data= dict(channel_name=item['snippet']['channelTitle'],
                    channel_id=item['snippet']['channelId'],
                    video_id=item['id'],
                    Tittle=item['snippet']['title'],
                    Tags=item['snippet'].get('tags'),
                    Thumbnail=item['snippet']['thumbnails']['default']['url'],
                    Description=item['snippet']['description'],
                    Published=item['snippet']['publishedAt'],
                    View_count=item['statistics']['viewCount'],
                    Like_count=item['statistics']['likeCount'],
                    Favorite_Count=item['statistics']['favoriteCount'],
                    Comment_count=item['statistics'].get('commentCount'),
                    Duration=item['contentDetails']['duration'],
                    Caption_Status=item['contentDetails']['caption'])
        video_data.append(data)
    return video_data 

def get_comment_info(video_Ids):
    comment_data=[]
    try:
            for i in video_Ids:
                request_1 = youtube.commentThreads().list(
                part="snippet,replies",
                videoId=i,
                maxResults=50
                )
                response = request_1.execute()

                for item in response['items']:
                    data=dict(comment_id=item['snippet']['topLevelComment']['id'],
                            video_id=item['snippet']['topLevelComment']['snippet']['videoId'],
                            Comment_text=item['snippet']['topLevelComment']['snippet']['textDisplay'],
                            Comment_Author=item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                            Comment_Published_date=item['snippet']['topLevelComment']['snippet']['publishedAt'])
                    
                    comment_data.append(data)

    except:
      pass 
    return comment_data         
# get playlist details
def get_playlis_details(channel_id):

    next_page_token=None
    play_data=[]
    while True:

        request = youtube.playlists().list(
                part="snippet,contentDetails",
                channelId=channel_id,
                maxResults=50,
                pageToken=next_page_token
            )
        response = request.execute()

        for item in response['items']:
            datas=dict(Playlist_id=item['id'],
                        Channel_id=item['snippet']['channelId'],
                        Title=item['snippet']['title'],
                        Channel_name=item['snippet']['channelTitle'])
            play_data.append(datas)
            
        next_page_token=response.get('nextPageToken')
        if next_page_token is None:
            break 
    return play_data

# insert data to mongodb
client = MongoClient('mongodb://localhost:27017/')
db = client['Youtube_data']

def channel_details(channel_id):
    Ch_id=get_channel_info(channel_id)
    plays_id=get_playlis_details(channel_id)
    vi_ids=get_videos_ids(channel_id)
    vid_details=get_video_info(vi_ids)
    comm_id=get_comment_info(vi_ids)
    collection=db["Youtube_data"]
    if existing_data:
        return "Channel data already exists"

    collection.insert_one({"channel_information":Ch_id,"playlist_info":plays_id,"video info":vid_details,"comment_info":comm_id})
    
    return "upload sucessfully"
# sql table creation
def create_dataframes(channel_name):
    db = client["Youtube_data"]
    collection=db["Youtube_data"]
    #= ch_name    
    ch_data =collection.find_one({'channel_information.channel_name':channel_name},{"_id":0})
    if  ch_data:  
        df_channel = pd.DataFrame(ch_data.get('channel_information', {}), index=[0])
        df_video = pd.DataFrame(ch_data.get('video info', []))
        df_playlist = pd.DataFrame(ch_data.get('playlist_info', []))
        df_comment = pd.DataFrame(ch_data.get('comment_info', []))
        return df_channel, df_video, df_playlist, df_comment
    else:
        # Return default empty DataFrames
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

import pymysql

def channel_table(df_channel,connection,cursor):
    config = {
        'user':'root', 'password':'1234',
        'host':'127.0.0.1','port':3306,'database':'youtubedata'
    }

    connection = pymysql.connect(**config) 
    cursor=connection.cursor()
    Create_Query = '''Create table if not exists channels(channel_name varchar(100),
                                                            channel_id varchar(50) primary key,
                                                            channel_description text,
                                                            views bigint,
                                                            total_videos int,
                                                            playlists varchar(80),
                                                            subscription_count bigint
                                                            )'''
    cursor.execute(Create_Query)
    connection.commit()

    for index,row in df_channel.iterrows():
        Insert_Query = """Insert into channels(channel_name,
                                                channel_id,
                                                channel_description,
                                                views,
                                                total_videos,
                                                playlists,
                                                subscription_count
                                                )
                                                
                                                values(%s,%s,%s,%s,%s,%s,%s)"""
        values=(row['channel_name'],
                row['channel_id'],
                row['channel_description'],
                row['views'],
                row['total_videos'],
                row['playlists'],
                row['subscription_count'])
        
        cursor.execute(Insert_Query,values)
        connection.commit()
        
def playlist_table(df_playlist,connection,cursor):
    config = {
        'user':'root', 'password':'1234',
        'host':'127.0.0.1','port':3306,'database':'youtubedata'
    }

    connection = pymysql.connect(**config) 
    cursor=connection.cursor()
    Create_Query = '''Create table if not exists playlists(Playlist_id varchar(100)primary key,
                                                           Title varchar(100),
                                                        Channel_id varchar(100),
                                                        Channel_name varchar(100)
                                                        )'''

    cursor.execute(Create_Query)
    connection.commit()

    for index,row in df_playlist.iterrows():
        Insert_Query = """Insert into playlists(Playlist_id,
                                                Title,
                                                Channel_id,
                                                Channel_name)
                                                
                                                values(%s,%s,%s,%s)"""
        values=(row['Playlist_id'],
                row['Title'],
                row['Channel_id'],
                row['Channel_name']
                )
        
        cursor.execute(Insert_Query,values)
    connection.commit()
            
      #print('values are inserted')
    
import pymysql
import iso8601
import datetime
from dateutil.relativedelta import relativedelta
from datetime import datetime

def video_table(df_video,connection,cursor):
        config = {
        'user':'root', 'password':'1234',
        'host':'127.0.0.1','port':3306,'database':'youtubedata'
    }

        connection = pymysql.connect(**config) 
        cursor=connection.cursor()
        try:

                Create_Query = '''create table if not exists videos(channel_name varchar(100),
                                                                channel_id varchar(100),
                                                                video_id varchar(30) primary key,
                                                                Tittle varchar(150),
                                                                Tags text,
                                                                Thumbnail varchar(200),
                                                                Description text,
                                                                Published datetime,
                                                                View_count bigint,
                                                                Like_count bigint,
                                                                Favorite_Count int,
                                                                Comment_count int,
                                                                Duration time,
                                                                Caption_Status varchar(50))'''


                cursor.execute(Create_Query)
                connection.commit()
        except:
               print('table created')
                
        for index,row in df_video.iterrows():
                #print(type(row['Tags']),row['Tags'])
                tags=row['Tags']
                if tags is None:
                   tags=''
                else:
                   tags=''.join(row['Tags'])
                published_datetime= datetime.fromisoformat(row['Published'].replace('Z','+00:00'))
                #conversion duration into time
                duration_str = row['Duration']
                duration_parts = duration_str.split('T')[-1].split('M')
                if len(duration_parts) == 1:
                        duration_parts = duration_parts[0].split('S')
                        if len(duration_parts) == 1:
                              minutes,seconds = 0,0
                        else:
                                minutes,seconds =0, float(duration_parts[0])
                                
                else:
                        duration_parts = duration_parts[1].split('S')
                        minutes= int(duration_parts[0]) if duration_parts[0] else 0
                        seconds=float('0.' + duration_parts[1]) if len(duration_parts) > 1 else 0
                duration_obj = relativedelta(minutes=minutes, seconds=seconds)
                duration_time = datetime(1, 1, 1) + duration_obj

                duration_time = str(duration_time)
                duration_time = duration_time[11:19]
                duration_time = datetime.strptime(duration_time, '%H:%M:%S').time()
     
                Insert_Query = """Insert into videos(channel_name,
                                                        channel_id,
                                                        video_id,
                                                        Tittle,
                                                        Tags,
                                                        Thumbnail,
                                                        Description,
                                                        Published,
                                                        View_count,
                                                        Like_count,
                                                        Favorite_Count,
                                                        Comment_count,
                                                        Duration,
                                                        Caption_Status
                                                        )
                                                        
                                                        
                                                        values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
                
                values=(row['channel_name'],
                        row['channel_id'],
                        row['video_id'],
                        row['Tittle'],
                        tags,
                        row['Thumbnail'],
                        row['Description'],
                        published_datetime,
                        row['View_count'],
                        row['Like_count'],
                        row['Favorite_Count'],
                        row['Comment_count'],
                        duration_time,
                        row['Caption_Status']
                        )
                              
                cursor.execute(Insert_Query,values)
                connection.commit()
        
import iso8601
import datetime
from datetime import datetime
def comment_table(df_comment,connection,cursor):
    config = {
        'user':'root', 'password':'1234',
        'host':'127.0.0.1','port':3306,'database':'youtubedata'
    }

    connection = pymysql.connect(**config) 
    cursor=connection.cursor()
    Create_Query = '''Create table if not exists comments(comment_id varchar(100) primary key,
                                                        video_id varchar(50),
                                                        Comment_text text,
                                                        Comment_Author varchar(150),
                                                        Comment_Published_date datetime
                                                                    )'''


    cursor.execute(Create_Query)
    connection.commit()


    for index,row in df_comment.iterrows():
             published_date = datetime.fromisoformat(row['Comment_Published_date'].replace('Z','+00:00'))
             #published_date = datetime.strptime(row['Comment_Published_date'].replace('Z', '+00:00'), "%Y-%m-%dT%H:%M:%S.%f%z")
             Insert_Query = """Insert into comments(comment_id,
                                                    video_id,
                                                    Comment_text,
                                                    Comment_Author,
                                                    Comment_Published_date)
                                                    
                                                    values(%s,%s,%s,%s,%s)"""
             values=(row['comment_id'],
                    row['video_id'],
                    row['Comment_text'],
                    row['Comment_Author'],
                    published_date      
                    )
            
             cursor.execute(Insert_Query,values)
             connection.commit()

def tables(df_channel, df_video, df_playlist, df_comment):
    config = {
        'user':'root', 'password':'1234',
        'host':'127.0.0.1','port':3306,'database':'youtubedata'
    }

    connection = pymysql.connect(**config) 
    cursor=connection.cursor()

    existing_tables_query = "SHOW TABLES;"
    cursor.execute(existing_tables_query)
    existing_tables = cursor.fetchall()       #fetch all the rows of a query result as a list of tuples

    table_names_to_check = ['channels', 'playlists', 'videos', 'comments']
    
    channel_table(df_channel,connection,cursor)
    playlist_table(df_playlist,connection,cursor)
    video_table(df_video,connection,cursor)
    comment_table(df_comment,connection,cursor)
    cursor.execute
    connection.commit()
    return "tables created"
channel_name =[]
df_channel, df_video, df_playlist, df_comment = create_dataframes(channel_name)
#result=tables(df_channel, df_video, df_playlist, df_comment)

#print(result)
#table creation
def show_channel_table(channel_name):
    ch_list = []
    db = client["Youtube_data"]
    collection = db["Youtube_data"]
    ch_data = collection.find_one({"channel_information.channel_name":channel_name}, {"_id": 0, "channel_information": 1})
    
    if ch_data and isinstance(ch_data["channel_information"], dict):
        ch_list.append(ch_data["channel_information"])

    df_channel = pd.DataFrame(ch_list)
    st.dataframe(df_channel)
    return df_channel


def show_playlist_table(channel_name):
    ply_list=[]
    db = client["Youtube_data"]
    collection=db["Youtube_data"]
    pl_data= collection.find_one({"channel_information.channel_name":channel_name}, {"_id": 0, "playlist_info": 1})

    if pl_data and isinstance(pl_data.get("playlist_info"), list):
                 ply_list.extend(pl_data["playlist_info"])
    df_playlist=pd.DataFrame(ply_list)
    st.dataframe(df_playlist)
    return df_playlist

def show_video_table(channel_name):
     vid_list=[]
     db = client["Youtube_data"]
     collection=db["Youtube_data"]
     vi_data = collection.find_one({"channel_information.channel_name":channel_name}, {"_id": 0, "video info": 1})
     if vi_data and isinstance(vi_data.get("video info"),list):
          vid_list.extend(vi_data["video info"])
     df_video=pd.DataFrame(vid_list)  
     st.dataframe(df_video)   
     return df_video 

     

def show_comment_table(channel_name):
    com_list=[]
    db = client["Youtube_data"]
    collection=db["Youtube_data"]
    comm_data= collection.find_one({"channel_information.channel_name":channel_name}, {"_id": 0, "comment_info": 1})
    if comm_data and isinstance(comm_data.get("comment_info"), list):            
                    com_list.extend(comm_data["comment_info"])
    df_comment=pd.DataFrame(com_list)
    st.dataframe(df_comment)
    return df_comment

# streamlit createtion

st.title('Welcome to Streamlit')
st.title(":blue[Youtube Data Harvesting And Warehousing]")
with st.sidebar:
    st.sidebar.title("Navigation")
    if st.sidebar.button("Home"):
        st.write("API Integration")
        st.write("Data fetching operation...")
        st.write("Data insertion to MongoDB operation...")
        st.write("Data migration to SQL operation...")
        
# class YoutubeData:
#     def __init__(self, channel_id):
#         self.channel_id = channel_id   # Add your initialization code here
#     def fetch_channel_data(self):
#         pass    
channel_id=st.text_input("Enter Youtube Channel ID")
if st.button("Fetch Channel Data"):
    client = MongoClient('mongodb://localhost:27017/')
    db = client["Youtube_data"]
    collection=db["Youtube_data"]
    if collection.find_one({"channel_information.channel_id": channel_id}):
                st.success("channel id already exists")
    else:
                #insert=YoutubeData(channel_id)
                Ch_id=get_channel_info(channel_id)
                plays_id=get_playlis_details(channel_id)
                vi_ids=get_videos_ids(channel_id)
                vid_details=get_video_info(vi_ids)
                comm_id=get_comment_info(vi_ids)


                collection.insert_one({"channel_information":Ch_id,"playlist_info":plays_id,"video info":vid_details,"comment_info":comm_id})
                st.success(f"Channel data inserted for {channel_id}")
                
    if st.checkbox("Show Existing Channel Details"):
        existing_data = collection.find_one({"channel_information.channel_id":channel_id})
        if existing_data:
            st.write(existing_data)
    else:
        st.info("No data available for the entered channel ID.")    
def main():    
    db = client["Youtube_data"]
    collection=db["Youtube_data"]        
    channel_names =[]
    for channel_info in collection.find({}, {"channel_information.channel_name": 1}):
        if "channel_information" in channel_info and "channel_name" in channel_info["channel_information"]:
            channel_names.append(channel_info["channel_information"]["channel_name"])

    selected_channel_name = st.selectbox("Select a channel name", channel_names)
    #st.write("Selected channel name:", selected_channel_name)        

    if st.button("Migrate to Sql"):
        df_channel, df_video, df_playlist, df_comment = create_dataframes(selected_channel_name)
        result = tables(df_channel, df_video, df_playlist, df_comment)
        st.success(result)
    return selected_channel_name
if __name__ == "__main__":
    select_channel=main()

show_table= st.radio("Select The Table For View",("CHANNELS","PLAYLISTS","VIDEOS","COMMENTS"))

if show_table=="CHANNELS":
    show_channel_table(select_channel)

elif  show_table=="PLAYLISTS":
    show_playlist_table(select_channel)

elif  show_table=="VIDEOS":
    show_video_table(select_channel)

elif show_table=="COMMENTS":
    show_comment_table(select_channel)

#Sql connection
config = {
        'user':'root', 'password':'1234',
        'host':'127.0.0.1','port':3306,'database':'youtubedata'
    }

connection = pymysql.connect(**config) 
cursor=connection.cursor()

question=st.selectbox("Select Your Question",("1.What are the names of all the videos and their corresponding channels",
                                              "2.Which channels have the most number of videos, and how many videos do they have",
                                              "3.What are the top 10 most viewed videos and their respective channels",
                                              "4.How many comments were made on each video, and what are corresponding video names",
                                              "5.Which videos have the highest number of likes, and what are their corresponding channel names",
                                              "6.What is the total number of likes and dislikes for each video, and what are their corresponding video names",
                                              "7.What is the total number of views for each channel, and what are their corresponding channel names",
                                              "8.What are the names of all the channels that have published videos in the year2022",
                                              "9.What is the average duration of all videos in each channel, and what are their corresponding channel names",
                                              "10.Which videos have the highest number of comments, and what are their corresponding channel names" ))

if question== "1.What are the names of all the videos and their corresponding channels":
    query1="""select tittle as videos,channel_name as channelname from videos"""
    cursor.execute(query1)

    t1=cursor.fetchall()
    df=pd.DataFrame(t1,columns=['video tittle','channel name'])
    st.write(df)
elif question== "2.Which channels have the most number of videos, and how many videos do they have":
    query2="""select channel_name as channelname,total_videos as no_videos from channels
                order by total_videos desc"""
    cursor.execute(query2)

    t2=cursor.fetchall()
    df1=pd.DataFrame(t2,columns=['channel name','No of videos'])
    st.write(df1)

elif question== "3.What are the top 10 most viewed videos and their respective channels":
    query3="""select view_count as views, channel_name as channelname,tittle as videotitle from videos
                where view_count is not null order by views desc limit 10"""
    cursor.execute(query3)
    t3=cursor.fetchall()
    df2=pd.DataFrame(t3,columns=['views','channel name','videotitle'])
    st.write(df2)

elif question=="4.How many comments were made on each video, and what are corresponding video names":
    query4="""select Comment_count as no_comments,Tittle as videotitle from videos where Comment_count is not null 
                order by no_comments desc limit 10"""
    cursor.execute(query4)
    t4=cursor.fetchall()
    df3=pd.DataFrame(t4,columns=['no_comments','videotitle'])
    st.write(df3)
    
elif question=="5.Which videos have the highest number of likes, and what are their corresponding channel names":
    query5="""select Like_count as likes,Tittle as videotitle,channel_name as channelname from videos where Like_count is not null 
                order by likes desc limit 10"""
    cursor.execute(query5)
    t5=cursor.fetchall()
    df4=pd.DataFrame(t5,columns=['likes','videotitle','channelname'])
    st.write(df4)

elif question=="6.What is the total number of likes and dislikes for each video, and what are their corresponding video names":
    query6="""select Like_count as likes,Tittle as videotitle from videos"""
    cursor.execute(query6)
    t6=cursor.fetchall()
    df5=pd.DataFrame(t6,columns=['likes','videotitle'])
    st.write(df5)       

elif question=="7.What is the total number of views for each channel, and what are their corresponding channel names":
    query7="""select channel_name as channelname,views as totalviews from channels"""
    cursor.execute(query7)
    t7=cursor.fetchall()
    df6=pd.DataFrame(t7,columns=['channelname','totalviews'])
    st.write(df6)

elif question=="8.What are the names of all the channels that have published videos in the year2022":
    query8="""select channel_name as channelname,Tittle as videotitle,Published as videorelese from videos
                where extract(year from Published)=2022"""
    cursor.execute(query8)
    t8=cursor.fetchall()
    df7=pd.DataFrame(t8,columns=['channelname','videotitle','videorelese'])
    st.write(df7)

elif question=="9.What is the average duration of all videos in each channel, and what are their corresponding channel names":
    query9='''SELECT TIME_FORMAT(SEC_TO_TIME(AVG(TIME_TO_SEC(STR_TO_DATE(duration, '%H:%i:%s')))), '%H:%i:%s') AS average_duration,channel_name
                FROM videos GROUP BY channel_name'''
    cursor.execute(query9)
    t9=cursor.fetchall()
    df8=pd.DataFrame(t9,columns=['average_duration','channelname'])
    st.write(df8)

elif question=="10.Which videos have the highest number of comments, and what are their corresponding channel names":
    query10="""select Comment_count as comments,channel_name as channelname, tittle as videotitle from videos 
                where Comment_count is not null order by comments desc """
    cursor.execute(query10)
    t10=cursor.fetchall()
    df9=pd.DataFrame(t10,columns=['comments','channelname','videotitle'])
    st.write(df9)
