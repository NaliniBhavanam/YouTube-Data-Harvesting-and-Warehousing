from googleapiclient.discovery import build
from pymongo import MongoClient
import pandas as pd, pymongo, mysql.connector, streamlit as st


with st.sidebar:
    selected = st.radio(":red[YouTube Data Harvesting and Warehousing]",
        ("Project Details","Get Data & Transform","SQL Query")
    )

# YOUTUBE API KEY CONNECTION:
def api_conn():
  api_key = 'YOUR_API_KET'
  api_service_name = "youtube"  
  api_version = "v3"
  youtube = build(api_service_name, api_version, developerKey = api_key)
  return youtube
youtube = api_conn()



# FUNCTION TO GET CHANNEL DETAILS:
def get_channel_info(channel_id):
    ch_data = []
    response = youtube.channels().list(part='snippet,contentDetails,statistics',
                                       id=channel_id).execute()

    for i in range(len(response['items'])):
        data = dict(Channel_id=channel_id[i],
                    Channel_Name=response['items'][i]['snippet']['title'],
                    Playlist_id=response['items'][i]['contentDetails']['relatedPlaylists']['uploads'],
                    Subscribers=response['items'][i]['statistics']['subscriberCount'],
                    Views=response['items'][i]['statistics']['viewCount'],
                    Total_videos=response['items'][i]['statistics']['videoCount'],
                    Description=response['items'][i]['snippet']['description'],
                    Country=response['items'][i]['snippet'].get('country')
                    )
        ch_data.append(data)
    return ch_data


# FUNCTION TO GET PLAYLISTS DETAILS:
def get_playlist_info(channel_id):
    play_data = []
    
    for channel_id in channel_id:
        next_page_token = None
        
        while True:
            try:
                request = youtube.playlists().list(
                    part='snippet,contentDetails',
                    channelId=channel_id,
                    maxResults=50,
                    pageToken=next_page_token
                )
                response = request.execute()
                
                for item in response.get('items', []):
                    data = {
                        'Playlist_Id': item['id'],
                        'Channel_Id': item['snippet']['channelId'],
                        'Channel_Name': item['snippet']['channelTitle'],
                        'Playlist_Name': item['snippet']['title'],
                        'PublishedAt': item['snippet']['publishedAt'],
                        'Video_Count': item['contentDetails'].get('itemCount', 0)
                    }
                    play_data.append(data)
                    
                next_page_token = response.get('nextPageToken')
                if not next_page_token:
                    break
            except Exception as e:
                print(f"An error occurred for channel {channel_id}: {e}")
                break
                
    return play_data


# FUNCTION TO GET VIDEO IDS:
def get_channel_videos(channel_id):
    Video_ids = []
    response = youtube.channels().list(
        id=channel_id,
        part="contentDetails"
    ).execute()
    playlist_id = response['items'][0]['contentDetails']['relatedPlaylists']['uploads']

    next_page_token = None

    while True:
        try:
            video_response = youtube.playlistItems().list(
                part='snippet',
                playlistId=playlist_id,
                maxResults=50,
                pageToken=next_page_token
            ).execute()

            Video_ids.extend(
                item['snippet']['resourceId']['videoId']
                for item in video_response['items']
            )

            next_page_token = video_response.get('nextPageToken')
            if not next_page_token:
                break
        except Exception as e:
            print(f"Error fetching video IDs: {str(e)}")
            break

    return Video_ids


# FUNCTION TO GET VIDEO DETAILS
def get_video_info(video_ids):
    video_stats = []

    for i in range(0, len(video_ids), 50):
        response = youtube.videos().list(
            part="snippet,contentDetails,statistics",
            id=','.join(video_ids[i:i + 50])).execute()
        for video in response['items']:
            video_details = dict(Channel_Name=video['snippet']['channelTitle'],
                                 Channel_Id=video['snippet']['channelId'],
                                 Video_Id=video['id'],
                                 Video_Name=video['snippet']['title'],
                                 Tags = video['snippet'].get('tags'),
                                 Thumbnail=video['snippet']['thumbnails']['default']['url'],
                                 Description=video['snippet']['description'],
                                 Published_date=video['snippet']['publishedAt'],
                                 Duration=video['contentDetails']['duration'],
                                 View_Count=video['statistics']['viewCount'],
                                 Like_Count=video['statistics'].get('likeCount'),
                                 Comment_Count=video['statistics'].get('commentCount'),
                                 Dislike_Count=video['statistics']['favoriteCount'],
                                 Favorite_count=video['statistics']['favoriteCount'],
                                 Definition=video['contentDetails']['definition'],
                                 Caption_Status=video['contentDetails']['caption']
                                 )
            video_stats.append(video_details)
    return video_stats


# FUNCTION TO GET COMMENT DETAILS:
def get_comment_info(video_ids):
    comment_data = []    
    try:
        for video_id in video_ids:
            request = youtube.commentThreads().list(
                part="snippet",
                videoId=video_id,
                maxResults=100
            )
            response = request.execute()
            for cmt in response["items"]:
                comment = {
                    "Comment_Id": cmt["snippet"]["topLevelComment"]["id"],
                    "Video_Id": cmt["snippet"]["videoId"],
                    "Comment_Text": cmt["snippet"]["topLevelComment"]["snippet"]["textOriginal"],
                    "Comment_Author": cmt["snippet"]["topLevelComment"]["snippet"]["authorDisplayName"],
                    "Comment_Published": cmt["snippet"]["topLevelComment"]["snippet"]["publishedAt"]
                }
                comment_data.append(comment)
            # Pagination
            while "nextPageToken" in response:
                nextPageToken = response["nextPageToken"]
                response = youtube.commentThreads().list(
                    part="snippet",
                    videoId=video_id,
                    maxResults=100,
                    pageToken=nextPageToken
                ).execute()              
                for cmt in response["items"]:
                    comment = {
                        "Comment_Id": cmt["snippet"]["topLevelComment"]["id"],
                        "Video_Id": cmt["snippet"]["videoId"],
                        "Comment_Text": cmt["snippet"]["topLevelComment"]["snippet"]["textOriginal"],
                        "Comment_Author": cmt["snippet"]["topLevelComment"]["snippet"]["authorDisplayName"],
                        "Comment_Published": cmt["snippet"]["topLevelComment"]["snippet"]["publishedAt"]
                    }
                    comment_data.append(comment)
    except Exception as e:
        print("An error occurred:", e) 
    return comment_data


#BRIDGING TO A CONNECTION WITH MONGODB AND CREATE A NEW DATA BASE:
client = pymongo.MongoClient('mongodb+srv://USERNAME:PASSWORD@cluster0.pmhcrw5.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0')
db = client["youtube_channelsdata"]

# UPLOAD TO MONGODB:
def channel_details(channel_id):
    ch_details=get_channel_info(channel_id)
    pl_details=get_playlist_info(channel_id)
    video_ids=get_channel_videos(channel_id)
    vi_details=get_video_info(video_ids)
    cmt_details=get_comment_info(video_ids)

    coll1 = db["channel_data"]
    coll1.insert_one({"channel_information":ch_details,
                      "playlist_information":pl_details,
                      "video_information":vi_details,
                      "comment_information":cmt_details})
    
    return "upload completed successfully"


# FUNCTIONS TO CREATE CHANNELS TABLES IN MYSQL DATABASE, AND 
# TO FETCH CHANNEL DETAILS OF A CHANNEL FROM MONGODB ATLAS TO STORE IN TABLE  "channels" IN MYSQL DB:
def channels_table(channel_name):
    conn = mysql.connector.connect(
        host='localhost',
        user='root',
        password='NANA',
        database='youtube_channelsdata'
    )
    cursor = conn.cursor()

    try:
        create_query = '''create table if not exists channels(Channel_Name varchar(100), Channel_Id varchar(100) primary key, 
                                                            Subscription_Count bigint, Views bigint, Total_Videos int,
                                                            Channel_Description text, Playlist_Id varchar(100))'''
        cursor.execute(create_query)
        conn.commit()
    except:
        print('channel already there')

    ch_list = []
    for ch_data in coll1.find({},{"_id":0,"channel_information":1}):
                ch_list.append(ch_data["channel_information"])
    df = pd.DataFrame(ch_list)


    for index,row in df.iterrows():
        insert_query = '''INSERT IGNORE into channels(Channel_Name, Channel_Id, Subscription_Count, Views, Total_Videos, Channel_Description, Playlist_Id)
                                        VALUES(%s,%s,%s,%s,%s,%s,%s)'''
        
        values =(row['Channel_Name'], row['Channel_Id'], row['Subscription_Count'], row['Views'], row['Total_Videos'], 
                row['Channel_Description'], row['Playlist_Id'])
        try:                     
            cursor.execute(insert_query,values)
            conn.commit()    
        except:
            print("Channels values are already inserted")


# FUNCTIONS TO CREATE PLAYLISTS TABLES IN MYSQL DATABASE:, AND
# TO FETCH PLAYLIST DETAILS OF A CHANNEL FROM MONGODB ATLAS TO STORE IN TABLE  "playlists" IN MYSQL DB:
def playlists_table(channel_name):
    conn = mysql.connector.connect(
        host='localhost',
        user='root',
        password='NANA',
        database='youtube_channelsdata'
    )
    cursor = conn.cursor()



    try:
        create_query = '''create table if not exists playlists(PlaylistId varchar(100) primary key,
                        Title varchar(80), 
                        ChannelId varchar(100), 
                        ChannelName varchar(100),
                        PublishedAt timestamp,
                        VideoCount int
                        )'''
        cursor.execute(create_query)
        conn.commit()
    except:
            print("Playlists Table alredy created")

    pl_list = []
    for pl_data in coll1.find({},{"_id":0,"playlist_information":1}):
        for i in range(len(pl_data["playlist_information"])):
                pl_list.append(pl_data["playlist_information"][i])
    df = pd.DataFrame(pl_list)
    df["PublishedAt"]=df["PublishedAt"].str.replace("T", " ").str.replace("Z", " ")

    for index,row in df.iterrows():
            insert_query = '''INSERT IGNORE into playlists(PlaylistId, Title, ChannelId, ChannelName, PublishedAt, VideoCount)
                                            VALUES(%s,%s,%s,%s,%s,%s)'''            
            values =(
                    row['PlaylistId'],
                    row['Title'],
                    row['ChannelId'],
                    row['ChannelName'],
                    row['PublishedAt'],
                    row['VideoCount'])
                    
            try:                     
                cursor.execute(insert_query,values)
                conn.commit()    
            except:
                print("Playlists values are already inserted")
        

# FUNCTIONS TO CREATE VIDEOS TABLES IN MYSQL DATABASE:, AND
# TO FETCH VIDEO DETAILS OF A CHANNEL FROM MONGODB ATLAS TO STORE IN TABLE  "videos" IN MYSQL DB:
def videos_table(channel_name):
    conn = mysql.connector.connect(
        host='localhost',
        user='root',
        password='NANA',
        database='youtube_channelsdata'
    )
    cursor = conn.cursor()



    try:
        create_query = '''create table if not exists videos(Channel_Name varchar(150), Channel_Id varchar(100), Video_Id varchar(50) primary key, 
                                                            Title varchar(150), Tags text, Thumbnail varchar(225), Description text, 
                                                            Published_Date datetime, Duration time, Views bigint, Likes bigint,Comments int,
                                                            Favorite_Count int, Definition varchar(10), Caption_Status varchar(50))''' 
                                
        cursor.execute(create_query)             
        mydb.commit()
    except:
        print("Videos Table alrady created")

    vi_list = []
    for vi_data in coll1.find({},{"_id":0,"video_information":1}):
        for i in range(len(vi_data["video_information"])):
            vi_list.append(vi_data["video_information"][i])
    df2 = pd.DataFrame(vi_list)
    df2["Published_Date"]=df2["Published_Date"].str.replace("T", " ").str.replace("Z", " ")
    df3=df2['Tags'][0]
    listToStr=' , '.join([str(elem) for elem in df3])
    df2["Tags"]=listToStr

    for index, row in df2.iterrows():
            insert_query = '''INSERT IGNORE INTO videos (Channel_Name, Channel_Id, Video_Id, Title, Tags, Thumbnail, Description, Published_Date,
                                                        Duration, Views, Likes, Comments, Favorite_Count, Definition, Caption_Status )
                                                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'''
            
            duration=row['Duration']    
            pattern = r'PT(?:(\d+)M)?(?:(\d+)S)?'
            match = re.match(pattern, duration)
            if match:
                minutes = int(match.group(1) or 0)
                seconds = int(match.group(2) or 0)
            total_seconds = minutes * 60 + seconds
            formatted_duration = '{:02d}:{:02d}:00'.format(total_seconds // 3600, (total_seconds % 3600) // 60)
            values = (row['Channel_Name'], row['Channel_Id'], row['Video_Id'], row['Title'], row['Tags'], row['Thumbnail'], row['Description'],
                    row['Published_Date'], row['Duration'], row['Views'], row['Likes'], row['Comments'], row['Favorite_Count'],
                    row['Definition'], row['Caption_Status'])                       
            try:    
                cursor.execute(insert_query,values)
                conn.commit()
            except:
                print("videos values already inserted in the table")

# FUNCTIONS TO CREATE COMMENTS TABLES IN MYSQL DATABASE:, AND
# FUNCTION TO FETCH COMMENT DETAILS OF A CHANNEL FROM MONGODB ATLAS TO STORE IN TABLE  "COMMENTS" IN MYSQL DB:
def comments_table(channel_name):
    conn = mysql.connector.connect(
        host='localhost',
        user='root',
        password='NANA',
        database='youtube_channelsdata'
    )
    cursor = conn.cursor()

    try:
        create_query = '''CREATE TABLE if not exists comments(Comment_Id varchar(100) primary key, Video_Id varchar(80), Comment_Text text, 
                                                            Comment_Author varchar(150), Comment_Published datetime)'''
        cursor.execute(create_query)
        conn.commit()
    except:
        print("Commentsp Table already created")

    com_list = []
    for com_data in coll1.find({},{"_id":0,"comment_information":1}):
        for i in range(len(com_data["comment_information"])):
            com_list.append(com_data["comment_information"][i])
    df3 = pd.DataFrame(com_list)
    df3["Comment_Published"]=df3["Comment_Published"].str.replace("T", " ").str.replace("Z", " ")

    for index, row in df3.iterrows():
        insert_query = '''
            INSERT IGNORE INTO comments (Comment_Id, Video_Id, Comment_Text, Comment_Author, Comment_Published)
                                    VALUES (%s, %s, %s, %s, %s)'''
        values = (row['Comment_Id'], row['Video_Id'], row['Comment_Text'], row['Comment_Author'], row['Comment_Published'])
        try:
            cursor.execute(insert_query,values)
            conn.commit()
        except:
                print("This comments are already exist in comments table")


# STREAMLIT  

# HOME PAGE
if selected == "Project Details":
   
    st.title(":red[YouTube] :orange[Data :red[API] V3] :red[Project]")
    st.markdown("## :green[Domain] : Social Media ")
    st.markdown("## :orange[Technologies used] : Youtube Data API, Python, MongoDB, MySql, Streamlit")
    st.markdown(
        "## :blue[Overview] : Retrieving the Youtube channels data from the Google API Key and Channel ID, stored in a MongoDB as data lake, migrated of the MYSQL data warehouse and streamlit framework is to display the results to the web application.")

if selected == "Get Data & Transform":
    tab1,tab2 = st.tabs(["GET DATA" , "TRANSFORM TO SQL "])

    # GET DATA TAB
    with tab1:
        st.markdown("#    ")
        st.write("### Enter YouTube Channel_ID below :")
        channel_id = st.text_input(
            "Hint : Go to the YouTube website (https://www.youtube.com) > channel's home page > Right click > View page source > Find channel_id").split(
            ',')  
        
        if st.button("Upload to MongoDB"):
      
                with st.spinner('Please wait for it...'):
                    
                    ch_ids = []
                    db = client["youtube_channelsdata"]
                    coll1 = db["channel_data"]
                    for ch_data in coll1.find({}, {"_id": 0, "Channel_Information": 1}):
                        channel_info = ch_data.get("Channel_Information")
                        if channel_info and isinstance(channel_info, dict) and "Channel_Id" in channel_info:
                            ch_ids.append(channel_info["Channel_Id"])
                        else:
                            print("Unexpected structure or missing key in Channel_Information:", ch_data)

                st.success("Upload to MongoDB successful!")


        if st.button("Extract Data"):
            with st.spinner('Please wait for it...'):

                def tables(channel_name):

                    newCh= channels_table(channel_name)
                    if newCh:
                        st.write(newCh)
                    else:
                        playlists_table(channel_name)
                        videos_table(channel_name)
                        comments_table(channel_name)
            st.success("Transformation to MySQL Successful !!")


    # TRANSFORM TAB
    with tab2:
        st.markdown("#   ")
        st.markdown("###  To Retrive Data from MySQL DB")

        if channel_id and st.button("Channel Data"):
            ch_details=get_channel_info(channel_id)  # Passing ch_ids instead of Channel_ids
            if ch_details:  # Check if ch_details is not empty
                st.write(f'#### Extracted data from :green["{ch_details[0]["Channel_Name"]}"] channels')
                st.dataframe(ch_details)
            else:
                st.write("No channel details found.")
        
        if channel_id and st.button("Platlist Data"):
            pl_details=get_playlist_info(channel_id)  
            if pl_details:
                st.write(f'#### Extracted data from :green["{pl_details[0]["Channel_Name"]}"] playlists')
                st.dataframe(pl_details)
            else:
                st.write("No playlist details found.")

        if st.button("Videos Data"):
            video_ids=get_channel_videos(channel_id)
            if video_ids:
                if isinstance(video_ids, list):
                    video_ids_list = video_ids  
                else:
                    video_ids_list = v_ids.split(",")  
                vi_details = get_video_info(video_ids_list)
                if vi_details:
                    st.write(f'#### Extracted data from :green["{vi_details[0]["Channel_Name"]}"] videos')
                    st.dataframe(vi_details)
                else:
                    st.write("No video details found.")
            else:
                st.write("No video IDs found for the selected channel(s).")


        if st.button("Comments Data"):
            video_ids=get_channel_videos(channel_id) 
            if video_ids:
                if isinstance(video_ids, list):
                    video_ids_list = video_ids  
                else:
                    video_ids_list = v_ids.split(",")  

                # Corrected variable name from 'com_details' to 'cmt_details'
                cmt_details = get_comment_info(video_ids_list)

                if cmt_details:  # Corrected variable name from 'com_details' to 'cmt_details'
                    st.write(f'#### Extracted data from :green["{cmt_details[0]["Comment_Id"]}"] comments')
                    st.dataframe(cmt_details)  # Corrected variable name from 'com_details' to 'cmt_details'
                else:
                    st.write("No comments details found.")

            else:
                st.write("No comments found for the selected channel(s).")
                

# VIEW PAGE
# Initialize the database connection
conn = mysql.connector.connect(
    host='localhost',
    user='root',
    password='NANA',
    database='youtube_channelsdata'
)
cursor = conn.cursor()

if selected == "SQL Query":

    st.write("## :green[Take your pick from the questions below]")
    questions = st.selectbox('Questions',
                             ['1. What are the names of all the videos and their corresponding channels?',
                              '2. Which channels have the most number of videos, and how many videos do they have?',
                              '3. What are the top 10 most viewed videos and their respective channels?',
                              '4. How many comments were made on each video, and what are their corresponding video names?',
                              '5. Which videos have the highest number of likes, and what are their corresponding channel names?',
                              '6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?',
                              '7. What is the total number of views for each channel, and what are their corresponding channel names?',
                              '8. What are the names of all the channels that have published videos in the year 2022?',
                              '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?',
                              '10. Which videos have the highest number of comments, and what are their corresponding channel names?'])
    
    if questions == '1. What are the names of all the videos and their corresponding channels?':
        qu1 = '''select Title,Channel_Name  from videos;'''
        cursor.execute(qu1)
        q1=cursor.fetchall()
        conn.commit()
        df1=pd.DataFrame(q1, columns=["video title","channel name"])
        st.write(df1)
    
    elif questions == '2. Which channels have the most number of videos, and how many videos do they have?':
        qu2 = "select Channel_Name, Total_Videos from channels order by Total_Videos desc;"
        cursor.execute(qu2)
        q2=cursor.fetchall()
        conn.commit()
        df2=pd.DataFrame(q2, columns=["Channel Name","Num Of Videos"])  
        st.write(df2)
    
    elif questions == '3. What are the top 10 most viewed videos and their respective channels?':
        qu3 = '''SELECT Channel_Name, Title, Views FROM videos WHERE Views is not null order by Views desc limit 10;'''
        cursor.execute(qu3)
        q3 = cursor.fetchall()
        conn.commit()
        df3=pd.DataFrame(q3, columns = ["channel Name","video title","views"])
        st.write(df3)
    
    elif questions == '4. How many comments were made on each video, and what are their corresponding video names?':
        qu4 = "SELECT Title, Comments FROM videos WHERE Comments is not null;"
        cursor.execute(qu4)
        q4=cursor.fetchall()
        conn.commit()
        df4=pd.DataFrame(q4, columns=["Video Title", "No Of Comments"])
        st.write(df4)

    elif questions == '5. Which videos have the highest number of likes, and what are their corresponding channel names?':
        qu5 = '''SELECT Title, Channel_Name, Likes FROM videos WHERE Likes is not null ORDER BY Likes desc;'''
        cursor.execute(qu5)
        q5 = cursor.fetchall()
        conn.commit()
        df5=pd.DataFrame(q5, columns=["video Title","channel Name","like count"])
        st.write(df5) 

    elif questions == '6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?':
        qu6 = '''SELECT Title, Likes FROM Videos;'''
        cursor.execute(qu6)
        q6 = cursor.fetchall()
        conn.commit()
        df6=pd.DataFrame(q6, columns=["video title", "like count"]) 
        st.write(df6) 

    elif questions == '7. What is the total number of views for each channel, and what are their corresponding channel names?':
        qu7 = "SELECT Channel_Name, Views FROM Channels;"
        cursor.execute(qu7)
        q7=cursor.fetchall()
        conn.commit()
        df7=pd.DataFrame(q7, columns=["channel name","total views"])
        st.write(df7)

    elif questions == '8. What are the names of all the channels that have published videos in the year 2022?':    
        qu8 = '''SELECT Title, Published_Date, Channel_Name FROM Videos WHERE extract(year from Published_Date) = 2022;'''
        cursor.execute(qu8)
        q8=cursor.fetchall()
        conn.commit()
        df8=pd.DataFrame(q8,columns=["Name", "Video Publised On", "ChannelName"])
        st.write(df8)

    elif questions == '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?':
        qu9 = """SELECT Channel_Name,AVG(Duration)/60 AS "Average_Video_Duration (mins)" FROM Videos GROUP BY Channel_Name ORDER BY AVG(Duration)/60 DESC"""
        cursor.execute(qu9)
        q9=cursor.fetchall()
        conn.commit()
        df9=pd.DataFrame(q9,columns=["Channel Name", "Average Duration"])
        st.write(df9)

    elif questions == '10. Which videos have the highest number of comments, and what are their corresponding channel names?':    
        qu10 = '''SELECT Title, Channel_Name, Comments FROM videos WHERE Comments is not null ORDER BY Comments desc;'''
        cursor.execute(qu10)
        q10=cursor.fetchall()
        conn.commit()
        df10=pd.DataFrame(q10, columns=['Video Title', 'Channel Name', 'NO Of Comments'])
        st.write(df10)
