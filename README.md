# YouTube Data Harvesting and Warehousing

## Get Data and Transform to SQL Using YouTube Video Data from Channels


### *Introduction:*

YouTube, founded in 2005, has evolved into the second largest search engine globally, processing over 3 billion searches per month. Despite its popularity, the workings of the YouTube algorithm remain shrouded in mystery. YouTube boasts one of the most sophisticated recommendation systems.

This project focuses on retrieving data from the YouTube API using an API key and channel ID. The collected data is then stored in MongoDB and subsequently migrated to a MySQL data warehouse. Python code is utilized to execute all processes, while the Streamlit framework is employed to display the results, facilitating user interaction with the web application.

The primary utility of this project lies in the analysis and comparison of YouTube channels using their API key and channel ID. Additionally, it offers the capability to store data and records of YouTube channel video comments, among other functionalities.


### *Aims and objectives:*

Within this project, I would like to explore the following:

1. Getting acquainted with the YouTube API and obtaining video data
2. To develop a YouTube data harvesting and warehousing system for collecting, storing, and analyzing YouTube channel and video data
    - What are the names of all the videos and their corresponding channels?
    - Which channels have the greatest number of videos, and how many videos do they have?
    -	What are the top 10 most viewed videos and their respective channels?
    -	How many comments were made on each video, and what are their corresponding video names?
    -	Which videos have the highest number of likes, and what are their corresponding channel names?



### *Steps of the project:*

1.	Obtain video meta data via YouTube API for the top 10 channels in the data science niche (this includes several small steps: create a developer key, request data and transform the responses into a usable data format)
2.	Preprocess data and engineer additional features for analysis
3.	Exploratory Streamlit
4.	Conclusions
   

### *Data selection:*

I created my own dataset using the Google Youtube Data API version 3.0. The exact steps of data creation are presented in section.


### *Data limitations:*

The dataset is a real-world dataset and suitable for the research. However, the selection of the top 10 Youtube channels to include in the research is purely based on my knowledge of the channels in data science field and might not be accurate. My definition is "popular" is only based on subscriber count but there are other metrics that could be taken into consideration as well (e.g. views, engagement). The top 10 also seems arbitrary given the plethora of channels on Youtube. There might be smaller channels that might also very interesting to look into, which could be the next step of this project.


### *Ethics of data source:*

According to Youtube API's guide, the usage of Youtube API is free of charge given that your application sends requests within a quota limit. "The YouTube Data API uses a quota to ensure that developers use the service as intended and do not create applications that unfairly reduce service quality or limit access for others. " The default quota allocation for each application is 10,000 units per day, and you could request additional quota by completing a form to YouTube API Services if you reach the quota limit.

Since all data requested from Youtube API is public data (which everyone on the Internet can see on Youtube), there is no particular privacy issues as far. In addition, the data is obtained only for research purposes in this case and not for any commercial interests.


### *Data creation with Youtube API:*

I first created a project on Google Developers Console, then requested an authorization credential (API key). Afterwards, I enabled Youtube API for my application, so that I can send API requests to Youtube API services. Then, I went on Youtube and checked the channel ID of each of the channels that I would like to include in my research scope (using their URLs). Then I created the functions for getting the channel statistics via the API.


### *Data Storage with MongoDB:*

The collected data is stored in MongoDB, providing a flexible and scalable solution for managing large volumes of YouTube data. MongoDB's document-oriented architecture enables efficient storage and retrieval of structured and unstructured data. Stores data in flexible JSON-like documents, making it suitable for storing structured and unstructured data from YouTube API responses. Each document can vary in structure, allowing for easy storage of diverse data types such as video metadata, channel information, and comments.


### *Data Warehousing with MYSQL:*

The project facilitates the migration of data from MongoDB to a MySQL data warehouse, enabling users to perform complex analytical queries and aggregations. SQL's powerful querying capabilities allow for in-depth analysis of YouTube channel and video data. MySQL provides a structured, tabular data storage format, making it well-suited for storing structured data from MongoDB collections. By migrating data to MySQL, you can organize and store YouTube data in a relational format, facilitating complex queries and analysis.


### *User Interface with Streamlit:*

The application's front-end is built using Streamlit, offering a simple and intuitive interface for interacting with the data. Users can easily retrieve, store, and query YouTube data through the Streamlit web application, without requiring extensive technical expertise. Streamlit offers a straightforward and easy-to-use interface for building web applications with Python. Users can interact with the application through widgets, sliders, dropdowns, and text inputs, providing a seamless experience for data retrieval, storage, and querying.


### *Technologies Used:*

1. *YouTube API:* Interface for accessing YouTube data
2. *Python:* The programming language used for building the application and scripting tasks
3. *Pandas:* Python library used for data manipulation and analysis.
4. *MongoDB:* NoSQL database for storing unstructured data
5. *SQL:* Query language for interacting with relational databases
6. *Streamlit:* Python library for building web applications


### *Conclusions and future research ideas:*

In this project, explored the video data of the 9 channels and revealed many interesting findings for anyone who are starting out with a YouTube channel in data science or another topic:
  •	The more likes and comments a video. Likes seem to be a better indicator for interaction than comments and the number of likes seem to follow the "social proof", which means the more views the video has, the      more people will like it.
  •	Most videos have between 5 and 30 tags.
  •	Most-viewed videos tend to have average title length of 30-70 characters. Too short or too long titles seem to harm viewership.
  •	Comments on videos are generally positive, we noticed a lot "symbols".


### *Project limitation:*

The findings should also be taken with a grain of salt for a number of reasons:
  •	The number of videos is quite small (the dataset has only ~3,700 videos)
  •	I have only considered the first 10 comments on each video, which might not be representative for all comments


### *References/ Resources used:*

1.  YouTube API. Available at https://developers.google.com/youtube/v3
2.  Python. Available at https://www.python.org/dev/peps/pep-0008/
3.  MongoDB. Available at https://www.mongodb.com/docs/
4.  MYSQL. Available at https://dev.mysql.com/doc/
5.  Streamlit. Available at https://docs.streamlit.io/library/api-reference

