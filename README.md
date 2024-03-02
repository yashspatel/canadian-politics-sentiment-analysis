# Sentiment Analysis: Analyzing Canadian Political Discourse on Reddit in Real Time

### Overview 
Welcome to the GitHub repository for the Reddit Sentiment Tracker: Canadian Politics Edition project. This end-to-end Sentiment Analysis project focuses on Canadian politics, analyzing sentiment from Reddit posts related to the Trudeau and Poilievre governments.

<p float="left">
  <img src="https://github.com/yashspatel/canadian-politics-sentiment-analysis/blob/main/ETL%20flow.jpg" width="300" />
</p>

### Key Features
Live Data Fetching: The project is designed to fetch live data, ensuring that sentiment analysis is continually updated to reflect the most recent opinions on Reddit.

ETL Process: Leveraging ETL (Extract, Transform, Load) principles, the project seamlessly extracts data from Reddit, transforms it using the NLTK library for sentiment analysis, and loads it into AWS Athena for further analysis.

PowerBI Dashboard: A comprehensive PowerBI dashboard provides real-time insights into public sentiment towards Canadian political figures. The dashboard includes KPIs, extreme sentiments, and comparisons between Trudeau and Poilievre.

### Screenshots

<p float="left">
  <img src="https://raw.githubusercontent.com/yashspatel/canadian-politics-sentiment-analysis/main/Raw%20data.jpg" width="300" />
  <p>
    Raw Data: Scraped and stored in AWS S3.
  </p>
  <img src="https://github.com/yashspatel/canadian-politics-sentiment-analysis/blob/main/Transformed%20data.jpg" width="300" />
  <p>
    Transformed Data: Cleaned and transformed using pandas and nltk (VADER, punkt) and stored in AWS S3.
  </p>
  
</p>
<p float="left">
  <img src="https://github.com/yashspatel/canadian-politics-sentiment-analysis/blob/main/Dashboard.jpg" width="300" />
  <p>
    Dashboard: Designed and visualized in Microsoft PowerBI.
  </p>
</p>
This is the screenshot of an output video, where the model is detecting the weed from the other type of grass by bounding boxes. 

### Technologies Used
[*AWS Lambda*](https://aws.amazon.com/lambda/)<br>
[*AWS S3*](https://aws.amazon.com/s3/)<br>
[*NLTK Library*](https://www.nltk.org/)<br>
[*PowerBI*](https://www.microsoft.com/en-us/power-platform/products/power-bi)<br>
[*AWS Athena*](https://aws.amazon.com/athena/)<br>

## *Contact*

To learn more about this project and others:

- [*Portfolio*](https://yashspatel.netlify.app/)
- [*LinkedIn*](https://www.linkedin.com/in/yashsanjaykumarpatel/)
- [*GitHub*](https://github.com/yashspatel)
- [*My Resume*](https://yashspatel.netlify.app/images/Yash's%20Resume.pdf) 
