# TedX-Talks-Dat-Analysis
 The TedX Talks Data Analysis repository offers code, scripts, and resources for exploring and gaining insights from the extensive TedX Talks data. It assists data scientists, researchers, and enthusiasts in visualizing and drawing meaningful conclusions from the diverse content of TedX presentations.


TED Talks Analytics using Spark in Scala

This repository contains code for performing analytics on TED Talks dataset using Apache Spark in Scala. The analytics provide insights into various aspects of the TED Talks, such as popularity based on views, like-to-view ratio, frequency analysis by month, speaker analysis, and related talks based on tags.

Dataset

The analysis is performed on a TED Talks dataset, which is provided as a CSV file. The dataset contains information about various TED Talks, including the title, speaker, date, views, likes, and tags.

Prerequisites

To run the analysis, you need to have the following software installed:
Apache Spark (version X.X.X)
Scala (version X.X.X)

Getting Started

Clone the repository to your local machine or download the code files.
Make sure you have the TED Talks dataset file (dataset.csv) available. You can place it in the project directory or provide the path to the file in the code.
Open the code file (Code.scala) in your preferred Scala IDE or editor.

Analysis Overview

Analysis 1: Finding the most popular TED talks based on views

Identifies the top 10 TED talks with the highest views.
Displays the titles of the most popular talks.

Analysis 2: Number of above-average talks based on views

Calculates the average number of views for all talks.
Counts the number of talks with views above the average.

Analysis 3: Most popular speakers based on the number of talks

Determines the most popular TED talks speakers based on the number of talks delivered.
Displays the top speakers and the respective number of talks they have given.

Analysis 4: TED talks with the best view-to-like ratio

Calculates the view-to-like ratio for each talk.
Retrieves the top 10 talks with the highest view-to-like ratio.

Analysis 5: Month-wise analysis of TED talk frequency

Analyzes the frequency of TED talks by month.
Displays the number of talks for each month in chronological order.

Analysis 6: Finding the most popular TED talks speaker

Determines the most popular TED talks speaker based on the cumulative number of views for their talks.
Displays the top speakers and the respective cumulative views.

Analysis 7: Related talks based on user-defined liked titles

Allows users to input a liked title and find related talks based on shared tags.
Displays the speakers who have given talks related to the liked title and the respective number of talks they have given.

Running the Analysis

Set up the Spark configuration by providing the appropriate Spark master URL and application name in the code.

Load the TED Talks dataset as an RDD.

Run the desired analysis by uncommenting the corresponding code sections. Each analysis is labeled with a comment indicating its purpose.

Customize the analysis as per your requirements by modifying the code and adding additional logic or transformations.

Execute the code to perform the analysis.

Analysis Results

The analysis results will be displayed in the console output. The results include information such as the most popular TED talks based on views, TED talks with the best view-to-like ratio, month-wise analysis of TED talk frequency, most popular TED talks speakers, and related talks based on user-defined liked titles.

Conclusion

This repository provides a Scala implementation of analytics on the TED Talks dataset using Apache Spark. The code can be used as a starting point for further exploration and customization of the analysis. Feel free to modify and extend the code to suit your specific requirements.

Please refer to the code comments and the documentation within the code for more detailed explanations of each analysis and their implementation.

Note: Make sure to replace "path/to/data.csv" with the actual path to your dataset file before running the code.

References

https://spark.apache.org/docs/latest/
https://docs.scala-lang.org/
https://www.kaggle.com/datasets/ashishjangra27/ted-talks (Kaggle)
