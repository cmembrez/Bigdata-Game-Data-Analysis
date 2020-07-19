# Big Data - Online Course

This repo summarize my final project for an online course on Big Data.
Given users data from a video game, the project was about finding options to increase revenue for the company.
I used Splunk, KNIME, Apache Spark in Python, and Neo4J.

## Introduction
The course requirements split the project into four main parts.

The first three parts are about in-game data:
- Exploring the data
- Classification of users through a decision tree
- Finding clusters of users

The last part of the project is about chat data and I applied graph analytic techniques.

## Data Exploration

Splunk allowed me to have a better understanding of the in-game data.
The syntax to search is intuitive and the Splunk Community is extensive and helpful.
By taking extra steps, I created a preliminary dashboard. Please refer to the video and technical appendix
for actual examples (in the folder .\reports).


## Classification

The instructions were to implement a decision tree algorithm using KNIME.
Setting up the workflow was straightforward; it is convenient to add nodes and configure the parameters through 
such a graphic-user-interface.

Data preparation included removing empty data by rows and removing irrelevant column data (e.g. user id).
I created a new column to account for our target: classify the users based on the average price of their purchases.
The data was split into train and test data. The decision tree algorithm used a pruning technique. Finally,
a confusion matrix was generated.

Please refer to the video and technical appendix in the folder .\reports for details and examples.


## Clustering

In a Cloudera VM setup, I used Spark and wrote my code in Python.
You find the Jupyter Notebook (and its .py version) in the folder .\clustering.

## Graph Analytics

While the first three parts are about in-game data, this last part is about chat data. But without any text analysis.
The instructions were to understand the strength and relationships among users. I used Neo4J and its language Cypher.
A few Cypther queries are available in the folder .\graphanalytics.


