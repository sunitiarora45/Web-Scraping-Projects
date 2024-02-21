import json
import pymysql
import pymongo
from cassandra.cluster import Cluster
import os
import time
import uuid

# Get the current working directory
current_directory = os.getcwd()
print("Current Directory:", current_directory)

# MySQL database connection
connection_mysql = pymysql.connect(
    host='127.0.0.1',
    user='root',
    password='Scraped_Data',
    db='scraped_data',
    charset='utf8mb4',
    cursorclass=pymysql.cursors.DictCursor
)

# MongoDB database connection
client = pymongo.MongoClient("mongodb://root:example@localhost:27017/")
db_mongo = client["scraped_data"]

# Cassandra database connection
cluster = Cluster(['localhost'], port=9042)  # Cassandra cluster's IP address or hostname
session_cassandra = cluster.connect()
session_cassandra.execute("USE scraped_data")  # Cassandra keyspace


try:
    # MySQL: Load Reddit data
    with connection_mysql.cursor() as cursor:
        with open('redditpostsfile.json', 'r') as reddit_file:
            reddit_data = json.load(reddit_file)
        
        for item in reddit_data:
            title = item['title']
            author = item['author']
            post_time = item['time']
            subreddit = item['subreddit']
            initial_post = item['initial_post']

            sql = "INSERT INTO reddit_posts (title, author, post_time, subreddit, initial_post) VALUES (%s, %s, %s, %s, %s)"
            cursor.execute(sql, (title, author, post_time, subreddit, initial_post))

    connection_mysql.commit()

    # MongoDB: Load Reddit data
    reddit_collection = db_mongo["redditposts_coll"]
    reddit_collection.insert_many(reddit_data)


    # Cassandra: Load Reddit data
    for item in reddit_data:
        title = item['title']
        author = item['author']
        post_time = item['time']
        subreddit = item['subreddit']
        initial_post = item['initial_post']

        # Generate a unique UUID for each row
        unique_id = uuid.uuid4()

        query = "INSERT INTO reddit_posts (id, title, author, post_time, subreddit, initial_post) VALUES (%s, %s, %s, %s, %s, %s)"
        session_cassandra.execute(query, (unique_id, title, author, post_time, subreddit, initial_post))

  
    # MySQL: Load Best Buy data
    with open('bestbuyfile.json', 'r') as bestbuy_file:
        bestbuy_data = json.load(bestbuy_file)

    for item in bestbuy_data:
        category = item['Category']
        name = item['Name']
        price = item['Price']
        rating = item['Rating']
        reviews = int(item['Reviews'][0]) if item['Reviews'] and item['Reviews'][0] else None
        savings = item['Savings']

        with connection_mysql.cursor() as cursor:
            sql = f"INSERT INTO bestbuy_products (category, name, price, rating, reviews, savings) VALUES (%s, %s, %s, %s, %s, %s)"
            cursor.execute(sql, (category, name, price, rating, reviews, savings))

    connection_mysql.commit()

    # MongoDB: Load Best Buy data
    bestbuy_collection = db_mongo["bestbuy_coll"]
    bestbuy_collection.insert_many(bestbuy_data)

    # Cassandra: Load Best Buy data
    for item in bestbuy_data:
        category = item['Category']
        name = item['Name']
        price = item['Price']
        rating = item['Rating']
        reviews = int(item['Reviews'][0]) if item['Reviews'] and item['Reviews'][0] else None
        savings = item['Savings']

        # Generate a unique UUID for each row
        bestbuy_unique_id = uuid.uuid4()

        query = "INSERT INTO best_buy_products (id, category, name, price, rating, reviews, savings) VALUES (%s, %s, %s, %s, %s, %s, %s)"
        session_cassandra.execute(query, (bestbuy_unique_id, category, name, price, rating, reviews, savings))

    # MySQL: Search by subreddit
    with connection_mysql.cursor() as cursor:
        subreddit = 'datascience'
        author = 'charlesowo445'
        query = f"SELECT * FROM reddit_posts WHERE subreddit = '{subreddit}' AND author = '{author}' LIMIT 3"
        cursor.execute(query)
        subreddit_results = cursor.fetchall()

    print(f"Posts from subreddit '{subreddit}' (MySQL) by author '{author}':")
    for row in subreddit_results:
        print(row)
    print("--------------------------------------------------------------------------------------------------")

    # MongoDB: Search by subreddit
    subreddit_results_mongo = reddit_collection.find({'subreddit': subreddit, 'author': 'charlesowo445'}).limit(3)
    print(f"Posts from subreddit '{subreddit}' (MongoDB) by author 'charlesowo445':")
    for post in subreddit_results_mongo:
        print(post)
    print("--------------------------------------------------------------------------------------------------")

    # Cassandra: Search by subreddit
    query = f"SELECT * FROM reddit_posts WHERE subreddit = '{subreddit}' AND author = '{author}' LIMIT 3 ALLOW FILTERING"
    results_cassandra = session_cassandra.execute(query)

    print(f"Posts from subreddit '{subreddit}' (Cassandra) by author '{author}':")
    for row in results_cassandra:
        print(row)
    print("--------------------------------------------------------------------------------------------------")

    # MySQL: Finding the average price of Best Buy products
    with connection_mysql.cursor() as cursor:
        query = "SELECT category, AVG(price) AS average_price FROM bestbuy_products GROUP BY category"
        cursor.execute(query)

        # Fetching the results
        results_mysql = cursor.fetchall()

        # Print the results
        print("MySQL - Average Prices for Best Buy Products:")
        for row in results_mysql:
            category = row['category']
            average_price = row['average_price']
            print(f"MySQL - Category: {category}, Average Price: {average_price}")
        print("--------------------------------------------------------------------------------------------------")

    # MongoDB: Finding the average price of Best Buy products
    pipeline = [
        {"$group": {"_id": "$Category", "average_price": {"$avg": "$Price"}}}
    ]

    results_mongodb = list(bestbuy_collection.aggregate(pipeline))

    # Print the results
    print("MongoDB - Average Prices for Best Buy Products:")
    for result in results_mongodb:
        category = result['_id']
        average_price = result['average_price']
        print(f"MongoDB - Category: {category}, Average Price: {average_price}")
    print("--------------------------------------------------------------------------------------------------")

    # Cassandra: Finding the average price of Best Buy products
    query = "SELECT category, AVG(price) AS average_price FROM best_buy_products GROUP BY category"
    results_cassandra = session_cassandra.execute(query)

    print("Cassandra - Average Prices for Best Buy Products:")
    for row in results_cassandra:
        category = row.category
        average_price = row.average_price
        print(f"Cassandra - Category: {category}, Average Price: {average_price}")
    print("--------------------------------------------------------------------------------------------------")

   
finally:
    connection_mysql.close()
    session_cassandra.shutdown()
    cluster.shutdown()



