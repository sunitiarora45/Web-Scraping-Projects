import scrapy

class RedditSpider(scrapy.Spider):
    name = "reddit"
    start_urls = [
        "https://old.reddit.com/r/datascience/",
        "https://old.reddit.com/r/artificial/",
        "https://old.reddit.com/r/machinelearning/",
        "https://old.reddit.com/r/explainlikeimfive/"
    ]
    

    def parse(self, response):
        # Extracting post div elements from the subreddit
        post_divs = response.css('div[class*="thing"]')

        for post_div in post_divs:
            post_link = response.urljoin(post_div.css('::attr(data-url)').get())
           
            post_details = {
                'title': post_div.css('p.title a::text').get(),
                'author': post_div.css('::attr(data-author)').get(),
                'time': post_div.css('time::attr(datetime)').get(),
                'subreddit': post_div.css('::attr(data-subreddit)').get(),
                'initial_post': post_link
            }
            yield post_details

        
