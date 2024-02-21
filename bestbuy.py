import scrapy

class BestBuy(scrapy.Spider):
    name = 'bestbuy'
    start_urls = ["https://www.bestbuy.ca/en-ca/category/laptops-macbooks/20352?icmp=computing_evergreen_computers_and_tablets_category_landing_category_icon_shopby_laptops_and_macbooks",
                  "https://www.bestbuy.ca/en-ca/category/televisions/21344?icmp=hta_categorydetail_shopby_tvs",
                  "https://www.bestbuy.ca/en-ca/category/cell-phones-plans/696304",
                  "https://www.bestbuy.ca/en-ca/category/smart-home/30438?icmp=home_shopbycategory_sh",
                  "https://www.bestbuy.ca/en-ca/category/wearable-technology/34444?icmp=home_shopbycategory_wearables"
                  ]

    def parse(self, response):
        # Extracting laptop product containers
        product_containers = response.css('div[class*="productListItem"]')


        category = response.css('h1[class*="title"]::text').get()
        
        for container in product_containers:
            # Extract product details
            product_name = container.css('div.productItemName_3IZ3c::text').get()
            # Extract product price
            product_price_text = container.css('div.price_2j8lL div::text').get()
            # Using a conditional operator to handle the case when the element is not found
            product_price = float(product_price_text.replace('$', '').replace(',', '')) if product_price_text is not None else None
            product_rating = container.css('span[data-automation="rating-count"]::text').get()
            product_reviews = container.css('meta[itemprop="reviewCount"]::attr(content)').get(),
            product_saving = container.css('span.productSaving_3T6HS::text').get()

            # Create a dictionary for the product and yield it
            product_info = {
                'Category': category,
                'Name': product_name,
                'Price': product_price,
                'Rating': product_rating,
                'Reviews': product_reviews,
                'Savings': product_saving
            }
            yield product_info

        
