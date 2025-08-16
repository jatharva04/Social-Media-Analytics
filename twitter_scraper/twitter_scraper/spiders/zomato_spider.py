import scrapy
import pandas as pd
import re
from pathlib import Path
from scrapy_playwright.page import PageMethod

def should_abort_request(request):
    if request.resource_type in ("image", "stylesheet", "font", "media"):
        return True
    if re.search(r"(google-analytics\.com|googlesyndication\.com|doubleclick\.net)", request.url):
        return True
    return False

class ZomatoSpiderSpider(scrapy.Spider):
    name = "zomato_spider"
    TWEET_LIMIT = 200

    def __init__(self, *args, **kwargs):
        super(ZomatoSpiderSpider, self).__init__(*args, **kwargs)
        self.scraped_items_count = 0
        self.output_file = 'zomato_tweets.json'
        self.seen_tweet_ids = set()

        if Path(self.output_file).exists():
            self.logger.info(f"Reading existing tweets from {self.output_file}")
            try:
                df = pd.read_csv(self.output_file)
                if 'tweet_id' in df.columns:
                    self.seen_tweet_ids = set(df['tweet_id'].astype(str))
                self.logger.info(f"Loaded {len(self.seen_tweet_ids)} existing tweet IDs.")
            except Exception as e:
                self.logger.error(f"Could not read existing CSV: {e}")

    def start_requests(self):
        search_query = "zomato"
        # UPDATED: Removed '&f=live' to get "Top" tweets instead of "Latest"
        url = f"https://x.com/search?q={search_query}&src=typed_query"
        
        yield scrapy.Request(
            url,
            meta=dict(
                playwright=True,
                playwright_include_page=True,
                playwright_page_coroutines=[
                    PageMethod("route", "**/*", should_abort_request)
                ],
                errback=self.errback,
            )
        )

    async def errback(self, failure):
        self.logger.error(repr(failure))
        page = failure.request.meta["playwright_page"]
        await page.close()

    async def parse(self, response):
        page = response.meta["playwright_page"]
        
        try:
            await page.wait_for_selector('div[data-testid="tweetText"]', timeout=60000)
            self.logger.info("Tweet text found. Starting main scrape loop...")
        except Exception:
            self.logger.error("Tweets did not load in time. Taking a screenshot and closing.")
            await page.screenshot(path="failure_screenshot.png")
            await page.close()
            return

        empty_scroll_attempts = 0
        while self.scraped_items_count < self.TWEET_LIMIT:
            tweet_articles = await page.query_selector_all('article[data-testid="tweet"]')
            
            new_tweets_found_this_scroll = 0
            for tweet_element in tweet_articles:
                try:
                    # --- NEW CONSOLIDATED LOGIC ---
                    # 1. Find the one reliable link that contains the timestamp and ID.
                    permalink_element = await tweet_element.query_selector('a[href*="/status/"]')
                    permalink = await permalink_element.get_attribute('href') if permalink_element else None

                    # If we can't find this link, we can't identify the tweet, so we skip it.
                    if not permalink:
                        continue

                    # 2. Extract everything from this one link.
                    parts = permalink.split('/')
                    author = f"@{parts[1]}"
                    tweet_id = parts[3].split('?')[0]
                    tweet_url = f"https://x.com{permalink}"

                    if tweet_id in self.seen_tweet_ids:
                        continue
                    
                    text_content_element = await tweet_element.query_selector('div[data-testid="tweetText"]')
                    if not text_content_element: continue
                    text = await text_content_element.inner_text()
                    
                    likes_element = await tweet_element.query_selector('button[data-testid="like"] span[data-testid="app-text-transition-container"] span')
                    retweet_button = await tweet_element.query_selector('button[data-testid="retweet"]')
                    retweets_aria_label = await retweet_button.get_attribute('aria-label') if retweet_button else ''
                    retweets = '0'
                    if retweets_aria_label:
                        found_numbers = re.search(r'(\d[\d,]*)', retweets_aria_label)
                        if found_numbers:
                            retweets = found_numbers.group(1).replace(',', '')
                    
                    hashtags = [await link.inner_text() for link in await text_content_element.query_selector_all('a') if (await link.inner_text() or '').startswith('#')]
                    self.seen_tweet_ids.add(tweet_id)
                    self.scraped_items_count += 1
                    new_tweets_found_this_scroll += 1
                    
                    yield {
                        'tweet_id': tweet_id,
                        'tweet_url': tweet_url,
                        'author': author,
                        'text': text,
                        'likes': await likes_element.text_content() if likes_element else '0',
                        'retweets': retweets,
                        'hashtags' : hashtags if hashtags else None
                    }

                    if self.scraped_items_count >= self.TWEET_LIMIT:
                        break
                except Exception:
                    continue
            
            if self.scraped_items_count >= self.TWEET_LIMIT:
                break
            
            if new_tweets_found_this_scroll == 0 and len(tweet_articles) > 0:
                empty_scroll_attempts += 1
                if empty_scroll_attempts >= 3:
                    self.logger.info("Ending batch after 3 consecutive empty scrolls.")
                    break
            else:
                empty_scroll_attempts = 0

            self.logger.info(f"Scraped {self.scraped_items_count} new tweets, scrolling for more...")
            await page.evaluate("window.scrollTo(0, document.body.scrollHeight);")
            await page.wait_for_timeout(3000)

        self.logger.info(f"Finished batch. Total new tweets scraped this run: {self.scraped_items_count}.")
        await page.close()
        
        # # --- UPDATED: Dynamic Wait Logic ---
        #     self.logger.info(f"Scraped {self.scraped_items_count} new tweets, scrolling for more...")
            
        #     # 1. Get the current number of tweets on the page
        #     previous_tweet_count = len(tweet_articles)
            
        #     # 2. Scroll down
        #     await page.evaluate("window.scrollTo(0, document.body.scrollHeight);")

        #     # 3. Wait for the number of tweets to INCREASE
        #     try:
        #         # This JavaScript function waits until the number of tweets is greater than our previous count.
        #         wait_function = f"() => document.querySelectorAll('article[data-testid=\"tweet\"]').length > {previous_tweet_count}"
        #         # Wait up to 10 seconds for this to happen
        #         await page.wait_for_function(wait_function, timeout=10000) 
        #     except Exception:
        #         self.logger.warning("Scroll did not load new tweets in time. Ending batch.")
        #         break # Exit the loop if no new tweets appear after a scroll

        # self.logger.info(f"Finished batch. Total new tweets scraped this run: {self.scraped_items_count}.")
        # await page.close()