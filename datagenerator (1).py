import json
from concurrent.futures import ThreadPoolExecutor
from requests.exceptions import RequestException
from bs4 import BeautifulSoup
from datetime import datetime
from azure.eventhub import EventHubProducerClient, EventData
import requests
import time
import base64
#event hub details
eventhub_namespace = "ebayeventhub0908"
eventhub_name = "ebay0908"
shared_access_key_name = "ebaypolicy0908"
shared_access_key = "pTTkv564U4CkRv0zDvea8lv062mjGq09F+AEhOgSDMI="

current_date = datetime.now().strftime("%Y-%m-%d")

def create_eventhub_producer():
    try:
        # Try to create an instance of the Event Hub producer
        producer = EventHubProducerClient.from_connection_string(
            f"Endpoint=sb://{eventhub_namespace}.servicebus.windows.net/;SharedAccessKeyName={shared_access_key_name};SharedAccessKey={shared_access_key}",
            eventhub_name=eventhub_name
        )
        return producer
    except Exception as e:
        print(f"Error creating Event Hub producer: {e}")
        return None

def check_eventhub_credentials():
    producer = create_eventhub_producer()
    if producer:
        print("Event Hub credentials are valid. Proceeding further.")
        producer.close()

def get_webpage_content(url: str, max_retries=2, timeout=10) -> str:
    for attempt in range(max_retries):
        try:
            response = requests.get(url, timeout=timeout)
            response.raise_for_status()

            if response.is_redirect:
                url = response.headers['location']
                print(f"Redirecting to: {url}")
                continue

            print(f"Final URL: {url}")
            return response.text

        except requests.exceptions.HTTPError as e:
            print(f"HTTPError: {e}")
            if e.response.status_code == 404:
                print("Page not found.")
            return ""
        except requests.exceptions.RequestException as e:
            print(f"RequestException: {e}")
            return ""

def extract_href_from_soup(soup, class_name):
    elements = soup.find_all("a", class_=class_name)
    hrefs = [element.get("href") for element in elements]
    return hrefs



def productDetails(url, product, page_number):
    try:
        content = get_webpage_content(url)
        item_number_title = None
        item_number = None
        soup = BeautifulSoup(content, 'html.parser')
        title_span = soup.find('span', class_='ux-textspans ux-textspans--BOLD')
        title = title_span.text.strip() if title_span else None
        price_div = soup.find('div', class_='x-price-primary')
        
        item_number_div = soup.find('div', class_='ux-layout-section__textual-display ux-layout-section__textual-display--itemId')
        if item_number_div:
            item_number_title_span = item_number_div.find('span', class_='ux-textspans ux-textspans--SECONDARY')
            item_number_title = item_number_title_span.text.strip()
            item_number_span = item_number_div.find('span', class_='ux-textspans ux-textspans--BOLD')
            item_number = int(item_number_span.text.strip())
        
        price = None
        if price_div:
            price_text_span = price_div.find('span', class_='ux-textspans')
            price_split = price_text_span.text.split('/')[0].split(" ")[1].replace('$','')

            price = price_split if price_split else None

        condition_element = soup.find('div', class_='x-item-condition-text').find('span', class_='ux-textspans')
        condition_text = condition_element.get_text(strip=True)

        main_div = soup.find('div', class_='ux-layout-section-module-evo')
        data_dict = {}
        if main_div:
            rows = main_div.find_all('div', class_='ux-layout-section-evo__row')
            if rows:
                for row_index, row in enumerate(rows, start=1):
                    cols = row.find_all('div', class_='ux-layout-section-evo__col')
                    if cols:
                        for col in cols:
                            labels_div = col.find('div', class_='ux-labels-values__labels-content')
                            values_div = col.find('div', class_='ux-labels-values__values-content')
                            labels_text = labels_div.find('span', class_='ux-textspans').text.strip() if labels_div else None
                            values_text = values_div.find('span', class_='ux-textspans').text.strip() if values_div else None
                            if values_text is not None:
                                value_split = values_text.split(' ')
                                if (len(value_split)>1) and value_split[1] in ['lbs', 'in', 'GB', 'MB', 'TB', 'GHz','pounds','lb','g','kg']:
                                    values_text = value_split[0]
                                elif values_text:
                                    for unit in ['lbs', 'in', 'GB', 'MB', 'TB', 'GHz','pounds','lb','g','kg']:
                                        values_text = values_text.replace(unit, '')
                                else:
                                    values_text
                            else :
                                values_text

                            if labels_text != "Condition":
                                data_dict[labels_text] = values_text
        else:
            print("Main div not found. Check HTML structure.")

        product_det = {
            "Product": product,
            "PageNumber": page_number,
            item_number_title: item_number,
            "Title": title,
            "Price (In Dollars)": price,
            "fetchDate": current_date,
            "Condition":condition_text
        }

        product_det.update(data_dict)
        return product_det
    except Exception as e:
        print(f"Error extracting product details from {url}: {e}")
        return {}

def send_events_in_batches(producer, futures, batch_size=20):
    with producer as eventhub_producer:
        for i in range(0, len(futures), batch_size):
            batch_futures = futures[i:i + batch_size]
            event_data_batch = eventhub_producer.create_batch()
            
            for future in batch_futures:
                product_data = future.result()
                if not product_data:
                    continue
                cleaned_data = {k: v for k, v in product_data.items() if v is not None and k is not None}
                print(cleaned_data)
                my_data = json.dumps(cleaned_data,indent=2,allow_nan=False)
                utf8_encoded_data = my_data.encode('utf-8')
                base64_encoded_data = base64.b64encode(utf8_encoded_data)

                event_data_batch.add(EventData(my_data))
            
            time.sleep(3)
            eventhub_producer.send_batch(event_data_batch)
            print("Batch is send")

def scrape_product_pages(product, start_page, end_page):
    try:
        with ThreadPoolExecutor(max_workers=6) as executor:
            futures = []
            for page_number in range(start_page, end_page + 1):
                ebay_url = f"https://www.ebay.com/sch/i.html?_from=R40&_nkw={product}&_sacat=0&_ipg=240&_pgn={page_number}"
                content = get_webpage_content(ebay_url)
                soup = BeautifulSoup(content, 'lxml')
                hrefs = extract_href_from_soup(soup, "s-item__link")

                for href in hrefs:
                    future = executor.submit(productDetails, href, product, page_number)
                    futures.append(future)

                # Sending data as a batch
                with create_eventhub_producer() as producer:
                    send_events_in_batches(producer, futures)
                futures.clear()

    except RequestException as e:
        print(f"Error accessing eBay pages for {product}: {e}")

if __name__ == "__main__":
    check_eventhub_credentials()
    products_item = ["laptops", "smartphones", "tablets", "smartwatches", "trimmers"]
    start_page = 1
    end_page = 45
    
    try:
        for product in products_item:
            time.sleep(5)
            scrape_product_pages(product, start_page, end_page)
            
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
