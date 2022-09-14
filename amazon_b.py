import urllib3
urllib3.disable_warnings()

import concurrent.futures

import csv

import random
from lxml import html
, loads
from requests import get
from bs4 import BeautifulSoup
from re import sub
import pandas as pd

from time import sleep
from datetime import datetime

review_total_pages = []
headers = {}
urllib3.disable_warnings()

def parse_json_to_csv(name, json):from json import dump
    with open(f"{name}.csv", mode='w', newline='', encoding="utf-8") as file:
        employee_writer = csv.writer(file, delimiter=',')
        employee_writer.writerow(json['reviews'][0].keys())
        for data in json['reviews']:
            employee_writer.writerow(data.values())


def get_random_user_agent():
    user_agent_list = [
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.103 Safari/537.36',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/72.0.3626.121 Safari/537.36',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/71.0.3578.80 Safari/537.36',
        'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/71.0.3578.80 Safari/537.36',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/71.0.3578.98 Safari/537.36',
        'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/71.0.3578.98 Safari/537.36',
        'Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/71.0.3578.98 Safari/537.36',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.102 Safari/537.36',
        'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.102 Safari/537.36',
        'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.67 Safari/537.36'
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.67 Safari/537.36'
        'Mozilla/5.0 (Windows NT 6.3; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.102 Safari/537.36',
        'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.67 Safari/537.36',
        'Mozilla/5.0 (Windows NT 6.3; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.102 Safari/537.36',
        'Mozilla/5.0 (Windows NT 6.3; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.67 Safari/537.36',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.77 Safari/537.36',
        'Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.67 Safari/537.36']
    return random.choice(user_agent_list)


def get_summary(asin):
    amazon_url = 'https://www.amazon.in/product-reviews/' + asin + '/ref=cm_cr_getr_d_paging_btm_next_16?ie=UTF8&reviewerType=avp_only_reviews&sortBy=recent&pageNumber=1'
    headers = {'User-Agent': get_random_user_agent()}
    for i in range(5):
        response = get(amazon_url, headers=headers, verify=False, timeout=30)
        if response.status_code == 404:
            return {"url": amazon_url, "error": "page not found"}
        if response.status_code != 200:
            continue

        # Removing the null bytes from the response.
        cleaned_response = response.text.replace('\x00', '')
        soup = BeautifulSoup(cleaned_response, 'html.parser')
        product_name = soup.find('a', attrs={"data-hook": "product-link"}).text
        overall_rating = soup.find('span', attrs={"data-hook": "rating-out-of-text"}).text
        number_reviews = soup.find('div', attrs={"data-hook": "total-review-count"}).find('span').text
        res = [str(i) for i in number_reviews if i.isdigit()]
        number_reviews = int("".join(res))
        number_page_reviews = 20
        #number_page_reviews = min(int(number_reviews/10),10);
        print(number_page_reviews)
        rating_table_rows = soup.find('table', id='histogramTable').findChildren(['tr'])
        ratings_dict = {}
        for ratings in rating_table_rows:
            x = ratings.findChildren(['a'])
            ratings_dict[x[0].text.strip()] = x[2].text.strip()

        ls = soup.find('div', attrs={'id': "cm_cr-review_list"}).findChildren('div', attrs={'data-hook': "review"})
        reviewList = []
        for l in ls:
            dic = {}
            dic['name'] = l.find('span', attrs={'class': "a-profile-name"}).get_text().strip()
            dic['rating'] = l.find('i', attrs={'data-hook': "review-star-rating"}).get_text().strip()
            dic['title'] = l.find('a', attrs={'data-hook': "review-title"}).get_text().strip()
            dic['date'] = l.find('span', attrs={'data-hook': "review-date"}).get_text().strip()
            dic['review_body'] = l.find('span', attrs={'data-hook': "review-body"}).get_text().strip()
            reviewList.append(dic)
        return  product_name, number_reviews,overall_rating, ratings_dict, number_page_reviews, headers ,reviewList


def get_review(url,headers):
    response = get(url, headers=headers, verify=False, timeout=30)
    if response.status_code == 404:
        return []
    if response.status_code != 200:
        return []
    try:
        soup = BeautifulSoup(response.text, 'html.parser')
        ls = soup.find('div', attrs={'id': "cm_cr-review_list"}).findChildren('div', attrs={'data-hook': "review"})
        reviewList = []
        for l in ls:
            dic = {}
            dic['name'] = l.find('span', attrs={'class': "a-profile-name"}).get_text().strip()
            dic['rating'] = l.find('i', attrs={'data-hook': "review-star-rating"}).get_text().strip()
            dic['title'] = l.find('a', attrs={'data-hook': "review-title"}).get_text().strip()
            dic['date'] = l.find('span', attrs={'data-hook': "review-date"}).get_text().strip()
            dic['review_body'] = l.find('span', attrs={'data-hook': "review-body"}).get_text().strip()
            reviewList.append(dic)
        return reviewList
    except Exception as e:
        print(e)
        return []

def get_all_reviews(asin):
    try:
        global review_total_pages
        url_list = []
        product_name, number_reviews, overall_rating,ratings_dict, number_page_reviews, headers,reviewList = get_summary(asin)
        print("**************"+asin+"*******************")
        print (" Name: "+product_name+ " Total No of Review: "+ str(number_reviews) + " Overall Rating: "+ overall_rating)
        for page_number in range(1, number_page_reviews+1):
            amazon_url = 'https://www.amazon.in/product-reviews/' + asin + '/ref=cm_cr_getr_d_paging_btm_next_' + str(
                page_number) + '?ie=UTF8&reviewerType=avp_only_reviews&sortBy=recent&pageNumber=' + str(page_number)
            url_list.append(amazon_url)
        all_review = []
        all_review.extend(reviewList)
        for url in url_list:
            print("Running for url ",url)
            reviewList = get_review(url, headers)
            print("Reviews found: " + str(len(reviewList)))
            all_review.extend(reviewList)
            sleep(10)
        df = pd.DataFrame(all_review)
        print("Total Reviews found: " + str(len(all_review)))
        now = datetime.now()
        timestr = now.strftime("%Y%m%d-%H%M%S")
        df.to_csv(asin+"_"+timestr+".csv")
        print("-------------Complete--------------------------")
    except Exception as e:
        print(e)




def core():
    try:
        data = {'asin_list': ['B07HJGMPJQ'],
                'format': 'csv'}
        if data['format'] == 'csv':
            for asin in data['asin_list']:
                print(f"IN PROCESS FOR: {asin}")
                response = get_all_reviews(asin)
                sleep(200)
                print("starting")
                parse_json_to_csv(asin, response)
                print("done")
        else:
            for asin in data['asin_list']:
                print(f"IN PROCESS FOR: {asin}")
                temp = get_all_reviews(asin)
                f = open(asin + '.json', 'w')
                dump(temp, f, indent=4)
                f.close()

        return f'Success download {data["format"]} file in root directory'
    except Exception as e:
        return f'Error: {e}'


if __name__ == '__main__':
    core()