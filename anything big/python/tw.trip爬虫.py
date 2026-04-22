
import requests
from bs4 import BeautifulSoup



# 1. 获取页面HTML
# page_url = 'https://tw.trip.com/events/5208400-south-korea-concerts-collection/?curr=CNY' 
page_url = 'https://tw.trip.com/events/5208400-south-korea-concerts-collection/?curr=CNY'
html = requests.get(page_url).text
soup = BeautifulSoup(html, "lxml")  # 使用lxml解析器

# 2. 获取所有class="item"的div（返回列表）
event_content_items = soup.find_all("div", class_="event-content") 

# 3. 遍历列表，提取每个div的信息
for index, div in enumerate(event_content_items, 1):
    
    event_date = div.find("div", class_="event-date")

    event_title = div.find('h3', class_='event-title').get_text(strip=True)
    event_date = div.find("div", class_="event-date").find('span').get_text(strip=True)
    event_location = div.find("div", class_="event-location").find('span').get_text(strip=True)
    event_tags = div.find("div", class_="event-tags").find('span').get_text(strip=True)

    # print(type(event_title))    
    part1 = event_title.split("·")
    print(event_title)
    print(part1)
    print("/n")
    # part2 = part1.split("|")[0]      <h3 class="event-title">ZB1演唱會2024首爾站 | 首爾奧林匹克體操競技場</h3>
    # print(event_title)
    # print(event_date)
    # print(event_location)
    # print(event_tags)
# 定位目标div（根据class="event-card"筛选）
# event_content = soup.find('div', class_='event-content')








