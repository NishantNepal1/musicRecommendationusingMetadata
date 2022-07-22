import requests
import json
from bs4 import BeautifulSoup

send_url = "http://api.ipstack.com/check?access_key={<key>}"
geo_req = requests.get(send_url)
geo_json = json.loads(geo_req.text)
city = geo_json['city']

print(city)

# creating url and requests instance
url = "https://www.google.com/search?hl=en&q="+"weather"+city+"&oq="+"weather"+city
html = requests.get(url).content
 
# getting raw data
soup = BeautifulSoup(html, 'html.parser')
temp = soup.find('div', attrs={'class': 'BNeawe iBp4i AP7Wnd'}).text
str = soup.find('div', attrs={'class': 'BNeawe tAd8D AP7Wnd'}).text
 
# formatting data
data = str.split('\n')
time = data[0]
sky = data[1]
 
# getting all div tag
listdiv = soup.findAll('div', attrs={'class': 'BNeawe s3v9rd AP7Wnd'})
strd = listdiv[5].text
 
# getting other required data
pos = strd.find('Wind')
other_data = strd[pos:]
 
# printing all data
print("Temperature is", temp)
print("Time: ", time)
print("Sky Description: ", sky)