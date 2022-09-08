import requests
import sys
import csv
import time
from cities_list import cities

def clean_weather_info(weather_info):
  cleaned_weather_info = {}

  # Simplify the forecast retrieved from YR:
  #  * Sunny if cloud fraction <= 50
  #  * Precipitation if there is > 0.1 forecasted for the next hour
  #  * Keep temperature and time
  #  * Throw away everything else

  # Keys are cities' names, values are YR's forecast for the given city
  for k, v in weather_info.items():
    info_item = {}
    info_item["measure_time"] = v["time"]
    info_item["temperature"] = v["data"]["instant"]["details"]["air_temperature"]
    info_item["clear_sky"] = "no" if v["data"]["instant"]["details"]["cloud_area_fraction"] > 50 else "yes"
    info_item["precipitation"] = "yes" if v["data"]["next_1_hours"]["details"]["precipitation_amount"] > 0.1 else "no"
    cleaned_weather_info[k] = info_item

  return cleaned_weather_info


def fetch_weather_info():
  
  weather_info = {}
  
  # YR requires an agent that identifies itself
  headers = {
    'User-Agent': 'Dmytrotestapp dmykarp@yahoo.com'
  }
  
  for city in cities:
    query = { "lat": city["latitude"], "lon": city["longitude"] }
    response = requests.get("https://api.met.no/weatherapi/locationforecast/2.0/compact", headers=headers, params=query)
    if response.status_code != 200:
      sys.exit(1)
    resp_body = response.json()
    weather_info[city["city_name"]] = resp_body["properties"]["timeseries"][0]

  return clean_weather_info(weather_info)


def main():
  analyzed_locations = {city["city_name"] for city in cities}
  weather_info = fetch_weather_info()

  # Fetch all the centers in locations of interest and write their codes to a separate file, 
  # alongside with the weather info in the corresponding city
  with open('ClubInformation.csv', encoding='utf-8') as csv_infile, open("ClubInfoWeather_%s.csv" % time.strftime("%Y%m%d-%H%M%S"), "w") as outfile:
    csv_reader = csv.reader(csv_infile, delimiter=',')
    csv_writer = csv.writer(outfile, delimiter=',')
    csv_writer.writerow(["CENTER_CODE", "TIME", "TEMPERATURE", "CLEAR_SKY", "PRECIPITATION"])
    # Skip the header
    next(csv_reader, None)
    for row in csv_reader:
      city = row[1]
      if city in analyzed_locations:
        club_code = row[0]
        # We only need club codes to correlate weather with visits
        csv_writer.writerow([club_code, weather_info[city]["measure_time"],
                            weather_info[city]["temperature"], weather_info[city]["clear_sky"], 
                            weather_info[city]["precipitation"]])

if __name__ == "__main__":
    main()
