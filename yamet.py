import requests
import jsons
import os

api_v1 = []
logs_api = []

'''
url_sources_direct_platforms = f"https://api-metrika.yandex.net/stat/v1/data?limit=10000&date1=2021-01-01&date2=2022-02-10&ids=86274673&direct_client_logins=ooobbb-373901-kl32&dimensions=ym:s:gender,ym:s:ageInterval,ym:s:interest,ym:s:regionArea,ym:s:deviceCategory,ym:s:regionCity&metrics=ym:s:manPercentage"
url_sources_direct_platforms = requests.get(url=url_sources_direct_platforms, headers={
    'Authorization': f"OAuth AQAAAABcfJZBAAeIuEurAa8cK0dFhSuwv20kzEg"}).json()

path = os.getcwd() + '\\data\\kawo.json'
jsons.write_to_file(content=url_sources_direct_platforms, path=path)

print(url_sources_direct_platforms)
'''