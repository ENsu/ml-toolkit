import requests


def lonlat2add(long, lat):
    parameter = {'latlng': "%s,%s" % (lat, long)}
    r = requests.get('https://maps.googleapis.com/maps/api/geocode/json', params=parameter)
    r = r.json()
    return r['results'][0]['formatted_address']


def lonlat2city(long, lat):
    parameter = {'latlng': "%s,%s" % (lat, long)}
    r = requests.get('https://maps.googleapis.com/maps/api/geocode/json', params=parameter)
    r = r.json()
    city_info = r['results'][0]['address_components']
    return (city_info[-1]['long_name'], city_info[-2]['long_name'], city_info[-3]['long_name'], city_info[-4]['long_name'])
