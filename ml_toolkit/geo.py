import requests


def lnglat2add(lng, lat):
    parameter = {'latlng': "%s,%s" % (lat, lng)}
    r = requests.get('https://maps.googleapis.com/maps/api/geocode/json', params=parameter)
    r = r.json()
    if len(r['results']) == 0:
        return None
    return r['results'][0]['formatted_address']


def lnglat2city(lng, lat):
    parameter = {'latlng': "%s,%s" % (lat, lng)}
    r = requests.get('https://maps.googleapis.com/maps/api/geocode/json', params=parameter)
    r = r.json()
    if len(r['results']) == 0:
        return None
    city_info = r['results'][0]['address_components']
    return ','.join([i['long_name'] for i in city_info])
    # return (city_info[-1]['long_name'], city_info[-2]['long_name'], city_info[-3]['long_name'], city_info[-4]['long_name'])
