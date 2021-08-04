import json
import urllib.parse

import pandas as pd
from jsonpath_ng.ext import parse

import requests


def run_beer_query(beer_name: str, brewery_name: str):
    url = "https://9wbo4rq3ho-dsn.algolia.net/1/indexes/*/queries?x-algolia-agent=Algolia%20for%20vanilla%20JavaScript%203.24.7&x-algolia-application-id=9WBO4RQ3HO&x-algolia-api-key=1d347324d67ec472bb7132c66aead485&referer=https%3A%2F%2Fios.untappd.com&x-untappd-app-version=3.5.4"

    beer = urllib.parse.quote(f"{brewery_name} {beer_name}")

    payload = json.dumps({"requests": [
        {"indexName": "beer", "params": f"query={beer}&hitsPerPage=25&clickAnalytics=true&page=0&analytics=true"}]})
    headers = {
        'Content-Type': 'application/x-www-form-urlencoded',
        'User-Agent': 'Mozilla/5.0 (iPhone; CPU iPhone OS 14_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Mobile/15E148/pwpvdqoehluosjquguhokzarecyfgxfx',
        'Origin': 'ionic://untappd'
    }

    response = requests.request("POST", url, headers=headers, data=payload)
    result = response.json()
    # print(json.dumps(result, indent=4))
    if result:
        beer_match = parse(
            '$.results[0].hits[?(@._highlightResult.beer_index.matchLevel == "full")]')
        ben = beer_match.find(result)
        if len(ben) > 0:
            return ben[0].value['bid']
        else:
            return None


def get_beer_untappd(beer_id, assumed_beer_name):
    if beer_id is None:
        return dict(csv_name=assumed_beer_name, data="couldn't locate on untappd")

    url = f"https://api.untappd.com/v4/beer/info/{beer_id}?access_token=875B901EEB88A25C9E8D4F50529462714DFD1FE1&compact=true"

    payload = {}
    headers = {}

    response = requests.request("GET", url, headers=headers, data=payload)

    result = response.json()

    # print(json.dumps(result, indent=4))
    beer = result['response']['beer']
    name = beer['beer_name']
    abv = beer['beer_abv']
    description = beer['beer_description']
    rating_score = beer['rating_score']
    rating_count = beer['rating_count']
    beer_img = beer['beer_label_hd']

    beer_dict = dict(csv_name=assumed_beer_name, name=name, abv=abv, description=description, beer_id=beer_id,
                     rating_score=rating_score, rating_count=rating_count, beer_img=beer_img)

    return beer_dict


def run_brewery_query(brewery_name: str):
    url = "https://9wbo4rq3ho-dsn.algolia.net/1/indexes/*/queries?x-algolia-agent=Algolia%20for%20vanilla%20JavaScript%203.24.7&x-algolia-application-id=9WBO4RQ3HO&x-algolia-api-key=1d347324d67ec472bb7132c66aead485&referer=https%3A%2F%2Fios.untappd.com&x-untappd-app-version=3.5.4"

    brewery = urllib.parse.quote(brewery_name)
    # print(brewery)
    payload = json.dumps({"requests": [{"indexName": "brewery",
                                        "params": f"query={brewery}&hitsPerPage=25&clickAnalytics=true&page=0&analytics=true"}]})
    # print(payload)
    headers = {
        'Content-Type': 'application/json'
    }

    response = requests.request("POST", url, headers=headers, data=payload)
    # print(json.dumps(response.json(), indent=4))
    # parsed = json.loads(response.json())
    result = response.json()
    if result:
        # Don't Ask! Something dodgy is happening with North Brewing
        if brewery_name == "North Brewing Co.":
            brewery_match = parse(
                '$.results[0].hits[?(@._highlightResult.brewery_index.matchLevel == "full" & @._highlightResult.brewery_name.matchLevel == "full" & @._highlightResult.brewery_index.fullyHighlighted == true)]')
        else:
            brewery_match = parse(
                '$.results[0].hits[?(@._highlightResult.brewery_index.matchLevel == "full" & @._highlightResult.brewery_name.matchLevel == "full")]')

        ben = brewery_match.find(result)
        if len(ben) > 0:
            return ben[0].value['brewery_id']
        else:
            return None


def get_brewery_untappd(brewery_id, assumed_brewery_name):
    url = f"https://api.untappd.com/v4/brewery/info/{brewery_id}?access_token=875B901EEB88A25C9E8D4F50529462714DFD1FE1"

    payload = {}
    headers = {}

    response = requests.request("GET", url, headers=headers, data=payload)

    result = response.json()

    # print(json.dumps(result, indent=4))
    brewery = result['response']['brewery']
    name = brewery['brewery_name']
    follower_count = brewery['claimed_status']['follower_count']
    total_votes = brewery['rating']['count']
    rating_score = brewery['rating']['rating_score']
    popularity_this_week = brewery['stats']['weekly_count']
    location_city = brewery['location']['brewery_city']
    brewery_img = brewery['brewery_label_hd']

    brewery_dict = dict(csv_name=assumed_brewery_name, name=name, brewery_id=brewery_id, follower_count=follower_count,
                        total_votes=total_votes,
                        rating_score=rating_score, popularity_this_week=popularity_this_week,
                        location_city=location_city, brewery_img=brewery_img)

    # print(brewery_dict)
    return brewery_dict


def load_csv(csv_location):
    df = pd.read_csv(csv_location, na_values=['N/A', ''])
    df = df.iloc[:, :-1]
    # print(df.head(25))
    breweries = df['brewery'].unique()
    # print(breweries)

    brewery_list = []
    for brewery in breweries:
        brew_data = brewery_data(brewery)
        brewery_lines = df.loc[df['brewery'] == brewery]
        beer_list = []
        for beer in brewery_lines['beer_name']:
            beer_id = run_beer_query(beer, brew_data['name'])
            if beer_id is None:
                beer_id = run_beer_query(beer, brewery)
                if beer_id is None:
                    print(f"Couldn't find beer: {beer} from {brew_data['name']}")

            beer_list.append(get_beer_untappd(beer_id, beer))
        brew_data['beers'] = beer_list
        brewery_list.append(brew_data)

    beer_list = []
    for entry in brewery_list:
        beers = entry['beers']
        # ben = dict(brewery=entry['name'])
        for beer in beers:
            mydict = dict(brewery=entry['name'])
            if 'name' in beer:
                anotherdict = dict(beer_name=beer['name'], abv=beer['abv'], description=beer['description'],
                                   rating=beer['rating_score'], rating_count=beer['rating_count'], img=beer['beer_img'])
            else:
                anotherdict = dict(beer_name=beer['csv_name'], abv=0, description="Unavailable", rating=0,
                                   rating_count=0, img="Unavailable")
            beer_list.append({**mydict, **anotherdict})

    return brewery_list, beer_list


def brewery_data(brewery_name):
    brewery_id = run_brewery_query(brewery_name)
    data = {}
    if brewery_id is not None:
        data = get_brewery_untappd(brewery_id, brewery_name)
        data['beers'] = []
    return data


if __name__ == '__main__':
    load_csv("beer.csv")
