import json
from pytest import fixture

from django.conf import settings

from dataman import elastic


DEFAULT_FORMAT = 'json'
API_TWEETS = "/api/tweet/"


@fixture
def tweets():
    data = [
        {
            "location": {
                "lat": 35.22709,
                "lon": -80.84313
            },
            "created_at": "2018-06-23T13:31:38",
            "text": "NWS has issued a Flash Flood Guidance  https://t.co/vKuSPSFI33",
            "place": "Atlanta, GA",
            "lang": "en",
            "tweetid": "10",
            "flood_probability": 0.988,
            "country": "USA",
            "tokens": ["ha", "nw", "flood", "issu", "flash", "guidanc"],
            "representative": True
        },
        {
            "location": {
                "lat": 40.43085,
                "lon": -95.42809
            },
            "created_at": "2018-06-23T14:53:11",
            "text": "Event extended (time). Flood Warning from 6/26/2018 2:12 PM CDT until further notice for Atchison County. More information.",
            "place": "St Joseph, MO",
            "lang": "en",
            "tweetid": "11",
            "flood_probability": 0.999,
            "country": "USA",
            "tokens": ["pm", "26", "atchison", "from", "counti", "event", "cdt", "flood", "extend", "more", "warn", "until", "12", "2018", "further", "inform", "time", "notic"],
            "representative": True
        },
        {
            "location": {
                "lat": 42.49604,
                "lon": -96.39066
            },
            "created_at": "2018-06-23T08:14:16",
            "text": "Viet Nam: Litter in canals and sewers worsens floods in City https://t.co/SXQNY7DdDg via @skinnergj",
            "place": "Sydney, New South Wales",
            "lang": "en",
            "tweetid": "12",
            "flood_probability": 0.991,
            "country": "USA",
            "tokens": ["worsen", "litter", "sewer", "via", "flood", "canal", "nam", "citi", "viet"],
            "representative": True
        },
        {
            "location": {
                "lat": 32.73628,
                "lon": -94.94148
            },
            "created_at": "2018-06-23T18:33:05",
            "text": "Since the Flooding occurred in South Texas our home has gone through extensive damage and is it not suitable for living https://t.co/szzVCZsIu5",
            "place": "Mercedes, TX",
            "lang": "en",
            "tweetid": "13",
            "flood_probability": 0.801,
            "country": "USA",
            "tokens": ["gone", "occur", "our", "extens", "ha", "south", "flood", "through", "sinc", "damag", "li", "texa", "suitabl", "home"],
            "representative": True
        },
        {
            "location": {
                "lat": 39.43701,
                "lon": -83.70381
            },
            "created_at": "2018-06-24T10:14:25",
            "text": "Can you guys please rt and spread this link our home was devastated by the floods in south Texas @kkimthai https://t.co/qfZPAxu6lt",
            "place": "Mercedes, TX",
            "lang": "en",
            "tweetid": "14",
            "flood_probability": 0.945,
            "country": "USA",
            "tokens": ["can", "pleas", "our", "spread", "you", "flood", "devast", "link", "texa", "south", "home", "gui"],
            "representative": True
        },
        {
            "location": {
                "lat": 32.73628,
                "lon": -94.94148
            },
            "created_at": "2018-06-23T19:19:51",
            "text": "RT @isaabitch_: Since the Flooding occurred in South Texas our home has gone through extensive damage and is it not suitable for living.",
            "place": "Mercedes, TX",
            "lang": "en",
            "tweetid": "15",
            "flood_probability": 0.876,
            "country": "USA",
            "tokens": ["we", "gone", "occur", "our", "extens", "ha", "live", "south", "flood", "through", "sinc", "damag", "texa", "suitabl", "home"],
            "representative": False
        },
        {
            "location": {
                "lat": 53.478878,
                "lon": -2.238560
            },
            "created_at": "2018-06-23T05:17:21",
            "text": "#PeoplesVoteMarch Today's weather forecast. \nSnowflakes all over Westminster. Floods of tears; boiled piss. https://t.co/PfR4mBjyyR",
            "place": "Manchester, England",
            "lang": "en",
            "tweetid": "16",
            "flood_probability": 0.881,
            "country": "UK",
            "tokens": ["forecast", "piss", "amp", "weather", "PeoplesVoteMarch", "todai", "peoplesvotemarch", "snowflak", "flood", "tear", "boil", "over", "westminst", "all"],
            "representative": True
        },
        {
            "location": {
                "lat": -25.999180,
                "lon": 28.126293
            },
            "created_at": "2018-06-24T11:14:11",
            "text": "RT @NdlovuMatome: Last night a pipe burst in #SoshanvuveBlockAA and @CityTshwane has done nothing about it. Houses have flooded walls and rooms",
            "place": "Midrand, South Africa",
            "lang": "en",
            "tweetid": "17",
            "flood_probability": 0.865,
            "country": "SA",
            "tokens": ["night", "CityOfTshwaneIsUseless", "done", "about", "Tshwane", "burst", "soshanvuveblockaa", "ha", "wall", "SoshanvuveBlockAA", "noth", "flood", "CityOfTshwaneMustFall", "last", "have", "pipe", "hous"],
            "representative": True
        },
        {
            "location": {
                "lat": 42.00027,
                "lon": -93.50049
            },
            "created_at": "2018-06-23T23:32:36",
            "text": "RT @MikeSoron: A derailed BNSF train just dumped 870,000 litres of Alberta crude oil into a flooded Iowa river. https://t.co/ejVzmxh8Zb",
            "place": "Ventura, CA",
            "lang": "en",
            "tweetid": "18",
            "flood_probability": 0.418,
            "country": "USA",
            "tokens": ["ab…", "litr", "alberta", "870,000", "ableg", "train", "derail", "iowa", "just", "cdnpoli", "flood", "river", "dump", "ab", "oil", "bnsf", "crude"],
            "representative": True
        },
        {
            "location": {
                "lat": 32.73628,
                "lon": -94.94148
            },
            "created_at": "2018-06-23T20:04:53",
            "text": "RT @isaabitch_: Since the Flooding occurred in South Texas our home has gone through extensive damage and is it not suitable for living.",
            "place": "Mercedes, TX",
            "lang": "en",
            "tweetid": "19",
            "flood_probability": 0.505,
            "country": "USA",
            "tokens": ["we", "gone", "occur", "our", "extens", "ha", "live", "south", "flood", "through", "sinc", "damag", "texa", "suitabl", "home"],
            "representative": False
        }
    ]
    for rec in data:
        elastic.create_or_update_doc(rec["tweetid"], rec)


def get_credentials(user):
    return {
        'username': user.username,
        'api_key': user.api_key.key
        }


def get_params(user, **parm):
    parm.update(get_credentials(user))
    if 'format' not in parm.keys():
        parm.update(format=DEFAULT_FORMAT)
    return parm


def get_content(client, url, params):
    """Helper function to get responce content."""
    resp = client.get(url, params)
    return json.loads(resp.content.decode('utf-8'))


def get_objects(client, url, params):
    """Helper function too get responce content['objects']."""
    return get_content(client, url, params)[settings.API_OBJECTS_KEY]

