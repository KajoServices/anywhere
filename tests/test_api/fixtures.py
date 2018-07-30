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
                "lat": 26.145053,
                "lon": -97.941707
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
                "lat": 26.167650,
                "lon": -97.916853
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
                "lat": 26.162797,
                "lon": -97.887043
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
                "lat": 26.167157,
                "lon": -97.894937
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
    elastic.ensure_mapping()
    for rec in data:
        elastic.create_or_update_doc(rec["tweetid"], rec)


@fixture
def tweet_post_doc():
    return {
        "tweetid": "1007091630611017728",
        "created_at": "2018-06-14T02:45:30",
        "annotations": {
            "flood_probability": 0.7786412835121155
        },
        "tweet": {
            "created_at": "Thu Jun 14 02:45:30 +0000 2018",
            "id": 1007091630611017728,
            "id_str": "1007091630611017728",
            "text": "Primeras bandas de lluvia moderada a fuerte de la tormenta tropical #Bud, estar\u00e1n llegando esta misma noche a Los C\u2026 https://t.co/xGLIeW1DkL",
            "display_text_range": [
                0,
                140
            ],
            "source": "<a href=\"http://twitter.com/download/android\" rel=\"nofollow\">Twitter for Android</a>",
            "truncated": True,
            "in_reply_to_status_id": None,
            "in_reply_to_status_id_str": None,
            "in_reply_to_user_id": None,
            "in_reply_to_user_id_str": None,
            "in_reply_to_screen_name": None,
            "user": {
                "id": 53521262,
                "id_str": "53521262",
                "name": "MetMEX B.C.S.",
                "screen_name": "metmexBCS",
                "location": "La Paz, B.C.S., M\u00e9xico",
                "url": "https://metmexbcs.wordpress.com/",
                "description": "Informaci\u00f3n meteorol\u00f3gica en tiempo real del noroeste de M\u00e9xico. 11vo aniversario 15/09/18. contratos metmexbcs@gmail.com | Jorge Garza director de @metmex",
                "translator_type": "regular",
                "protected": False,
                "verified": False,
                "followers_count": 9595,
                "friends_count": 1052,
                "listed_count": 130,
                "favourites_count": 3497,
                "statuses_count": 20158,
                "created_at": "Fri Jul 03 22:40:51 +0000 2009",
                "utc_offset": None,
                "time_zone": None,
                "geo_enabled": True,
                "lang": "es",
                "contributors_enabled": False,
                "is_translator": False,
                "profile_background_color": "352726",
                "profile_background_image_url": "http://abs.twimg.com/images/themes/theme5/bg.gif",
                "profile_background_image_url_https": "https://abs.twimg.com/images/themes/theme5/bg.gif",
                "profile_background_tile": False,
                "profile_link_color": "D02B55",
                "profile_sidebar_border_color": "FFFFFF",
                "profile_sidebar_fill_color": "99CC33",
                "profile_text_color": "3E4415",
                "profile_use_background_image": True,
                "profile_image_url": "http://pbs.twimg.com/profile_images/2614337216/mjo0p3v67sx68lq156k2_normal.png",
                "profile_image_url_https": "https://pbs.twimg.com/profile_images/2614337216/mjo0p3v67sx68lq156k2_normal.png",
                "profile_banner_url": "https://pbs.twimg.com/profile_banners/53521262/1522814182",
                "default_profile": False,
                "default_profile_image": False,
                "following": None,
                "follow_request_sent": None,
                "notifications": None
            },
            "geo": None,
            "coordinates": None,
            "place": {
                "id": "bfc0e5ed37b66a6c",
                "url": "https://api.twitter.com/1.1/geo/id/bfc0e5ed37b66a6c.json",
                "place_type": "city",
                "name": "Los Cabos",
                "full_name": "Los Cabos, Baja California Sur",
                "country_code": "MX",
                "country": "M\u00e9xico",
                "bounding_box": {
                    "type": "Polygon",
                    "coordinates": [
                        [
                            [
                                -110.119479,
                                22.871889
                            ],
                            [
                                -110.119479,
                                23.667869
                            ],
                            [
                                -109.413058,
                                23.667869
                            ],
                            [
                                -109.413058,
                                22.871889
                            ]
                        ]
                    ]
                },
                "attributes": {}
            },
            "contributors": None,
            "is_quote_status": False,
            "extended_tweet": {
                "full_text": "Primeras bandas de lluvia moderada a fuerte de la tormenta tropical #Bud, estar\u00e1n llegando esta misma noche a Los Cabos.\n\nPrecauci\u00f3n al conducir por la carretera Transpeninsular desde Migri\u00f1o hasta Cadua\u00f1o (pasando por Cabo San Lucas y San Jos\u00e9 del Cabo). https://t.co/LXhbhIS6Cg",
                "display_text_range": [
                    0,
                    255
                ],
                "entities": {
                    "hashtags": [
                        {
                            "text": "Bud",
                            "indices": [
                                68,
                                72
                            ]
                        }
                    ],
                    "urls": [],
                    "user_mentions": [],
                    "symbols": [],
                    "media": [
                        {
                            "id": 1007091622176124928,
                            "id_str": "1007091622176124928",
                            "indices": [
                                256,
                                279
                            ],
                            "media_url": "http://pbs.twimg.com/media/DfnofpwUcAAvOlz.jpg",
                            "media_url_https": "https://pbs.twimg.com/media/DfnofpwUcAAvOlz.jpg",
                            "url": "https://t.co/LXhbhIS6Cg",
                            "display_url": "pic.twitter.com/LXhbhIS6Cg",
                            "expanded_url": "https://twitter.com/metmexBCS/status/1007091630611017728/photo/1",
                            "type": "photo",
                            "sizes": {
                                "thumb": {
                                    "w": 150,
                                    "h": 150,
                                    "resize": "crop"
                                },
                                "medium": {
                                    "w": 800,
                                    "h": 600,
                                    "resize": "fit"
                                },
                                "large": {
                                    "w": 800,
                                    "h": 600,
                                    "resize": "fit"
                                },
                                "small": {
                                    "w": 680,
                                    "h": 510,
                                    "resize": "fit"
                                }
                            }
                        }
                    ]
                },
                "extended_entities": {
                    "media": [
                        {
                            "id": 1007091622176124928,
                            "id_str": "1007091622176124928",
                            "indices": [
                                256,
                                279
                            ],
                            "media_url": "http://pbs.twimg.com/media/DfnofpwUcAAvOlz.jpg",
                            "media_url_https": "https://pbs.twimg.com/media/DfnofpwUcAAvOlz.jpg",
                            "url": "https://t.co/LXhbhIS6Cg",
                            "display_url": "pic.twitter.com/LXhbhIS6Cg",
                            "expanded_url": "https://twitter.com/metmexBCS/status/1007091630611017728/photo/1",
                            "type": "photo",
                            "sizes": {
                                "thumb": {
                                    "w": 150,
                                    "h": 150,
                                    "resize": "crop"
                                },
                                "medium": {
                                    "w": 800,
                                    "h": 600,
                                    "resize": "fit"
                                },
                                "large": {
                                    "w": 800,
                                    "h": 600,
                                    "resize": "fit"
                                },
                                "small": {
                                    "w": 680,
                                    "h": 510,
                                    "resize": "fit"
                                }
                            }
                        }
                    ]
                }
            },
            "quote_count": 0,
            "reply_count": 0,
            "retweet_count": 0,
            "favorite_count": 0,
            "entities": {
                "hashtags": [
                    {
                        "text": "Bud",
                        "indices": [
                            68,
                            72
                        ]
                    }
                ],
                "urls": [
                    {
                        "url": "https://t.co/xGLIeW1DkL",
                        "expanded_url": "https://twitter.com/i/web/status/1007091630611017728",
                        "display_url": "twitter.com/i/web/status/1\u2026",
                        "indices": [
                            117,
                            140
                        ]
                    }
                ],
                "user_mentions": [],
                "symbols": []
            },
            "favorited": False,
            "retweeted": False,
            "possibly_sensitive": False,
            "filter_level": "low",
            "lang": "es",
            "timestamp_ms": "1528944330252"
        },
        "latlong": {
            "lat": 23.05888,
            "long": -109.69771
        },
        "lang": "es",
        "geotags": {
            "country_predicted": "USA"
        }
    }

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

