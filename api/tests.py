from django.test import TestCase

# Create your tests here.

fixture = {
    "tweetid": "1001236768572747777",
    "created_at": "2018-05-28T21:00:22",
    "annotations": {"flood_probability": 0.7},
    "lang": "en",
    "latlong": {
        "lat": 35.22709,
        "long": -80.84313
    },
    "geotags": {
        "country_predicted": "United States",
        "place_name": "Manhattan"
    },
    "tweet": {
        "quote_count": 0,
        "contributors": "null",
        "truncated": "false",
        "text": "open water. https://t.co/QWvcJqXjmQ",
        "is_quote_status": "true",
        "in_reply_to_status_id": "null",
        "reply_count": 0,
        "id": 993957866846507008,
        "favorite_count": 0,
        "source": "<a href=\"http://tapbots.com/tweetbot\" rel=\"nofollow\">Tweetbot for i\u039fS</a>",
        "quoted_status_id": 993946159961071619,
        "retweeted": "false",
        "coordinates": "null",
        "timestamp_ms": "1525812996957",
        "quoted_status": {
            "quote_count": 76,
            "contributors": "null",
            "truncated": "false",
            "text": "I wanna watch the worst movie ever made. What movie is it?",
            "is_quote_status": "false",
            "in_reply_to_status_id": "null",
            "reply_count": 13,
            "id": 993946159961071619,
            "favorite_count": 1,
            "source": "<a href=\"http://tweetlogix.com\" rel=\"nofollow\">Tweetlogix</a>",
            "retweeted": "false",
            "coordinates": "null",
            "entities": {
                "user_mentions": [],
                "symbols": [],
                "hashtags": [],
                "urls": []
            },
            "in_reply_to_screen_name": "null",
            "in_reply_to_user_id": "null",
            "retweet_count": 0,
            "id_str": "993946159961071619",
            "favorited": "false",
            "user": {
                "follow_request_sent": "null",
                "profile_use_background_image": "true",
                "id": 17843782,
                "verified": "false",
                "translator_type": "none",
                "profile_image_url_https": "https://pbs.twimg.com/profile_images/986753851515723776/pA4Jw94K_normal.jpg",
                "profile_sidebar_fill_color": "DDEEF6",
                "is_translator": "false",
                "geo_enabled": "false",
                "profile_text_color": "333333",
                "followers_count": 2470,
                "protected": "false",
                "location": "The Great Outchea",
                "default_profile_image": "false",
                "id_str": "17843782",
                "utc_offset": -18000,
                "statuses_count": 341915,
                "description": "Songwriter. Creative. INFP. Ravenclaw. Scout's Regiment. Chicken enthusiast. Wrestling smark. Objective hater. Violence isn\u2019t the answer,but sometimes it is.",
                "friends_count": 404,
                "profile_link_color": "0084B4",
                "profile_image_url": "http://pbs.twimg.com/profile_images/986753851515723776/pA4Jw94K_normal.jpg",
                "notifications": "null",
                "profile_background_image_url_https": "https://pbs.twimg.com/profile_background_images/93204852/Slicknigga.jpg",
                "profile_background_color": "C0DEED",
                "profile_banner_url": "https://pbs.twimg.com/profile_banners/17843782/1484104507",
                "profile_background_image_url": "http://pbs.twimg.com/profile_background_images/93204852/Slicknigga.jpg",
                "screen_name": "MyNig",
                "lang": "en",
                "profile_background_tile": "true",
                "favourites_count": 4775,
                "name": "El Idolo",
                "url": "http://TheTwitterMixtape.bandcamp.com",
                "created_at": "Wed Dec 03 18:27:11 +0000 2008",
                "contributors_enabled": "false",
                "time_zone": "Quito",
                "profile_sidebar_border_color": "C0DEED",
                "default_profile": "false",
                "following": "null",
                "listed_count": 129
            },
            "geo": "null",
            "in_reply_to_user_id_str": "null",
            "lang": "en",
            "created_at": "Tue May 08 20:10:05 +0000 2018",
            "filter_level": "low",
            "in_reply_to_status_id_str": "null",
            "place": "null"
        },
        "entities": {
            "user_mentions": [],
            "symbols": [],
            "hashtags": [],
            "urls": [
                {
                    "url": "https://t.co/QWvcJqXjmQ",
                    "indices": [
                        12,
                        35
                    ],
                    "display_url": "twitter.com/MyNig/status/9\u2026",
                    "unwound": {
                        "url": "https://twitter.com/MyNig/status/993946159961071619",
                        "status": 200,
                        "description": "\u201cI wanna watch the worst movie ever made. What movie is it?\u201d",
                        "title": "El Idolo on Twitter"
                    },
                    "expanded_url": "https://twitter.com/MyNig/status/993946159961071619"
                }
            ]
        },
        "in_reply_to_screen_name": "null",
        "id_str": "993957866846507008",
        "display_text_range": [
            0,
            11
        ],
        "retweet_count": 0,
        "in_reply_to_user_id": "null",
        "favorited": "false",
        "user": {
            "follow_request_sent": "null",
            "profile_use_background_image": "true",
            "id": 47695096,
            "verified": "false",
            "translator_type": "none",
            "profile_image_url_https": "https://pbs.twimg.com/profile_images/991387789051727877/XLCevK_m_normal.jpg",
            "profile_sidebar_fill_color": "EADEAA",
            "is_translator": "false",
            "geo_enabled": "true",
            "profile_text_color": "333333",
            "followers_count": 4963,
            "protected": "false",
            "location": "nyc",
            "default_profile_image": "false",
            "id_str": "47695096",
            "utc_offset": -14400,
            "statuses_count": 317531,
            "description": "such a vivrant thing.",
            "friends_count": 582,
            "derived": {
                "locations": [
                    {
                        "country_code": "US",
                        "locality": "New York City",
                        "country": "United States",
                        "region": "New York",
                        "full_name": "New York City,New York,United States",
                        "geo": {
                            "type": "point",
                            "coordinates": [
                                -74.00597,
                                40.71427
                            ]
                        }
                    }
                ]
            },
            "profile_link_color": "FF0000",
            "profile_image_url": "http://pbs.twimg.com/profile_images/991387789051727877/XLCevK_m_normal.jpg",
            "notifications": "null",
            "profile_background_image_url_https": "https://pbs.twimg.com/profile_background_images/512552205/lauryn_hill.jpeg",
            "profile_background_color": "642D8B",
            "profile_banner_url": "https://pbs.twimg.com/profile_banners/47695096/1410308378",
            "profile_background_image_url": "http://pbs.twimg.com/profile_background_images/512552205/lauryn_hill.jpeg",
            "screen_name": "spinnellii",
            "lang": "en",
            "profile_background_tile": "false",
            "favourites_count": 769,
            "name": "Auntie Can",
            "url": "http://candicedrakeford.com",
            "created_at": "Tue Jun 16 18:08:53 +0000 2009",
            "contributors_enabled": "false",
            "time_zone": "Eastern Time (US & Canada)",
            "profile_sidebar_border_color": "D9B17E",
            "default_profile": "false",
            "following": "null",
            "listed_count": 121
        },
        "geo": "null",
        "in_reply_to_user_id_str": "null",
        "possibly_sensitive": "true",
        "lang": "nl",
        "matching_rules": [
            {
                "tag": "bbls__2__1522949886000",
                "id": 981949112319971328,
                "id_str": "981949112319971328"
            }
        ],
        "created_at": "Tue May 08 20:56:36 +0000 2018",
        "quoted_status_id_str": "993946159961071619",
        "filter_level": "low",
        "in_reply_to_status_id_str": "null",
        "place": {
            "country_code": "US",
            "url": "https://api.twitter.com/1.1/geo/id/4d083cc896344b18.json",
            "country": "United States",
            "place_type": "neighborhood",
            "bounding_box": {
                "type": "Polygon",
                "coordinates": [
                    [
                        [
                            -74.017681,
                            40.699759
                        ],
                        [
                            -74.017681,
                            40.717256
                        ],
                        [
                            -74.001994,
                            40.717256
                        ],
                        [
                            -74.001994,
                            40.699759
                        ]
                    ]
                ]
            },
            "full_name": "Financial District,Manhattan",
            "attributes": {},
            "id": "4d083cc896344b18",
            "name": "Financial District"
        }
    }
}
