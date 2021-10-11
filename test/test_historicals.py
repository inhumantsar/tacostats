from datetime import datetime
import logging
from pandas.core.frame import DataFrame
import pytest
from unittest.mock import patch

from tacostats.historicals import  is_thunderdome_day


TEST_DATA = {
    "top_emoji": [
        [
            1692,
            "\ud83d\ude24"
        ],
        [
            588,
            "\ud83d\udc11"
        ],
        [
            284,
            "\ud83d\ude02"
        ],
        [
            134,
            "\ud83d\ude21"
        ],
        [
            130,
            "\ud83d\ude0e"
        ],
        [
            116,
            "\ud83d\ude14"
        ],
        [
            112,
            "\ud83e\udd2c"
        ],
        [
            108,
            "\ud83e\udd14"
        ],
        [
            100,
            "\ud83d\ude44"
        ],
        [
            98,
            "\ud83d\ude2d"
        ],
        [
            90,
            "\ud83d\udc4f"
        ],
        [
            86,
            "\ud83e\uddd0"
        ],
        [
            80,
            "\ud83e\udd88"
        ],
        [
            66,
            "\ud83c\udde8\ud83c\udde6"
        ],
        [
            66,
            "\ud83d\ude33"
        ],
        [
            66,
            "\ud83d\ude10"
        ],
        [
            64,
            "\ud83c\udf41"
        ],
        [
            64,
            "\u270a"
        ],
        [
            62,
            "\ud83e\udd23"
        ],
        [
            62,
            "\ud83c\uddeb\ud83c\uddf7"
        ],
        [
            48,
            "\ud83d\udc40"
        ],
        [
            46,
            "\ud83e\udd1d"
        ],
        [
            46,
            "\ud83e\udd70"
        ],
        [
            36,
            "\ud83d\udc46"
        ],
        [
            34,
            "\ud83d\ude0d"
        ],
        [
            32,
            "\ud83d\ude4f"
        ],
        [
            32,
            "\ud83d\uded1"
        ],
        [
            32,
            "\ud83c\udf29"
        ],
        [
            30,
            "\ud83d\udd2b"
        ],
    ],
    "upvoted_comments": [
        {
            "author": "Fishin_Mission",
            "author_flair_text": ":friedman: \ud83e\udd47 Nate Gold",
            "score": 127,
            "id": "hdkjr8u",
            "permalink": "/r/neoliberal/comments/prpiul/discussion_thread/hdkjr8u/",
            "body": "***DO NOT BUY THE iPHONE 13!*** \ud83d\udcf5\n\nIT CAN\u2019T BE TRUSTED!\n\n-\tbrought to market in less than a year \ud83d\udcc6\n-\tcontains microchips \ud83d\udcf2\n-\thas 5G \ud83d\udcf6\n-\t*NOT* FDA approved \ud83e\udd7c\n-\ttracks your location \ud83d\udccd\n-\tmy cousin got it right before he got married and now his testicals are huge and his fianc\u00e9e called the wedding off! \ud83d\udc70\u200d\u2642\ufe0f",
            "created_utc": 1632128735.0,
            "emoji_count": 7,
            "word_count": 50
        },
        {
            "author": "MinimalMalarkey",
            "author_flair_text": ":polis: Jared Polis",
            "score": 87,
            "id": "hdkvz7e",
            "permalink": "/r/neoliberal/comments/prpiul/discussion_thread/hdkvz7e/",
            "body": "Alexandria Ocasio-Cortez and Joe Manchin walk into a bar. Negotiations falter immediately and as such there is no money left to fund a punchline for this joke.",
            "created_utc": 1632138516.0,
            "emoji_count": 0,
            "word_count": 27
        },
        {
            "author": "vhgomes12",
            "author_flair_text": ":belmondo: Not a Gnome",
            "score": 87,
            "id": "hdlaohk",
            "permalink": "/r/neoliberal/comments/prpiul/discussion_thread/hdlaohk/",
            "body": "Ok, but this is actually a big deal\n\n[Meet the Prevent Senior scandal](https://g1.globo.com/sp/sao-paulo/noticia/2021/09/16/prevent-senior-entenda-as-acusacoes-contra-a-empresa-envolvendo-pesquisa-sobre-cloroquina.ghtml)\n\nPretty much: the Brazilian government worked together with a health insurance company to test the use of azythromicin and HCQ in patients with covid\n\nHowever, the patients were not informed of the treatment or the trials, which wasn't approved by the Medical Ethics authority\n\nTo top it off, the company banned doctors from using masks\n\nOh, and did I mention not every patient did a PCR to check whether or not they had covid? \n\nAnd to top it off, they forged death certificates to hide the deaths of the HCQ group\n\n*ping LATAM I know Bolsonaro=Nazi is a terrible comparison but he did help arrange the trial so it's valid",
            "created_utc": 1632146300.0,
            "emoji_count": 0,
            "word_count": 117
        }
    ]
}


TEST_DATA_THUNDERDOME = {
    "upvoted_comments": [
        {
            "author": "BidenWon",
            "author_flair_text": ":polis: Jared Polis",
            "score": 55,
            "id": "hdnbmei",
            "permalink": "/r/neoliberal/comments/ps50rg/\u00e9lections_canadiennes_d\u00f4me_du_tonnerre/hdnbmei/",
            "body": "Twenty minutes ago I knew nothing about Canadian politics.\n\nBut I'm now an expert and I hate the guts of anyone that votes New Democrat, Green, or People's Party.",
            "created_utc": 1632176822.0,
            "emoji_count": 0,
            "word_count": 28
        },
        {
            "author": "embertimber_v2",
            "author_flair_text": ":duflo: Esther Duflo",
            "score": 54,
            "id": "hdnbesd",
            "permalink": "/r/neoliberal/comments/ps50rg/\u00e9lections_canadiennes_d\u00f4me_du_tonnerre/hdnbesd/",
            "body": "If Trudeau loses, it's Miami-Dade's fault.",
            "created_utc": 1632176723.0,
            "emoji_count": 0,
            "word_count": 6
        },
    ]
}

def test_is_thunderdome_day_true():
    now = datetime.now()
    data = TEST_DATA["upvoted_comments"] + TEST_DATA_THUNDERDOME['upvoted_comments']
    with patch("tacostats.historicals.get_dataframe", return_value=DataFrame(data)) as mocked:
        assert is_thunderdome_day(now.date()) == True

def test_is_thunderdome_day_false():
    now = datetime.now()
    data = TEST_DATA["upvoted_comments"]
    with patch("tacostats.historicals.get_dataframe", return_value=DataFrame(data)) as mocked:
        assert is_thunderdome_day(now.date()) == False