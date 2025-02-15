import json
import re
from typing import List

from bs4 import BeautifulSoup, SoupStrainer

import utils as utils


def extract_article_urls(document: str, _: bool) -> List[str]:
    document = document[document.find('<ul class="type06_headline">'):]

    # Extract article url containers.
    list1 = document[: document.find("</ul>")]
    list2 = document[document.find("</ul>") + 5:]
    list2 = list2[: list2.find("</ul>")]

    document = list1 + list2

    # Extract all article urls from the containers.
    article_urls = []
    while "<dt>" in document:
        document = document[document.find("<dt>"):]
        container = document[: document.find("</dt>")]

        if not container.strip():
            continue

        article_urls.append(re.search(r'<a href="(.*?)"', container).group(1))
        document = document[document.find("</dt>"):]

    return article_urls


def parse_article_content(document: str, include_reporter_name: bool) -> str:
    content_strainer = SoupStrainer("article", attrs={"id": "dic_area"})
    content_document = BeautifulSoup(document, "lxml", parse_only=content_strainer)
    content_content = content_document.find("article")
    
    title_strainer = SoupStrainer("h2", attrs={"id": "title_area"})
    title_document = BeautifulSoup(document, "lxml", parse_only=title_strainer)
    title_content = title_document.find("span")
    
    date_strainer = SoupStrainer("div", attrs={"class": "media_end_head_info_datestamp_bunch"})
    date_document = BeautifulSoup(document, "lxml", parse_only=date_strainer)
    date_content = date_document.find("span")
    
    # Skip invalid articles which do not contain news contents.
    if content_content is None:
        raise ValueError("there is no any news article content.")
    
    if title_content is None:
        raise ValueError("there is no any news article content.")
    
    if date_content is None:
        raise ValueError("there is no any news article content.")

    # Remove unnecessary tags except `<br>` elements for preserving line-break
    # characters.
    for child in content_content.find_all():
        if child.name != "br":
            child.clear()
            
    for child in title_content.find_all():
        if child.name != "br":
            child.clear()

    for child in date_content.find_all():
        if child.name != "br":
            child.clear()
            
    content_content = content_content.get_text(separator="\n").strip()
    content_content = "\n".join([line.strip() for line in content_content.split('\n')])
    
    title_content = title_content.get_text(separator="\n").strip()
    title_content = "\n".join([line.strip() for line in title_content.split('\n')])
    
    date_content = date_content.get_text(separator="\n").strip()
    date_content = "\n".join([line.strip() for line in date_content.split('\n')])

    # Skip the contents which contain too many non-Korean characters.
    if utils.korean_character_ratio(content_content) < 0.5:
        raise ValueError("there are too few Korean characters in the content.")

    # Normalize the contents by removing abnormal sentences.
    content_content = "\n".join(
        [
            line
            for line in content_content.splitlines()
            if line[-1] == "."
        ]
    )

    # Remove reporter name part if set.
    if not include_reporter_name:
        splitted = content_content.split(sep='\n')
        content_content = "\n".join(splitted[1:])
        content_content = utils.remove_reporter_name(splitted[0]) + content_content

    # Remove empty string
    if content_content == "":
        raise ValueError("there is no news article content.")

    return json.encoder.encode_basestring(title_content),json.encoder.encode_basestring(content_content), json.encoder.encode_basestring(date_content.split()[0][:-1])
