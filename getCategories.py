#!/usr/bin/env python

import urllib2
import re
from BeautifulSoup import BeautifulSoup

__author__ = "Benjamin Henne"

URL = "https://play.google.com/store/apps/category/GAME?feature=category-nav"
s = set()

request = urllib2.Request(URL)
request.add_header("User-Agent", "PermissionCrawler")
handle = urllib2.build_opener()
content = handle.open(request).read()
soup = BeautifulSoup(content)

tags = soup('a')
for tag in tags:
    href = tag.get("href")
    if href is not None and re.search('/category/', href):
        x = href.partition('/category/')[2].partition('/')[0]
        if "?" in x:
            x = x.partition("?")[0]
        s.add(x)

print " ".join(s)
