import sys
import sqlite3 as sqlite
dbfilename = sys.argv[1]
connection = sqlite.connect(dbfilename)
cursor = connection.cursor()

import urllib2
from BeautifulSoup import BeautifulSoup
from lxml import etree
permissionlabels = "https://android.googlesource.com/platform/frameworks/base/+/master/core/res/res/values-de/strings.xml"
request = urllib2.Request(permissionlabels)
request.add_header("User-Agent", "PermissionCrawler")
handle = urllib2.build_opener()
content = handle.open(request).read()
soup = BeautifulSoup(content)
soup2 = BeautifulSoup(unicode(soup.find("pre").text), convertEntities=BeautifulSoup.HTML_ENTITIES)
labelroot = etree.fromstring(str(soup2))
labels = labelroot.xpath("//string")
l = {}
ll = {}
for label in labels:
    if label.text == None:
        continue
    text = label.text.encode('ascii', 'replace')
    l[text] = label.get('name')
    l[label.get('name')] = text

permissioninfo = "https://android.googlesource.com/platform/frameworks/base/+/master/core/res/AndroidManifest.xml"
request = urllib2.Request(permissioninfo)
request.add_header("User-Agent", "PermissionCrawler")
handle = urllib2.build_opener()
content = handle.open(request).read()
soup = BeautifulSoup(content)
soup2 = BeautifulSoup(unicode(soup.find("pre").text), convertEntities=BeautifulSoup.HTML_ENTITIES)
permsroot = etree.fromstring(str(soup2))
perms = permsroot.xpath("//permission")
p = {}
for perm in perms:
    p[perm.get('{http://schemas.android.com/apk/res/android}label')] = perm.get('{http://schemas.android.com/apk/res/android}name')
print p

cursor.execute("SELECT id, permission FROM permissions")
for row in cursor.fetchall():
    id = row[0]
    text = row[1].encode('ascii', 'replace')
    plabel = l[text]
    print plabel
