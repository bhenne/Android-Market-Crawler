#!usr/bin/env python

"""
Google Android Market Crawler
For the sake of research
1) database file name
2 through n) all the categories we want to explore
OR
1) database file name .and. 2) "p" => prints db content

stop crawling with `kill -s SIGTERM <pid>`
"""

import sys
import re
import urllib2
import urlparse
import signal
import sqlite3 as sqlite
import threading
from BeautifulSoup import BeautifulSoup

__author__ = "Sergio Bernales, Benjamin Henne"

TERMAPP = False

if len(sys.argv) < 2:
    sys.exit("Not Enough arguments!");
else:
    dbfilename = sys.argv[1]

    if sys.argv[2] == "p":
        connection = sqlite.connect(dbfilename)
        cursor = connection.cursor()
        S = """SELECT app_names.appname, categories.category, permissions.permission, url 
                FROM app_permissions 
                 JOIN app_names ON (app_permissions.appname=app_names.id) 
                 JOIN permissions ON (app_permissions.permission=permissions.id) 
                 JOIN categories on (app_permissions.category=categories.id);"""
        cursor.execute(S)
        for row in cursor.fetchall():
            print "\t".join(row)
        connection.close()
        sys.exit()

    argLen = len(sys.argv) - 1
    categories = [x.upper() for x in sys.argv[2::]]

#DB Connection: create it and/or just open it
connection = sqlite.connect(dbfilename)
cursor = connection.cursor()

#tables that will contain all the permissions of an app of a certain category - table layout not perfect but fix solution
cursor.execute('CREATE TABLE IF NOT EXISTS app_names (id INTEGER PRIMARY KEY, appname VARCHAR(256) UNIQUE, url VARCHAR(256))')
cursor.execute('CREATE TABLE IF NOT EXISTS permissions (id INTEGER PRIMARY KEY, permission VARCHAR(256) UNIQUE)')
cursor.execute('CREATE TABLE IF NOT EXISTS categories (id INTEGER PRIMARY KEY, category VARCHAR(256) UNIQUE)')
cursor.execute('CREATE TABLE IF NOT EXISTS app_permissions (id INTEGER PRIMARY KEY, appname INTEGER, category INTEGER, permission INTEGER)')
#cursor.execute('CREATE TABLE IF NOT EXISTS urls_to_crawl (category VARCHAR(256), url VARCHAR(256))')

connection.commit()

class MarketCrawler(threading.Thread):
    mainURL = "https://play.google.com"
    topfreeURL = "https://play.google.com/store/apps/category/%s/collection/topselling_free?start=%d&num=%d"
    topfreeURL = "https://play.google.com/store/apps/category/%s/collection/topselling_paid?start=%d&num=%d"
    pageIncrements = 24;
    apps = {}
    permissions = {}
    categories = {}

    """
    run()
    This will be the entry point for the thread and it will loop through every
    category provided by the user
    crawl process
    """
    def run(self):
        for cat in categories:
            print cat
            self.crawlAppsForCategory(cat)

    def crawlAppsForCategory(self, cat):
        pageIndex = 0
        curl = self.topfreeURL % (cat, pageIndex, self.pageIncrements)
        currentURL = curl + str(pageIndex)

        while True:
            try:
                request = urllib2.Request(currentURL)
                request.add_header("User-Agent", "PermissionCrawler")
                handle = urllib2.build_opener()
                content = handle.open(request).read()
                soup = BeautifulSoup(content)

                print "Currently on page " + str(pageIndex) + " of the list of app for this Category"
                appURLS = self.extractAppUrls(soup)
                self.extractPermissionsIntoDB(appURLS, cat)

                pageIndex+=self.pageIncrements
                currentURL = curl + str(pageIndex)

                if TERMAPP == True:
                    connection.close()
                    sys.exit()

            except urllib2.HTTPError, error:
                if error.code == 404:
                    print >> sys.stderr, "404 ERROR: %s -> %s" % (error, error.url)
                if error.code == 403:
                    print >> sys.stderr, "403 (NO MORE APP PAGES FOR THIS CATEGORY)ERROR: %s -> %s" % (error, error.url)
                else:
                    print >> sys.stderr, "ERROR: %s" % error
                break
            except Exception, e:
                print >> sys.stderr, "iSERROR: %s" % e
    

    """
    From the page the lists a page of pageIncrements apps of the particular category,
    extract the links to those apps
    """
    def extractAppUrls(self, soup):
        tags = soup('a')
        #to get rid of duplicates since the href get returns links twice
        skip = False         

        appURLS = []
        for tag in  tags:
            href = tag.get("href")
            if skip:
                skip = False
                continue
            if href is not None and re.search('/details', href):
                #print href
                appURLS.append(self.mainURL+href)
                skip = True
        
        return appURLS


    """
    Fetch all the URLS in appURLS and extract the permissions.
    Put these permission into the DB
    """
    def extractPermissionsIntoDB(self, appURLS, cat):
        #we can put this URL stuff into its own object /code repetition
        for url in appURLS:
            request = urllib2.Request(url)
            request.add_header("User-Agent", "PyCrawler")
            handle = urllib2.build_opener()
            content = handle.open(request).read()
            soup = BeautifulSoup(content)
            
            appName = soup.find('h1','doc-banner-title').contents[0]
            permissions = soup.findAll('div','doc-permission-description')
            self.pushToDB(appName, cat, permissions, url)
    
    """
    Pushes permissions of a certain app into the DB
    cursor.execute('CREATE TABLE IF NOT EXISTS app_permissions (id INTEGER, appname VARCHAR(256), category VARCHAR(256), permission VARCHAR(256), url VARCHAR(256))')
    """
    def pushToDB(self, appName, cat, permissions, url):
        for p in permissions:
            #print appName, cat, p.contents[0], url 

            if len(self.apps) > 1000:
                apps = {}
            if appName in self.apps:
                appId = self.apps[appName]
            else:
                cursor.execute("INSERT OR IGNORE INTO app_names VALUES ((?), (?), (?))", (None, appName, url))
                cursor.execute("SELECT id FROM app_names WHERE appname=(?)", [appName])
                appId = self.apps[appName] = cursor.fetchone()[0]
            permission = p.contents[0]

            if permission in self.permissions:
                permissionId = self.permissions[permission]
            else:
                cursor.execute("INSERT OR IGNORE INTO permissions VALUES ((?), (?))", (None, permission))
                cursor.execute("SELECT id FROM permissions WHERE permission=(?)", [permission])
                permissionId = self.permissions[permission] = cursor.fetchone()[0]

            if cat in self.categories:
                catId = self.categories[cat]
            else:
                cursor.execute("INSERT OR IGNORE INTO categories VALUES ((?), (?))", (None, cat))
                cursor.execute("SELECT id FROM categories WHERE category=(?)", [cat])
                catId = self.categories[cat] = cursor.fetchone()[0]

            cursor.execute("INSERT INTO app_permissions VALUES ((?), (?), (?), (?))", (None, appId, catId, permissionId))
            connection.commit()

def SIGTERM_handler(signum, frame):
    global TERMAPP
    print '\n--- Caught SIGTERM; Attempting to quit gracefully ---'
    TERMAPP = True

signal.signal(signal.SIGTERM, SIGTERM_handler)

if __name__ == "__main__":
    #run the crawler thread
    MarketCrawler().run()
