#!usr/bin/env python

"""
Google Android Market Crawler
For the sake of research
1) database file name
2 through n) all the categories we want to explore
OR
1) database file name .and. 2) "p", "pa", "a", "aa", "c" => prints db content

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

TERMAPP = False #: flag for quitting gracefully
myThreshold = 15 #: another break on n duplicates threshold as a quick fix

if len(sys.argv) < 2:
    sys.exit("Not Enough arguments!");
else:
    dbfilename = sys.argv[1]

    if (sys.argv[2] == "p") or (sys.argv[2] == "P"):
        connection = sqlite.connect(dbfilename)
        cursor = connection.cursor()
        S = """SELECT apps.appname, categories.category, apps.appgroup, permissions.permission, url 
                FROM app_permissions 
                 JOIN apps ON (app_permissions.appname=apps.id) 
                 JOIN permissions ON (app_permissions.permission=permissions.id) 
                 JOIN categories on (apps.category=categories.id);"""
        cursor.execute(S)
	appgroups = { 0: "topFree", 1: "topPaid"}
	if (sys.argv[2] == "p"):
            for row in cursor.fetchall():
                print u"\t".join([row[0], row[1], appgroups[row[2]], row[3]])#, row[4]])
	else:
            for row in cursor.fetchall():
                u = u"\t".join([row[0], row[1], appgroups[row[2]], row[3]])#, row[4]])
		print u.encode("ascii", "replace")
        connection.close()
        sys.exit()

    if sys.argv[2] == "a":
        connection = sqlite.connect(dbfilename)
        cursor = connection.cursor()
        S = """SELECT appname FROM apps;"""
        cursor.execute(S)
        for row in cursor.fetchall():
            print u"\t".join(row)
        connection.close()
        sys.exit()

    if sys.argv[2] == "aa":
        connection = sqlite.connect(dbfilename)
        cursor = connection.cursor()
        S = """SELECT count(appname) FROM apps;"""
        cursor.execute(S)
        for row in cursor.fetchall():
            print str(row[0])
        connection.close()
        sys.exit()

    if sys.argv[2] == "c":
        connection = sqlite.connect(dbfilename)
        cursor = connection.cursor()
        S = """SELECT category FROM categories;"""
        cursor.execute(S)
        for row in cursor.fetchall():
            print u"\t".join(row)
        connection.close()
        sys.exit()

    argLen = len(sys.argv) - 1
    categories = [x.upper() for x in sys.argv[2::]]
    if len(categories) > 1:
        print "Crawling categories:", " ".join(categories)

#DB Connection: create it and/or just open it
connection = sqlite.connect(dbfilename)
cursor = connection.cursor()

#tables that will contain all the permissions of an app of a certain category - table layout not perfect but fix solution
cursor.execute('CREATE TABLE IF NOT EXISTS apps (id INTEGER PRIMARY KEY, appname VARCHAR(256) UNIQUE, category INTEGER, appgroup INTEGER, url VARCHAR(256))')
cursor.execute('CREATE TABLE IF NOT EXISTS permissions (id INTEGER PRIMARY KEY, permission VARCHAR(256) UNIQUE)')
cursor.execute('CREATE TABLE IF NOT EXISTS categories (id INTEGER PRIMARY KEY, category VARCHAR(256) UNIQUE)')
cursor.execute('CREATE TABLE IF NOT EXISTS app_permissions (id INTEGER PRIMARY KEY, appname INTEGER, permission INTEGER, UNIQUE (appname, permission) ON CONFLICT FAIL)')
#cursor.execute('CREATE TABLE IF NOT EXISTS urls_to_crawl (category VARCHAR(256), url VARCHAR(256))')

connection.commit()

class MarketCrawler(threading.Thread):
    mainURL = "https://play.google.com"
    topfreeURL = "https://play.google.com/store/apps/category/%s/collection/topselling_free?start=%d&num=%d"
    toppaidURL = "https://play.google.com/store/apps/category/%s/collection/topselling_paid?start=%d&num=%d"
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
            for url, cat2 in ((self.toppaidURL, 1), (self.topfreeURL, 0)):
                print ""
                print url % (cat, 0, self.pageIncrements)
                self.crawlAppsForCategory(url, cat, cat2)

    def crawlAppsForCategory(self, url, cat, cat2):
        pageIndex = 0
        curl = url % (cat, pageIndex, self.pageIncrements)
        twice = False

        while True:
            try:
                #print curl
                request = urllib2.Request(curl)
                request.add_header("User-Agent", "PermissionCrawler")
                handle = urllib2.build_opener()
                content = handle.open(request).read()
                soup = BeautifulSoup(content)

                print " crawling next %d entries starting with #%d" % (self.pageIncrements, pageIndex+1)
                appURLS = self.extractAppUrls(soup)
                duplicates = self.extractPermissionsIntoDB(appURLS, cat, cat2)

                if len(duplicates) == 0:
                    pageIndex+=self.pageIncrements
                # if we got first full repetition of page 1, go back one page and move on slowly until second full repetition
                elif ((len(duplicates) == self.pageIncrements) or (len(duplicates) >= myThreshold)) and (twice == False):
                    print >> sys.stderr, "  ! %d duplicate entries on last iteration" % len(duplicates)
                    pageIndex = max(pageIndex-self.pageIncrements, 0)
                    twice = True
                    duplicates = set()
                elif twice == True:
                    pageIndex+=1
                # resorting of top n apps may produce 1 or 2 duplicates - ignore low number of duplicates
                else:
                    pageIndex+=self.pageIncrements

                curl = url % (cat, pageIndex, self.pageIncrements)

                soup.decompose()

                if TERMAPP == True:
                    connection.close()
                    sys.exit()

                if ((len(duplicates) == self.pageIncrements) or (len(duplicates) >= myThreshold)) and (twice == True):
                    print >> sys.stderr, "INFO: stopped crawling categrory %s due to %s duplicates at last iteration twice" % (cat, len(duplicates))
                    return False

            except urllib2.HTTPError, error:
                if error.code == 404:
                    print >> sys.stderr, "404 ERROR: %s -> %s" % (error, error.url)
                if error.code == 403:
                    print >> sys.stderr, "403 (NO MORE APP PAGES FOR THIS CATEGORY)ERROR: %s -> %s" % (error, error.url)
                else:
                    print >> sys.stderr, "ERROR: %s" % error
                break
            #except Exception, e:
            #    print >> sys.stderr, "iSERROR: %s" % e
    

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
    def extractPermissionsIntoDB(self, appURLS, cat, cat2):
        #we can put this URL stuff into its own object /code repetition
        duplicates = set()
        for url in appURLS:
            request = urllib2.Request(url)
            request.add_header("User-Agent", "PyCrawler")
            handle = urllib2.build_opener()
            content = handle.open(request).read()
            soup = BeautifulSoup(content)
            
            appName = soup.find('h1','doc-banner-title').contents[0]
            permissions = soup.findAll('div','doc-permission-description')
            d = self.pushToDB(appName, cat, cat2, permissions, url)
            soup.decompose() 
            duplicates = duplicates | d
	if len(duplicates) > 0:
        	print " ", len(duplicates), "dups"
        return duplicates
    
    """
    Pushes permissions of a certain app into the DB
    cursor.execute('CREATE TABLE IF NOT EXISTS app_permissions (id INTEGER, appname VARCHAR(256), category VARCHAR(256), permission VARCHAR(256), url VARCHAR(256))')
    """
    def pushToDB(self, appName, cat, cat2, permissions, url):
        duplicates = set()
        for p in permissions:
            #print appName, cat, p.contents[0], url

            if cat in self.categories:
                catId = self.categories[cat]
            else:
                cursor.execute("INSERT OR IGNORE INTO categories VALUES ((?), (?))", (None, cat))
                cursor.execute("SELECT id FROM categories WHERE category=(?)", [cat])
                catId = self.categories[cat] = cursor.fetchone()[0]

            if len(self.apps) > 1000:
                apps = {}
            if appName in self.apps:
                appId = self.apps[appName]
            else:
                cursor.execute("INSERT OR IGNORE INTO apps VALUES ((?), (?), (?), (?), (?))", (None, appName, catId, cat2, url))
                cursor.execute("SELECT id FROM apps WHERE appname=(?)", [appName])
                appId = self.apps[appName] = cursor.fetchone()[0]
            permission = p.contents[0]

            if permission in self.permissions:
                permissionId = self.permissions[permission]
            else:
                cursor.execute("INSERT OR IGNORE INTO permissions VALUES ((?), (?))", (None, permission))
                cursor.execute("SELECT id FROM permissions WHERE permission=(?)", [permission])
                permissionId = self.permissions[permission] = cursor.fetchone()[0]

            try:
                cursor.execute("INSERT OR FAIL INTO app_permissions VALUES ((?), (?), (?))", (None, appId, permissionId))
            except sqlite.IntegrityError:
                duplicates.add(appName)
            connection.commit()
        return duplicates 

def SIGTERM_handler(signum, frame):
    global TERMAPP
    print '\n--- Caught SIGTERM; Attempting to quit gracefully ---'
    TERMAPP = True

signal.signal(signal.SIGTERM, SIGTERM_handler)

if __name__ == "__main__":
    #run the crawler thread
    MarketCrawler().run()
