from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from sqlite_client import SqliteClient
from util import Logger
import re
import time

class WebDriver:
    FB_URL = 'https://www.facebook.com/'
    PAGE_PAUSE_SEC = 1.5

    class Album:
        TABLE_NAME = 'Albums'
        FIELDS = ['id', 'name', 'gender', 'albums', 'photos']
        TYPES = ['text primary key', 'text', 'text', 'int', 'int']

    class MaleName:
        TABLE_NAME = 'MaleName'
        FIELDS = ['name', 'count']
        TYPES = ['text primary key', 'int']

    def __init__(self):
        chrome_options = webdriver.ChromeOptions()
        chrome_options.add_argument("--disable-notifications")
        self.driver = webdriver.Chrome('chromedriver.exe', chrome_options=chrome_options)
        self.browse_count = 1

    def __del__(self):
        if self.driver is not None:
            self.driver.close()

    def init_db(self):
        self.db_client = SqliteClient()
        self.db_client.connect(path='archive.db')
        self.db_client.create(WebDriver.Album.TABLE_NAME,
                              WebDriver.Album.FIELDS,
                              WebDriver.Album.TYPES)
        self.db_client.create(WebDriver.MaleName.TABLE_NAME,
                              WebDriver.MaleName.FIELDS,
                              WebDriver.MaleName.TYPES)

    def insert_album(self, id, name, gender, albums, photos):
        try:
            self.db_client.insert(WebDriver.Album.TABLE_NAME,
                                  WebDriver.Album.FIELDS,
                                  [id, name, gender, albums, photos],
                                  is_orreplace=True)
        except Exception as e:
            Logger.error("insert_album", "Error: %s" % e)

    def insert_malename(self, name, count):
        try:
            self.db_client.insert(WebDriver.MaleName.TABLE_NAME,
                                  WebDriver.MaleName.FIELDS,
                                  [name, count],
                                  is_orreplace=True)
        except Exception as e:
            Logger.error("insert_malename", "Error: %s" % e)

    def check_id_exists(self, id):
        ret = []
        try:
            ret = self.db_client.select(WebDriver.Album.TABLE_NAME,
                                  '*',
                                  'id = \'%s\'' % id)
        except Exception as e:
            Logger.error("check_id_exists", "Error: %s" % e)
        if ret is not None and len(ret) > 0:
            return True
        else:
            return False

    def get_male_name_count(self, name):
        ret = []
        try:
            ret = self.db_client.select(WebDriver.MaleName.TABLE_NAME,
                                        ['count'],
                                        'name=\'%s\'' % name)
        except Exception as e:
            Logger.error("get_male_name_count", "Error: %s" % e)

        if ret is not None and len(ret) > 0:
            return ret[0][0]
        else:
            return 0

    def go_to_url(self, url):
        self.driver.get(url)
        self.browse_count += 1

        if self.browse_count % 1000 == 0:
            time.sleep(30)
        elif self.browse_count % 100 == 0:
            time.sleep(10)
        else:
            time.sleep(1)

        self.check_account_locked()

    def connect(self, username, password):
        self.go_to_url(WebDriver.FB_URL)
        usr = self.driver.find_element_by_name("email")
        usr.clear()
        usr.send_keys(username)
        pwd = self.driver.find_element_by_name("pass")
        pwd.clear()
        pwd.send_keys(password)
        pwd.send_keys(Keys.RETURN)

    def scroll_bottom(self):
        lastHeight = self.driver.execute_script("return document.body.scrollHeight")
        while True:
            self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(WebDriver.PAGE_PAUSE_SEC)
            newHeight = self.driver.execute_script("return document.body.scrollHeight")
            if newHeight == lastHeight:
                break
            lastHeight = newHeight

    def go_friend_list(self, target_user_name):
        url = WebDriver.FB_URL + target_user_name + '/friends'
        self.go_to_url(url)
        self.scroll_bottom()
        friends = self.extract_all_friends()
        return friends

    def extract_all_friends(self):
        # Select all elements from the box
        friends = self.driver.find_elements_by_css_selector('div.fsl.fwb.fcb')
        # Get node "a"
        friends = [e.find_element_by_css_selector('a') for e in friends]
        # Get elements of [link, name]
        friends = [[e.get_attribute('href'), e.get_attribute('text')] for e in friends]
        # Get elements of [id, name]
        ret = []
        for friend in friends:
            id = re.compile('facebook\.com/profile\.php\?id=(.*)&fref.*').search(friend[0])
            if id is not None:
                ret.append([id.group(1), friend[1]])
            else:
                id = re.compile('facebook\.com/(.*)\?.*').search(friend[0])
                if id is not None:
                    ret.append([id.group(1), friend[1]])
        # Return it
        return ret

    def extract_gender(self, id):
        url = WebDriver.FB_URL + id + '/about?section=contact-info&pnref=about'
        self.go_to_url(url)
        try:
            info = self.driver.find_element_by_css_selector('li._3pw9._2pi4._2ge8._3ms8')
            info = info.find_elements_by_css_selector('span._50f4')
            gender = info[1].text
            return gender
        except Exception as e:
            return 'Unknown'

    def extract_albums(self, id):
        url = WebDriver.FB_URL + id + '/photos_albums'
        self.go_to_url(url)
        # self.scroll_bottom()
        sum_photos = 0
        albums = self.driver.find_elements_by_css_selector('div.fbPhotoAlbumActionList.fsm.fwn.fcg')
        for album in albums:
            text = album.text

            if text.find('photo') > -1:
                sum_photos += int(re.compile('(.*) photo.*').search(text).group(1).replace(',', ''))

        return len(albums), sum_photos

    def check_account_locked(self):
        headers = None
        try:
            headers = self.driver.find_element_by_css_selector('h2.uiHeaderTitle')
        except:
            pass
        finally:
            if headers is not None and headers.text.find('Sorry, this content isn\'t available at the moment') > -1:
                raise Exception('The account is locked.')

