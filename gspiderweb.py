from web_driver import WebDriver
from util import Logger
import logging
import argparse
import sys
import time

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Tool for backtesting tick data')
    parser.add_argument('-user', action='store', help='User name')
    parser.add_argument('-pwd', action='store', help='Password')
    args = parser.parse_args()
    Logger.init_log()
    default_male_count = 2
    user = 'chingjan.jang'

    webdriver = WebDriver()
    webdriver.init_db()
    webdriver.connect(username=args.user, password=args.pwd)
    friends = webdriver.go_friend_list(user)

    for i in range(0, len(friends)):
        friend = friends[i]
        Logger.info('main', '%d/%d: Running id %s' % (i, len(friends), friend[1]))
        # Replace characters
        friend[1] = friend[1].replace('\'', '')\
                             .replace('\"', '')\
                             .replace('(', '')\
                             .replace(')', '')\
                             .replace('[', '')\
                             .replace(']', '')\
                             .replace('`', '')
        name_split = friend[1].split(' ')
        if not webdriver.check_id_exists(friend[0]):
            # Try to guess the gender
            if len(name_split) == 2 and webdriver.get_male_name_count(name_split[0]) >= default_male_count:
                Logger.info('main', 'Id %s should be a male' % friend[1])
                continue

            # Continue to find the gender
            gender = webdriver.extract_gender(friend[0])
            if gender != 'Male':
                num_albums, num_photos = webdriver.extract_albums(friend[0])
                webdriver.insert_album(friend[0], friend[1], gender, num_albums, num_photos)
            else:
                webdriver.insert_album(friend[0], friend[1], gender, 0, 0)

                if gender == 'Male':
                    if len(name_split) == 2:
                        count = webdriver.get_male_name_count(name_split[0])
                        webdriver.insert_malename(name_split[0], count + 1)

    Logger.info('main', 'Finish to investigate user %s' % user)