import argparse, os, datetime, time, csv
from selenium import webdriver
from datetime import datetime
from sys import version
import pandas as pd
import multiprocessing as mp
import time
import traceback

os.system('clear')


class FacebookScraper(object):

    browser: webdriver.Firefox

    def __init__(self):
        self.browser = webdriver.Firefox()

    # --------------- Ask user to log in -----------------
    def fb_login(self, username, password):
        print("Opening browser...")
        time.sleep(1)
        self.browser.get("https://www.facebook.com/")

        # Log In
        email_id = self.browser.find_element_by_id("email")
        pass_id = self.browser.find_element_by_id("pass")
        email_id.send_keys(username)
        pass_id.send_keys(password)
        self.browser.find_element_by_id("loginbutton").click()
        # a = input("Please log into facebook and press enter after the page loads...")

    # --------------- Scroll to bottom of page -----------------
    def scroll_to_bottom(self):
        public_friends = self.browser \
            .find_elements_by_xpath('//div[@id="pagelet_timeline_medley_friends"]//div[@class="fsl fwb fcb"]/a')

        if not public_friends:
            return False;

        print("Scrolling to bottom...")
        while True:
                try:
                    self.browser.find_element_by_class_name('_4khu') # class after friend's list
                    print("Reached end!")
                    break
                except:
                    self.browser.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                    time.sleep(0.25)
                    pass

        return True

    # --------------- Get list of all friends on page ---------------
    def scan_friends(self):
        print('Scanning page for friends...')
        friends = []
        friend_cards = self.browser\
            .find_elements_by_xpath('//div[@id="pagelet_timeline_medley_friends"]//div[@class="fsl fwb fcb"]/a')

        for friend in friend_cards:
            if friend.get_attribute('data-hovercard') is None:
                print(" %s (INACTIVE)" % friend.text)
                friend_id = friend.get_attribute('ajaxify').split('id=')[1]
                friend_active = 0
            else:
                print(" %s" % friend.text)
                friend_id = friend.get_attribute('data-hovercard').split('id=')[1].split('&')[0]
                friend_active = 1

            friends.append({
                # 'name': friend.text.encode('ascii', 'ignore').decode('ascii'), # to prevent CSV writing issues
                'name': friend.text,
                'id': friend_id,
                'active': friend_active
                })
        print('Found %r friends on page!' % len(friends))
        return friends

    # ----------------- Load list from CSV -----------------
    def load_csv(self, filename):
        inactive = 0
        friends = []
        with open(filename, 'r', encoding='utf-8') as input_csv:
            reader = csv.DictReader(input_csv)
            for idx,row in enumerate(reader):
                if row['active'] is '1':
                    friends.append({
                        "name":row['B_name'],
                        "uid":row['B_id']
                        })
                else:
                    print("Skipping %s (inactive)" % row['B_name'])
                    inactive = inactive + 1
        print("%d friends in imported list" % (idx+1))
        print("%d ready for scanning (%d inactive)" % (idx - inactive + 1, inactive))

        return friends

    def load_from_df(self, df):
        df_to_process = df[df['active'] == 1][['B_name', 'B_id']].copy()
        inactive = df[df['active'] == 0].shape[0]
        df_to_process.columns = ['name', 'uid']
        print("%d friends in imported list" % (df.shape[0]))
        print("%d ready for scanning (%d inactive)" % (df.shape[0] - inactive, inactive))
        return df_to_process.to_dict('records')

    # --------------- Scrape 1st degree connections ---------------
    def scrape_1st_degrees(self):

        now = datetime.now()
        # Prep CSV Output File
        csv_out = '1st-degree_%s.csv' % now.strftime("%Y-%m-%d_%H%M")
        writer = csv.writer(open(csv_out, 'w', encoding='utf-8'))
        writer.writerow(['A_id','A_name','B_id','B_name','active'])

        # Get your unique Facebook ID
        profile_icon = self.browser.find_element_by_css_selector("[data-click='profile_icon'] > a > span > img")
        myid = profile_icon.get_attribute("id")[19:]

        # Scan your Friends page (1st-degree connections)
        print("Opening Friends page...")
        self.browser.get("https://www.facebook.com/" + myid + "/friends")
        self.scroll_to_bottom()
        my_friends = self.scan_friends()

        # Write connections to CSV File
        for friend in my_friends:
                writer.writerow([myid,"Me",friend['id'],friend['name'],friend['active']])

        print("Successfully saved to %s" % csv_out)
        self.browser.close()

    # --------------- Scrape 2nd degree connections. ---------------
    # This can take several days if you have a lot of friends!!
    def scrape_2nd_degrees(self, filename=None, df=None):
        time.sleep(1)
        now = datetime.now()
        # Prep CSV Output File
        csv_out = '2nd-degree_%s.csv' % now.strftime("%Y-%m-%d_%H%M%S")
        csv_skipped_out = '2nd-degree-SKIPPED_%s.csv' % now.strftime("%Y-%m-%d_%H%M%S")

        writer = csv.writer(open(csv_out, 'w', encoding='utf-8'))
        writer.writerow(['A_id', 'B_id', 'A_name','B_name','active'])

        writer_skipped = csv.writer(open(csv_skipped_out, 'w', encoding='utf-8'))
        writer_skipped.writerow(['B_id'])

        # Load friends from CSV Input File
        # script, filename = argv
        print("Loading list from %s..." % filename)
        friends = self.load_csv(filename) if filename else self.load_from_df(df)

        for idx,friend in enumerate(friends):
            # Load URL of friend's friend page
            scrape_url = "https://www.facebook.com/{}/friends?source_ref=pb_friends_tl".format(friend['uid'])
            self.browser.get(scrape_url)

            # Scan your friends' Friends page (2nd-degree connections)
            print("%d) %s" % (idx+1, scrape_url))
            has_public_friends = self.scroll_to_bottom()

            if not has_public_friends:
                print("NO PUBLIC FRIENDS FOR {}".format(friend['uid']))
                writer_skipped.writerow([friend['uid']])
                continue
            their_friends = self.scan_friends()

            # Write connections to CSV File
            print('Writing connections to CSV...')
            for person in their_friends:
                writer.writerow([friend['uid'], person['id'], friend['name'], person['name'], person['active']])

        self.browser.close()


def exec_worker(username, password, df):
        scraper = FacebookScraper()
        scraper.fb_login(username, password)
        scraper.scrape_2nd_degrees(df=df)
        return df.index


class ParallelProcessing(object):

    pool: mp.Pool

    to_process_csv = 'to_process.csv'

    workers: int

    pool: mp.Pool

    filename: str

    df_orig: pd.DataFrame

    def __init__(self, filename, workers=3):

        self.filename = filename
        self.df_orig = pd.read_csv(filename, index_col='index')
        df_temp = self.df_orig.copy()
        df_temp['index'] = df_temp.index
        df = df_temp[df_temp['processed'] == False].copy()
        df.to_csv(self.to_process_csv, index=False)

        self.workers = workers
        if self.workers > 1:
            self.pool = mp.Pool(workers)

    def run(self, username, password, chunk_size=50):
        total = self.df_orig[self.df_orig['processed']].shape[0]
        print(f'Processed records until now: {total}')

        start_time = time.time()

        reader = pd.read_csv(self.to_process_csv, index_col='index', chunksize=chunk_size)

        if self.workers > 1:
            worker_count = 0
            processes = []
            for df_chunk in reader:
                if worker_count == self.workers:
                    break
                p = self.pool.apply_async(exec_worker, [username, password, df_chunk])
                processes.append(p)
                worker_count += 1

            for p in processes:
                idxs = p.get()  # get processed indexes from df
                self.df_orig.loc[idxs, 'processed'] = True
        else:
            # Run in current thread. This is done to avoid socket permissions exception on certain Windows installations
            for df_chunk in reader:
                idxs = exec_worker(username, password, df_chunk)
                self.df_orig.loc[idxs, 'processed'] = True
                break

        total = self.df_orig[self.df_orig['processed']].shape[0]
        print(f'Total Processed records: {total}')

        self.df_orig.to_csv(self.filename)

        print(f'Total elapsed time: {time.time() - start_time} seconds')


if __name__ == "__main__":

    if not version.startswith('3.6'):
        print("You need Python 3.6.x to run this script")
        exit(1)

    parser = argparse.ArgumentParser(description='Facebook Scraper')
    parser.add_argument('-u', type=str, metavar='username', help='FB Username', required=True)
    parser.add_argument('-p', type=str, help='FB Password', required=True)
    parser.add_argument('--seeds', type=str, help='Seeds CSV Input File')
    parser.add_argument('--workers', type=int, help='No. of workers for parallel processing')
    args = parser.parse_args()

    # --------------- Start Scraping ---------------

    if not args.seeds:
        inst = FacebookScraper()
        inst.fb_login(args.u, args.p)
        inst.scrape_1st_degrees()
    elif not args.workers:
        inst = FacebookScraper()
        inst.fb_login(args.u, args.p)
        inst.scrape_2nd_degrees(filename=args.seeds)
    else:
        df = pd.read_csv(args.seeds)
        print(f'Total records to process: {df.shape[0]}')
        print(f'Records are going to be processed in parallel: {"YES" if args.workers > 1 else "NO"}')
        if 'processed' not in df.columns:
            df['processed'] = False
            df.to_csv(args.seeds, index_label='index')
        try:
            ParallelProcessing(args.seeds, workers=args.workers).run(args.u, args.p)
        except Exception as e:
            print(e)
            traceback.print_exc()
