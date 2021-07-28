!pip install -q cloudscraper
import os
import re
import sys
import argparse
import requests
import threading
import cloudscraper
import pandas as pd
from queue import Queue
from bs4 import BeautifulSoup


def get_chapter(code, resource):
  scraper = cloudscraper.create_scraper()
  chapter = scraper.get(f"https://www.bible.com/bible/{code}/{resource}").content
  soup = BeautifulSoup(chapter, 'html.parser')
  verse_wrappers = [s for s in soup.find_all('span') if 'data-usfm' in s.attrs.keys()]
  verse_nums = set([v.get('data-usfm') for v in verse_wrappers])
  verse_spans = []
  for v in verse_nums:
    span_list = soup.find_all('span',attrs={'data-usfm':v})
    all_verse_content = ""
    for s in span_list:
      individual_spans = s.find_all('span',attrs={'class':'content'})
      content = ' '.join([i.text.strip() for i in individual_spans])
      all_verse_content += content + ' '
    verse_spans.append([v, all_verse_content.strip()])
  verse_spans.sort(key=lambda pair: int(pair[0].split('.')[-1]))
  verse_spans = {line[0]:line[1] for line in verse_spans}
  return verse_spans


def worker(bible_obj):
while True:
code = bible_obj.code
resource = bible_obj.queue.get()
chapter = get_chapter(code, resource)
bible_obj.update(chapter)
bible_obj.queue.task_done()


class Bible:
  def __init__(self, code):
    self.code = code
    self.lock = threading.Lock()
    self.verses = {}
    self.queue = self.build_queue()

  def build_queue(self):
    url = "https://raw.githubusercontent.com/drewstackhouse/scraper/main/index.json"
    index = requests.get("https://raw.githubusercontent.com/drewstackhouse/scraper/main/index.json").json()
    pages = []
    for book in index:
      num_verses = int(index.get(book))
      book_codes = list(map(lambda x: f"{book}.{x}", list(range(1, num_verses+1))))
      for b in book_codes:
        pages.append(b)    
    queue = Queue()
    for p in pages:
      queue.put(p)
    return queue
  
  def update(self, new_verses):
    with self.lock:
      self.verses = {**self.verses, **new_verses}


def get_multiple(source_target_dict):
  source = source_target_dict['source']
  target = source_target_dict['target']
  master = {code:None for code in source + target}
  for code in master:
    bible = Bible(code)
    threads = []
    n_threads = 50
    for i in range(n_threads):
      t = threading.Thread(target=worker, args=(bible, ), daemon=True)
      threads.append(t)
    for t in threads:
      t.start()
    bible.queue.join()
    master[code] = bible.verses
    print(f'{code} saved.')
    print(f'Bible consumes {sys.getsizeof(master[code])/1000000} MB.')
  
    return master


def map_joins(outpath, master_dict):
  for s in source:
    src = pd.DataFrame(list(master_dict[s].items())).rename(columns={0:'verse',1:'src'}).set_index('verse')
    for t in target:
      tgt = pd.DataFrame(list(master_dict[t].items())).rename(columns={0:'verse',1:'tgt'}).set_index('verse')
      df = pd.merge(src, tgt, how='inner', on='verse')
      df.src = df.src.str.replace('\n','')
      df.tgt = df.tgt.str.replace('\n','')
      df.to_csv(outpath, mode='a', sep='\t', header=None, index=None)
      print(f'{s}_{t} added to {outpath}')


def restrict_length(path, words=100):
  new_path = f'{path}_filt.txt'
  with open(new_path,'a') as outfile:
    with open(path,'r') as infile:
      for line in infile:
        src, tgt = line.split('\t')
        src_split = src.split(' ')
        tgt_split = tgt.split(' ')
        max_len = max([len(src_split), len(tgt_split)])
        if max_len <= 100:
          outfile.write(line)
    os.remove(path)
    os.rename(new_path, path)


if __name__ == __main__:
  parser = argparse.ArgumentParser()
  parser.add_argument('--output','-o', type=str, help="output filename")
  parser.add_argument('--source','-s', nargs='+', type=int, help="codes of source language texts")
  parser.add_argument('--target','-t', nargs='+', type=int, help="codes of target language texts")
  args = parser.parse_args()

  source = args.source
  target = args.target
  outpath = args.output

  to_scrape = {'source':source, 'target':target}
  master = get_multiple(to_scrape)
  map_joins(outpath, master)
  restrict_length(outpath)
