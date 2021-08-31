import os
import re
import threading
import cloudscraper
import pandas as pd
from bs4 import BeautifulSoup

from bible import Bible

class Apostle:
  def __init__(self):
    self.master = {}

  def get_chapter(self, code, resource):
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

  def worker(self, bible_obj):
    while True:
      try:
        code = bible_obj.code
        resource = bible_obj.queue.get()
        chapter = self.get_chapter(code, resource)
        bible_obj.update(chapter)
        bible_obj.queue.task_done()
      except:
        print('An error occurred.')

  def get_multiple(self, source_target_dict):
    source = source_target_dict['source']
    target = source_target_dict['target']
    source_plus_target = {code:None for code in source + target}
    for code in source_plus_target:
      if code not in self.master:
        bible = Bible(code)
        threads = []
        n_threads = 50
        for i in range(n_threads):
          t = threading.Thread(target=self.worker, args=(bible, ), daemon=True)
          threads.append(t)
        for t in threads:
          t.start()
        bible.queue.join()
        self.master[code] = bible.verses
        print(f'{code} saved.')
      else:
        print(f'{code} already saved.')
    print('All bibles saved.')
    return self.master

  def map_joins(self, outpath, source, target):
    for s in source:
      src = pd.DataFrame(list(self.master[s].items())).rename(columns={0:'verse',1:'src'}).set_index('verse')
      for t in target:
        tgt = pd.DataFrame(list(self.master[t].items())).rename(columns={0:'verse',1:'tgt'}).set_index('verse')
        df = pd.merge(src, tgt, how='inner', on='verse')
        df.src = df.src.str.replace('\n','')
        df.tgt = df.tgt.str.replace('\n','')
        df.to_csv(outpath, mode='a', sep='\t', header=None, index=None)
        print(f'{s}_{t} added to {outpath}')

  def restrict_length(self, path, words=100):
    new_path = f'{path.split(".txt")[0]}_filt.txt'
    with open(new_path,'a') as outfile:
      with open(path,'r') as infile:
        for line in infile:
          src, tgt = line.split('\t')
          src_split = src.split(' ')
          tgt_split = tgt.split(' ')
          max_len = max([len(src_split), len(tgt_split)])
          if max_len <= words:
            outfile.write(line)
      os.remove(path)
      os.rename(new_path, path)
      print('Length restriction complete.')

  def build(self, outpath, source, target, expand=True):
    source_target_dict = {'source': source, 'target': target}
    self.get_multiple(source_target_dict)
    self.map_joins(outpath, source, target)
    if expand:
      self.expand(outpath)
    self.restrict_length(outpath)

  def expand(self, path):
    with open(path,'r') as infile:
      lines = infile.readlines()
    os.remove(path)

    newlines = []
    for line in lines:
      en, ch = line.split('\t')
      src_punc = re.findall('([\,\.\:\;\!\?\-])', en)
      ch_punc = re.findall('([\,\.\:\;\!\?\-])', ch)
      if src_punc == ch_punc:
        src_split = re.split('([\,\.\:\;\!\?\-])', en)
        ch_split = re.split('([\,\.\:\;\!\?\-])', ch)
        en = src_split[0:-1:2]
        ch = ch_split[0:-1:2]
        src_tgt = list(zip(en, ch))
        for line in src_tgt:
          newlines.append('\t'.join(line).strip())
      else:
        newlines.append(line)

    with open(path,'a+') as outfile:
      for line in newlines:
        if not line.endswith('\n'):
          line += '\n'
        outfile.write(line)


if __name__ == '__main__':
  import argparse
  parser = argparse.ArgumentParser()
  parser.add_argument('--output','-o', type=str, help="output filename")
  parser.add_argument('--source','-s', nargs='+', type=int, help="codes of source language texts")
  parser.add_argument('--target','-t', nargs='+', type=int, help="codes of target language texts")
  args = parser.parse_args()

  source = args.source
  target = args.target
  outpath = args.output

  a = Apostle()
  to_scrape = {'source':source, 'target':target}
  a.get_multiple(to_scrape)
  a.map_joins(outpath, source, target)
  a.restrict_length(outpath)
