import json
import threading
from queue import Queue

class Bible:
  def __init__(self, code):
    self.code = code
    self.lock = threading.Lock()
    self.verses = {}
    self.queue = self.build_queue()

  def build_queue(self):
    with open('index.json','r') as f:
      index = json.loads(f.read())

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
