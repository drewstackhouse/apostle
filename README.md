# Setup:

```bash
$ git clone https://github.com/drewstackhouse/apostle.git
$ cd apostle
$ pip install -r requirements.txt
```

# Usage:
Command line:
```bash
$ python3 apostle.py --source 1359 2079 406 12 1932 416 37 70 206 --target 1747 1531 --output en_sg.txt
```
Python:
```python
import apostle

source = [1359, 2079, 406, 12, 1932, 416, 37, 70, 206]
target = [1747, 1531]
outpath = 'en_sg.txt'

to_scrape = {'source':source, 'target':target}
master = apostle.get_multiple(to_scrape)
apostle.map_joins(outpath, master, source, target)
apostle.restrict_length(outpath)
```
