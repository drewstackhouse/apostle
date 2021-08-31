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
from apostle import Apostle

outpath = 'en_al.txt'
source = [1359, 2079, 406, 12, 1932, 416, 37, 70, 206]
target = [487]

a = Apostle()
a.build(outpath, source, target)
```
