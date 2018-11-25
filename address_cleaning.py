## Need to build this into a commandline tool that can take an input csv.

import re, csv


testdata = ['P O Box 337  216 West Front Street #11', '15355 Doc Rd Hwy 89 P.O. Box 717', 'Rte 1 Box 147']
            
pattern = re.compile(r"[Pp][\s|\.][Oo][\s|\.]*[BbOoXx]*[\s][0-9]*")
m = re.search(pattern, testdata[0])
m.group()
formattedAddress = re.sub(pattern, "", testdata[0], 1)
