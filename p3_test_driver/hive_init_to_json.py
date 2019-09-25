import re
import fileinput

for line in fileinput.input():
    m = re.match('set (.*)=(.*);', line)
    if m:
        print('"hiveconf:%s": %s,' % m.groups())
