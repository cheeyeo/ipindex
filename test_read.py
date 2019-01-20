import gzip
import csv
import re
from netaddr import iprange_to_cidrs

def parse_property(block: str, name: str):
    match = re.findall(u"^{0:s}:\s*(.*)$".format(name), block, re.MULTILINE)
    if match:
        return " ".join(match)
    else:
        return None


def parse_property_inetnum(block: str):
    # IPv4
    match = re.findall(
        "^inetnum:[\s]*((?:\d{1,3}\.){3}\d{1,3}[\s]*-[\s]*(?:\d{1,3}\.){3}\d{1,3})",
        block,
        re.MULTILINE,
    )
    if match:
        ip_start = re.findall(
            "^inetnum:[\s]*((?:\d{1,3}\.){3}\d{1,3})[\s]*-[\s]*(?:\d{1,3}\.){3}\d{1,3}",
            block,
            re.MULTILINE,
        )[0]
        ip_end = re.findall(
            "^inetnum:[\s]*(?:\d{1,3}\.){3}\d{1,3}[\s]*-[\s]*((?:\d{1,3}\.){3}\d{1,3})",
            block,
            re.MULTILINE,
        )[0]
        cidrs = iprange_to_cidrs(ip_start, ip_end)
        return "{}".format(cidrs[0])
    # IPv6
    else:
        match = re.findall(
            "^inet6num:[\s]*([0-9a-fA-F:\/]{1,43})", block, re.MULTILINE
        )
        if match:
            return match[0]


def parse_block(block, source):
  data = {
    "inetnum": parse_property_inetnum(block),
    "netname": parse_property(block, "netname"),
    "description": parse_property(block, "descr"),
    "country": parse_property(block, "country"),
    "maintained_by": parse_property(block, "mnt-by"),
    "created": parse_property(block, "created"),
    "last_modified": parse_property(block, "last-modified"),
    "source": source
  }

  return data

f = gzip.open("data/ripe.db.inetnum.gz", mode="rt", encoding="ISO-8859-1")

blocks = []

for record in csv.reader(f, delimiter='\n', lineterminator='\n\n'):
  # record is an array holding the string data itself
  print(record)
  if len(record) == 0:
    continue

  line = record[-1]
  print(line)

  if (
    line.startswith("%")
    or line.startswith("#")
    or line.startswith("remarks:")
    or line.startswith(" ")):
      continue

  if line.startswith("inetnum:") or line.startswith("inet6num:"):
    parse_block(line, "ripe")

f.close()


print(len(blocks))
print(blocks[0])
print(blocks[-1])
