import multiprocessing as mp
import os
import gzip
import re
from netaddr import iprange_to_cidrs

def parse_property(block, name):
    match = re.findall(u"^{0:s}:\s*(.*)$".format(name), block, re.MULTILINE)
    if match:
        return " ".join(match)
    else:
        return None

def parse_property_inetnum(block):
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


def process_wrapper(chunkStart, chunkSize):
  print("Process id:", os.getpid())

  with gzip.open("data/ripe.db.inetnum.gz", mode="rt", encoding="ISO-8859-1") as f:
    f.seek(chunkStart)

    # TODO: Fix me
    # problem here is that the lines can be split across non-word boundaries...
    lines = f.read(chunkSize).splitlines()
    print(len(lines))

    single_block = ""
    blocks = []
    for line in lines:
      print("LINE: {}\n".format(line))
      if (
        line.startswith("%")
        or line.startswith("#")
        or line.startswith("remarks:")
        or line.startswith(" ")):
          continue

      single_block += line + "\n"
      if "source:" in single_block:
        blocks.append(single_block)
        single_block = ""


    print("FIRST BLOCK: {}".format(blocks[0]))

def chunkify(fname, size=1024*1024):
  f2 = gzip.open(fname, mode="rt", encoding="ISO-8859-1")
  fileEnd = os.path.getsize(f2.name)
  with open(f2.name, 'rb') as f:
    chunkEnd = f.tell()
    while True:
      chunkStart = chunkEnd
      f.seek(size, 1)
      f.readline()
      chunkEnd = f.tell()
      yield chunkStart, chunkEnd-chunkStart
      if chunkEnd > fileEnd:
        break

def main():
  pool = mp.Pool(4)
  jobs = []

  for chunkStart, chunkSize in chunkify("data/ripe.db.inetnum.gz"):
    print("chunkStart: {}, chunkSize: {}".format(chunkStart, chunkSize))
    jobs.append(pool.apply_async(process_wrapper,(chunkStart,chunkSize)))

  for job in jobs:
    job.get()

  pool.close()


if __name__ == "__main__":
  main()
