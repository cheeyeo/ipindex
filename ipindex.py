import click
import re
import pymongo
import gzip
import json

from contextlib import closing
from urllib.request import urlopen
from pymongo import MongoClient
from pymongo import InsertOne
from pymongo.write_concern import WriteConcern


from netaddr import iprange_to_cidrs

import threading
from numpy import array_split

import csv

# mongo settings
DB_HOST = "localhost"
DB_PORT = 27017

FILE_LIST = {
    "ripe": [
        "ripe.db.inetnum.gz",
        "ftp://ftp.ripe.net/ripe/dbase/split/ripe.db.inetnum.gz",
    ],
    # "ripe-ipv6": [
    #     "ripe.db.inet6num.gz",
    #     "ftp://ftp.ripe.net/ripe/dbase/split/ripe.db.inet6num.gz",
    # ],
    # "arin": ["arin.db", "ftp://ftp.arin.net/pub/rr/arin.db"],
    # "afrinic": [
    #     "afrinic.db.gz",
    #     "ftp://ftp.afrinic.net/pub/dbase/afrinic.db.gz",
    # ],
    # "apnic": [
    #     "apnic.db.inetnum.gz",
    #     "ftp://ftp.apnic.net/pub/apnic/whois/apnic.db.inetnum.gz",
    # ],
    # "apnic-ipv6": [
    #     "apnic.db.inet6num.gz",
    #     "ftp://ftp.apnic.net/pub/apnic/whois/apnic.db.inet6num.gz",
    # ],
}


@click.group()
def cli():
  pass


def download_file(url, name):

    with closing(urlopen(url)) as source:
        with open(name, "wb") as target:
            target.write(source.read())


def connect_mongodb():
  """ mongodb connection """
  try:
    client = MongoClient(
        DB_HOST, DB_PORT, maxPoolSize=200, serverSelectionTimeoutMS=10
    )
    client.server_info()
  except pymongo.errors.ServerSelectionTimeoutError:
    click.secho("[ERROR] failed to connect to MongoDB", fg="red")
    exit(1)

  return client


def update_mongodb(client, data):
  """ update mongodb record """
  print("Inside update mongodb with thread {}".format(threading.current_thread().getName()))
  print(client)

  db = client['ipindex']

  # write concern sets journal to False?
  coll = db.get_collection('items', write_concern=WriteConcern(j=False))

  # Disable indexes during import
  coll.drop_indexes()

  items = [InsertOne(d) for d in data]

  print('ITEMS LENGTH: {:d}'.format(len(items)))
  print('CREATED ITEMS: {}'.format(items[0]))

  try:
    coll.bulk_write(items, ordered=False)
    # db.ipindex.items.bulk_write(data)
  except RuntimeError:
    click.secho("[ERROR] failed to update db record", fg="red")

  client.close()
  return True


def search_mongodb(client, field, query):
  db = client['ipindex']
  myquery = {field: {"$regex": query}}
  try:
      mydoc = db.items.find(myquery)
      return mydoc
  except RuntimeError:
      click.secho("[ERROR] failed to search db", fg="red")


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


def read_blocks(filename, source):
  if filename.endswith(".gz"):
    f = gzip.open(filename, mode="rt", encoding="ISO-8859-1")
  else:
    f = open(filename, mode="rt", encoding="ISO-8859-1")

  blocks = []

  for record in csv.reader(f, delimiter='\n', lineterminator='\n\n'):
    if len(record) == 0:
      continue

    line = record[-1]

    if (
      line.startswith("%")
      or line.startswith("#")
      or line.startswith("remarks:")
      or line.startswith(" ")):
        continue

    if (line.startswith("inetnum:")
      or line.startswith("inet6num:")):
        blocks.append(parse_block(line, source))

  f.close()
  return blocks

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

def parse_blocks(blocks, source):
  res = list()
  for block in blocks:
    data = {
      "inetnum": parse_property_inetnum(block),
      "netname": parse_property(block, "netname"),
      "description": parse_property(block, "descr"),
      "country": parse_property(block, "country"),
      "maintained_by": parse_property(block, "mnt-by"),
      "created": parse_property(block, "created"),
      "last_modified": parse_property(block, "last-modified"),
      "source": source,
    }
    res.append(data)
  return res

@cli.command()
@click.option("--parse", is_flag=True, help="import DB files", required=False)
@click.option("--search", help="search DB files", required=False)
@click.option(
    "--download",
    is_flag=True,
    help="download all WHOIS data files",
    required=False,
)

def main(parse, search, download):
  if download:
      for key, value in FILE_LIST.items():
          click.secho("[INFO] downloading %s" % key.upper(), fg="green")
          download_file(value[1], "data/" + value[0])

  if search:
    client = connect_mongodb()
    tmp_data = []
    result = search_mongodb(client, "description", search)

    # mongodb.count is deprecated, loop cursor and check
    for x in result:
        tmp_data.append(x)

    if tmp_data:
        pass
    else:
        click.secho("[INFO] no results found for %s" % search, fg="green")

  if parse:
    # create mongodb client
    # we only need to create one client since this is a single process
    client = connect_mongodb()

    for key, value in FILE_LIST.items():
      click.secho("[INFO] parsing %s" % key.upper(), fg="green")
      blocks = read_blocks("data/" + value[0], key.upper())
      click.secho("[INFO] netblocks found %s" % len(blocks), fg="green")

      # split data into nos of threads
      num_threads = 8
      data = array_split(blocks, num_threads)
      i = 1

      for d in data:
        thread_name = 'Thread: {:d}'.format(i)
        t = threading.Thread(name=thread_name, target=update_mongodb, args=(client, d,))
        t.start()
        i+=1
    exit(0)

if __name__ == "__main__":
    main()
