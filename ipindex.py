import click
import re
import pymongo
import gzip
import json
import multiprocessing as mp
import uuid
from netaddr import iprange_to_cidrs
from itertools import groupby
from contextlib import closing
from urllib.request import urlopen
import pymongo
from pymongo import MongoClient
from pymongo.write_concern import WriteConcern

# mongo settings
DB_HOST = "localhost"
DB_PORT = 27017

FILE_LIST = {
    "ripe": [
      "ripe.db.inetnum.gz",
      "ftp://ftp.ripe.net/ripe/dbase/split/ripe.db.inetnum.gz",
      "inetnum",
    ],
    "ripe-ipv6": [
      "ripe.db.inet6num.gz",
      "ftp://ftp.ripe.net/ripe/dbase/split/ripe.db.inet6num.gz",
      "inet6num",
    ],
    "arin": [
      "arin.db",
      "ftp://ftp.arin.net/pub/rr/arin.db",
      "as-set",
    ],
    "afrinic": [
      "afrinic.db.gz",
      "ftp://ftp.afrinic.net/pub/dbase/afrinic.db.gz",
      "as-block",
    ],
    "apnic": [
      "apnic.db.inetnum.gz",
      "ftp://ftp.apnic.net/pub/apnic/whois/apnic.db.inetnum.gz",
      "inetnum",
    ],
    "apnic-ipv6": [
      "apnic.db.inet6num.gz",
      "ftp://ftp.apnic.net/pub/apnic/whois/apnic.db.inet6num.gz",
      "inet6num",
    ],
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
        DB_HOST, DB_PORT, maxPoolSize=200, serverSelectionTimeoutMS=20
    )
    client.server_info()
  except pymongo.errors.ServerSelectionTimeoutError:
    click.secho("[ERROR] failed to connect to MongoDB", fg="red")
    exit(1)

  return client


def update_mongodb(client, data):
  """ update mongodb record """
  print("Inside update mongodb with process {}".format(os.getpid()))

  db = client['ipindex']
  # write concern sets journal to False?
  coll = db.get_collection('items', write_concern=WriteConcern(j=False))

  try:
    coll.insert_many(data, ordered=False)
  except pymongo.errors.BulkWriteError as e:
    print("[Error] Error with bulk update of db records: ", e.details['writeErrors'])
  except RuntimeError:
    print("[ERROR] failed to update db record")

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

def file_block(fp, number_of_blocks, block):
  '''
  A generator that splits a file into blocks and iterates
  over the lines of one of the blocks.
  '''
  assert 0 <= block and block < number_of_blocks
  assert 0 < number_of_blocks

  fp.seek(0,2)
  file_size = fp.tell()

  ini = file_size * block / number_of_blocks
  end = file_size * (1 + block) / number_of_blocks

  if ini <= 0:
    fp.seek(0)
  else:
    fp.seek(ini-1)
    fp.readline()

  while fp.tell() < end:
    yield fp.readline()

def make_grouper(tag):
  counter = 0
  def key(line):
    nonlocal counter
    if line.startswith(tag):
      counter += 1
    return counter
  return key

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

def process(num_workers, chunk_number, filename, source):
  print("Process id:", os.getpid())

  if filename.endswith(".gz"):
    fp = gzip.open(filename, mode="rt", encoding="ISO-8859-1")
  else:
    fp = open(filename, mode="rt", encoding="ISO-8859-1")

  blocks = []
  tag = FILE_LIST[source.lower()][-1]
  fbp = file_block(fp, num_workers, chunk_number)
  count = 0
  client = connect_mongodb()

  for k, group in groupby(fbp, key=make_grouper(tag)):
    blocks.append(''.join(group))
    # for every 1000 records we parse and save to db
    if len(blocks) >= 1000:
      count += len(blocks)
      parsed_blocks = parse_blocks(blocks, source)
      print("Parsed {:d} records".format(len(parsed_blocks)))
      # print(parsed_blocks[0])
      res = update_mongodb(client, parsed_blocks)
      print("Mongodb Update: ", res)
      blocks = []

  # if there are any remaining elements we save them
  if blocks and len(blocks) > 0:
    count += len(blocks)
    parsed_blocks = parse_blocks(blocks, source)
    print("Parsed Remaining {:d} records".format(len(parsed_blocks)))
    res = update_mongodb(client, parsed_blocks)
    print("Mongodb Update: ", res)
    blocks = []

  fp.close()
  return count

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
    num_workers = 4
    pool = mp.Pool(num_workers)
    jobs = []

    for key, value in FILE_LIST.items():
      filename = "data/" + value[0]
      print("[INFO] Parsing", filename)
      for chunk_number in range(num_workers):
        jobs.append(pool.apply_async(process, (num_workers, chunk_number, filename, key.upper())))

      count = 0
      for job in jobs:
        blocks = job.get()
        print("RETURNED BLOCKS: ", blocks)
        count += blocks

      print("Total records imported: {:d}".format(count))

    pool.close()
    exit(0)


if __name__ == "__main__":
    main()
