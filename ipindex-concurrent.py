import os
import gzip
import multiprocessing as mp
import re
import shutil
import uuid
from netaddr import iprange_to_cidrs
import pymongo
from pymongo import MongoClient
from pymongo import InsertOne
from pymongo.write_concern import WriteConcern


# mongo settings
DB_HOST = "127.0.0.1"
DB_PORT = 27017

FILE_LIST = {
    "ripe": [
      "ripe.db.inetnum.gz",
      "ftp://ftp.ripe.net/ripe/dbase/split/ripe.db.inetnum.gz",
    ],
    "ripe-ipv6": [
      "ripe.db.inet6num.gz",
      "ftp://ftp.ripe.net/ripe/dbase/split/ripe.db.inet6num.gz",
    ],
    "arin": [
      "arin.db",
      "ftp://ftp.arin.net/pub/rr/arin.db"
    ],
    "afrinic": [
      "afrinic.db.gz",
      "ftp://ftp.afrinic.net/pub/dbase/afrinic.db.gz",
    ],
    "apnic": [
      "apnic.db.inetnum.gz",
      "ftp://ftp.apnic.net/pub/apnic/whois/apnic.db.inetnum.gz",
    ],
    "apnic-ipv6": [
      "apnic.db.inet6num.gz",
      "ftp://ftp.apnic.net/pub/apnic/whois/apnic.db.inet6num.gz",
    ],
}

def parse_property(block, name):
    match = re.findall(u"{0:s}:(.+?)(?=\|.+\:)".format(name), block)
    if match:
        return " ".join(match).strip()
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

def chunkify(filename, size=1024*1024):
  fileEnd = os.path.getsize(filename)
  print("ORIG FILE SIZE:", fileEnd)

  with open(filename, "rb") as f:
    chunkEnd = f.tell()
    while True:
      chunkStart = chunkEnd
      f.seek(size, 1)

      # incomplete line
      l = f.readline()
      l = f.readline()
      if not l:
        return

      while l and (b"% RIPE-USER-RESOURCE" in l):
        p = f.tell()
        l = f.readline()
        f.seek(p) # revert one line

      chunkEnd = f.tell()
      yield chunkStart, chunkEnd - chunkStart
      if chunkEnd > fileEnd:
        break

def process_wrapper(chunkStart, chunkSize, filename, source):
  print("Process id:", os.getpid())

  single_block = ""
  blocks = []

  with open(filename, mode="rt", encoding="ISO-8859-1") as f:
    f.seek(chunkStart)
    lines = f.read(chunkSize).splitlines()

    for line in lines:
      if (
      line.startswith("%")
      or line.startswith("#")
      or line.startswith("remarks:")
      or line.startswith(" ")):
        continue

      if line.strip() == "":
        if single_block.startswith("inetnum:") or single_block.startswith("inet6num:"):
          blocks.append(single_block)
          single_block = ""
        else:
          single_block = ""
      else:
        single_block += line + '|'

  parsed_blocks = parse_blocks(blocks, source)
  print("Parsed {:d} records".format(len(parsed_blocks)))
  return parsed_blocks

def parse_blocks(blocks, source):
  res = list()
  for block in blocks:
    data = {
      "_id": str(uuid.uuid4()),
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

def connect_mongodb():
  """ mongodb connection """
  try:
    client = MongoClient(
        DB_HOST, DB_PORT, maxPoolSize=200, serverSelectionTimeoutMS=20
    )
    client.server_info()
  except pymongo.errors.ServerSelectionTimeoutError:
    print("[ERROR] failed to connect to MongoDB")
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

def remove_indexes(client, collection):
  db = client['ipindex']
  # write concern sets journal to False?
  coll = db.get_collection(collection)
  coll.drop_indexes()

def rebuild_indexes(client, collection):
  pass


def main():
  client = connect_mongodb()

  pool = mp.Pool(4)
  jobs = []

  for key, value in FILE_LIST.items():
    archive = "data/" + value[0]
    filename = "data/" + value[0].replace(".gz", "")
    print("[INFO] parsing", filename)
    # extract gzip file
    with gzip.open(archive, mode="rb") as f_in:
      with open(filename, "wb") as f_out:
        shutil.copyfileobj(f_in, f_out)

    for chunkStart, chunkSize in chunkify(filename):
      print("chunkStart: {}, chunkSize: {}".format(chunkStart, chunkSize))
      jobs.append(pool.apply_async(process_wrapper,(chunkStart,chunkSize, filename, key.upper())))

    count = 0
    for job in jobs:
      blocks = job.get()
      print("RETURNED BLOCKS: ", len(blocks))
      count += len(blocks)
      res = update_mongodb(client, blocks)
      print("Mongodb Update: ", res)

    print("Total records imported: {:d}".format(count))

    # delete extracted file
    os.remove(filename)

  pool.close()
  exit(0)


if __name__ == "__main__":
  main()
