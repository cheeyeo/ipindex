import os
import gzip
import multiprocessing as mp
import re
import uuid
from netaddr import iprange_to_cidrs
import pymongo
from pymongo import MongoClient
from pymongo.write_concern import WriteConcern


# mongo settings
DB_HOST = "127.0.0.1"
DB_PORT = 27017

FILE_LIST = {
    "ripe": [
      "ripe.db.inetnum.gz",
      "ftp://ftp.ripe.net/ripe/dbase/split/ripe.db.inetnum.gz",
    ],
    # "ripe-ipv6": [
    #   "ripe.db.inet6num.gz",
    #   "ftp://ftp.ripe.net/ripe/dbase/split/ripe.db.inet6num.gz",
    # ],
    # "arin": [
    #   "arin.db",
    #   "ftp://ftp.arin.net/pub/rr/arin.db"
    # ],
    # "afrinic": [
    #   "afrinic.db.gz",
    #   "ftp://ftp.afrinic.net/pub/dbase/afrinic.db.gz",
    # ],
    # "apnic": [
    #   "apnic.db.inetnum.gz",
    #   "ftp://ftp.apnic.net/pub/apnic/whois/apnic.db.inetnum.gz",
    # ],
    # "apnic-ipv6": [
    #   "apnic.db.inet6num.gz",
    #   "ftp://ftp.apnic.net/pub/apnic/whois/apnic.db.inet6num.gz",
    # ],
}

def parse_property(block, name):
  match = re.findall(u"^{0:s}:\s*(.*)$".format(name), block, re.MULTILINE)
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


def process_wrapper(filename, num_workers, chunk_number, source):
  print("Process id: {} Filename: {} Num workers: {} Chunk: {}".format(os.getpid(), filename, num_workers, chunk_number))

  if filename.endswith(".gz"):
    fp = gzip.open(filename, mode="rt", encoding="ISO-8859-1")
  else:
    fp = open(filename, mode="rt", encoding="ISO-8859-1")


  single_block = ""
  blocks = []

  for line in file_block(fp, num_workers, chunk_number):
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
      single_block += line

  fp.close()
  parsed_blocks = parse_blocks(blocks, source)
  print("Parsed {:d} records".format(len(parsed_blocks)))
  return parsed_blocks


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

  num_workers = 4
  pool = mp.Pool(num_workers)
  jobs = []

  for key, value in FILE_LIST.items():
    filename = "data/" + value[0]

    for chunk_number in range(num_workers):
      jobs.append(pool.apply_async(process_wrapper,(filename, num_workers, chunk_number, key.upper())))

    count = 0
    for job in jobs:
      blocks = job.get()
      print("RETURNED BLOCKS: ", len(blocks))
      count += len(blocks)
      res = update_mongodb(client, blocks)
      print("Mongodb Update: ", res)

    print("Total records imported: {:d}".format(count))

  pool.close()
  exit(0)


if __name__ == "__main__":
  main()
