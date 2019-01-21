import os
import gzip
import multiprocessing as mp
import re
import shutil
from netaddr import iprange_to_cidrs
from pymongo import MongoClient
from pymongo import InsertOne
from pymongo.write_concern import WriteConcern


# mongo settings
DB_HOST = "localhost"
DB_PORT = 27017

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

# get_uncompressed_size returns the actual uncompressed file size of a gzip archive
def get_uncompressed_size(file):
  pipe_in = os.popen('gzip -l %s' % file)
  list_1 = pipe_in.readlines()
  list_2 = list_1[1].split()
  c , u , r , n = list_2
  return int(u)

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
      if not l: return
      while l and b"% RIPE-USER-RESOURCE" in l:
        p = f.tell()
        l = f.readline()
        l = f.readline()
        f.seek(p) # revert one line

      chunkEnd = f.tell()
      yield chunkStart, chunkEnd - chunkStart
      if chunkEnd > fileEnd:
        break

def process_wrapper(chunkStart, chunkSize, filename):
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

  parsed_blocks = parse_blocks(blocks, "RIPE")
  print("Parsed {:d} records".format(len(parsed_blocks)))
  print("Example parsed record: {}".format(parsed_blocks[0]))
  return parsed_blocks

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

def connect_mongodb():
  """ mongodb connection """
  try:
    client = MongoClient(
        DB_HOST, DB_PORT, maxPoolSize=200, serverSelectionTimeoutMS=10
    )
    client.server_info()
  except pymongo.errors.ServerSelectionTimeoutError:
    print("[ERROR] failed to connect to MongoDB")
    exit(1)

  return client

def update_mongodb(client, data):
  """ update mongodb record """
  print("Inside update mongodb with thread {}".format(os.getpid()))
  print(client)

  db = client['ipindex']

  # write concern sets journal to False?
  coll = db.get_collection('items', write_concern=WriteConcern(j=False))

  # Disable indexes during import for faster access
  # Reindex collection afterwards
  coll.drop_indexes()

  items = [InsertOne(d) for d in data]

  print('ITEMS LENGTH: {:d}'.format(len(items)))
  print('CREATED ITEMS: {}'.format(items[0]))

  try:
    coll.bulk_write(items, ordered=False)
  except RuntimeError:
    print("[ERROR] failed to update db record")

  client.close()
  return True

def main():
  # client = connect_mongodb()
  pool = mp.Pool(4)
  jobs = []

  archive = "data/ripe.db.inetnum.gz"
  filename = "data/ripe.db.inetnum"
  # extract gzip file
  with gzip.open(archive, mode="rb") as f_in:
    with open(filename, "wb") as f_out:
      shutil.copyfileobj(f_in, f_out)

  for chunkStart, chunkSize in chunkify(filename):
    print("chunkStart: {}, chunkSize: {}".format(chunkStart, chunkSize))
    jobs.append(pool.apply_async(process_wrapper,(chunkStart,chunkSize, filename)))

  # delete extracted file
  os.remove(filename)

  count = 0
  for job in jobs:
    blocks = job.get()
    count += len(blocks)
    # res = update_mongodb(client, blocks)
    # print("Mongodb Update: ", res)

  print("Finished importing all records")
  print("Total records imported: {:d}".format(count))
  pool.close()
  exit(0)

if __name__ == "__main__":
  main()
