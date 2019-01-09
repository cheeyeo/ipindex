import click
import re
import pymongo
import gzip
import json

from contextlib import closing
from urllib.request import urlopen
from pymongo import MongoClient
from netaddr import iprange_to_cidrs

# mongo settings
DB_HOST = "localhost"
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
    "arin": ["arin.db", "ftp://ftp.arin.net/pub/rr/arin.db"],
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
            DB_HOST, DB_PORT, maxPoolSize=20, serverSelectionTimeoutMS=10
        )
        client.server_info()
    except pymongo.errors.ServerSelectionTimeoutError:
        click.secho("[ERROR] failed to connect to MongoDB", fg="red")
        exit(1)

    return client


def update_mongodb(data):
    """ update mongodb record """

    db = connect_mongodb()

    try:
        db.ipindex.items.bulk_write(data)
    except RuntimeError:
        click.secho("[ERROR] failed to update db record", fg="red")

    db.close()
    return True


def search_mongodb(field, query):

    db = connect_mongodb()

    myquery = {field: {"$regex": query}}
    try:
        mydoc = db.ipindex.items.find(myquery)
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


def read_blocks(filename: str) -> list:

    if filename.endswith(".gz"):
        f = gzip.open(filename, mode="rt", encoding="ISO-8859-1")
    else:
        f = open(filename, mode="rt", encoding="ISO-8859-1")

    single_block = ""
    blocks = []

    for line in f:

        if (
            line.startswith("%")
            or line.startswith("#")
            or line.startswith("remarks:")
            or line.startswith(" ")
        ):
            continue

        if line.strip() == "":
            if single_block.startswith("inetnum:") or single_block.startswith(
                "inet6num:"
            ):
                blocks.append(single_block)
                single_block = ""
            else:
                single_block = ""
        else:
            single_block += line

    f.close()
    global NUM_BLOCKS
    NUM_BLOCKS = len(blocks)

    return blocks


def parse_blocks(blocks, source):

    with open("data.json", "a") as file:
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

            json.dump(data, file)


@cli.command()
@click.option("--parse", is_flag=True, help="import DB files", required=False)
@click.option("--search", help="import DB files", required=False)
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
        tmp_data = []
        result = search_mongodb("description", search)

        # mongodb.count is deprecated, loop cursor and check
        for x in result:
            tmp_data.append(x)

        if tmp_data:
            pass
        else:
            click.secho("[INFO] no results found for %s" % search, fg="green")

    if parse:

        for key, value in FILE_LIST.items():

            click.secho("[INFO] parsing %s" % key.upper(), fg="green")
            blocks = read_blocks("data/" + value[0])
            click.secho("[INFO] netblocks found %s" % NUM_BLOCKS, fg="green")

            result = parse_blocks(blocks, key.upper())
        exit(0)


if __name__ == "__main__":
    main()
