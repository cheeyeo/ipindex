# IPIndex WIP

This project was my effort to replicate IP index sites of the past such as
[flumps][1].

The application will allow you to map IP address to network names, descriptions
and contact details. You can also reverse search on each of these fields to
find what IP address blocks contain certain keywords.

Currently the application allows the following:

  - Download WHOIS data for most registars
  - Create a MongoDB dump with all WHOIS registars:
    - RIPE IPv4 & IPv6
    - ARIN
    - APNIC IPv4 & IPv6
    - AFRINIC
  - Search the description based on a keyword

**TODO**

  - Create better searching
  - Create daily export.
  - Add support for LACNIC
  - Create web application

## Development

**Query WHOIS data directly**

Using something like pwhois or the dnspython modules we could get the BGP table
from the [route-view][2] project and then query the netblocks with this
information. Using something like a custom zmap plugin to gather the data as
quickly as possible.

So many reasons this was scrapped quite early on.

  - The BGP routes were a a unreliable source.
  - Zmap scanning would be hard to do safely.
  - Whois query's would be throttled by the registars unless using different
    hosts.
  - Running pwhois was a pain in the arse even via docker.

**Using the whois files > MongoDB**

After a while I found out that you can actually get a dump of the whois data
directly from all registars FTP sites(doh). This was great! Using these files my
first idea was to parse the files and import them directly into MongoDB
via a insert or bulkwrite - this turned out to be a nightmare.

This was super slow, even using multiproccesing it would have taken *hours*
for a basic import on a 8 core 16GB instance. This was just not a viable
solution as I wanted to import the data every day if possible.

During this investigation I found that MongoDB was super quick at importing
from a backup file using **mongoimport**. It takes about 4 minutes to parse and
import 6 million netblock details using this method!

[1]: https://www.flumps.org/ip/
[2]: http://bgplay.routeviews.org/
