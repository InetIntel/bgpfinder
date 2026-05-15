# BGPFinder

BGPFinder is a high-performance, database-backed indexer and broker for BGP data archives. It is designed to be a drop-in replacement for the [BGPStream Broker](https://bgpstream.caida.org/docs/api/broker) API. Users may use BGPFinder to host their own private or local brokers for BGP data, or they may query public BGPFinder instances hosted by other organizations.

## Key Features

*   **BGPStream Compatible**: Implements the BGPStream Broker API (v2), allowing standard BGPStream clients to interface with a BGPFinder service without modification.
*   **Multi-Project Support**: Native support for indexing both **University of Oregon RouteViews** and **RIPE RIS** data archives. The code is designed to support the easy addition of other projects that archive BGP MRT files.
*   **Persistent Indexing**: Uses a Postgres database to cache metadata about millions of BGP dumps, ensuring fast response times for common queries.


## Architecture

BGPFinder consists of three primary components:

1.  **The Scraper**: A periodic daemon that crawls the RouteViews and RIPE RIS web archives to discover new BGP dumps as they are published.
2.  **The Database**: A Postgres instance that stores the locations, timestamps, and metadata for every discovered BGP dump.
3.  **The API Service**: A Go-based HTTP server that handles broker queries from BGPStream clients, performing optimized database searches to return matching dump URLs.

## Deployment

Deployment is managed via Docker Compose, which handles the orchestration of the API service, the scraper, and the database.

For detailed setup instructions, please see: **[DEPLOY.md](./DEPLOY.md)**

## Acknowledgements

### Authors
BGPFinder was originally designed and implemented by **Alistair King**.

Notable contributors include:
*   **Shane Alcock**
*   **Zachary Bischof**
*   **Nahush H Kumta**
*   **Xie Qiu**

### Maintenance
This project is currently maintained by the [Internet Intelligence Lab](https://inetintel.cc.gatech.edu/) at the Georgia Institute of Technology. Please file any bug reports as issues on the [BGPFinder GitHub page](https://github.com/inetintel/bgpfinder).

## License
BGPFinder is released under the BSD 2-Clause License. See [LICENSE](./LICENSE) for details.
