# RSS Feed Adapter

## Overview

With the RSS Adapter the user can connect to any RSS feed, e.g. [CNN](rss.cnn.com/rss/cnn_latest.rss) and retrieve all news of this URL.
The Adapter is based on the ROME project, more precise the [ROME Fetcher](http://rometools.github.io/rome-fetcher/index.html).

## Capabilities

* Virtual Tables: RSSFEED is the only table supported and it has a fixed structure, the RSS news feed entry
* Initial Load: Partly supported only. The RSS feed returns the latest headlines only, hence an initial load will include those only. There is no RSS feed or other method to read all headlines and their links to the original article CNN ever produced.
* Pushdown: Not supported. The idea of RSS feeds is to query an URL and it returns the last changes and their dates. No filters are supported by RSS, hence there is nothing to be pushed down.
* Realtime: Is supported. The Adapter queries the URL using the ROME Fetcher library, checks the http header if the pages is changed and if it is, all entries are pushed to Hana with the RowType UPSERT. Deletes are not possible with RSS feeds.
* Realtime Recovery: No explicit support. As the URL returns the last e.g. 50 headlines and does UPSERTs always, a short down time results in no data loss. Headlines not included in the last successful run and not included in the first run after recovery are lost forever.

## Installation

Attached source code does not include the ROME libraries and their dependents. 
Hence put into the lib folder the files
* rome-fetcher-1.0.jar from [ROME Fetcher 1.0 download page](http://rometools.github.io/rome-fetcher/Releases/ROMEFetcher1.0.html)
* rome-1.0.jar from [ROME download page](http://rometools.github.io/rome/ROMEReleases/ROME1.0Release.html)
* jdom-1.1.3.jar from [jdom-1.1.3.zip\jdom\build](http://www.jdom.org/downloads/index.html)
* ant.jar, jaxen.jar, xalan.jar, xerces.jar, xml-api.jar from [jdom-1.1.3.zip\jdom\lib in above download](http://www.jdom.org/downloads/index.html)
* junit-4.11.jar
* xercesImpl.jar


## Contributing

This is an open source project under the Apache 2.0 license, and every contribution is welcome. Issues, pull-requests and other discussions are welcome and expected to take place here. 
