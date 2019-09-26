* BUILD

`$ mvn clean install -Prelease`

Will create zip archive with binary files required to run application.

* RUN

`$ java -jar ignite-page-analyzer-0.0.1-SNAPSHOT.jar page-statistics -d /path/to/pds/node00-81ed9fdb-b5d7-4c7a-98fa-1987712f1366`

Expected `/path/top/pds` content
```
~/tmp/pds$ tree -d
.
├── node00-1111
│   └── cacheGroup-CACHEGROUP_INDEX_union-module
└── node00-81ed9fdb-b5d7-4c7a-98fa-1987712f1366
    ├── cache-CacheQueryExampleOrganizations
    ├── cache-ignite-sys-cache
    ├── cp
    ├── metastorage
    └── TxLog
```

* Example output

```
*** FULLL STATISTICS FOR CacheQueryExampleOrganizations ***
 1 -> [  3107 pages, 1337,50K free, 10,76% unused]
 2 -> [  2048 pages,   0,00K free,  0,00% unused]
 6 -> [  1024 pages,   0,00K free,  0,00% unused]
10 -> [  1024 pages,   0,00K free,  0,00% unused]
12 -> [  2048 pages,   0,00K free,  0,00% unused]
13 -> [ 71373 pages, 274,42M free, 98,43% unused]
14 -> [  1024 pages,   0,00K free,  0,00% unused]
15 -> [  1024 pages,   0,00K free,  0,00% unused]
------------------
Full size  322,94M
Free size  275,73M
Free perent 85,38%
```
