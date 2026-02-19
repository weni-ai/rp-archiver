1.4.0
----------
 * Config for broadcast delete batch size
 * Config for db max connections

1.3.1
----------
 * Incrementing logs on deleting broadcasts with periodic infos
 * Allow delete active already archived runs

1.3.0
----------
 * Configuration to archive inactive orgs
 * Force expire and then archive runs that is out from retention period

1.2.4-archiver-7.1.1
----------
 * Update go version to 1.23
 * Update Dockerfile to go 1.23, alpine 3.20 and multistage build
 * Concurrent archive processing

1.2.3-archiver-7.1.1
----------
 * Configuration for rollup timeouts
 * fix to prevent stuck caused by broken pipe on db connection

1.2.2-archiver-7.1.1
----------
 * No longer consider retention time on delete operation

1.2.1-archiver-7.1.1
----------
 * Merge tag v7.1.1 from nyaruka into 1.2.0-archiver-7.1.0

1.2.0-archiver-7.1.0
----------
 * Merge tag v7.1.0 from nyaruka into 1.0.0-archiver-7.0.0

1.0.0-archiver-7.0.0
----------
 * Consider retention time on delete operation