Incremental Backup and Restore (IBR)
====================================

Goal of Incremental backup and Restore
--------------------------------------

The goal of an incremental backup is to back up only those mutations that have changed since a previous backup, and restore only the missing mutations to the cluster.

The primary reasons for making incremental backups as part of your strategy are:

 - For use in a strategy based on incrementally updated backups, where these incremental backups are used to periodically roll forward an image copy of the database.
 - To reduce the amount of time needed for daily backups
 - To save network bandwidth when backing up over a network

New command line tools
----------------------

 - Incremental backup tool

>   cbbackup-incr [options] source backup-dir

Examples:

    cbbackup-incr http://HOST:8091 /backups/backup-1
    cbbackup-incr couchbase://HOST:8091 /backups/backup-1

Options:

    -h BUCKET_SOURCE, --bucket-source=BUCKET_SOURCE single bucket from source to backup

 - Retention checking tool

>  cbretent [options] backup-dir retention-period

Examples:

    cbretent /backups/backup-1  14     // 14 days retention period

Options:

    --delete   automatically delete obsolete files. By default, it is false.

 - Coalesce multiple incremental backups

>  cbcoalese [options] backup-dir

Examples:

    cbcolesce /backups/backup-1

Options:

    --delete    automatically delete backups that have been merged. By default, it is false

Extend existing command line tools
----------------------------------

 - cbbackup with new options

>    cbbackup --report /backups/backup-1

It will report how many backup items in the specified diretory

 - cbrestore with new bahavior

>  cbrestore /backups/backup-1  http://HOST:8091

Instead of restoring all items in the backup, the restore tool will restore only the missing mutations to the destination cluster.

options:

    --full restore all data to the destination while ignoring SCN

Backup strategy
---------------

 - Combine with existing cbbackup

   First it is required to use cbbackup to do a full backup, i.e. backup all data from the very beginning.  Error will be thrown if the destination directory is not empty.

   Later on, cbbackup-incr will be used to do differential incremental backup, i.e. always backup changes since last run of cbbackup-incr. Error will be thrown if no full backup, or level-0 image exists.

   Use case

     - Sunday

     cbbackup runs to back up all bucket data in the cluster

     - Monday through Saturday

     On each day from Monday through Saturday, cbbackup-incr runs to back up all changes since the most recent incremental backup. The Monday backup contains changes since Sunday, the Tusday backup contains changes since Monday, and so forth.

 - Or, use cbbackup-incr tool only

    No need to run cbbackup before running cbbbackup-incr. First run of cbbackup-incr will create a level-0 backup image, identical to run cbbackup. Or an option -full is used to enforce a full level 0 backup

   Use case

    - Sunday

        With cbbackup-incr -full, it runs to back up all data for the bucket in the cluster

    - Monday through Saturday

        cbbackup-incr runs to have a differential incremental level 1 backup since the most recent incremental backup at level 1 or 0.

Incremental backup Algorithm
----------------------------

 - Mutation SCN

    Every couchbase mutation comes with a sequence change number (SCN). All changes with SCN lower than this SCN are guaranteed to happen before this mutation.

 - Incremental start SCN

    This SCN applies only to level 1 incremental backup. All mutations whose SCN is greater than or equal to the incremental start SCN are included in the backup. Mutatations whose SCN are lower than the incremental start SCN are not included in the backup.

 - Incremental session SCN

    Every run of incremental backup will persist the SCN at which the most recent change is made to the cluster.

    When cbbackup-incr makes a level 1 incremental backup, it will keep tracking every mutation's SCN and will persist the last SCN. If not any incremental start SCN exists under the destination directory, cbbackup-incr will notify couchbase server and all data in the cluster will be retrieved for a level 0 backup. If it exists, cbbackup-incr will pass the Incremental start SCN to couchbase server to skip any mutations with lower SCNs.

Backup retention policy
-----------------------

  As you produce backups over time, older backup become obsolete when no full backups exists that need them. Besides affecting full or level 0 backups, the retention policy affects level 1 incremental backups. cbretent can scan backup directories and identify obsolete files for you. It does not automatically delete them unless you explicitly ask for it.

Recovery window
---------------

   A recovery window is a period of time that begins with the current time and extends backward in time to the point of recoverability. The point of recoverability is the earliest time for a point-in-time recovery, i.e. that earliest point to which you can recover following a failure or disaster in the production cluster. For example, if the recovery window is set to two weeks, all the full backups and incremental backups for the past two weeks will be retained so that the couchbase cluster can be recovered up to two weeks in the past.

Back deletes of obsolete backups
--------------------------------

   You can use cbretent with delete option to clean up obsolete backup files. Assume that the retention period is two weeks, i.e. 14 days, you can run the following command:

    cbretent /backups/backup1 14 --delete

   All obsolete files that are older than the retention period will be deleted. But any deletion action will be recorded in auditing log file under the backup directory.

Backward compatibility
----------------------

 - Use new IBR tool agaist setup version 2.2 and older

    cbbackup should work as before, without SCN in scene. cbbackup will do a full backup as before. But it will persist the last mutation SCN as part of backup files to server for the Incrementatl start SCN in case cbbackup-incr runs.

   cbbackup-incr should print error message and exist without generating any backup files. Since cbbackup-incr won't find any Incremental start SCN, it won't run anyway.

   cbrestore should work as before, restoring all backup data as a whole. If it is interrupted, it will restore from the beginning again.

 - Use new cbrestore tool against existed backup files

    Since no Incremental session SCN found, cbrestore will restore all backup data as a whole. No restartable restore process supported though.

What if errors pop up while using IBR tools
----------------------------------------

 - When using cbbackup

    cbbackup will stop when error occurs during data transfering. As result, it won't be a complete and successful backup. No mutation SCN will be persisted though. cbbackup will start from beging when it runs again.

 - When using cbbackup-incr

    cbbackup-incr is restartable. cbbackup-incr will record the mutation SCN for the last change that is backed up successfully. When it runs again, it will use that SCN as the incremental Start SCN.

 - When using cbrestore

    cbrestore can be restartable. Unless option full is specified, cbrestore will only restore missing mutations to the destination based on the handshaking result to the destination cluster.

    If error pops up during cbrestore, it can begin with whatever leftover from the last cbrestore run.
