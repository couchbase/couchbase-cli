Incremental Backup and Restore (IBR)
====================================

Goal of Incremental backup and Restore
--------------------------------------

The goal of an incremental backup is to back up only those mutations that have changed since a previous backup, and restore only the missing mutations to the cluster.

The primary reasons for making incremental backups as part of your strategy are:

 - For use in a strategy based on incrementally updated backups, where these incremental backups are used to periodically roll forward an image copy of the database.
 - To reduce the amount of time needed for daily backups
 - To save network bandwidth when backing up over a network

Extend existing command line tools
----------------------------------

 - Extend cbbackup to support full backups or to back up only mutations that have changed since a previous backup

>    cbbackup [options] source backup_dir

Examples:

    cbbackup http://HOST:8091 /backups/backup-1
    cbbackup couchbase://HOST:8091 /backups/backup-1

    Options
        --mode  [full | incr-diff | incr-accu | auto]  backup running mode, default is auto, where
          full           force to have a full or level 0 backup.
          incr-diff      differential incremental backup. Only back up the delta changes from the last full or incremental. 
          incr-accu      accumulative incremental backup. Only back up the delta changes from the last full.
          auto           Tool will decide what the best mode to use. If no backup data, then it is a full backup. Otherwise, it is a differential incremental one. 

    Note, existed cbbackup options will be supported as before. For example, we can backup a single bucket by specify -b option. Or -v to increase verbose level.

 - Extend cbrestore to support partial restore

>  cbrestore /backups/backup-1  http://HOST:8091

Instead of restoring all items in the backup, the restore tool can restore partial data from specified period.

options:

    --from-date  data collected sooner than the specified date won't be restored. By default, it is from the very beginning
    --to-date    data collected later than the specified date won't be restored. By default, it is to the very end of collection.

cbrestore will browse through meta.json files under the latest backup chains and find all related .cbb files to restore. A backp chain is conceptually similar to a git branch.

New command line tools
----------------------

 - backup file management tool

>  cbbackup-manage [options] backup-dir

Note: this command won't be ready for 3.0

Examples:

    cbbackup-manage  --report /backups/backup-1
    cbbackup-manage  --prune 14 /backups/backup-1
    cbbackup-manage  --merge /backups/backup-1

Options:

    --report  report         how many backup mutation items in the specified backup directory
    --prune  RETENION-DAYS   set up retention policy for the backup directory.
    --delete                 automatically delete obsolete files. By default, it won't delete any files.
    --merge  LEVEL           merge multiple incremental level N backup files into one level N-1backup file, where N > 0

Incremental Backups
-------------------

By default, cbbackup makes full or level 0 backups. A full backup contains all the mutations and design docs/views in the cluster for all buckets or a specific bucket up to the moment when 
snapshots are taken.

Contrast to full backups, an incremental backup is to back up only those mutations that have changed since a previous either full backup or incremental backups. 

Multilevel incremental Backups
------------------------------

cbbackup can create multilevel incremental backups.

Level 0 backup is a full backup, which is the base for any subsequent incremental backups.

Level 1 backup is the delta change with respect to the most recent Level 0 or Level 1.

Level 2 backup is the delta change with respect to the most recent Lelve 1 or Level 2.

And so on (Imagine backup level at Level 3, 4, etc). Currently, we only support Level 1 incremental backup.

Any incremental backup can be either of the following types:

**A differential incremental backup**, which contains all mutations changed after the most recent incremental backup at level N or N-1.

**A cumulative incremental backup**, which contains all mutations changed after the most recent incremental backup at level N - 1

By default, incremental backup will be differential. However, cumulative backups are preferable to differential backups when recovery time is more important than disk space, because fewer incremental backups need to be applied during recovery.

Use case for differential incremental backups
---------------------------------------------

  - Sunday

     cbbackup runs to back up all bucket data in the cluster to create a level 0 backup image

  - Monday through Saturday

     On each day from Monday through Saturday at midnight, cbbackup takes level 1 backup. It runs to back up all changes since the most recent incremental backup.

     The Monday backup contains changes since Sunday, the Tusday backup contains changes since Monday, and so forth.

     Every hour, a Level 2 backup grabs a backup w.r.t the last level 2 or level 1, i.e. the previous hour.

Use case for cumulative incremental backups
-------------------------------------------

  - Sunday

     cbbackup runs to back up all bucket data in the cluster to create a level 0 backup image

  - Monday through Saturday

     On each day from Monday through Saturday at midnight, cbbackup takes level 1 backup. It runs to back up all changes since Sunday level 0 backup.

     The Monday backup contains changes since Sunday, so does for the Tusday, Wednesday, and so forth.

     Every hour, a Level 2 grabs a backup w.r.t the last level 1 backup that happens in midnight. So forth for other level 2 backups.

Backup file directory structure
-------------------------------

    backup root dir \
        + 2013-10-13_11_28_11 \
        |   + 2013-10-13_11_28_11-full \
        |       design.json
        |       + bucket-<bucketname> \
        |           + node-<nodename> \
        |              + meta \
        |                  meta data for backup
        |                  - dependent .cbb files which server for baseline for this backup
        |                  - user to create backup
        |                  - a bucket or all buckets
        |                  - compress or not
        |                  - retention policy defined or not
        |                  - merge level-1 backups or not
        |                  - auditing logs?
        |                  - when retention runs
        |                  - when merge happens
        |              *.cbb
        |   + 2013-10-14_11_30_00-diff \
        |       design.json
        |       + bucket-<bucketname> \
        |           + node-<nodename> \
        |              *.cbb
        |   + 2013-10-15_11_45_20-diff \
        |             ..
        |   + 2013-10-16-12_10_10-accu \
        + 2013-11-10_11_28_11 \
           ...

Under one root directory, there can be multiple full backup root directories where directory name is the date time with format as YYYY-MM-DD_HH_MM_SS when full backup starts.

Each full backup directory name start with the same time stamp and suffixed with "-full". It will have the current backup directory structure grouped as bucket-bucketname / node-nodename

Under the node directory, it will contain backup files suffixed as ".cbb", i.e. full backup files as baseline for future incremental backups.

Every incremental backup subdirectory sits within the same directory as the full backup directory, . The directory suffix ends with "diff" for differential incrementals and "accu" for accumulative incrementals. 

By default, all backup files won't be compressed. If you want to save disk space, 3rd party tools are needed to compress files. cbrestore tool won't recognize compressed database files. As a result, any compressed files should be decompressed before running cbrestore tool.

Built in compress function may be implemented in future releases.

Backup strategy
---------------

   When cbbackup runs against an empty root backup directory, a full backup or a level-0 backup will be generated to backup all data from the very beginning.

   When cbbackup runs against non empty root backup direcotry, it will generate a differential incremental backup, i.e. always backup changes since last run of cbbackup.

   Design-docs/views will be generated during full backups and incremental backups. But only the latest version of design docs should be used for restore.

    |        |   full exists   |  full non exists  |
    | ------ | --------------- | ----------------- |
    |  full  |     full        |      full         |
    |  accu  |     accu        |      full         |
    |  diff  |     diff        |      full         |

Incremental Backup Algorithm
----------------------------

 - Mutation SEQNO

    Every couchbase mutation comes with a sequential number (SEQNO). All changes with SEQNO lower than this SEQNO are guaranteed to happen before this mutation for a specific vbucket.

 - Incremental start SEQNO

    This SEQNO applies only to level 1 incremental backup. All mutations whose SEQNO is greater than or equal to the incremental start SEQNO are included in the backup. Mutatations whose SEQNO are lower than the incremental start SEQNO are not included in the backup.

 - Incremental session SEQNO

    Every run of incremental backup will persist the SEQNO at which the most recent change is made to the cluster.

    When cbbackup makes a level 1 incremental backup, it will keep tracking every mutation's SEQNO and will persist the last SEQNO. If not any incremental start SEQNO exists under the destination directory, cbbackup will notify couchbase server and all data in the cluster will be retrieved for a level 0 backup. If it exists, cbbackup will pass the Incremental start SEQNO to couchbase server to skip any mutations with lower SEQNOs.

Backup retention policy
-----------------------

  With growing number of backup files, disk space usage is always a factor that cannot be ignored. As you produce backups over time, older backup become obsolete when no full backups exists that need them.  A user-defined policy for determining how long backups and archived logs need to be retained for restore.

  You can define a retention policy in terms of a recovery window. All backups required to satisfy the current retention policy, and any related meta data files should be retained.

  The retention policy does not directly affect incremental level 1 or above backups. Rather, these files become obsolete when no full backups exist that need them.

  cbbackup-manage --retent can scan backup directories and identify obsolete files for you. It does not automatically delete them unless you explicitly ask for it

Recovery window
---------------

   A recovery window is a period of time that begins with the current time and extends backward in time to the point of recover-ability. The point of recover-ability is the earliest time for a point-in-time recovery, i.e. that earliest point to which you can recover following a failure or disaster in the production cluster. For example, if the recovery window is set to two weeks, all the full backups and incremental backups for the past two weeks will be retained so that the cluster can be recovered up to two weeks in the past.

 - Use case

The recovery window is 7 days.

A full or level 0 backup is scheduled ever one week on these days:

January 1

January 8

January 15

January 22

Differential level 1 incremental backups happen between these dates.

Suppose the current date is January 18 and the point of recover-ability is January 11. Hence the January 8 full backup is needed for recovery, so are the incremental backups. The backup files before January 8 are obsolete because they are not needed for recovery to a point within the window.

Batch deletes of obsolete backups
--------------------------------

   You can use cbbackup-manage --prune with delete option to clean up obsolete backup files. Assume that the retention period is two weeks, i.e. 14 days, you can run the following command:

    cbbackup-manage --prune 14 --delete /backups/backup1

   All obsolete files that are older than the retention period will be deleted. But any deletion action will be recorded in auditing log file under the backup directory.

Bacup file directory structure
-------------------------------

 1. Current file structure

           <backup_root>/
             bucket-<BUCKETNAME>/
               design.json
               node-<NODE>/
                 data-<XXXX>.cbb

 2. New file structure

           <backup_root> /
             <TIMESTAMP1> /
                <TIMESTAMP1-full> /
                   design.json
                   bucket-<BUCKETNAME>/
                     node-<NODE>/
                        data-<XXXX>.cbb
                        meta.json
                <TIMESTAMP2-diff>/
                   design.json
                   bucket-<BUCKETNAME>/
                     node-<NODE>/
                        data-<XXXX>.cbb
                        meta.json
                <TIMESTAMP3-diff>/
                   design.json
                   bucket-<BUCKETNAME>/
                     node-<NODE>/
                        data-<XXXX>.cbb
                        meta.json
                <TIMESTAMP4-accu>/
                   design.json
                   bucket-<BUCKETNAME>/
                     node-<NODE>/
                        data-<XXXX>.cbb
                        meta.json
             <TIMESTAMP5>/
                   ...

All the timestamp on the directory names will be in UTC format as [YYYY]-[MM]-[DD]T[hh][mm][ss]Z.  Example might be 2014-01-31T134511Z. In UTC, it is 1:45:11 PM on Jan 31, 2014.

Compared to 2.x backup directory structure, you can have multiple full backup directories under the backup root, with timestamp to distinguish from each other. And each backup run, wether it is full,
differential incremental or accumulative incrementals, are under the same directory with different suffix.

Backward compatibility
----------------------

 - Use new IBR tool agaist setup version 2.2 and older

    cbbackup should work as before, without SEQNO in scene. cbbackup will do a full backup as before. But it will persist the last mutation SEQNO as part of backup files to server for the Incrementatl start SEQNO in case cbbackup-incr runs.

   cbbackup should print error message and exist without generating any backup files. Since cbbackup-incr won't find any Incremental start SEQNO, it won't run anyway.

   cbrestore should work as before, restoring all backup data as a whole. If it is interrupted, it will restore from the beginning again.

 - Use new cbrestore tool against existed backup files

    Since no Incremental session SEQNO found, cbrestore will restore all backup data as a whole. No restartable restore process supported though.

## Source vs. Target   ##

    |        |   2.x  |  3.0   | 2.x BFF | 3.0 BFF |
    | ------ | ------ | ------ | ------- | ------- |
    |  2.x   |   TAP  |   TAP  |   Y     |   N/A   |
    |  3.0   |   TAP  |   DCP  |   Y     |    Y    |

IBR should automatically identify the version of target server and source data file format. For 2.x target server, TAP protocol will be used as before. For 2.x data format, default value will be used for restore.

What if errors pop up while using IBR tools
----------------------------------------

 - When using cbbackup

    cbbackup -m full will stop when error occurs during data transfering. As result, it won't be a complete and successful backup. No mutation SEQNO will be persisted though. cbbackup will start from beging when it runs again.

 - When using cbbackup-incr

    cbbackup -m incr-diff is restartable. it will record the mutation SEQNO for the last change that is backed up successfully. When it runs again, it will use that SEQNO as the incremental Start SEQNO.

 - When using cbrestore

    Unless specified the restore period, cbrestore will always restore data from all the cbb files from the latest backup chains, i.e. from full, differential and/or accumulative incrementals.

    If error pops up during cbrestore, it can begin with whatever leftover from the last cbrestore run.

 - Use case when nodes fail

  1. Sunday, user takes full backup
  2. More mutations happen
  3. Monday, user takes incremental backup, saving the delta of mutations from step 2.
  4. More mutations happen
  5. Tuesday, user takes incremental backup, saving the delta of mutations from step 4.
  6. More mutations happen
  7. Wednesday, user takes incremental backup, saving the delta of mutations from step 6.
  8. A node fails!  Failover happens and a replica takes over as master.

UNFORTUNATELY, the newly promoted master (formerly, the replica) is way behind (perhaps replication was way slow), and the new master only has mutations on Sunday, and partially from Monday (call that time point 4.1). Part of mutations from Monday and all from Tuesday and Wednesday are missing!

The next scheduled incremental backup job runs at Thursday. Now what?

Ideally, this incremental backup run (step 9) should take the mutations again from Mondayy, i.e,step 3 onwards.

Now the backup directory tree has gotten complex, with branching history in it...

      . full backup from step 1 (covering time from 0 to 1)
      |
      |. incremental backup from step 3 (covering time from 1 to 3)
      | \
      |   |. incremental backup from step 5 (covering time from 3 to 5)
      |   |. incremental backup from step 7 (covering time from 5 to 7)
      |
      |. incremental backup from step 9 (covering time from 3 to 4.1 to 9, but it doesn't have mutations from 4.1 to 8!)

Next, what happens when the user tries to restore from that backup_dir?  How does the cbrestore allow the user to figure out what happened and to choose what to restore?

Based on Incremental Session SEQNO, cbbackup should tell that Wednesday's session SEQNO is way ahead of the incremental Start SEQNO retrieved from master node.

As result, it should be able to mark backup state from Tusday and Wednesday as out of sync. cbrestore can take advantage of backup state and skip them as well.
