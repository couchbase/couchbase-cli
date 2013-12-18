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

    --full            force to have a full or level 0 backup. Errors throws if backup directory is not empty.
    --incr-diff       differential incremental backup. Error throws if no level 0 backup exists on backup dir. By default, it is true.
    --incr-cumulative cumulative incremental backup. Error throws if no level 0 backup exists on backup dir. By default, it is false

Note, existed cbbackup options will be supported as before. For example, we can backup a single bucket by specify -b option. Or -v to increase verbose level.

 - Extend cbrestore to support incremental restore

>  cbrestore /backups/backup-1  http://HOST:8091

Instead of restoring all items in the backup, the restore tool will restore only the missing mutations to the destination cluster based on the current cluster vbucket sequence number.

options:

    --full force to restore all data to the destination while ignoring SCN

New command line tools
----------------------

 - backup file management tool

>  cbbackup-manage [options] backup-dir

Examples:

    cbbackup-manage  --report /backups/backup-1
    cbbackup-manage  --retent 14 /backups/backup-1
    cbbackup-manage  --merge /backups/backup-1

Options:

    --report      report how many backup mutation items in the specified backup directory
    --retent  RETENION-DAYS  set up retention policy for the backup directory.
    --delete      automatically delete obsolete files. By default, it won't delete any files.
    --merge  LEVEL   merge multiple incremental level N backup files into one level N-1backup file, where N > 0

Incremental Backups
-------------------

By default, cbbackup makes full or level 0 backups. A full backup contains all the mutations and design docs/views in the cluster for all buckets or a specific bucket up to the moment when 
snapshots are taken.

Contrast to full backups, an incremental backup is to back up only those mutations that have changed since a previous backup. No design docs or views will be backed up for level N incremental backups.

Multilevel incremental Backups
------------------------------

cbbackup can create multilevel incremental backups.

Level 0 backup is a full backup, which is the base for any subsequent incremental backups.

Level 1 backup is the delta change with respect to the most recent Level 0 or Level 1.

Level 2 backup is the delta change with respect to the most recent Lelve 1 or Level 2.

And so on (Imagine backup level at Level 3, 4, etc)

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
        + full-10-13-2013 \
        |   meta data for backup
        |    - user to create backup
        |    - a bucket or all buckets
        |    - compress or not
        |    - retention policy defined or not
        |    - merge level-1 backups or not
        |    - auditing logs?
        |    - when backup created
        |    - when retention runs
        |    - when merge happens
        |   *.mb
        |   + incr-10-14-2013 \
        |      *.mb
        |   + incr-10-14-2013 \
        |     ..
        |   + incr-10-19-2013
        + full-10-20-2103 \
           ...

By default, all backup files won't be compressed. If you want to save disk space, 3rd party tools are needed to compress files. cbrestore tool won't recognize compressed database files. As a result, any compressed files should be decompressed before running cbrestore tool.

Built in compress function may be implemented in future releases.

Backup strategy
---------------

   When cbbackup runs against an empty root backup directory, a full backup or a level-0 backup will be generated to backup all data from the very beginning. Errors will be thrown if --incr option is specified for an incremental backup but against an empty root backup directory.

   When cbbackup runs against non empty root backup direcotry, it will generate a differential incremental backup, i.e. always backup changes since last run of cbbackup. Errors will be thrown if --full option is specified for a full backup.

   Design-docs/views will be generated during full backups, but NOT for incremental backups.


Incremental Backup Algorithm
----------------------------

 - Mutation SCN

    Every couchbase mutation comes with a sequence change number (SCN). All changes with SCN lower than this SCN are guaranteed to happen before this mutation.

 - Incremental start SCN

    This SCN applies only to level 1 incremental backup. All mutations whose SCN is greater than or equal to the incremental start SCN are included in the backup. Mutatations whose SCN are lower than the incremental start SCN are not included in the backup.

 - Incremental session SCN

    Every run of incremental backup will persist the SCN at which the most recent change is made to the cluster.

    When cbbackup makes a level 1 incremental backup, it will keep tracking every mutation's SCN and will persist the last SCN. If not any incremental start SCN exists under the destination directory, cbbackup will notify couchbase server and all data in the cluster will be retrieved for a level 0 backup. If it exists, cbbackup will pass the Incremental start SCN to couchbase server to skip any mutations with lower SCNs.

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

   You can use cbbackup-manage --retent with delete option to clean up obsolete backup files. Assume that the retention period is two weeks, i.e. 14 days, you can run the following command:

    cbbackup-manage --retent 14 --delete /backups/backup1

   All obsolete files that are older than the retention period will be deleted. But any deletion action will be recorded in auditing log file under the backup directory.

Bacup file directory structure
-------------------------------

 1. Current file structure

           <backup_root>/
             bucket-<BUCKETNAME>/
               design.json
               node-<NODE>/
                 data-<XXXX>.cbb

 2. Proposed new file structure

           <backup_root>/
             bucket-<BUCKETNAME>/
               design.json
               node-<NODE>/
                    data-<XXXX>.cbb
                    <TIMESTAMP2>-1/
                       data-<XXXX>.cbb
                    <TIMESTAMP3>-1/
                       data-<XXXX>.cbb

Full backup or level 0 backup files are under node directory as before. And incremental backup files are under TIMESTAMP-1 directory.

Backward compatibility
----------------------

 - Use new IBR tool agaist setup version 2.2 and older

    cbbackup should work as before, without SCN in scene. cbbackup will do a full backup as before. But it will persist the last mutation SCN as part of backup files to server for the Incrementatl start SCN in case cbbackup-incr runs.

   cbbackup should print error message and exist without generating any backup files. Since cbbackup-incr won't find any Incremental start SCN, it won't run anyway.

   cbrestore should work as before, restoring all backup data as a whole. If it is interrupted, it will restore from the beginning again.

 - Use new cbrestore tool against existed backup files

    Since no Incremental session SCN found, cbrestore will restore all backup data as a whole. No restartable restore process supported though.
What if errors pop up while using IBR tools
----------------------------------------

 - When using cbbackup

    cbbackup --full will stop when error occurs during data transfering. As result, it won't be a complete and successful backup. No mutation SCN will be persisted though. cbbackup will start from beging when it runs again.

 - When using cbbackup-incr

    cbbackup --incr-diff is restartable. it will record the mutation SCN for the last change that is backed up successfully. When it runs again, it will use that SCN as the incremental Start SCN.

 - When using cbrestore

    cbrestore can be restartable. Unless option full is specified, cbrestore will only restore missing mutations to the destination based on the handshaking result to the destination cluster.

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

Based on Incremental Session SCN, cbbackup should tell that Wednesday's session SCN is way ahead of the incremental Start SCN retrieved from master node.

As result, it should be able to mark backup state from Tusday and Wednesday as out of sync. cbrestore can take advantage of backup state and skip them as well.
