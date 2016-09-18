Checksummer - filesystem intelligence
=====================================

Create checksums and check if files are corrupted. Useful to fight bitrot!

Files on your new RAID-controller get corrupt?

You want to move your data to an encrypted filesystem and want to be sure that every file is being written and read correctly?

checksummer creates a checksum of EVERY file and saves it into a sqlite3 db. You can change the base path (i.e. your data has new mountpoint) and recheck again. Every bit of every file is being read and checksummer reports differences.

## Installation

`go get github.com/claudehohl/checksummer`

## Bonus features

These are the positive side-effects when we have the filename, checksum, filesize and modification time in one database:

### Search files by filename

Search your files. Results are sorted by size.

### Find duplicates

Find out which files have the same content.

### List all files, sorted by modification date

Scroll through the history of your files and discover RFC textfiles that are dated 1986.

## Usage

Just provide the location where you want the sqlite3 database.

It's a good habit to put that on the root of the filesystem you want to check as a hidden file, e.g. /mnt/Data/.checksummer.db

First run: Provide the base path; from which checksummer will scan files. You can change that anytime.

`checksummer /mnt/Data/.checksummer.db`

## Main menu

### Collecting files

Just type in *cf* and [Enter], and checksummer will scan every file starting from the base path and collects file infos like size and modification time.

Now we can show all files, sorted by size: *r*

Or by modification time: *m*

### Creating checksums

*mc* - this process can take very long of course, because every file is being read.

After that, duplicates can be listed.

### Checking checksums

*rc* Reindex & check. You can do that from time to time...

If you've moved your files on a different location, you can change the base path first, and then run *rc*. That allows checking file content independent of file systems.

## Search quickly

Just append the search term:

`checksummer /mnt/Data/.checksummer.db movies` find any file- or folder name containing "movies"

`checksummer /mnt/Data/.checksummer.db .flac` list all .flac files

`checksummer /mnt/Data/.checksummer.db .flac | wc -l` how many .flac files do you have?

