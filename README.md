checksummer
===========

Be in control about your file's integrity.

Files on your new RAID-controller get corrupt?

You want to move your data to an encrypted filesystem and want to be sure, that every file is saved correctly?

checksummer.py creates a checksum of EVERY file. You can change the base path (i.e. your data has new mountpoint) and recheck again. checksummer.py reports differences.

## Installation

`go get github.com/claudehohl/checksummer`

## Bonus features

### Search files by filename

Since we save every filename in an sqlite-db, you can search your files very quickly. Results are sorted by size.

### Find duplicates

Since we save the checksum of every file, we can find out which files have the same content. Blew my mind when i was running this on my data drive…

### List all files, sorted by modification date

Scroll through the history of your files. I've downloaded some RFC textfiles; they are dated 1986. Blew my mind, too.

## Usage

Just provide the location where you want the sqlite3 database.
It's a good habit to put that on the root of your filesystem as a hidden file, e.g. /mnt/Data/.checksummer.db

./checksummer.py /mnt/Data/.checksummer.db

First run: Provide the base path; from which checksummer will scan files. You can change that anytime.

## Main menu

### Collecting files

Just type in *cf* and [Enter], and checksummer will scan every file starting from the base path.

Now you can search your files. But we don't have the size, yet.

### Collecting file stats

Second step: Get the size and modification time of your files: *cs*.

Now we can show all files, sorted by size: *r*

Or by modification time: *m*

### Creating checksums

*mc* - this process can take very long of course, because every file is being read.
After that, duplicates can be listed.

### Checking checksums

*rc* Reindex & check. You can do that from time to time…

If you've moved your files on a different location, you can change the base path first, and then run *rc*.

## Search quickly

Just append the search term:

./checksummer.py /mnt/Data/.checksummer.db movies

