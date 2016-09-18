#!/usr/bin/env python3

import os, sys
from os.path import join, getsize
from datetime import datetime
import hashlib
import sqlite3



# utils

def pager(string, autoquit = False):
    if autoquit:
        pipe = os.popen('less -X --quit-if-one-screen', 'w')
    else:
        pipe = os.popen('less -X', 'w')
    try:
        pipe.write(str(string))
    except:
        pass
    pipe.close()

def byteformat(num):
    try:
        num = float(num)
    except:
        num = float(0)
    for ext in ['B', 'KB', 'MB', 'GB', 'TB', 'PB', 'EB', 'ZB', 'YB']:
        if num < 1024.0:
            return "%3.1f%s" % (num, ext)
        num /= 1024.0

def dateformat(num):
    try:
        num = int(num)
    except:
        num = int(0)
    date = datetime.fromtimestamp(num).strftime('%Y-%m-%d %H:%M:%S')
    return date

def hash_file(filename):
    try:
        with open(filename, 'rb') as f:
            sha = hashlib.sha256()
            for chunk in iter(lambda: f.read(8192), b''):
                sha.update(chunk)
        return sha.hexdigest()
    except:
        raise



# checksummer

class Checksummer:

    def __init__(self, database):
        self.database = database
        self.db = sqlite3.connect(database)
        self.db.text_factory = sqlite3.OptimizedUnicode
        self.init_db()

        # get basepath
        self.basepath = self.get_option('basepath')
        if not self.basepath:
            self.change_basepath()

    def main(self):
        files_check = self.check('files')
        filesize_check = self.check('filesize')
        checksum_check = self.check('checksum')
        deleted = self.check('deleted')
        changed = self.check('changed')
        totalsize = self.check('totalsize')

        os.system('clear')
        print('')
        print('basepath is: ' + self.basepath)
        print('total size: ' + totalsize)
        print('')
        print('=== Collecting ===')
        print('[cf] collect files')
        if files_check:
            print('[cs] collect filestats')
        if filesize_check:
            print('[mc] make checksums')
        if checksum_check:
            print('[rc] reindex & check all files')
        print('')
        print('=== Stats ===')
        if files_check:
            print('[s] search files')
        if filesize_check:
            print('[r] rank by filesize')
            print('[m] recently modified files')
        if checksum_check:
            print('[ld] list duplicate files')
        if deleted > 0:
            print('[d] show ' + str(deleted) + ' deleted files')
            print('[pd] prune deleted files')
        if changed > 0:
            print('[ch] show ' + str(changed) + ' changed files')
            print('[pc] prune changed files')
        print('')
        print('[cb] change basepath')
        print('[q] exit')
        print('')
        choice = input('Select: ')

        if choice == 'q':
            sys.exit()

        elif choice == 'cf':
            self.collect_files()
            input('Press Enter to continue...')
            self.main()

        elif choice == 'cs':
            self.collect_filestats()
            input('Press Enter to continue...')
            self.main()

        elif choice == 'mc':
            self.make_checksums()
            input('Press Enter to continue...')
            self.main()

        elif choice == 'rc':
            self.reindex()
            input('Press Enter to continue...')
            self.main()

        elif choice == 's':
            self.search()
            self.main()

        elif choice == 'r':
            self.filesize_stats()
            self.main()

        elif choice == 'm':
            self.mtime_stats()
            self.main()

        elif choice == 'ld':
            self.duplicate_stats()
            self.main()

        elif choice == 'd':
            self.deleted_stats()
            self.main()

        elif choice == 'pd':
            self.prune_deleted()
            self.main()

        elif choice == 'ch':
            self.changed_stats()
            self.main()

        elif choice == 'pc':
            self.prune_changed()
            self.main()

        elif choice == 'cb':
            self.change_basepath()
            self.main()

        else:
            self.main()

    def init_db(self):
        c = self.db.cursor()
        try:
            c.execute("""CREATE TABLE files (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            filename TEXT UNIQUE,
            checksum_sha256 TEXT,
            filesize INTEGER,
            mtime INTEGER,
            file_found INTEGER,
            checksum_ok INTEGER
            )""")
            c.execute("""CREATE TABLE options (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            o_name TEXT UNIQUE,
            o_value TEXT
            )""")
        except:
            pass

    def check(self, subject):
        c = self.db.cursor()

        if subject == 'files':
            c.execute("""SELECT id FROM files LIMIT 1""")
            if c.fetchone() != None:
                return True

        if subject == 'filesize':
            c.execute("""SELECT id FROM files WHERE filesize IS NOT NULL LIMIT 1""")
            if c.fetchone() != None:
                return True

        if subject == 'checksum':
            c.execute("""SELECT id FROM files WHERE checksum_sha256 IS NOT NULL LIMIT 1""")
            if c.fetchone() != None:
                return True

        # too laggy with sqlite3
        # if subject == 'duplicates':
        # c.execute("""SELECT filename, COUNT(checksum_sha256) AS count
        # FROM files
        # GROUP BY checksum_sha256
        # HAVING (COUNT(checksum_sha256) > 1) LIMIT 1""")
        # if c.fetchone() != None:
        #     return True

        if subject == 'deleted':
            c.execute("""SELECT id FROM files WHERE file_found = '0'""")
            return len(c.fetchall())

        if subject == 'changed':
            c.execute("""SELECT id FROM files WHERE checksum_ok = '0'""")
            return len(c.fetchall())

        if subject == 'totalsize':
            c.execute("""SELECT SUM(filesize) FROM files""")
            return byteformat(c.fetchone()[0])

    def collect_files(self):
        print('collecting files...')
        c = self.db.cursor()
        count = 0

        for root, dirs, files in os.walk(self.basepath):
            filenames = [os.path.join(root, file) for file in files]

            for filename in filenames:
                try:
                    filename = filename.replace(self.basepath, '')
                    filename = filename.replace("'", "\'")
                    filename = filename.replace('"', '\"')

                    try:
                        c.execute("""INSERT INTO files(filename) VALUES(?)""", [filename])
                    except:
                        pass

                except:
                    print('malformed filename: ' + filename)

                count += 1
                if (count % 10000) == 0:
                    print(str(count), end="\n")
                    self.db.commit()

        self.db.commit()

    def collect_filestats(self):
        print('collecting filestats...')
        c = self.db.cursor()
        uc = self.db.cursor()
        c.execute("""SELECT id, filename FROM files""")

        allfiles = c.fetchall()
        count = len(allfiles)
        for r in allfiles:
            id = r[0]
            filename = r[1]

            try:
                stat = os.stat(self.basepath + filename)
                filesize = stat.st_size
                mtime = stat.st_mtime
                uc.execute("""UPDATE files SET filesize = ?, mtime = ?, file_found = 1 WHERE id = ?""", [filesize, mtime, id])

            except:
                # file not found
                uc.execute("""UPDATE files SET file_found = 0 WHERE id = ?""", [id])

            if (count % 10000) == 0:
                print(str(count), end="\n")
                self.db.commit()
            count -= 1

        self.db.commit()

    def make_checksums(self):
        c = self.db.cursor()
        uc = self.db.cursor()
        c.execute("""SELECT id, filename, filesize FROM files WHERE checksum_sha256 IS NULL AND file_found = '1'""")

        allfiles = c.fetchall()
        count = len(allfiles)
        for r in allfiles:
            id = r[0]
            filename = r[1]
            filesize = byteformat(r[2])

            try:
                print('(' + str(count) + ') making checksum: ' + self.basepath + filename + ' (' + filesize + ')')
                checksum = hash_file(self.basepath + filename)
                uc.execute("""UPDATE files SET checksum_sha256 = ? WHERE id = ?""", [checksum, id])

            except:
                # file not found
                uc.execute("""UPDATE files SET file_found = 0 WHERE id = ?""", [id])

            if (count % 1000) == 0:
                self.db.commit()
            count -= 1

        self.db.commit()

    def reindex(self):
        self.collect_files()
        self.collect_filestats()
        self.make_checksums()

        # set to check
        print('preparing to check files...')
        uc = self.db.cursor()
        uc.execute("""UPDATE files SET checksum_ok = NULL WHERE file_found = '1'""")
        self.db.commit()

        # check checksum
        c = self.db.cursor()
        c.execute("""SELECT id, filename, filesize, checksum_sha256 FROM files WHERE checksum_ok IS NULL AND file_found = '1'""")

        allfiles = c.fetchall()
        count = len(allfiles)
        for r in allfiles:
            id = r[0]
            filename = r[1]
            filesize = byteformat(r[2])
            checksum_sha256 = r[3]

            try:
                print('(' + str(count) + ') checking checksum: ' + self.basepath + filename + ' (' + filesize + ')')
                checksum = hash_file(self.basepath + filename)

                if checksum == checksum_sha256:
                    uc.execute("""UPDATE files SET checksum_ok = '1' WHERE id = ?""", [id])
                else:
                    print('checksum mismatch')
                    uc.execute("""UPDATE files SET checksum_ok = '0' WHERE id = ?""", [id])

            except:
                # file not found
                uc.execute("""UPDATE files SET file_found = 0 WHERE id = ?""", [id])

            if (count % 1000) == 0:
                self.db.commit()
            count -= 1

        self.db.commit()

    def search(self, searchterm = '', autoquit = False):
        if searchterm == '':
            searchterm = input('Enter searchterm: ')

        c = self.db.cursor()
        c.execute("""SELECT filename, filesize FROM files WHERE filename LIKE ? ORDER BY filesize DESC""", ['%' + searchterm + '%'])
        res = []

        for r in c.fetchall():
            filename = r[0]
            filesize = byteformat(r[1])
            res.append(filesize + "\t" + self.basepath + filename)

        pager("\n".join(res), autoquit)

    def filesize_stats(self):
        print('populating list...')
        c = self.db.cursor()
        c.execute("""SELECT filename, filesize FROM files WHERE filesize IS NOT NULL ORDER BY filesize DESC""")
        res = []

        for r in c.fetchall():
            filename = r[0]
            filesize = byteformat(r[1])
            res.append(filesize + "\t" + self.basepath + filename)

        pager("\n".join(res))

    def mtime_stats(self):
        print('populating list...')
        c = self.db.cursor()
        c.execute("""SELECT filename, filesize, mtime FROM files WHERE file_found = '1' ORDER BY mtime DESC""")
        res = []

        for r in c.fetchall():
            filename = r[0]
            filesize = byteformat(r[1])
            date = dateformat(r[2])
            res.append(date + "\t" + filesize + "\t" + self.basepath + filename)

        pager("\n".join(res))

    def duplicate_stats(self):
        print('populating list...')
        c = self.db.cursor()
        c.execute("""SELECT filename, COUNT(checksum_sha256) AS count
        FROM files
        GROUP BY checksum_sha256
        HAVING (COUNT(checksum_sha256) > 1)
        ORDER BY count DESC""")
        res = []

        for r in c.fetchall():
            filename = r[0]
            count = str(r[1])
            res.append(count + "\t" + self.basepath + filename)

        pager("\n".join(res))

    def deleted_stats(self):
        c = self.db.cursor()
        c.execute("""SELECT filename, filesize FROM files WHERE file_found = '0' ORDER BY filesize DESC""")
        res = []

        for r in c.fetchall():
            filename = r[0]
            filesize = byteformat(r[1])
            res.append(filesize + "\t" + self.basepath + str(filename))

        pager("\n".join(res))

    def prune_deleted(self):
        print('pruning deleted files...')
        c = self.db.cursor()
        c.execute("""DELETE FROM files WHERE file_found = '0'""")
        self.db.commit()

    def changed_stats(self):
        c = self.db.cursor()
        c.execute("""SELECT filename, filesize FROM files WHERE checksum_ok = '0' ORDER BY filesize DESC""")
        res = []

        for r in c.fetchall():
            filename = r[0]
            filesize = byteformat(r[1])
            res.append(filesize + "\t" + self.basepath + filename)

        pager("\n".join(res))

    def prune_changed(self):
        print('removing checksum from changed files...')
        c = self.db.cursor()
        c.execute("""UPDATE files SET checksum_sha256 = NULL, checksum_ok = NULL, filesize = NULL WHERE checksum_ok = 0""")
        self.db.commit()

    def change_basepath(self):
        print('Choose base path')
        basepath = input('(enter full path, without trailing slash): ')
        if basepath != '':
            self.basepath = basepath
            self.set_option('basepath', basepath)

    def set_option(self, key, val):
        c = self.db.cursor()
        try:
            c.execute("""INSERT INTO options(o_name, o_value) VALUES(?, ?)""", [key, val])
        except:
            c.execute("""UPDATE options SET o_value = ? WHERE o_name = ?""", [val, key])
        self.db.commit()

    def get_option(self, key):
        c = self.db.cursor()
        c.execute("""SELECT o_value FROM options WHERE o_name = ?""", [key])
        val = c.fetchone()
        if val:
            return val[0]
        else:
            return False



# run

if len(sys.argv) >= 2 and sys.argv[1] != '':
    database = sys.argv[1]
else:
    print('Usage:   ' + __file__ + ' sqlite3.db [search arguments]')
    print('')
    print('Example: ' + __file__ + ' myfiles.db')
    print('')
    sys.exit()

c = Checksummer(database)

searchterm = ' '.join(sys.argv[2:])
if searchterm != '':
    c.search(searchterm, autoquit = True)
else:
    if os.geteuid() != 0:
        print('You are not root. Collecting files is NOT recommended!')
        input('Press Enter to continue...')
    c.main()

