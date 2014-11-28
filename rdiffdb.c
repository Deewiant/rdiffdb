// File created: 2014-11-27 10:43:47

#define _GNU_SOURCE // O_NOATIME, maybe others
#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

#include <sqlite3.h>

static int fail_sql(const char *action, const char *errmsg) {
   fprintf(stderr, "Error %s: %s\n", action, errmsg);
   return 4;
}

static int go(
   char *dirpath, const size_t dirpath_len, size_t dirpath_cap,
   const int dirfd, const ino_t dir_inode, DIR *dir, struct dirent *entry,
   long name_max,
   sqlite3 *db,
   sqlite3_stmt *select_stmt,
   sqlite3_stmt *insert_stmt,
   sqlite3_stmt *delete_stmt,
   sqlite3_stmt *temp_insert_stmt,
   sqlite3_stmt *temp_truncate_stmt);

int main(int argc, char **argv) {
   if (argc != 2) {
      fprintf(stderr, "Usage: %s dbfile\n",
              argc > 0 ? argv[0] : "rdiffdb");
      return 2;
   }

   const char *db_path = argv[1];

   sqlite3 *db;
   int err = sqlite3_open(db_path, &db);
   if (err) {
      fprintf(stderr, "Error opening DB at '%s': %s\n",
              db_path, sqlite3_errstr(err));
      return 3;
   }

   char *errmsg;
   sqlite3_exec(db, "PRAGMA journal_mode = MEMORY", NULL, NULL, &errmsg);
   if (errmsg) return fail_sql("setting pragma", errmsg);
   sqlite3_exec(db, "PRAGMA synchronous = OFF", NULL, NULL, &errmsg);
   if (errmsg) return fail_sql("setting pragma", errmsg);
   sqlite3_exec(db, "PRAGMA encoding = \"UTF-8\"", NULL, NULL, &errmsg);
   if (errmsg) return fail_sql("setting pragma", errmsg);
   sqlite3_exec(db, "PRAGMA temp_store = MEMORY", NULL, NULL, &errmsg);
   if (errmsg) return fail_sql("setting pragma", errmsg);

   sqlite3_exec(
      db,
      "CREATE TABLE IF NOT EXISTS paths ("
      "inode INTEGER UNIQUE NOT NULL,"
      // Theoretically this should have a foreign key constraint to the above,
      // but doing this allows doing recursive deletes bottom-up instead of
      // top-down.
      "parent_inode INTEGER,"
      "name TEXT NOT NULL,"
      "mode INTEGER NOT NULL,"
      "uid INTEGER NOT NULL,"
      "gid INTEGER NOT NULL,"
      "size INTEGER NOT NULL,"
      "mtime DATETIME NOT NULL,"
      "atime DATETIME NOT NULL,"
      "ctime DATETIME NOT NULL,"
      "link_target TEXT,"
      "rdev INTEGER,"
      "PRIMARY KEY (parent_inode, name))",
      NULL, NULL, &errmsg);
   if (errmsg) return fail_sql("creating table", errmsg);

   // We can't bind an inline set like we would want in "WHERE x NOT IN ?", so
   // instead we use a temporary table.
   sqlite3_exec(
      db, "CREATE TEMPORARY TABLE temp.inodes (inode INTEGER NOT NULL)",
      NULL, NULL, &errmsg);
   if (errmsg) return fail_sql("creating temporary table", errmsg);

   sqlite3_stmt *select_stmt;
   err = sqlite3_prepare_v2(
      db,
      "SELECT inode, mode, uid, gid, size, mtime, atime, ctime, link_target, "
         "rdev "
      "FROM paths WHERE parent_inode IS ?1 AND name = ?2",
      -1, &select_stmt, NULL);
   if (err)
      return fail_sql("preparing SELECT", sqlite3_errstr(err));

   sqlite3_stmt *insert_stmt;
   err = sqlite3_prepare_v2(
      db, "INSERT OR REPLACE INTO paths VALUES "
          "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
      -1, &insert_stmt, NULL);
   if (err)
      return fail_sql("preparing INSERT", sqlite3_errstr(err));

   sqlite3_stmt *delete_stmt;
   err = sqlite3_prepare_v2(
      db,
      "DELETE FROM paths "
      "WHERE parent_inode IS ? AND inode NOT IN (SELECT * FROM temp.inodes)",
      -1, &delete_stmt, NULL);
   if (err)
      return fail_sql("preparing DELETE", sqlite3_errstr(err));

   sqlite3_stmt *temp_insert_stmt;
   err = sqlite3_prepare_v2(
      db, "INSERT INTO temp.inodes VALUES (?)", -1, &temp_insert_stmt, NULL);
   if (err)
      return fail_sql("preparing temp INSERT", sqlite3_errstr(err));

   sqlite3_stmt *temp_truncate_stmt;
   err = sqlite3_prepare_v2(
      db, "DELETE FROM temp.inodes", -1, &temp_truncate_stmt, NULL);
   if (err)
      return fail_sql("preparing temp truncate", sqlite3_errstr(err));

   const int root_fd = open(".", O_DIRECTORY | O_NOATIME | O_RDONLY);
   if (root_fd == -1) {
      perror("open('.')");
      return 1;
   }
   DIR *root_dir = fdopendir(root_fd);
   if (!root_dir) {
      perror("open('.') && fdopendir('.')");
      return 1;
   }

   long name_max = fpathconf(root_fd, _PC_NAME_MAX);
   if (name_max == -1)
      name_max = NAME_MAX;
   struct dirent *entry =
      malloc(offsetof(struct dirent, d_name) + name_max + 1);

   const long path_max = fpathconf(root_fd, _PC_PATH_MAX);
   const size_t dirpath_cap = path_max == -1 ? PATH_MAX : path_max;
   char *dirpath = malloc(dirpath_cap);

   if (!entry || !dirpath) {
      perror("malloc");
      return 4;
   }

   strcpy(dirpath, "");

   sqlite3_exec(db, "BEGIN IMMEDIATE TRANSACTION", NULL, NULL, &errmsg);
   if (errmsg) return fail_sql("beginning transaction", errmsg);

   err = go(dirpath, 0, dirpath_cap, root_fd, 0, root_dir, entry, name_max, db,
            select_stmt, insert_stmt, delete_stmt, temp_insert_stmt,
            temp_truncate_stmt);
   if (err)
      return err;

   // Complete any unfinished recursive deletions.
   do {
      sqlite3_exec(db, "DELETE FROM paths "
                       "WHERE parent_inode NOT IN (SELECT inode FROM paths)",
                   NULL, NULL, &errmsg);
      if (errmsg) return fail_sql("deleting recursively", errmsg);
   } while (sqlite3_changes(db));

   sqlite3_exec(db, "COMMIT", NULL, NULL, &errmsg);
   if (errmsg) return fail_sql("committing", errmsg);
}

static int go(
   char *dirpath, const size_t dirpath_len, size_t dirpath_cap,
   const int dirfd, const ino_t dir_inode, DIR *dir, struct dirent *entry,
   long name_max,
   sqlite3 *db,
   sqlite3_stmt *select_stmt,
   sqlite3_stmt *insert_stmt,
   sqlite3_stmt *delete_stmt,
   sqlite3_stmt *temp_insert_stmt,
   sqlite3_stmt *temp_truncate_stmt)
{
   const long new_name_max = pathconf(dirpath, _PC_NAME_MAX);
   if (new_name_max > name_max) {
      name_max = new_name_max;
      entry = realloc(entry, offsetof(struct dirent, d_name) + name_max + 1);
      if (!entry) {
         perror("realloc");
         return 4;
      }
   }

   ino_t *inodes = NULL;
   size_t inodes_len = 0, inodes_cap = 0;

   for (;;) {
      struct dirent *p;
      if (readdir_r(dir, entry, &p)) {
         fprintf(stderr, "readdir_r('%s'): %s\n",
                 dirpath, strerror(errno));
         return 1;
      }
      if (!p)
         break;

      if (!strcmp(entry->d_name, ".") || !strcmp(entry->d_name, ".."))
         continue;

      int entry_name_len;
      {
         const size_t len = strlen(entry->d_name);
         if (len > (size_t)INT_MAX) {
            fprintf(stderr, "Name of inode %ju in '%s' too long %zd\n",
                    (uintmax_t)entry->d_ino, dirpath, len);
            return 1;
         }
         entry_name_len = (int)len;
      }

      if (inodes_len == inodes_cap) {
         inodes_cap = 2*inodes_cap + 1024;
         inodes = realloc(inodes, inodes_cap * sizeof *inodes);
         if (!inodes) {
            perror("realloc");
            return 4;
         }
      }
      inodes[inodes_len++] = entry->d_ino;

      struct stat st;
      int fd = -1;

      if (entry->d_type == DT_DIR || entry->d_type == DT_UNKNOWN) {
         // It might be a directory: use openat, and fstat later.

         const int oflags =
            (entry->d_type == DT_DIR ? O_DIRECTORY : 0) |
            O_CLOEXEC | O_NOATIME | O_NOFOLLOW | O_RDONLY;

         fd = openat(dirfd, entry->d_name, oflags);
         if (fd == -1) {
            // Note: a possible reason is EPERM due to O_NOATIME, since it
            // requires write permissions.
            fprintf(stderr, "openat('%s', '%s', %#x): %s\n",
                    dirpath, entry->d_name, oflags, strerror(errno));
            return 1;
         }
      }

      // This may be somewhat overkill since we don't even handle files
      // disappearing out from under us, but oh well.
      char *entry_link_target = NULL;
      int entry_link_len = 0;
      for (;;) {
         if ((fd == -1
                 ? fstatat(dirfd, entry->d_name, &st, AT_SYMLINK_NOFOLLOW)
                 : fstat(fd, &st)) == -1)
         {
            fprintf(stderr, fd == -1 ? "fstatat('%s', '%s'): %s\n"
                                     : "fstat('%s'/'%s'): %s\n",
                    dirpath, entry->d_name, strerror(errno));
            return 1;
         }

         if (!S_ISLNK(st.st_mode))
            break;

         entry_link_target = realloc(entry_link_target, st.st_size + 1);
         if (!entry_link_target) {
            perror("realloc");
            return 4;
         }

         const ssize_t len = readlinkat(dirfd, entry->d_name,
                                        entry_link_target, st.st_size + 1);
         if (len == -1) {
            fprintf(stderr, "readlinkat('%s', '%s'): %s\n",
                    dirpath, entry->d_name, strerror(errno));
            return 1;
         }
         if (len > (ssize_t)INT_MAX) {
            fprintf(stderr, "readlinkat('%s', '%s'): too long %zd\n",
                    dirpath, entry->d_name, len);
            return 1;
         }
         entry_link_len = (int)len;
         if (len != st.st_size) {
            // Link's target changed to a different one between the
            // fstat/fstatat and the readlinkat, so update all info.
            continue;
         }
         entry_link_target[entry_link_len] = '\0';
         break;
      }

      if (fd != -1 && !S_ISDIR(st.st_mode)) {
         // We only needed it for the fstat, so get rid of it.
         close(fd);
      }

      char *entry_path = NULL;
      size_t entry_path_len;
      if (S_ISDIR(st.st_mode)) {
         // Reäppropriate dirpath as a buffer for this entry's path.
         const bool need_slash = dirpath_len != 0;
         const size_t dir_slashed_len = dirpath_len + (need_slash ? 1 : 0);
         entry_path_len = dir_slashed_len + entry_name_len;
         if (entry_path_len >= dirpath_cap) {
            dirpath_cap = entry_path_len + 1 + name_max;
            dirpath = realloc(dirpath, dirpath_cap);
            if (!dirpath) {
               perror("realloc");
               return 4;
            }
         }
         entry_path = dirpath;
         if (need_slash)
            entry_path[dirpath_len] = '/';
         memcpy(entry_path + dir_slashed_len, entry->d_name, entry_name_len);
         entry_path[dir_slashed_len + entry_name_len] = '\0';
      }

#define CHECK_BIND(x) do { \
   if (err) \
         return fail_sql("binding to " x, sqlite3_errstr(err)); \
} while (0)
      int err;
      err = dirpath_len
         ? sqlite3_bind_int64(select_stmt, 1, dir_inode)
         : sqlite3_bind_null(select_stmt, 1);
      CHECK_BIND("SELECT");
      err = sqlite3_bind_text(select_stmt, 2, entry->d_name, entry_name_len,
                              SQLITE_STATIC);
      CHECK_BIND("SELECT");

      err = sqlite3_step(select_stmt);
      if (!(err == SQLITE_ROW || err == SQLITE_DONE))
         return fail_sql("stepping SELECT", sqlite3_errstr(err));

      bool entry_is_new;
      if (err == SQLITE_ROW) {
#define CHECK_NOMEM(x) do { \
   if ((x) == 0 && sqlite3_errcode(db) == SQLITE_NOMEM) \
      return fail_sql("fetching column", sqlite3_errstr(err)); \
} while (0)
         const int64_t inode = sqlite3_column_int64(select_stmt, 0);
         CHECK_NOMEM(inode);
         const int mode = sqlite3_column_int(select_stmt, 1);
         CHECK_NOMEM(mode);
         const int uid = sqlite3_column_int(select_stmt, 2);
         CHECK_NOMEM(uid);
         const int gid = sqlite3_column_int(select_stmt, 3);
         CHECK_NOMEM(gid);
         const int64_t size = sqlite3_column_int64(select_stmt, 4);
         CHECK_NOMEM(size);
         const int64_t mtime = sqlite3_column_int64(select_stmt, 5);
         CHECK_NOMEM(mtime);
         const int64_t atime = sqlite3_column_int64(select_stmt, 6);
         CHECK_NOMEM(atime);
         const int64_t ctime = sqlite3_column_int64(select_stmt, 7);
         CHECK_NOMEM(ctime);
         const char *const link_target = (const char *)
            sqlite3_column_text(select_stmt, 8);
         CHECK_NOMEM(link_target);
         const int64_t rdev = sqlite3_column_int64(select_stmt, 9);
         CHECK_NOMEM(rdev);

         entry_is_new =
            st.st_ino != inode ||
            // We don't care about permission changes.
            (st.st_mode & S_IFMT) != (mode & S_IFMT) ||
            st.st_size != size ||
            st.st_mtime != mtime ||
            (S_ISLNK(st.st_mode) && strcmp(entry_link_target, link_target)) ||
            ((S_ISBLK(st.st_mode) | S_ISCHR(st.st_mode))
                && st.st_rdev != rdev);
      } else
         entry_is_new = true;
      sqlite3_reset(select_stmt);

      if (entry_is_new) {
         if (entry_path)
            puts(entry_path);
         else {
            if (*dirpath) {
               fputs(dirpath, stdout);
               putchar('/');
            }
            puts(entry->d_name);
         }
      }

      err = sqlite3_bind_int64(insert_stmt, 1, st.st_ino);
      CHECK_BIND("INSERT");
      err = dirpath_len
         ? sqlite3_bind_int64(insert_stmt, 2, dir_inode)
         : sqlite3_bind_null(insert_stmt, 2);
      CHECK_BIND("INSERT");
      err = sqlite3_bind_text(insert_stmt, 3, entry->d_name,
                              entry_name_len, SQLITE_STATIC);
      CHECK_BIND("INSERT");
      err = sqlite3_bind_int(insert_stmt, 4, st.st_mode);
      CHECK_BIND("INSERT");
      err = sqlite3_bind_int(insert_stmt, 5, st.st_uid);
      CHECK_BIND("INSERT");
      err = sqlite3_bind_int(insert_stmt, 6, st.st_gid);
      CHECK_BIND("INSERT");
      err = sqlite3_bind_int64(insert_stmt, 7, st.st_size);
      CHECK_BIND("INSERT");
      err = sqlite3_bind_int64(insert_stmt, 8, st.st_mtime);
      CHECK_BIND("INSERT");
      err = sqlite3_bind_int64(insert_stmt, 9, st.st_atime);
      CHECK_BIND("INSERT");
      err = sqlite3_bind_int64(insert_stmt, 10, st.st_ctime);
      CHECK_BIND("INSERT");
      err = S_ISLNK(st.st_mode)
               ? sqlite3_bind_text(insert_stmt, 11, entry_link_target,
                                   entry_link_len, free)
               : sqlite3_bind_null(insert_stmt, 11);
      CHECK_BIND("INSERT");
      err = S_ISBLK(st.st_mode) || S_ISCHR(st.st_mode)
               ? sqlite3_bind_int64(insert_stmt, 12, st.st_rdev)
               : sqlite3_bind_null (insert_stmt, 12);
      CHECK_BIND("INSERT");

      err = sqlite3_step(insert_stmt);
      if (err != SQLITE_DONE)
         return fail_sql("stepping INSERT", sqlite3_errstr(err));
      sqlite3_reset(insert_stmt);

      if (!S_ISDIR(st.st_mode))
         continue;

      DIR *entry_dir = fdopendir(fd);
      if (!entry_dir) {
         fprintf(stderr, "fdopendir('%s'): %s\n", entry_path, strerror(errno));
         return 1;
      }

      err = go(entry_path, entry_path_len, dirpath_cap, fd, st.st_ino,
               entry_dir, entry, name_max, db, select_stmt, insert_stmt,
               delete_stmt, temp_insert_stmt, temp_truncate_stmt);
      if (err)
         return err;
      closedir(entry_dir);
      entry_path[dirpath_len] = '\0';
   }

   int err;
   for (size_t i = 0; i < inodes_len; ++i) {
      err = sqlite3_bind_int64(temp_insert_stmt, 1, inodes[i]);
      CHECK_BIND("temp INSERT");
      err = sqlite3_step(temp_insert_stmt);
      if (err != SQLITE_DONE)
         return fail_sql("stepping temp INSERT", sqlite3_errstr(err));
      sqlite3_reset(temp_insert_stmt);
   }
   free(inodes);

   err = dirpath_len
      ? sqlite3_bind_int64(delete_stmt, 1, dir_inode)
      : sqlite3_bind_null(delete_stmt, 1);
   CHECK_BIND("DELETE");
   err = sqlite3_step(delete_stmt);
   if (err != SQLITE_DONE)
      return fail_sql("stepping DELETE", sqlite3_errstr(err));
   sqlite3_reset(delete_stmt);

   err = sqlite3_step(temp_truncate_stmt);
   if (err != SQLITE_DONE)
      return fail_sql("stepping temp truncate", sqlite3_errstr(err));
   sqlite3_reset(temp_truncate_stmt);

   return 0;
}
