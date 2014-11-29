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

#include <leveldb/c.h>

struct db_val {
   ino_t inode;
   off_t size;
   struct timespec mtime, atime, ctime;
   dev_t rdev;
   long link_target_len;
   mode_t mode;
   uid_t uid;
   gid_t gid;
   char link_target[];
};

static int fail_act(const char *action, const char *errmsg) {
   fprintf(stderr, "Error %s: %s\n", action, errmsg);
   return 4;
}

static leveldb_readoptions_t *ropts;
static leveldb_writeoptions_t *wopts;

static int go(
   char *dirpath, const size_t dirpath_len, size_t dirpath_cap,
   const int dirfd, const ino_t dir_inode, DIR *dir, struct dirent *entry,
   long name_max, char *entry_link_target, size_t entry_link_target_cap,
   dev_t *seen_devs, size_t seen_devs_count, size_t seen_devs_cap,
   leveldb_t *db);

static int rmr(ino_t dir_inode, leveldb_t *db, leveldb_writebatch_t *batch);

int main(int argc, char **argv) {
   if (argc != 2) {
      fprintf(stderr, "Usage: %s dbfile\n",
              argc > 0 ? argv[0] : "rdiffdb");
      return 2;
   }

   const char *db_path = argv[1];

   leveldb_options_t *opts = leveldb_options_create();
   leveldb_options_set_filter_policy(
      opts, leveldb_filterpolicy_create_bloom(10));
   leveldb_options_set_create_if_missing(opts, 1);

   char *err = NULL;
   leveldb_t *db = leveldb_open(opts, db_path, &err);
   if (err) {
      fprintf(stderr, "Error opening DB at '%s': %s\n", db_path, err);
      return 3;
   }

   leveldb_options_destroy(opts);

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
   const size_t link_target_buf_cap = dirpath_cap;
   char *dirpath = malloc(dirpath_cap);
   char *link_target_buf = malloc(link_target_buf_cap);

   if (!entry || !dirpath || !link_target_buf) {
      perror("malloc");
      return 4;
   }

   strcpy(dirpath, "");

   ropts = leveldb_readoptions_create();
   wopts = leveldb_writeoptions_create();

   return go(dirpath, 0, dirpath_cap, root_fd, 0, root_dir, entry, name_max,
             link_target_buf, link_target_buf_cap, NULL, 0, 0, db);
}

static int go(
   char *dirpath, const size_t dirpath_len, size_t dirpath_cap,
   const int dirfd, const ino_t dir_inode, DIR *dir, struct dirent *entry,
   long name_max, char *entry_link_target, size_t entry_link_target_cap,
   dev_t *seen_devs, size_t seen_devs_count, size_t seen_devs_cap,
   leveldb_t *db)
{
   leveldb_writebatch_t *batch = leveldb_writebatch_create();

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

         if (st.st_size >= entry_link_target_cap) {
            entry_link_target_cap *= 2;
            entry_link_target =
               realloc(entry_link_target, entry_link_target_cap);
            if (!entry_link_target) {
               perror("realloc");
               return 4;
            }
         }

         const ssize_t len =
            readlinkat(dirfd, entry->d_name, entry_link_target,
                       entry_link_target_cap);
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
         if (len != st.st_size) {
            // Link's target changed to a different one between the
            // fstat/fstatat and the readlinkat, so update all info.
            continue;
         }
         entry_link_len = (int)len;
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

         if (st.st_ino == 0) {
            fprintf(stderr, "Directory '%s' has unsupported inode value 0\n",
                    entry_path);
            return 1;
         }
      }

      size_t keylen = entry_name_len + sizeof dir_inode;
      char *key = malloc(keylen);
      if (!key) {
         perror("malloc");
         return 4;
      }
      memcpy(key, &dir_inode, sizeof dir_inode);
      memcpy(key + sizeof dir_inode, entry->d_name, entry_name_len);

      char *err = NULL;
      size_t vallen;
      char *valbuf = leveldb_get(db, ropts, key, keylen, &vallen, &err);
      if (err)
         return fail_act("getting", err);

      struct db_val *val = (struct db_val*)valbuf;

      const bool entry_is_new =
         !val ||
         st.st_ino != val->inode ||
         // We don't care about permission changes.
         (st.st_mode & S_IFMT) != (val->mode & S_IFMT) ||
         st.st_size != val->size ||
         memcmp(&st.st_mtim, &val->mtime, sizeof st.st_mtim) ||
         (S_ISLNK(st.st_mode) &&
             (entry_link_len != val->link_target_len ||
              memcmp(entry_link_target, val->link_target, entry_link_len))) ||
         ((S_ISBLK(st.st_mode) | S_ISCHR(st.st_mode))
             && st.st_rdev != val->rdev);

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

      if (!val) {
         vallen = entry_link_len + sizeof *val;
         val = calloc(1, vallen);
         if (!val) {
            perror("calloc");
            return 4;
         }
      }

      val->inode = st.st_ino;
      val->size = st.st_size;
      val->mtime = st.st_mtim;
      val->atime = st.st_atim;
      val->ctime = st.st_ctim;
      val->rdev = st.st_rdev;
      val->link_target_len = entry_link_len;
      val->mode = st.st_mode;
      val->uid = st.st_uid;
      val->gid = st.st_gid;
      memcpy(val->link_target, entry_link_target, entry_link_len);

      leveldb_writebatch_put(batch, key, keylen, (const char*)val, vallen);
      free(key);
      if (valbuf)
         leveldb_free(valbuf);
      else
         free(val);

      if (!S_ISDIR(st.st_mode))
         continue;

      DIR *entry_dir = fdopendir(fd);
      if (!entry_dir) {
         fprintf(stderr, "fdopendir('%s'): %s\n", entry_path, strerror(errno));
         return 1;
      }

      bool seen_dev = false;
      for (size_t i = 0; i < seen_devs_count; ++i) {
         if (seen_devs[i] == st.st_dev) {
            seen_dev = true;
            break;
         }
      }

      if (!seen_dev) {
         if (seen_devs_count == seen_devs_cap) {
            seen_devs_cap += 16;
            seen_devs = realloc(seen_devs, seen_devs_cap * sizeof *seen_devs);
            if (!seen_devs) {
               perror("realloc");
               return 4;
            }
         }
         seen_devs[seen_devs_count++] = st.st_dev;

         const long new_name_max = fpathconf(fd, _PC_NAME_MAX);
         if (new_name_max > name_max) {
            name_max = new_name_max;
            entry =
               realloc(entry, offsetof(struct dirent, d_name) + name_max + 1);
            if (!entry) {
               perror("realloc");
               return 4;
            }
         }
      }

      int s = go(entry_path, entry_path_len, dirpath_cap, fd, st.st_ino,
                 entry_dir, entry, name_max, entry_link_target,
                 entry_link_target_cap, seen_devs, seen_devs_count,
                 seen_devs_cap, db);
      if (s)
         return s;
      closedir(entry_dir);
      entry_path[dirpath_len] = '\0';
   }

   // Delete any preëxisting entries in the DB that have disappeared from the
   // file system, recursively.

   char *err = NULL;
   leveldb_iterator_t *iter = leveldb_create_iterator(db, ropts);
   leveldb_iter_get_error(iter, &err);
   if (err)
      return fail_act("creating iterator", err);
   leveldb_iter_seek(iter, (const char*)&dir_inode, sizeof dir_inode);
   leveldb_iter_get_error(iter, &err);
   if (err)
      return fail_act("seeking iterator", err);

   while (leveldb_iter_valid(iter)) {
      size_t keylen;
      const char *key = leveldb_iter_key(iter, &keylen);
      if (memcmp(key, &dir_inode, sizeof dir_inode))
         break;

      size_t vallen;
      const char *valbuf = leveldb_iter_value(iter, &vallen);

      struct db_val *val = (struct db_val*)valbuf;

      bool found = false;
      for (size_t i = 0; i < inodes_len; ++i) {
         if (inodes[i] == val->inode) {
            found = true;
            break;
         }
      }

      if (!found) {
         leveldb_writebatch_delete(batch, key, keylen);
         if (S_ISDIR(val->mode)) {
            int s = rmr(val->inode, db, batch);
            if (s)
               return s;
         }
      }

      leveldb_iter_next(iter);
      leveldb_iter_get_error(iter, &err);
      if (err)
         return fail_act("nexting iterator", err);
   }
   free(inodes);
   leveldb_iter_destroy(iter);

   leveldb_write(db, wopts, batch, &err);
   if (err)
      return fail_act("writing", err);
   leveldb_writebatch_destroy(batch);

   return 0;
}

static int rmr(ino_t dir_inode, leveldb_t *db, leveldb_writebatch_t *batch) {
   char *err = NULL;

   leveldb_iterator_t *iter = leveldb_create_iterator(db, ropts);
   leveldb_iter_get_error(iter, &err);
   if (err)
      return fail_act("creating iterator", err);
   leveldb_iter_seek(iter, (const char*)&dir_inode, sizeof dir_inode);
   leveldb_iter_get_error(iter, &err);
   if (err)
      return fail_act("seeking iterator", err);

   while (leveldb_iter_valid(iter)) {
      size_t keylen;
      const char *key = leveldb_iter_key(iter, &keylen);
      if (memcmp(key, &dir_inode, sizeof dir_inode))
         break;

      size_t vallen;
      const char *valbuf = leveldb_iter_value(iter, &vallen);

      struct db_val *val = (struct db_val*)valbuf;

      leveldb_writebatch_delete(batch, key, keylen);
      if (S_ISDIR(val->mode)) {
         int s = rmr(val->inode, db, batch);
         if (s)
            return s;
      }

      leveldb_iter_next(iter);
      leveldb_iter_get_error(iter, &err);
      if (err)
         return fail_act("nexting iterator", err);
   }
   leveldb_iter_destroy(iter);
   return 0;
}
