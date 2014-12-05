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

#include <glib.h>
#include <leveldb/c.h>
#include "marisa.h"

#ifdef __clang__
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wpadded"
#endif
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
#ifdef __clang__
#pragma clang diagnostic pop
#endif

// Stores a string either inline or not.
struct len_str {
   size_t len;
   const char *p;
   char str[];
};

enum {
   OLD = 0,
   NEW = 1,
   MOD = 2,
};

#ifdef __clang__
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wpadded"
#endif
struct seen_name {
   size_t len;
   uint8_t new;
   char name[];
};
#ifdef __clang__
#pragma clang diagnostic pop
#endif

static guint len_str_hash(gconstpointer p) {
   const struct len_str *ls = p;
   const char *s = ls->p ? ls->p : ls->str;
   // Larson's.
   guint h = 0;
   for (size_t i = 0, e = ls->len; i < e; ++i)
      h = 101*h + s[i];
   return h;
}
static gboolean len_str_equal(gconstpointer p1, gconstpointer p2) {
   const struct len_str *ls1 = p1, *ls2 = p2;
   return ls1->len == ls2->len &&
      !memcmp(ls1->p ? ls1->p : ls1->str, ls2->p ? ls2->p : ls2->str,
              ls1->len);
}

static int write_name_to_dirpath(
   const char *name, const size_t name_len, size_t *ppath_len,
   char **dirpath, const size_t dirpath_len, size_t *dirpath_cap,
   const size_t name_max)
{
   const bool need_slash = dirpath_len != 0;
   const size_t dir_slashed_len = dirpath_len + (need_slash ? 1 : 0);
   const size_t path_len = *ppath_len = dir_slashed_len + name_len;
   if (path_len >= *dirpath_cap) {
      *dirpath_cap = path_len + 1 + name_max;
      char *p = realloc(*dirpath, *dirpath_cap);
      if (!p) {
         perror("realloc");
         return 4;
      }
      *dirpath = p;
   }
   if (need_slash)
      (*dirpath)[dirpath_len] = '/';
   memcpy(*dirpath + dir_slashed_len, name, name_len);
   (*dirpath)[path_len] = '\0';
   return 0;
}

static int fail_act(const char *action, const char *errmsg) {
   fprintf(stderr, "Error %s: %s\n", action, errmsg);
   return 4;
}

static leveldb_readoptions_t *ropts;
static leveldb_writeoptions_t *wopts;

static int go(
   char **dirpath, const size_t dirpath_len, size_t *dirpath_cap,
   const int dirfd, const ino_t dir_inode, DIR *dir, const bool dir_was_new,
   const bool dir_was_included, struct dirent **pentry, long *name_max,
   char **entry_link_target, size_t *entry_link_target_cap, dev_t **pseen_devs,
   size_t *seen_devs_count, size_t *seen_devs_cap, char **key, size_t *keycap,
   struct db_val **newval, size_t *newvalcap, const marisa_trie *inc,
   const marisa_trie *exc, marisa_agent *agent, GHashTable *all_inodes,
   leveldb_t *db, const bool dry_run);

static int rmr(
   ino_t dir_inode, char **dirpath, const size_t dirpath_len,
   size_t *dirpath_cap, const size_t name_max, bool actually_delete,
   GHashTable *all_inodes, leveldb_t *db, leveldb_writebatch_t *batch);

static int rmr_at_iter(
   const char *key, const size_t keylen, const leveldb_iterator_t *iter,
   char **dirpath, const size_t dirpath_len, size_t *dirpath_cap,
   const size_t name_max, bool actually_delete, GHashTable *all_inodes,
   leveldb_t *db, leveldb_writebatch_t *batch);

int main(int argc, char **argv) {
   if (!(argc >= 2 && argc <= 5)) {
      fprintf(
         stderr,
         "Usage: %s dbfile [rootdir [include-exclude-file]] [--dry-run]\n"
         "\n"
         "include-exclude-file is a file specifying paths relative to "
         "rootdir, one per\nline, with an initial '- ' signifying exclusion. "
         "(If you want to specify paths\ncontaining line breaks or starting "
         "with '- ', you're out of luck.)\n",
         argc > 0 ? argv[0] : "rdiffdb");
      return 2;
   }

   const bool dry_run = argc > 2 && !strcmp(argv[argc-1], "--dry-run");
   if (dry_run)
      --argc;

   const char *db_path = argv[1];
   const char *rootdir = argc > 2 ? argv[2] : NULL;
   const char *incexc_path = argc > 3 ? argv[3] : NULL;

   marisa_trie *inc, *exc;
   if (incexc_path) {
      marisa_keyset *iks = marisa_keyset_init();
      marisa_keyset *eks = marisa_keyset_init();
      if (!iks || !eks) {
         perror("marisa_keyset_init");
         return 4;
      }

      FILE *incexc_file = fopen(incexc_path, "r");
      if (!incexc_file) {
         fprintf(stderr, "fopen('%s'): %s\n", incexc_path, strerror(errno));
         return 3;
      }
      char *line = NULL;
      size_t line_cap = 0;
      for (;;) {
         errno = 0;
         ssize_t line_len = getline(&line, &line_cap, incexc_file);
         if (line_len == -1)
            break;

         if (line_len && line[line_len-1] == '\n')
            --line_len;

         bool include = true;
         uint8_t offset = 0;
         if (line_len > 2 && !memcmp(line, "- ", 2 )) {
            line_len -= 2;
            offset = 2;
            include = false;
         }

         if ((uintmax_t)SSIZE_MAX > (uintmax_t)UINT32_MAX &&
             line_len > (ssize_t)UINT32_MAX)
         {
            fprintf(stderr, "Too large inclusion/exclusion path length %zd\n",
                    line_len);
            return 3;
         }

         marisa_keyset_push(include ? iks : eks,
                            line + offset, (uint32_t)line_len);
      }
      if (errno) {
         fprintf(stderr, "getline() from '%s': %s\n",
                 incexc_path, strerror(errno));
         return 3;
      }
      free(line);
      fclose(incexc_file);

      // These are probably quite small so don't bother with much caching.
      inc = marisa_trie_init(iks, MARISA_MIN_NUM_TRIES | MARISA_TINY_CACHE);
      if (!inc) {
         perror("marisa_trie_init");
         return 4;
      }
      marisa_keyset_free(iks);
      exc = marisa_trie_init(eks, MARISA_MIN_NUM_TRIES | MARISA_TINY_CACHE);
      if (!exc) {
         perror("marisa_trie_init");
         return 4;
      }
      marisa_keyset_free(eks);
   } else {
      marisa_keyset *ks = marisa_keyset_init();
      if (!ks) {
         perror("marisa_keyset_init");
         return 4;
      }
      inc = marisa_trie_init(ks, 0);
      exc = marisa_trie_init(ks, 0);
      if (!inc || !exc) {
         perror("marisa_trie_init");
         return 4;
      }
      marisa_keyset_free(ks);
   }

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

   if (rootdir && chdir(rootdir) == -1) {
      fprintf(stderr, "chdir('%s'): %s\n", rootdir, strerror(errno));
      return 1;
   }

   DIR *root_dir = opendir(".");
   if (!root_dir) {
      perror("opendir('.')");
      return 1;
   }

   long name_max = fpathconf(AT_FDCWD, _PC_NAME_MAX);
   if (name_max == -1)
      name_max = NAME_MAX;
   struct dirent *entry =
      malloc(offsetof(struct dirent, d_name) + name_max + 1);

   const long path_max = fpathconf(AT_FDCWD, _PC_PATH_MAX);
   size_t dirpath_cap = path_max == -1 ? PATH_MAX : path_max;
   size_t link_target_buf_cap = dirpath_cap;
   size_t valcap = link_target_buf_cap + sizeof (struct db_val);
   char *dirpath = malloc(dirpath_cap);
   char *link_target_buf = malloc(link_target_buf_cap);
   struct db_val *val = calloc(1, valcap);

   if (!entry || !dirpath || !link_target_buf || !val) {
      perror("malloc");
      return 4;
   }

   marisa_agent *agent = marisa_agent_init();
   if (!agent) {
      perror("marisa_agent_init");
      return 4;
   }

   strcpy(dirpath, "");

   ropts = leveldb_readoptions_create();
   wopts = dry_run ? NULL : leveldb_writeoptions_create();

   dev_t *seen_devs = NULL;
   size_t seen_devs_count = 0;
   size_t seen_devs_cap = 0;
   char *key = NULL;
   size_t keycap = 0;

   GHashTable *all_inodes = g_hash_table_new(NULL, NULL);

   const int s =
      go(&dirpath, 0, &dirpath_cap, AT_FDCWD, 0, root_dir, false,
         marisa_trie_empty(inc), &entry, &name_max, &link_target_buf,
         &link_target_buf_cap, &seen_devs, &seen_devs_count, &seen_devs_cap,
         &key, &keycap, &val, &valcap, inc, exc, agent, all_inodes, db,
         dry_run);
   if (s)
      return s;
   leveldb_close(db);
}

static int go(
   char **dirpath, const size_t dirpath_len, size_t *dirpath_cap,
   const int dirfd, const ino_t dir_inode, DIR *dir, const bool dir_was_new,
   const bool dir_was_included, struct dirent **pentry, long *name_max,
   char **entry_link_target, size_t *entry_link_target_cap, dev_t **pseen_devs,
   size_t *seen_devs_count, size_t *seen_devs_cap, char **key, size_t *keycap,
   struct db_val **newval, size_t *newvalcap, const marisa_trie *inc,
   const marisa_trie *exc, marisa_agent *agent, GHashTable *all_inodes,
   leveldb_t *db, const bool dry_run)
{
   leveldb_writebatch_t *batch = dry_run ? NULL : leveldb_writebatch_create();

   GHashTable *names =
      g_hash_table_new_full(len_str_hash, len_str_equal, free, NULL);

   GHashTable *names_by_inode = g_hash_table_new_full(NULL, NULL, NULL, free);

   int s;

   for (;; (*dirpath)[dirpath_len] = '\0') {

      // Usually we've done the *dirpath reäppropriation (see below) by the
      // time we want to 'continue;', which is why we reset it in the for loop
      // directly, so we don't have to remember it separately. For those cases
      // when we haven't yet done it, we don't bother doing the dead store and
      // come straight here instead.
      quick_continue:;

      struct dirent *entry;
      if (readdir_r(dir, *pentry, &entry)) {
         fprintf(stderr, "readdir_r('%s'): %s\n",
                 *dirpath, strerror(errno));
         return 1;
      }
      if (!entry)
         break;

      if (!strcmp(entry->d_name, ".") || !strcmp(entry->d_name, ".."))
         goto quick_continue;

      int entry_name_len;
      {
         const size_t len = strlen(entry->d_name);
         if (len > (size_t)INT_MAX) {
            fprintf(stderr, "Name of inode %ju in '%s' too long %zu\n",
                    (uintmax_t)entry->d_ino, *dirpath, len);
            return 1;
         }
         entry_name_len = (int)len;
      }

      // Reäppropriate *dirpath as a buffer for this entry's path.
      size_t entry_path_len;
      s = write_name_to_dirpath(entry->d_name, entry_name_len,
                                &entry_path_len, dirpath, dirpath_len,
                                dirpath_cap, *name_max);
      if (s)
         return s;
      if (entry_path_len > (size_t)UINT32_MAX) {
         fprintf(stderr, "Path to inode %ju in '%s' too long %zu\n",
                 (uintmax_t)entry->d_ino, *dirpath, entry_path_len);
         return 1;
      }
      const char *entry_path = *dirpath;

      marisa_agent_set_query(agent, entry_path, (uint32_t)entry_path_len);
      if (marisa_trie_lookup(exc, agent))
         continue;

      bool entry_included = dir_was_included;
      if (!entry_included) {
         marisa_agent_set_query(agent, entry_path, (uint32_t)entry_path_len);
         if (!marisa_trie_predictive_search(inc, agent)) {
            // We can't reach anything in our set of inclusions from this
            // entry, so don't bother looking further into it.
            continue;
         }
         const char *k;
         uint32_t l;
         marisa_agent_get_key(agent, &k, &l);
         entry_included = l == entry_path_len && !memcmp(k, entry_path, l);
      }

      struct len_str *ls = calloc(1, entry_name_len + sizeof *ls);
      if (!ls) {
         perror("calloc");
         return 4;
      }
      ls->len = entry_name_len;
      memcpy(ls->str, entry->d_name, ls->len);
      g_hash_table_add(names, ls);

      g_hash_table_insert(all_inodes, GINT_TO_POINTER(entry->d_ino),
                          GINT_TO_POINTER(dir_inode));

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
                    *dirpath, entry->d_name, oflags, strerror(errno));
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
                    *dirpath, entry->d_name, strerror(errno));
            return 1;
         }

         if (!S_ISLNK(st.st_mode))
            break;

         if (st.st_size >= *entry_link_target_cap) {
            *entry_link_target_cap *= 2;
            char *p = realloc(*entry_link_target, *entry_link_target_cap);
            if (!p) {
               perror("realloc");
               return 4;
            }
            *entry_link_target = p;
         }

         const ssize_t len =
            readlinkat(dirfd, entry->d_name, *entry_link_target,
                       *entry_link_target_cap);
         if (len == -1) {
            fprintf(stderr, "readlinkat('%s', '%s'): %s\n",
                    *dirpath, entry->d_name, strerror(errno));
            return 1;
         }
         if (len > (ssize_t)INT_MAX) {
            fprintf(stderr, "readlinkat('%s', '%s'): too long %zd\n",
                    *dirpath, entry->d_name, len);
            return 1;
         }
         if (len != st.st_size) {
            // Link's target changed to a different one between the
            // fstat/fstatat and the readlinkat, so update all info.
            continue;
         }
         entry_link_len = (int)len;
         (*entry_link_target)[entry_link_len] = '\0';
         break;
      }

      if (S_ISDIR(st.st_mode)) {
         if (st.st_ino == 0) {
            fprintf(stderr, "Directory '%s' has unsupported inode value 0\n",
                    entry_path);
            return 1;
         }
      } else if (fd != -1) {
         // We only needed it for the fstat, so get rid of it.
         close(fd);
      }

      const size_t keylen = entry_name_len + sizeof dir_inode;
      if (keylen > *keycap) {
         *keycap = keylen;
         char *k = realloc(*key, *keycap);
         if (!k) {
            perror("realloc");
            return 4;
         }
         *key = k;
      }
      memcpy(*key, &dir_inode, sizeof dir_inode);
      memcpy(*key + sizeof dir_inode, entry->d_name, entry_name_len);

      char *err = NULL;
      size_t vallen;
      char *valbuf = leveldb_get(db, ropts, *key, keylen, &vallen, &err);
      if (err)
         return fail_act("getting", err);

      struct db_val *val = (struct db_val*)valbuf;

      const bool entry_is_new =
         !val ||
         st.st_ino != val->inode ||
         // We don't care about permission (or ownership) changes.
         (st.st_mode & S_IFMT) != (val->mode & S_IFMT) ||
         st.st_size != val->size ||
         memcmp(&st.st_mtim, &val->mtime, sizeof st.st_mtim) ||
         (S_ISLNK(st.st_mode) &&
             (entry_link_len != val->link_target_len ||
              memcmp(*entry_link_target, val->link_target, entry_link_len))) ||
         ((S_ISBLK(st.st_mode) | S_ISCHR(st.st_mode))
             && st.st_rdev != val->rdev);

      struct seen_name *sn = malloc(entry_name_len + sizeof *sn);
      sn->len = entry_name_len;
      sn->new = (dir_was_new || !val) ? NEW : entry_is_new ? MOD : OLD;
      memcpy(sn->name, entry->d_name, entry_name_len);
      g_hash_table_insert(names_by_inode, GINT_TO_POINTER(entry->d_ino), sn);

#if defined(RDIFFDB_DEBUG) && RDIFFDB_DEBUG > 0
      if (dir_was_new || entry_is_new) {
         printf("%s is new because:", entry_path);
         if (dir_was_new)
            printf("\tparent dir was new\n");
         if (!val)
            printf("\twas not in DB\n");
         else {
            if (st.st_ino != val->inode)
               printf("\tinodes: FS %ju != DB %ju\n",
                      (uintmax_t)st.st_ino, (uintmax_t)val->inode);
            if ((st.st_mode & S_IFMT) != (val->mode & S_IFMT))
               printf("\ttypes: FS 0o%o != DB 0o%o\n",
                      (unsigned)(st.st_mode & S_IFMT),
                      (unsigned)(val->mode & S_IFMT));
            if (st.st_size != val->size)
               printf("\tsizes: FS %ju != DB %ju\n",
                      (uintmax_t)st.st_size, (uintmax_t)val->size);
            if (memcmp(&st.st_mtim, &val->mtime, sizeof st.st_mtim))
               printf("\tmtimes: FS %jd.%09lu != DB %jd.%09lu\n",
                      (intmax_t)st.st_mtim.tv_sec, st.st_mtim.tv_nsec,
                      (intmax_t)val->mtime.tv_sec, val->mtime.tv_nsec);
            if (S_ISLNK(st.st_mode) &&
                (entry_link_len != val->link_target_len ||
                 memcmp(*entry_link_target, val->link_target, entry_link_len)))
               printf("\tlink targets: FS '%s' != DB '%.*s'\n",
                      *entry_link_target, (int)val->link_target_len,
                      val->link_target);
            if ((S_ISBLK(st.st_mode) | S_ISCHR(st.st_mode)) &&
                st.st_rdev != val->rdev)
               printf("\trdevs: FS %ju != DB %ju\n",
                      (uintmax_t)st.st_rdev, (uintmax_t)val->rdev);
         }
      }
#endif

      // Write the entry to the DB only if the entry would change at all. Since
      // we store some things that we don't consider as differences from the
      // reporting POV, this does some more checking.
      const bool update_db =
         entry_is_new ||
         st.st_mode != val->mode ||
         memcmp(&st.st_atim, &val->atime, sizeof st.st_atim) ||
         memcmp(&st.st_ctim, &val->ctime, sizeof st.st_ctim) ||
         st.st_uid != val->uid ||
         st.st_gid != val->gid;

      if (update_db && !dry_run) {
         if (!val) {
            vallen = entry_link_len + sizeof *val;
            if (vallen > *newvalcap) {
               *newvalcap = vallen;
               struct db_val *p = realloc(*newval, *newvalcap);
               if (!p) {
                  perror("realloc");
                  return 4;
               }
               *newval = p;
            }
            val = *newval;
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
         memcpy(val->link_target, *entry_link_target, entry_link_len);

         leveldb_writebatch_put(batch, *key, keylen, (const char*)val, vallen);
      }

      if (valbuf)
         leveldb_free(valbuf);

      if (!S_ISDIR(st.st_mode))
         continue;

      DIR *entry_dir = fdopendir(fd);
      if (!entry_dir) {
         fprintf(stderr, "fdopendir('%s'): %s\n", entry_path, strerror(errno));
         return 1;
      }

      dev_t *seen_devs = *pseen_devs;
      bool seen_dev = false;
      for (size_t i = 0, e = *seen_devs_count; i < e; ++i) {
         if (seen_devs[i] == st.st_dev) {
            seen_dev = true;
            break;
         }
      }

      if (!seen_dev) {
         if (*seen_devs_count == *seen_devs_cap) {
            *seen_devs_cap += 16;
            seen_devs = realloc(seen_devs, *seen_devs_cap * sizeof *seen_devs);
            if (!seen_devs) {
               perror("realloc");
               return 4;
            }
            *pseen_devs = seen_devs;
         }
         seen_devs[*seen_devs_count++] = st.st_dev;

         const long new_name_max = fpathconf(fd, _PC_NAME_MAX);
         if (new_name_max > *name_max) {
            *name_max = new_name_max;
            entry =
               realloc(entry, offsetof(struct dirent, d_name) + *name_max + 1);
            if (!entry) {
               perror("realloc");
               return 4;
            }
            *pentry = entry;
         }
      }

      s = go(dirpath, entry_path_len, dirpath_cap, fd, st.st_ino, entry_dir,
             !valbuf, entry_included, pentry, name_max, entry_link_target,
             entry_link_target_cap, pseen_devs, seen_devs_count, seen_devs_cap,
             key, keycap, newval, newvalcap, inc, exc, agent, all_inodes, db,
             dry_run);
      if (s)
         return s;
      closedir(entry_dir);
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
      const char *k = leveldb_iter_key(iter, &keylen);
      if (memcmp(k, &dir_inode, sizeof dir_inode))
         break;

      const struct len_str ls =
         { keylen - sizeof dir_inode, .p = k + sizeof dir_inode };

      if (!g_hash_table_contains(names, &ls)) {
         size_t vallen;
         const char *valbuf = leveldb_iter_value(iter, &vallen);
         const ino_t inode = ((struct db_val*)valbuf)->inode;

         const struct seen_name *sn =
            g_hash_table_lookup(names_by_inode, GINT_TO_POINTER(inode));
         if (sn) {
            fputs("R ", stdout);
            if (**dirpath) {
               fputs(*dirpath, stdout);
               putchar('/');
            }
            fwrite(ls.p, 1, ls.len, stdout);
            fputs(" // ", stdout);
            if (**dirpath) {
               fputs(*dirpath, stdout);
               putchar('/');
            }
            fwrite(sn->name, 1, sn->len, stdout);
            putchar('\n');
            g_hash_table_remove(names_by_inode, GINT_TO_POINTER(inode));
         }

         if ((s = rmr_at_iter(k, keylen, iter, dirpath, dirpath_len,
                              dirpath_cap, *name_max, !dry_run, all_inodes, db,
                              batch)))
            return s;
      }

      leveldb_iter_next(iter);
      leveldb_iter_get_error(iter, &err);
      if (err)
         return fail_act("nexting iterator", err);
   }
   g_hash_table_destroy(names);
   leveldb_iter_destroy(iter);

   GHashTableIter hiter;
   g_hash_table_iter_init(&hiter, names_by_inode);
   for (gpointer k, v; g_hash_table_iter_next(&hiter, &k, &v);) {
      const struct seen_name *sn = v;
      if (!sn->new)
         continue;

      fputs(sn->new == NEW ? "A " : "M ", stdout);
      if (**dirpath) {
         fputs(*dirpath, stdout);
         putchar('/');
      }
      fwrite(sn->name, 1, sn->len, stdout);
      putchar('\n');
   }
   g_hash_table_destroy(names_by_inode);

   if (!dry_run) {
      leveldb_write(db, wopts, batch, &err);
      if (err)
         return fail_act("writing", err);
      leveldb_writebatch_destroy(batch);
   }

#if defined(RDIFFDB_DEBUG) && RDIFFDB_DEBUG > 1
   iter = leveldb_create_iterator(db, ropts);
   leveldb_iter_get_error(iter, &err);
   if (err)
      return fail_act("creating iterator (DEBUG)", err);
   leveldb_iter_seek(iter, (const char*)&dir_inode, sizeof dir_inode);
   leveldb_iter_get_error(iter, &err);
   if (err)
      return fail_act("seeking iterator (DEBUG)", err);
   while (leveldb_iter_valid(iter)) {
      size_t keylen;
      const char *k = leveldb_iter_key(iter, &keylen);
      if (memcmp(k, &dir_inode, sizeof dir_inode))
         break;

      size_t vallen;
      const char *valbuf = leveldb_iter_value(iter, &vallen);
      const struct db_val *val = (struct db_val*)valbuf;

      printf("\tremains dir %ju: entry %ju '%.*s'\n",
             (uintmax_t)dir_inode, (uintmax_t)val->inode,
             (int)(keylen - sizeof dir_inode), k + sizeof dir_inode);

      leveldb_iter_next(iter);
      leveldb_iter_get_error(iter, &err);
      if (err)
         return fail_act("nexting iterator (DEBUG)", err);
   }
   leveldb_iter_destroy(iter);
#endif

   return 0;
}

static int rmr(
   ino_t dir_inode, char **dirpath, const size_t dirpath_len,
   size_t *dirpath_cap, const size_t name_max, bool actually_delete,
   GHashTable *all_inodes, leveldb_t *db, leveldb_writebatch_t *batch)
{
   int s;
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

      if ((s = rmr_at_iter(key, keylen, iter, dirpath, dirpath_len,
                           dirpath_cap, name_max, actually_delete, all_inodes,
                           db, batch)))
         return s;

      leveldb_iter_next(iter);
      leveldb_iter_get_error(iter, &err);
      if (err)
         return fail_act("nexting iterator", err);
   }
   leveldb_iter_destroy(iter);
   return 0;
}

static int rmr_at_iter(
   const char *key, const size_t keylen, const leveldb_iterator_t *iter,
   char **dirpath, const size_t dirpath_len, size_t *dirpath_cap,
   const size_t name_max, bool actually_delete, GHashTable *all_inodes,
   leveldb_t *db, leveldb_writebatch_t *batch)
{
   size_t vallen;
   const char *valbuf = leveldb_iter_value(iter, &vallen);
   const struct db_val *val = (struct db_val*)valbuf;

   // If we've seen this inode with the same parent inode previously, i.e. this
   // entry (or one of its parents) has been moved to another location, don't
   // delete it from the DB.
   if (actually_delete)
      actually_delete =
         g_hash_table_lookup(all_inodes, GINT_TO_POINTER(val->inode)) !=
            GINT_TO_POINTER(*(const ino_t*)key);

   fputs("D ", stdout);

   const char *name = key + sizeof (ino_t);
   const size_t name_len = keylen - sizeof (ino_t);

   if (S_ISDIR(val->mode)) {
      size_t path_len;
      int s = write_name_to_dirpath(name, name_len, &path_len, dirpath,
                                    dirpath_len, dirpath_cap, name_max);
      if (s)
         return s;
      fputs(*dirpath, stdout);
      putchar('\n');
      if ((s = rmr(val->inode, dirpath, path_len, dirpath_cap, name_max,
                   actually_delete, all_inodes, db, batch)))
         return s;
      (*dirpath)[dirpath_len] = '\0';
   } else {
      if (**dirpath) {
         fputs(*dirpath, stdout);
         putchar('/');
      }
      fwrite(name, 1, name_len, stdout);
      putchar('\n');
   }

   if (actually_delete)
      leveldb_writebatch_delete(batch, key, keylen);

   return 0;
}
