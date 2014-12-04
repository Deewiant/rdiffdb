// File created: 2014-12-03 21:22:22

#include <new>
#include <stdlib.h>

#include <marisa/trie.h>

#include "marisa.h"

extern "C" {

struct marisa_keyset { marisa::Keyset ks; };
struct marisa_trie { marisa::Trie t; };
struct marisa_agent { marisa::Agent a; };

/* TODO: look into what can throw, catch the exception, and change return types
         accordingly. */

marisa_keyset* marisa_keyset_init(void) {
   marisa_keyset *ks = static_cast<marisa_keyset *>(malloc(sizeof *ks));
   if (ks)
      new (ks) marisa_keyset();
   return ks;
}
void marisa_keyset_push(marisa_keyset *keyset,
                        const char *key, uint32_t length)
{
   keyset->ks.push_back(key, length);
}
void marisa_keyset_free(marisa_keyset *keyset) {
   keyset->~marisa_keyset();
   free(keyset);
}

marisa_trie* marisa_trie_init(marisa_keyset *keyset, int flags) {
   marisa_trie *t = static_cast<marisa_trie *>(malloc(sizeof *t));
   if (t) {
      new (t) marisa_trie();

      // This is bundled into initialization since it's evidently necessary to
      // call, otherwise even emptiness checks fail with internal errors.
      t->t.build(keyset->ks, flags);
   }
   return t;
}
void marisa_trie_free(marisa_trie *trie) {
   trie->~marisa_trie();
   free(trie);
}

void marisa_trie_read(marisa_trie *trie, int fd) {
   trie->t.read(fd);
}
void marisa_trie_write(const marisa_trie *trie, int fd) {
   trie->t.write(fd);
}

marisa_agent* marisa_agent_init(void) {
   marisa_agent *a = static_cast<marisa_agent *>(malloc(sizeof *a));
   if (a)
      new (a) marisa_agent();
   return a;
}
void marisa_agent_get_key(const marisa_agent *agent,
                          const char **key, uint32_t *length)
{
   const marisa::Key &k = agent->a.key();
   *key    = k.ptr();
   *length = static_cast<uint32_t>(k.length());
}
void marisa_agent_set_query(marisa_agent *agent, const char *q, uint32_t len) {
   agent->a.set_query(q, len);
}
void marisa_agent_free(marisa_agent *agent) {
   agent->~marisa_agent();
   free(agent);
}

bool marisa_trie_empty(const marisa_trie *trie) {
   return trie->t.empty();
}
bool marisa_trie_lookup(const marisa_trie *trie, marisa_agent *agent) {
   return trie->t.lookup(agent->a);
}
bool marisa_trie_predictive_search(const marisa_trie *trie,
                                   marisa_agent *agent)
{
   return trie->t.predictive_search(agent->a);
}

}
