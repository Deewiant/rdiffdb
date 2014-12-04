// File created: 2014-12-03 21:23:26

#include <stddef.h>

#include <marisa/base.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct marisa_keyset marisa_keyset;
typedef struct marisa_trie marisa_trie;
typedef struct marisa_agent marisa_agent;

/* TODO: look into what can throw and change return types accordingly. */

marisa_keyset* marisa_keyset_init(void);
void marisa_keyset_push(marisa_keyset *keyset,
                        const char *key, uint32_t length);
void marisa_keyset_free(marisa_keyset *keyset);

marisa_trie* marisa_trie_init(marisa_keyset *keyset, int flags);
void marisa_trie_free(marisa_trie *trie);

void marisa_trie_read(marisa_trie *trie, int fd);
void marisa_trie_write(const marisa_trie *trie, int fd);

marisa_agent* marisa_agent_init(void);
void marisa_agent_get_key(const marisa_agent *agent,
                          const char **key, uint32_t *length);
void marisa_agent_set_query(marisa_agent *agent, const char *q, uint32_t len);
void marisa_agent_free(marisa_agent *agent);

bool marisa_trie_empty(const marisa_trie *trie);
bool marisa_trie_lookup(const marisa_trie *trie, marisa_agent *agent);
bool marisa_trie_predictive_search(const marisa_trie *trie,
                                   marisa_agent *agent);

#ifdef __cplusplus
}
#endif
