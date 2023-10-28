#include <stdint.h>
#include <stddef.h>
#include <stdbool.h>

#include "hashtrie.h"

void hashtrie_init(struct HashTrie *trie, struct HashTrieNode *data) {
    trie->count = 1;
    trie->data = data;
    trie->data[0] = (struct HashTrieNode){0}; // root node
}

struct HashTrieNode*
hashtrie_find(struct HashTrie *trie, uint64_t key, bool upsert) {
    uint64_t h = key;
    struct HashTrieNode *node = trie->data;

    while (1) {
        uint32_t nidx = node->slots[h & 3];
        if (!nidx) {
            if (upsert) {
                node->slots[h & 3] = trie->count;
                node = trie->data + trie->count;
                *node = (struct HashTrieNode){.key = key};
                trie->count++;
                return node;
            } else {
                return NULL;
            }
        }
        node = trie->data + nidx;
        if (node->key == key) {
            return node;
        }
        h >>= 2;
    }
}

void hashtrie_set(struct HashTrie *trie, uint64_t key, int32_t value) {
    hashtrie_find(trie, key, true)->value = value;
}

int32_t hashtrie_get(struct HashTrie *trie, uint64_t key) {
    struct HashTrieNode *node = hashtrie_find(trie, key, false);
    if (node) {
        return node->value;
    }
    return -1;
}
