struct HashTrieNode {
    uint64_t key;
    int32_t value;
    uint32_t slots[4];
};

struct HashTrie {
    struct HashTrieNode *data;
    size_t count;
};

void hashtrie_init(struct HashTrie *trie, struct HashTrieNode *data);

void hashtrie_set(struct HashTrie *trie, uint64_t key, int32_t value);

int32_t hashtrie_get(struct HashTrie *trie, uint64_t key);
