typedef struct {
    uint8_t** tag_ids;
    // uint8_t* min_names_buf;
    uint8_t** min_names;
    size_t count;
    int min_names_filled;
} LMDB_tags_state;

typedef struct {
    LMDB_tags_state* tag_states;
    uint8_t cname_buf[4];
    uint8_t *cname;
    size_t count;
} LMDB_scan_state;

typedef struct {
    uint8_t key[4];
} hash_key;

typedef struct {
    hash_key key;
    uint8_t *value;
    size_t size;
} hash_item;

int lmdb_scan_init(const char *);

size_t lmdb_scan_tags(void*, LMDB_scan_state*, uint8_t *out, size_t out_count);

void lmdb_scan_names(void* pyobj_name_cursor, void* pyobj_revtag_cursor,
                     uint8_t* name_ids, size_t name_ids_count, void* pyobj_result_list,
                     hash_item **cache_state);

void lmdb_scan_free_cache(hash_item **cache_state);
