#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <dlfcn.h>

#define PY_SSIZE_T_CLEAN
#include <Python.h>

#include "lmdb_misc.h"
#include "lmdb_scan.h"

#define STB_DS_IMPLEMENTATION
#include "stb_ds.h"


static int (*mdb_cursor_get)(MDB_cursor *cursor, MDB_val *key, MDB_val*data, int op) = NULL;

/* Loads all needed symbols from provided lmdb dll */
int lmdb_scan_init(const char *lmdb_so) {
    void *handle = dlopen(lmdb_so, RTLD_NOW);

    mdb_cursor_get = dlsym(handle, "mdb_cursor_get");
    if (!mdb_cursor_get) {
        fprintf(stderr, "%s\n", dlerror());
        return 1;
    }
    return 0;
}

static
uint32_t id2int(const uint8_t id[4]) {
    if (!id) {
        return 0xfffffff5;
    }
    return ((uint32_t)id[0] << 24) | ((uint32_t)id[1] << 16) | ((uint32_t)id[2] << 8) | (uint32_t)id[3];
}

static void int2id(uint8_t dest[4], uint32_t value) {
    dest[3] = value & 0xff;
    dest[2] = (value >> 8) & 0xff;
    dest[1] = (value >> 16) & 0xff;
    dest[0] = (value >> 24) & 0xff;
}

uint8_t* get_val(MDB_cursor* cursor, MDB_val *key, MDB_val *val, int op) {
    int rc = mdb_cursor_get(cursor, key, val, op);
    if (!rc && val->mv_size) {
        return val->mv_data;
    }
    return NULL;
}

static
uint8_t* next_item_single(MDB_cursor* cursor, LMDB_tags_state *ts, uint8_t* name_id) {
    MDB_val key = {4, ts->tag_ids[0]};
    /* fprintf(stderr, "---  %08x %08x\n", id2int(key.mv_data), id2int(name_id)); */
    if (name_id) {
        MDB_val val = {4, name_id};
        return get_val(cursor, &key, &val, MDB_GET_BOTH_RANGE);
    } else {
        MDB_val val = {0, NULL};
        return get_val(cursor, &key, &val, MDB_SET_KEY);
    }
}

static
uint8_t* next_item_multi(MDB_cursor* cursor, LMDB_tags_state *ts, uint8_t* name_id) {
    uint8_t *min_name = NULL;
    if (name_id) {
        for(size_t i=0; i < ts->count; i++) {
            uint8_t *name = ts->min_names[i];
            if ((!ts->min_names_filled) || (name && memcmp(name, name_id, 4) < 0)) {
                MDB_val key = {4, ts->tag_ids[i]};
                MDB_val val = {4, name_id};
                name = ts->min_names[i] = get_val(cursor, &key, &val, MDB_GET_BOTH_RANGE);
            }
            if (name && (!min_name || memcmp(name, min_name, 4) < 0)) {
                min_name = name;
            }
        }
    } else {
        MDB_val val = {0, NULL};
        for(size_t i=0; i < ts->count; i++) {
            MDB_val key = {4, ts->tag_ids[i]};
            uint8_t *name = get_val(cursor, &key, &val, MDB_SET_KEY);
            ts->min_names[i] = name;
            if (name && (!min_name || memcmp(name, min_name, 4) < 0)) {
                min_name = name;
            }
        }
    }
    ts->min_names_filled = 1;
    return min_name;
}

static
uint8_t* next_item(MDB_cursor* cursor, LMDB_tags_state *ts, uint8_t* name_id) {
    if (ts->count > 1) {
        return next_item_multi(cursor, ts, name_id);
    } else {
        return next_item_single(cursor, ts, name_id);
    }
}

size_t lmdb_scan_tags(void* pyobj_cursor, LMDB_scan_state* state, uint8_t *out, size_t out_count) {
    size_t ri = 0;
    size_t tstate_count = state->count;
    size_t loop_start = 0;
    size_t start = 0;
    size_t i;

    uint8_t* cname = state->cname;
    uint8_t* nname = NULL;
    uint8_t tmp_cname[4] = {0};

    MDB_cursor* cursor = ((struct CursorObject *)pyobj_cursor)->curs;

    while (ri < out_count) {
        for(i=loop_start; i < tstate_count; i++) {
            /* size_t ridx =(i + start) % tstate_count; */
            LMDB_tags_state* ts = &(state->tag_states[(i + start) % tstate_count]);
            if (!cname) {
                cname = next_item(cursor, ts, NULL);
                /* fprintf(stderr, "@@@ null %li %li %08x\n", start, ridx, id2int(cname)); */
                if (!cname) return ri;
                memcpy(tmp_cname, cname, 4);
                cname = tmp_cname;
            } else {
                nname = next_item(cursor, ts, cname);
                /* fprintf(stderr, "@@@ full %li %li %08x\n", start, ridx, id2int(nname)); */
                if (!nname) return ri;
                if (memcmp(nname, cname, 4) != 0) {
                    memcpy(tmp_cname, nname, 4);
                    cname = tmp_cname;
                    start = i;
                    loop_start = 1;
                    break;
                }
            }
        }
        if (i == tstate_count) {
            start = 0;
            loop_start = 0;
            memcpy(out + ri*4, cname, 4);
            ri++;
            int2id(tmp_cname, id2int(cname) + 1);
            cname = tmp_cname;
        }
    }
    memmove(state->cname_buf, cname, 4);
    state->cname = state->cname_buf;
    return ri;
}

void lmdb_scan_names(void* pyobj_name_cursor, void* pyobj_revtag_cursor,
                     uint8_t* name_ids, size_t name_ids_count, void* pyobj_result_list,
                     hash_item **cache_state) {

    char buf[2048] = {0};
    MDB_cursor* name_cursor = ((struct CursorObject *)pyobj_name_cursor)->curs;
    MDB_cursor* revtag_cursor = ((struct CursorObject *)pyobj_revtag_cursor)->curs;

    hash_item *hash = cache_state[0];

    for(size_t i=0; i < name_ids_count; i++) {
        MDB_val key = {4, name_ids+(i*4)};
        MDB_val val = {0, NULL};
        uint8_t *tag_ids = get_val(name_cursor, &key, &val, MDB_SET_KEY);
        if (!tag_ids) continue;

        /* fprintf(stderr, "    tag %lu, %08x\n", val.mv_size, id2int(tag_id)); */
        size_t pos = 0;
        for(size_t j=0; j < val.mv_size; j += 4) {
            uint8_t *tagvalue = NULL;
            size_t tagvalue_size = 0;
            hash_key hkey = {0};
            memcpy(hkey.key, tag_ids+j, 4);

            /* fprintf(stderr, "key %08x\n", id2int(hkey.key)); */

            hash_item* item = hmgetp_null(hash, hkey);
            if (item) {
                /* fprintf(stderr, "hit %.*s\n", (int)item.size, item.value); */
                tagvalue = item->value;
                tagvalue_size = item->size;
            } else {
                MDB_val tkey = {4, tag_ids + j};
                MDB_val tval = {0, NULL};
                tagvalue = get_val(revtag_cursor, &tkey, &tval, MDB_SET_KEY);
                tagvalue_size = tval.mv_size;
                if ((tagvalue_size >= 6) && (memcmp(tagvalue, "name=", 5) == 0)) {
                    tagvalue += 5;
                    tagvalue_size -= 5;
                }
                hash_item s = {hkey, tagvalue, tagvalue_size};
                hmputs(hash, s);
            }

            if (tagvalue && (pos + tagvalue_size + 2 < sizeof(buf))) {
                if (pos > 0) {
                    buf[pos] = ';';
                    pos++;
                }
                memcpy(buf+pos, tagvalue, tagvalue_size);
                pos += tagvalue_size;
                buf[pos] = 0;
            }
        }

        if (pos) {
            PyObject *bo = PyBytes_FromStringAndSize(buf, pos);
            PyList_Append(pyobj_result_list, bo);
            Py_DECREF(bo);
        }
        /* fprintf(stderr, "    |%s|\n", buf); */
    }
    cache_state[0] = hash;
    /* for (int i=0; i < hmlen(hash); ++i) */
    /*      fprintf(stderr, "-- %08x %.*s\n", id2int(hash[i].key.key), (int)(hash[i].size), hash[i].value); */
}

void lmdb_scan_free_cache(hash_item **cache_state) {
    hmfree(cache_state[0]);
}
