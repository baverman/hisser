/* lmdb declarations */
typedef struct MDB_cursor MDB_cursor;

enum MDB_cursor_op {
    MDB_FIRST,
    MDB_FIRST_DUP,
    MDB_GET_BOTH,
    MDB_GET_BOTH_RANGE,
    MDB_GET_CURRENT,
    MDB_GET_MULTIPLE,
    MDB_LAST,
    MDB_LAST_DUP,
    MDB_NEXT,
    MDB_NEXT_DUP,
    MDB_NEXT_MULTIPLE,
    MDB_NEXT_NODUP,
    MDB_PREV,
    MDB_PREV_DUP,
    MDB_PREV_NODUP,
    MDB_SET,
    MDB_SET_KEY,
    MDB_SET_RANGE,
};

struct MDB_val {
    size_t mv_size;
    void *mv_data;
};

typedef struct MDB_val MDB_val;

/*
*  Excerpt from lmdb cpython code to be able to get `curs` field from a provided
*  PyObject.
*/
struct list_head {
    struct lmdb_object *prev;
    struct lmdb_object *next;
};

#define LmdbObject_HEAD \
    PyObject_HEAD \
    struct list_head siblings; \
    struct list_head children; \
    int valid;

struct CursorObject {
    LmdbObject_HEAD
    void *trans;
    int positioned;
    MDB_cursor *curs;
};
/* END lmdb.cpython */
