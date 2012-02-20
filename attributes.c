/*
 * Copyright (c) 2012 Rob Hoelz <rhoelz@inoc.com>
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of
 * this software and associated documentation files (the "Software"), to deal in
 * the Software without restriction, including without limitation the rights to
 * use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
 * the Software, and to permit persons to whom the Software is furnished to do so,
 * subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
 * FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
 * COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
 * IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
 * CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
*/

#include "sqlite3ext.h"

SQLITE_EXTENSION_INIT1;

#include <stdlib.h>
#include <string.h>

#define MODULE_NAME "attributes"
#define MODULE_VERSION 1

#define RECORD_SEPARATOR     '\x1f'
#define RECORD_SEPARATOR_STR "\x1f"

#define VIRT_TABLE_SCHEMA\
    "CREATE TABLE t ("\
    "  id         INTEGER PRIMARY KEY,"\
    "  attributes TEXT    NOT NULL"\
    ")"

#define SEQ_SCHEMA_NAME  "\"%w\".\"%w_Sequence\""
#define ATTR_SCHEMA_NAME "\"%w\".\"%w_Attributes\""

#define SEQ_SCHEMA_TMPL\
    "CREATE TABLE " SEQ_SCHEMA_NAME " ("\
    "  seq_id     INTEGER PRIMARY KEY, "\
    "  attributes TEXT NOT NULL"\
    ")"

#define ATTR_SCHEMA_TMPL\
    "CREATE TABLE " ATTR_SCHEMA_NAME " ("\
    "  seq_id     INTEGER REFERENCES \"%w_Sequence\" (seq_id) ON DELETE CASCADE, "\
    "  attr_name  TEXT    NOT NULL, "\
    "  attr_value TEXT    NOT NULL "\
    ")"

#define ATTR_INDEX_TMPL\
    "CREATE UNIQUE INDEX \"%w\".\"%w_attr_index\" ON \"%w_Attributes\" "\
    " ( attr_name, seq_id ) "

#define INSERT_SEQ_TMPL\
    "INSERT INTO " SEQ_SCHEMA_NAME " (attributes, seq_id) VALUES (?, ?)"

#define INSERT_ATTR_TMPL\
    "INSERT INTO " ATTR_SCHEMA_NAME " VALUES ( ?, ?, ? )"

#define DELETE_SEQ_TMPL\
    "DELETE FROM " SEQ_SCHEMA_NAME " WHERE seq_id = ?"

#define DELETE_ATTR_TMPL\
    "DELETE FROM " ATTR_SCHEMA_NAME " WHERE seq_id = ?"

#define SELECT_CURS_TMPL\
    "SELECT seq_id, attributes FROM " SEQ_SCHEMA_NAME

#define SELECT_CURS_WITH_KEY_TMPL\
    "SELECT s.seq_id, s.attributes FROM " SEQ_SCHEMA_NAME " AS s "\
    "INNER JOIN " ATTR_SCHEMA_NAME " AS a ON a.seq_id = s.seq_id "\
    "WHERE a.attr_name = ?"

#define SELECT_CURS_WITH_KEY_VALUE_TMPL\
    "SELECT s.seq_id, s.attributes FROM " SEQ_SCHEMA_NAME " AS s "\
    "INNER JOIN " ATTR_SCHEMA_NAME " AS a ON a.seq_id = s.seq_id "\
    "WHERE a.attr_name = ? AND a.attr_value = ?"

#define SCHEMA_PREFIX_SIZE            (sizeof(SCHEMA_PREFIX) - 1)
#define SCHEMA_SUFFIX_SIZE            (sizeof(SCHEMA_SUFFIX) - 1)
#define DEFAULT_ATTRIBUTE_COLUMN_SIZE (sizeof(DEFAULT_ATTRIBUTE_COLUMN) - 1)

#define INSERT_SEQ_ATTR_COL 1
#define INSERT_SEQ_ID_COL   2

#define INSERT_ATTR_SEQ_COL 1
#define INSERT_ATTR_KEY_COL 2
#define INSERT_ATTR_VAL_COL 3

#define UPDATE_ARG_ROWID 1
#define UPDATE_ARG_ID    2
#define UPDATE_ARG_ATTRS 3

#define DELETE_SEQ_ARG_ROWID  1
#define DELETE_ATTR_ARG_ROWID 1

#define CURS_SEQ_COL 0

#define ATTR_NAME_INDEX 1

#define SCHEMA_ID_COL   0
#define SCHEMA_ATTR_COL 1

#define UNIMPLD(vtab)\
    __unimplemented(vtab, __FUNCTION__)

#include <stdio.h>

#define diag(fmt, args...)\
    fprintf(stderr, "# " fmt "\n", ##args);

static int attributes_disconnect( sqlite3_vtab * );
static int attributes_destroy( sqlite3_vtab * );

struct attribute_vtab {
    sqlite3_vtab vtab;
    sqlite3 *db;
    char *database_name;
    char *table_name;
    sqlite3_stmt *insert_seq_stmt;
    sqlite3_stmt *insert_attr_stmt;
};

struct attribute_cursor {
    sqlite3_vtab_cursor cursor;
    sqlite3_stmt *stmt;
    int eof;
};

/* Constants for use in iterate_over_kv_pairs */
#define CONTINUE 0
#define BREAK    1

typedef int (*kv_iter_cb)(const char *, size_t, const char *, size_t, void *);

static int __unimplemented(struct attribute_vtab *vtab, const char *func_name)
{
    vtab->vtab.zErrMsg = sqlite3_mprintf("function '%s' is not yet implemented", func_name);
    return SQLITE_ERROR;
}

/* key-value pairs are separated by RECORD_SEPARATOR, and each member of the pair
 * is also separated by RECORD_SEPARATOR.  So the layout of attributes looks kind of
 * like this:
 *
 * key1 RS value1 RS key2 RS value2 RS key3 RS value3
 */
static const char *extract_attribute_value(const char *attributes, const char *key, size_t *value_len)
{
    char *needle;
    const char *attr_location = NULL;
    size_t key_len;

    key_len = strlen( key );
    needle  = sqlite3_malloc( key_len + 3 ); /* one for NULL, one for
                                                record separator */

    needle[0] = RECORD_SEPARATOR;
    strcpy(needle + 1, key);
    needle[key_len + 1] = RECORD_SEPARATOR;
    needle[key_len + 2] = '\0';

    if(! strncmp( attributes, needle + 1, key_len + 1 )) {
        attr_location = attributes + key_len + 1;
    }

    if(! attr_location) {
        attr_location = strstr( attributes, needle );
        if(attr_location) {
            attr_location += key_len + 2;
        }
    }

    if(attr_location) {
        const char *attr_end;

        attr_end = strchr( attr_location, RECORD_SEPARATOR );
        if(! attr_end) {
            attr_end = attr_location + strlen( attr_location ); /* end of string */
        }
        *value_len = attr_end - attr_location;
        return attr_location;
    } else {
        return NULL;
    }
}

static void iterate_over_kv_pairs( const char *attributes,
    kv_iter_cb callback, void *udata )
{
    const char *key_endp;
    int status;

    while(key_endp = strchr( attributes, RECORD_SEPARATOR )) {
        const char *key;
        const char *value;
        const char *value_endp;
        size_t key_length;
        size_t value_length;

        value_endp = strchr( key_endp + 1, RECORD_SEPARATOR );

        if(! value_endp) {
            value_endp = key_endp + strlen(key_endp); /* end of string */
        }

        key          = attributes;
        key_length   = key_endp - key;
        value        = key_endp + 1;
        value_length = value_endp - value;

        status = callback( key, key_length, value, value_length, udata );

        if(status == BREAK) {
            break;
        }

        if(*value_endp == RECORD_SEPARATOR) {
            attributes = value_endp + 1;
        } else {
            break;
        }
    }

}

static void sql_get_attr( sqlite3_context *ctx, int nargs,
    sqlite3_value **values )
{
    const char *attributes;
    const char *attr_name;
    const char *attr_value;
    size_t value_length;

    attributes = sqlite3_value_text( values[0] );
    attr_name  = sqlite3_value_text( values[1] );
    attr_value = extract_attribute_value( attributes, attr_name,
        &value_length );

    if(attr_value) {
        sqlite3_result_text( ctx, attr_value, value_length, SQLITE_TRANSIENT );
    } else {
        sqlite3_result_null( ctx );
    }
}

static char *_build_schema( int argc, const char * const *argv )
{
    return sqlite3_mprintf("%s", VIRT_TABLE_SCHEMA);
}

static int _initialize_statements( struct attribute_vtab *vtab )
{
    char *sql;
    int status;

    sql = sqlite3_mprintf( INSERT_SEQ_TMPL, vtab->database_name,
        vtab->table_name );

    if(! sql) {
        /* our caller handles the mess */
        return SQLITE_NOMEM;
    }

    status = sqlite3_prepare_v2( vtab->db, sql, -1, &(vtab->insert_seq_stmt), NULL );

    sqlite3_free( sql );

    if(status != SQLITE_OK) {
        /* our caller handles the mess */
        return status;
    }

    sql = sqlite3_mprintf( INSERT_ATTR_TMPL, vtab->database_name,
        vtab->table_name );

    status = sqlite3_prepare_v2( vtab->db, sql, -1, &(vtab->insert_attr_stmt), NULL );

    sqlite3_free( sql );

    if(status != SQLITE_OK) {
        /* our caller handles the mess */
        return status;
    }

    return SQLITE_OK;
}

static int _init_vtab( sqlite3 *db, void *udp, int argc,
    char const * const *argv, sqlite3_vtab **vtab, char **errMsg,
    int initStmts )
{
    char *sql;
    struct attribute_vtab *avtab;
    int status;

    *vtab   = NULL;
    *errMsg = NULL;

    avtab = (struct attribute_vtab *) sqlite3_malloc(sizeof(struct attribute_vtab));
    if(! avtab) {
        return SQLITE_NOMEM;
    }
    memset(avtab, 0, sizeof(struct attribute_vtab));
    avtab->db = db;

    avtab->database_name = sqlite3_mprintf( "%s", argv[1] );
    if(! avtab->database_name) {
        attributes_disconnect((sqlite3_vtab *) avtab);
        return SQLITE_NOMEM;
    }

    avtab->table_name = sqlite3_mprintf( "%s", argv[2] );
    if(! avtab->table_name) {
        attributes_disconnect((sqlite3_vtab *) avtab);
        return SQLITE_NOMEM;
    }

    sql = _build_schema(argc - 3, argv + 3);
    if(! sql) {
        attributes_disconnect((sqlite3_vtab *) avtab);
        return SQLITE_NOMEM;
    }

    status = sqlite3_declare_vtab( db, sql );
    sqlite3_free(sql);

    if(status != SQLITE_OK) {
        attributes_disconnect((sqlite3_vtab *) avtab);
        return status;
    }

    if(initStmts) {
        status = _initialize_statements( avtab );

        if(status != SQLITE_OK) {
            attributes_disconnect((sqlite3_vtab *) avtab);
            return status;
        }
    }

    *vtab = (sqlite3_vtab *) avtab;

    return SQLITE_OK;
}

static int attributes_connect( sqlite3 *db, void *udp, int argc,
    char const * const *argv, sqlite3_vtab **vtab, char **errMsg )
{
    return _init_vtab( db, udp, argc, argv, vtab, errMsg, 1 );
}

static int attributes_create( sqlite3 *db, void *udp, int argc,
    char const * const *argv, sqlite3_vtab **vtab, char **errMsg )
{
    const char *database_name = argv[1];
    const char *table_name    = argv[2];
    char *sql                 = NULL;

    int status = _init_vtab( db, udp, argc, argv, vtab, errMsg, 0 );

    if(status != SQLITE_OK) {
        goto error_handler;
    }

    sql = sqlite3_mprintf( SEQ_SCHEMA_TMPL, database_name,
        table_name );

    if(! sql) {
        status = SQLITE_NOMEM;
        goto error_handler;
    }

    status = sqlite3_exec( db, sql, NULL, NULL, errMsg );

    if(status != SQLITE_OK) {
        goto error_handler;
    }

    sqlite3_free( sql );

    sql = sqlite3_mprintf( ATTR_SCHEMA_TMPL, database_name,
        table_name, table_name );

    if(! sql) {
        status = SQLITE_NOMEM;
        goto error_handler;
    }

    status = sqlite3_exec( db, sql, NULL, NULL, errMsg );

    if(status != SQLITE_OK) {
        goto error_handler;
    }

    sqlite3_free( sql );

    sql = sqlite3_mprintf( ATTR_INDEX_TMPL, database_name, table_name,
        table_name);

    if(! sql) {
        goto error_handler;
    }

    status = sqlite3_exec( db, sql, NULL, NULL, errMsg );

    if(status != SQLITE_OK) {
        goto error_handler;
    }

    sqlite3_free( sql );

    sql = NULL;

    status = _initialize_statements( (struct attribute_vtab *) *vtab );

    if(status != SQLITE_OK) {
        goto error_handler;
    }

    goto done;

error_handler:
    if(sql) {
        sqlite3_free( sql );
    }
    if(*vtab) {
        attributes_destroy( *vtab );
    }
done:
    return status;
}

static int attributes_disconnect( sqlite3_vtab *_vtab )
{
    struct attribute_vtab *vtab = (struct attribute_vtab *) _vtab;

    sqlite3_finalize( vtab->insert_attr_stmt );
    sqlite3_finalize( vtab->insert_seq_stmt );
    sqlite3_free( vtab->database_name );
    sqlite3_free( vtab->table_name );
    sqlite3_free( vtab );

    return SQLITE_OK;
}

static int attributes_destroy( sqlite3_vtab *_vtab )
{
    char *sql;
    struct attribute_vtab *vtab;
    sqlite3 *db;
    char *database_name;
    char *table_name;
    int status;
    int return_status = SQLITE_OK;

    vtab          = (struct attribute_vtab *) _vtab;
    db            = vtab->db;
    database_name = vtab->database_name;
    table_name    = vtab->table_name;

    sql = sqlite3_mprintf( "DROP TABLE " SEQ_SCHEMA_NAME, database_name,
        table_name );

    if(! sql ) {
        return_status = SQLITE_NOMEM;
    } else {
        status = sqlite3_exec( db, sql, NULL, NULL, NULL );
        if(status != SQLITE_OK) {
            return_status = status;
        }

        sqlite3_free( sql );

        sql = sqlite3_mprintf( "DROP TABLE " ATTR_SCHEMA_NAME, database_name,
            table_name );

        if(! sql) {
            return_status = SQLITE_NOMEM;
        } else {
            status = sqlite3_exec( db, sql, NULL, NULL, NULL );
            if(status != SQLITE_OK) {
                return_status = status;
            }

            sqlite3_free( sql );
        }
    }

    status = attributes_disconnect( _vtab );
    if(status != SQLITE_OK) {
        return_status = status;
    }

    return return_status;
}

struct _insert_attribute_info {
    sqlite3_stmt *stmt;
    sqlite3_int64 rowid;
    int error_code;
};

static int _insert_attributes( const char *key, size_t key_len,
    const char *value, size_t value_len, void *udata )
{
    struct _insert_attribute_info *info = (struct _insert_attribute_info *) udata;

    sqlite3_bind_int64( info->stmt, INSERT_ATTR_SEQ_COL, info->rowid );
    sqlite3_bind_text(  info->stmt, INSERT_ATTR_KEY_COL, key,   key_len,   SQLITE_TRANSIENT );
    sqlite3_bind_text(  info->stmt, INSERT_ATTR_VAL_COL, value, value_len, SQLITE_TRANSIENT );

    info->error_code = sqlite3_step( info->stmt );
    sqlite3_reset( info->stmt );

    if(info->error_code == SQLITE_DONE) {
        return CONTINUE;
    } else {
        return BREAK;
    }
}

static int _perform_insert( struct attribute_vtab *vtab, int argc, sqlite3_value **argv, sqlite_int64 *rowid )
{
    int status;
    const char *attributes;
    const char *key_endp;
    struct _insert_attribute_info info;

    attributes = sqlite3_value_text( argv[UPDATE_ARG_ATTRS] );

    if(*rowid == 0) { /* we provide our own ROWID */
        sqlite3_bind_null( vtab->insert_seq_stmt, INSERT_SEQ_ID_COL );
    } else {
        sqlite3_bind_int64( vtab->insert_seq_stmt, INSERT_SEQ_ID_COL, *rowid );
    }

    /* XXX bind_value? */
    sqlite3_bind_text( vtab->insert_seq_stmt, INSERT_SEQ_ATTR_COL, attributes, -1, SQLITE_TRANSIENT );
    status = sqlite3_step( vtab->insert_seq_stmt );

    if(status != SQLITE_DONE) {
        sqlite3_reset( vtab->insert_seq_stmt );
        vtab->vtab.zErrMsg = sqlite3_mprintf( "%s", sqlite3_errmsg( vtab->db ) );
        return status;
    }
    *rowid = sqlite3_last_insert_rowid( vtab->db );
    sqlite3_reset( vtab->insert_seq_stmt );

    info.stmt       = vtab->insert_attr_stmt;
    info.rowid      = *rowid;
    info.error_code = SQLITE_OK;

    iterate_over_kv_pairs( attributes, _insert_attributes, &info );

    if(info.error_code == SQLITE_DONE) {
        return SQLITE_OK;
    }
    return info.error_code;
}

static int _perform_delete( struct attribute_vtab *vtab, sqlite3_int64 rowid )
{
    char *sql;
    sqlite3_stmt *stmt;
    int status;

    sql = sqlite3_mprintf( DELETE_SEQ_TMPL, vtab->database_name,
        vtab->table_name );

    if(! sql) {
        return SQLITE_NOMEM;
    }

    status = sqlite3_prepare_v2( vtab->db, sql, -1, &stmt, NULL );

    sqlite3_free( sql );

    if(status != SQLITE_OK) {
        vtab->vtab.zErrMsg = sqlite3_mprintf( "%s",
            sqlite3_errmsg( vtab->db ) );
        return status;
    }

    sqlite3_bind_int64( stmt, DELETE_SEQ_ARG_ROWID, rowid );

    status = sqlite3_step( stmt );

    sqlite3_finalize( stmt );

    if(status != SQLITE_DONE) {
        return status;
    }

    sql = sqlite3_mprintf( DELETE_ATTR_TMPL, vtab->database_name,
        vtab->table_name );

    if(! sql) {
        return SQLITE_NOMEM;
    }

    status = sqlite3_prepare_v2( vtab->db, sql, -1, &stmt, NULL );

    sqlite3_free( sql );

    if(status != SQLITE_OK) {
        vtab->vtab.zErrMsg = sqlite3_mprintf( "%s",
            sqlite3_errmsg( vtab->db ) );
        return status;
    }

    sqlite3_bind_int64( stmt, DELETE_ATTR_ARG_ROWID, rowid );

    status = sqlite3_step( stmt );

    sqlite3_finalize( stmt );

    if(status != SQLITE_DONE) {
        return status;
    }

    return SQLITE_OK;
}

static int attributes_update( sqlite3_vtab *_vtab, int argc, sqlite3_value **argv, sqlite_int64 *rowid )
{
    struct attribute_vtab *vtab = (struct attribute_vtab *) _vtab;

    if(argc == 1) { /* DELETE */
        return _perform_delete( vtab, sqlite3_value_int64( argv[0] ) );
    } else if(sqlite3_value_type(argv[0]) == SQLITE_NULL) { /* INSERT */
        int type_rowid;
        int type_id;

        type_rowid = sqlite3_value_type(argv[UPDATE_ARG_ROWID]);
        type_id    = sqlite3_value_type(argv[UPDATE_ARG_ID]);

        if(type_rowid == SQLITE_NULL && type_id == SQLITE_NULL) {
            *rowid = 0;
        } else if(type_rowid != SQLITE_NULL) {
            *rowid = sqlite3_value_int64( argv[UPDATE_ARG_ROWID] );
        } else { /* type_id != SQLITE_NULL */
            *rowid = sqlite3_value_int64( argv[UPDATE_ARG_ID] );
        }

        return _perform_insert( vtab, argc, argv, rowid );
    } else { /* UPDATE */
        int status;

        *rowid = sqlite3_value_int64( argv[0] );
        status = _perform_delete( vtab, *rowid );
        if(status != SQLITE_OK) {
            /* XXX rollback transaction? */
            return status;
        }
        return _perform_insert( vtab, argc, argv, rowid );
    }

    return SQLITE_ERROR;
}

static int attributes_best_index( sqlite3_vtab *_vtab, sqlite3_index_info *index_info )
{
    int i;

    for(i = 0; i < index_info->nConstraint; i++) {
        struct sqlite3_index_constraint *constraint = index_info->aConstraint + i;

        if(constraint->iColumn == SCHEMA_ATTR_COL && constraint->op == SQLITE_INDEX_CONSTRAINT_MATCH) {
            index_info->aConstraintUsage[i].argvIndex = 1;
            index_info->aConstraintUsage[i].omit      = 1 ;
            index_info->idxNum                        = ATTR_NAME_INDEX;
            index_info->estimatedCost                 = 1; /* dummy value for now */
            break;
        }
    }

    return SQLITE_OK;
}

static int attributes_rename( sqlite3_vtab *_vtab, const char *new_name )
{
    struct attribute_vtab *vtab = (struct attribute_vtab *) _vtab;
    return UNIMPLD(vtab);
}

static int attributes_open_cursor( sqlite3_vtab *_vtab, sqlite3_vtab_cursor **cursor )
{
    struct attribute_vtab *vtab = (struct attribute_vtab *) _vtab;

    *cursor = sqlite3_malloc( sizeof(struct attribute_cursor) );
    if(! cursor) {
        return SQLITE_NOMEM;
    }
    memset( *cursor, 0, sizeof(struct attribute_cursor) );

    return SQLITE_OK;
}

static int attributes_close_curor( sqlite3_vtab_cursor *_cursor )
{
    struct attribute_cursor *c = (struct attribute_cursor *) _cursor;

    sqlite3_finalize( c->stmt );
    sqlite3_free( c );

    return SQLITE_OK;
}

static int attributes_get_row( struct attribute_cursor *cursor )
{
    int status;

    if(cursor->eof) {
        return SQLITE_OK;
    }

    status = sqlite3_step( cursor->stmt );

    if(status == SQLITE_ROW) {
        return SQLITE_OK;
    }

    sqlite3_reset( cursor->stmt );
    cursor->eof = 1;

    return ( status == SQLITE_DONE ? SQLITE_OK : status );
}

static int attributes_filter( sqlite3_vtab_cursor *_cursor, int idx_num,
    const char *idx_name, int argc, sqlite3_value **argv )
{
    struct attribute_vtab *vtab = (struct attribute_vtab *) _cursor->pVtab;
    struct attribute_cursor *c  = (struct attribute_cursor *) _cursor;
    int status;
    char *sql;

    if(idx_num == ATTR_NAME_INDEX) {
        const char *match = sqlite3_value_text( argv[0] );

        if(strchr(match, RECORD_SEPARATOR)) {
            sql = sqlite3_mprintf( SELECT_CURS_WITH_KEY_VALUE_TMPL,
                vtab->database_name, vtab->table_name,
                vtab->database_name, vtab->table_name);
        } else {
            sql = sqlite3_mprintf( SELECT_CURS_WITH_KEY_TMPL,
                vtab->database_name, vtab->table_name,
                vtab->database_name, vtab->table_name);
        }
    } else {
        sql = sqlite3_mprintf( SELECT_CURS_TMPL, vtab->database_name, vtab->table_name );
    }

    if(! sql) {
        sqlite3_free( c );
        return SQLITE_NOMEM;
    }

    sqlite3_finalize( c->stmt );
    status = sqlite3_prepare_v2( vtab->db, sql, -1, &(c->stmt), NULL );
    sqlite3_free( sql );

    if(status != SQLITE_OK) {
        return status;
    }
    c->eof = 0;

    if(idx_num == ATTR_NAME_INDEX) {
        const char *match = sqlite3_value_text( argv[0] );

        if(strchr(match, RECORD_SEPARATOR)) {
            const char *key;
            const char *value;

            key   = match;
            value = strchr(match, RECORD_SEPARATOR);

            sqlite3_bind_text( c->stmt, 1, key, value - key, SQLITE_TRANSIENT );

            value++;

            sqlite3_bind_text( c->stmt, 2, value, -1, SQLITE_TRANSIENT );
        } else {
            /* XXX bind_value? */
            sqlite3_bind_text( c->stmt, 1, match, -1, SQLITE_TRANSIENT );
        }
    }

    return attributes_get_row( c );
}

static int attributes_next( sqlite3_vtab_cursor *_cursor )
{
    return attributes_get_row( (struct attribute_cursor *) _cursor );
}

static int attributes_eof( sqlite3_vtab_cursor *_cursor )
{
    struct attribute_cursor *c = (struct attribute_cursor *) _cursor;

    return c->eof;
}

static int attributes_row_id( sqlite3_vtab_cursor *_cursor, sqlite_int64 *rowid )
{
    struct attribute_cursor *c = (struct attribute_cursor *) _cursor;

    *rowid = sqlite3_column_int64( c->stmt, CURS_SEQ_COL );

    return SQLITE_OK;
}

static int attributes_column( sqlite3_vtab_cursor *_cursor, sqlite3_context *ctx,
    int col_index )
{
    struct attribute_cursor *cursor = (struct attribute_cursor *) _cursor;

    sqlite3_result_value( ctx, sqlite3_column_value( cursor->stmt, col_index ) );

    return SQLITE_OK;
}

static void _attribute_match_func(sqlite3_context *ctx, int nargs, sqlite3_value **values)
{
    const char *query;
    const char *attributes;
    const char *rs_location;
    const char *attr_value;
    size_t value_len;

    query      = sqlite3_value_text(values[0]);
    attributes = sqlite3_value_text(values[1]);

    if(rs_location = strchr(query, RECORD_SEPARATOR)) { /* searching for a key value pair */
        char *key = sqlite3_malloc( rs_location - query + 1 ); /* one for the NULL */
        strncpy( key, query, rs_location - query );

        attr_value = extract_attribute_value( attributes, key, &value_len );

        sqlite3_free( key );

        sqlite3_result_int( ctx, !strncmp( rs_location + 1, attr_value, value_len ) );
    } else { /* searching for whether or not a key is present */
        attr_value = extract_attribute_value( attributes, query, &value_len );

        sqlite3_result_int( ctx, attr_value != NULL );
    }
}

static int attributes_find_function(sqlite3_vtab *_vtab, int nArg,
    const char *zName, void (**pxFunc)(sqlite3_context *, int, sqlite3_value **),
    void **ppArg)
{
    struct attribute_vtab *vtab = (struct attribute_vtab *) _vtab;

    if(strcmp(zName, "match")) {
        *pxFunc = NULL;
        return 0;
    }

    *pxFunc = _attribute_match_func;
    *ppArg  = NULL;

    return 1;
}

static sqlite3_module module_definition = {
    .iVersion      = MODULE_VERSION,
    .xCreate       = attributes_create,
    .xConnect      = attributes_connect,
    .xDisconnect   = attributes_disconnect,
    .xDestroy      = attributes_destroy,
    .xUpdate       = attributes_update,
    .xBestIndex    = attributes_best_index,
    .xRename       = attributes_rename,
    .xOpen         = attributes_open_cursor,
    .xClose        = attributes_close_curor,
    .xFilter       = attributes_filter,
    .xNext         = attributes_next,
    .xEof          = attributes_eof,
    .xRowid        = attributes_row_id,
    .xColumn       = attributes_column,
    .xFindFunction = attributes_find_function
};

int sql_attr_init( sqlite3 *db, char **error,
    const sqlite3_api_routines *api )
{
    SQLITE_EXTENSION_INIT2(api);

    sqlite3_create_function( db, "get_attr", 2, SQLITE_UTF8, NULL,
        sql_get_attr, NULL, NULL );

    sqlite3_create_module( db, MODULE_NAME, &module_definition, NULL );

    return SQLITE_OK;
}
