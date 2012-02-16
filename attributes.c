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
    "  id         INTEGER NOT NULL,"\
    "  attributes TEXT    NOT NULL"\
    ")"

#define SEQ_SCHEMA_NAME  "\"%w\".\"%w_Sequence\""
#define ATTR_SCHEMA_NAME "\"%w\".\"%w_Attributes\""

#define SEQ_SCHEMA_TMPL\
    "CREATE TABLE " SEQ_SCHEMA_NAME " ("\
    "  seq_id INTEGER NOT NULL PRIMARY KEY "\
    ")"

#define ATTR_SCHEMA_TMPL\
    "CREATE TABLE " ATTR_SCHEMA_NAME " ("\
    "  seq_id     INTEGER NOT NULL REFERENCES \"%w_Sequence\" (seq_id) ON DELETE CASCADE, "\
    "  attr_name  TEXT    NOT NULL, "\
    "  attr_value TEXT    NOT NULL "\
    ")"

#define INSERT_SEQ_TMPL\
    "INSERT INTO " SEQ_SCHEMA_NAME " DEFAULT VALUES"

#define INSERT_ATTR_TMPL\
    "INSERT INTO " ATTR_SCHEMA_NAME " VALUES ( ?, ?, ? )"

#define SELECT_CURS_TMPL\
    "SELECT seq_id, group_concat(attr_name || '" RECORD_SEPARATOR_STR\
    "' || attr_value, '" RECORD_SEPARATOR_STR "') FROM " ATTR_SCHEMA_NAME\
    " GROUP BY seq_id"

#define SCHEMA_PREFIX_SIZE            (sizeof(SCHEMA_PREFIX) - 1)
#define SCHEMA_SUFFIX_SIZE            (sizeof(SCHEMA_SUFFIX) - 1)
#define DEFAULT_ATTRIBUTE_COLUMN_SIZE (sizeof(DEFAULT_ATTRIBUTE_COLUMN) - 1)

#define ATTR_SEQ_COL 1
#define ATTR_KEY_COL 2
#define ATTR_VAL_COL 3

#define CURS_SEQ_COL 0

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

static int __unimplemented(struct attribute_vtab *vtab, const char *func_name)
{
    vtab->vtab.zErrMsg = sqlite3_mprintf("function '%s' is not yet implemented", func_name);
    return SQLITE_ERROR;
}

static void sql_has_attr( sqlite3_context *ctx, int nargs,
    sqlite3_value **values )
{
}

static void sql_get_attr( sqlite3_context *ctx, int nargs,
    sqlite3_value **values )
{
    const char *attributes;
    const char *attr_name;
    char *needle;
    const char *attr_location = NULL;
    size_t name_len;

    attributes = sqlite3_value_text( values[0] );
    attr_name  = sqlite3_value_text( values[1] );

    name_len = strlen( attr_name );
    needle   = sqlite3_malloc( name_len + 2 ); /* one for NULL, one for
                                                record separator */

    needle[0] = RECORD_SEPARATOR;
    strcpy(needle + 1, attr_name);
    needle[name_len + 1] = RECORD_SEPARATOR;
    needle[name_len + 2] = '\0';

    if(! strncmp( attributes, needle + 1, name_len + 1 )) {
        attr_location = attributes + name_len + 1;
    }

    if(! attr_location) {
        attr_location = strstr( attributes, needle );
        if(attr_location) {
            attr_location += name_len + 2;
        }
    }

    if(attr_location) {
        const char *attr_end;

        attr_end = strchr( attr_location, RECORD_SEPARATOR );
        if(! attr_end) {
            attr_end = attr_location + strlen( attr_location );
        }
        sqlite3_result_text( ctx, attr_location, attr_end - attr_location, SQLITE_TRANSIENT );
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

    sql = NULL;

    /* XXX index attributes! */

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

static int _perform_insert( struct attribute_vtab *vtab, int argc, sqlite3_value **argv, sqlite_int64 *rowid )
{
    int status;
    const char *attributes;
    const char *key_endp;

    status = sqlite3_step( vtab->insert_seq_stmt );

    if(status != SQLITE_DONE) {
        sqlite3_reset( vtab->insert_seq_stmt );
        return SQLITE_ERROR;
    }
    *rowid = sqlite3_last_insert_rowid( vtab->db );
    sqlite3_reset( vtab->insert_seq_stmt );

    attributes = sqlite3_value_text( argv[3] );
    while(key_endp = strchr( attributes, RECORD_SEPARATOR )) {
        const char *key;
        const char *value;
        const char *value_endp;
        size_t key_length;
        size_t value_length;

        value_endp = strchr( key_endp + 1, RECORD_SEPARATOR );

        if(! value_endp) {
            value_endp = key_endp + strlen(key_endp);
        }

        key          = attributes;
        key_length   = key_endp - key;
        value        = key_endp + 1;
        value_length = value_endp - value;

        sqlite3_bind_int64( vtab->insert_attr_stmt, ATTR_SEQ_COL, *rowid );
        sqlite3_bind_text(  vtab->insert_attr_stmt, ATTR_KEY_COL, key,   key_length,   SQLITE_TRANSIENT );
        sqlite3_bind_text(  vtab->insert_attr_stmt, ATTR_VAL_COL, value, value_length, SQLITE_TRANSIENT );

        status = sqlite3_step( vtab->insert_attr_stmt );
        sqlite3_reset( vtab->insert_attr_stmt );
        if(status != SQLITE_DONE) {
            return status;
        }

        if(*value_endp == RECORD_SEPARATOR) {
            attributes = value_endp + 1;
        } else {
            break;
        }
    }

    return SQLITE_OK;
}

static int attributes_update( sqlite3_vtab *_vtab, int argc, sqlite3_value **argv, sqlite_int64 *rowid )
{
    if(argc == 1) { /* DELETE */
        _vtab->zErrMsg = sqlite3_mprintf( "%s", "deleting from the table is forbidden" );
    } else if(sqlite3_value_type(argv[0]) == SQLITE_NULL) { /* INSERT */
        if(sqlite3_value_type(argv[1]) == SQLITE_NULL) {
            return _perform_insert( (struct attribute_vtab *) _vtab, argc, argv, rowid );
        } else {
            _vtab->zErrMsg = sqlite3_mprintf( "%s", "providing your own ROWID is forbidden" );
        }
    } else { /* UPDATE */
        _vtab->zErrMsg = sqlite3_mprintf( "%s", "updating the table is forbidden" );
    }

    return SQLITE_ERROR;
}

static int attributes_best_index( sqlite3_vtab *_vtab, sqlite3_index_info *index_info )
{
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
    struct attribute_cursor *c  = NULL;
    int status;
    char *sql;

    *cursor = NULL;

    c = sqlite3_malloc( sizeof(struct attribute_cursor) );
    if(! c) {
        return SQLITE_NOMEM;
    }

    sql = sqlite3_mprintf( SELECT_CURS_TMPL, vtab->database_name, vtab->table_name );

    if(! sql) {
        sqlite3_free( c );
        return SQLITE_NOMEM;
    }

    status = sqlite3_prepare_v2( vtab->db, sql, -1, &(c->stmt), NULL );
    sqlite3_free( sql );

    if(status != SQLITE_OK) {
        sqlite3_free( c );
        return status;
    }

    *cursor = (sqlite3_vtab_cursor *) c;
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

    status = sqlite3_reset( c->stmt );

    if(status != SQLITE_OK) {
        return status;
    }
    c->eof = 0;

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
    int found;

    query      = sqlite3_value_text(values[0]);
    attributes = sqlite3_value_text(values[1]);

    if(strchr(query, RECORD_SEPARATOR)) { /* searching for a key value pair */
        size_t query_len;
        char *needle;

        /* If there is only a single attribute, and it matches our
         * query, return with success */
        if(! strcmp(attributes, query)) {
            sqlite3_result_int(ctx, 1);
            return;
        }

        query_len = strlen(query);
        needle    = sqlite3_malloc(query_len + 3); /* one for the NULL
                                                      one for each
                                                      record separator
                                                   */
        needle[0] = RECORD_SEPARATOR;
        strcpy(needle + 1, query);
        needle[query_len + 1] = RECORD_SEPARATOR;
        needle[query_len + 2] = '\0';

        /* check at the beginning of the string */
        found = !strncmp(attributes, needle + 1, query_len + 1);

        /* check at the end of the string */
        if(! found) {
            needle[query_len + 1] = '\0';
            found = !strcmp(attributes + strlen(attributes) - query_len - 1,
                needle);
            needle[query_len + 1] = RECORD_SEPARATOR;
        }

        if(! found) {
            found = strstr(attributes, needle) != NULL;
        }

        sqlite3_free(needle);

        sqlite3_result_int(ctx, found);
    } else { /* searching for whether or not a key is present */
        size_t query_len;
        char *needle;

        query_len = strlen(query);
        needle    = sqlite3_malloc(query_len + 3); /* one for the NULL
                                                      one for each
                                                      record seperator
                                                   */
        needle[0] = RECORD_SEPARATOR;
        strcpy(needle + 1, query);
        needle[query_len + 1] = RECORD_SEPARATOR;
        needle[query_len + 2] = '\0';

        found = !strncmp(attributes, needle + 1, query_len + 1);

        if(! found) {
            found = strstr(attributes, needle) != NULL;
        }

        sqlite3_free(needle);

        sqlite3_result_int(ctx, found);
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

    sqlite3_create_function( db, "has_attr", 2, SQLITE_UTF8, NULL,
        sql_has_attr, NULL, NULL );

    sqlite3_create_function( db, "get_attr", 2, SQLITE_UTF8, NULL,
        sql_get_attr, NULL, NULL );

    sqlite3_create_module( db, MODULE_NAME, &module_definition, NULL );

    return SQLITE_OK;
}
