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

#define SCHEMA_PREFIX            "CREATE TABLE t ("
#define SCHEMA_SUFFIX            ")"
#define DEFAULT_ATTRIBUTE_COLUMN "attributes TEXT"

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

#define SCHEMA_PREFIX_SIZE            (sizeof(SCHEMA_PREFIX) - 1)
#define SCHEMA_SUFFIX_SIZE            (sizeof(SCHEMA_SUFFIX) - 1)
#define DEFAULT_ATTRIBUTE_COLUMN_SIZE (sizeof(DEFAULT_ATTRIBUTE_COLUMN) - 1)

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

static void sql_has_attr( sqlite3_context *ctx, int nargs,
    sqlite3_value **values )
{
}

static void sql_get_attr( sqlite3_context *ctx, int nargs,
    sqlite3_value **values )
{
}

static int _calculate_column_length( const char *arg,
    int *has_seen_attributes_p )
{
    char *p;

    if(p = strstr(arg, "ATTRIBUTES")) {
        *has_seen_attributes_p = 1;
        return p - arg + sizeof("TEXT");
    } else {
        return strlen(arg);
    }
}

static char *_allocate_schema_buffer( int argc, const char * const *argv,
    size_t *buffer_length )
{
    int i;
    size_t length = SCHEMA_PREFIX_SIZE + SCHEMA_SUFFIX_SIZE;
    int has_seen_attributes = 0;

    for(i = 0; i < argc; i++) {
        length += _calculate_column_length( argv[i], &has_seen_attributes );
        if(i != argc - 1) {
            /* if it's not the last column definition, add another
             * byte for the seperating comma */
            length++;
        }
    }

    if(! has_seen_attributes) {
        length += DEFAULT_ATTRIBUTE_COLUMN_SIZE;
        if(argc) {
            length++; /* add another comma byte for the last column in our
                       * previous loop that we didn't count */
        }
    }

    length++; /* for the NULL */

    *buffer_length = length;

    return sqlite3_malloc(length);
}

static char *_build_schema( int argc, const char * const *argv )
{
    int i;
    size_t buffer_length;
    int has_seen_attributes = 0;
    char *buffer = _allocate_schema_buffer( argc, argv, &buffer_length );

    strncpy( buffer, SCHEMA_PREFIX, buffer_length );

    // keywords - KEY, ATTRIBUTES

    for(i = 0; i < argc; i++) {
        char *p;

        if(p = strstr( argv[i], "ATTRIBUTES" )) {
            char *dest = buffer + strlen( buffer );
            // XXX bounds checking?
            memcpy( dest, argv[i], p - argv[i] );
            dest += p - argv[i];
            memcpy( dest, "TEXT", sizeof("TEXT") ); /* this includes the NULL
                                                   * for us */
            has_seen_attributes = 1;
        } else {
            // XXX check error
            strncat( buffer, argv[i], buffer_length );
        }
        if(i != argc - 1) {
            strncat( buffer, ",", buffer_length );
        }
    }

    if(! has_seen_attributes) {
        if(argc) {
            strncat( buffer, ",", buffer_length );
        }
        strncat( buffer, "attributes TEXT", buffer_length );
    }

    strncat( buffer, SCHEMA_SUFFIX, buffer_length );

    return buffer;
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

static sqlite3_module module_definition = {
    .iVersion    = MODULE_VERSION,
    .xCreate     = attributes_create,
    .xConnect    = attributes_connect,
    .xDisconnect = attributes_disconnect,
    .xDestroy    = attributes_destroy,
    .xUpdate     = attributes_update
};

int sql_attr_init( sqlite3 *db, char **error,
    const sqlite3_api_routines *api )
{
    SQLITE_EXTENSION_INIT2(api);

    sqlite3_create_function( db, "has_attr", 1, SQLITE_UTF8, NULL,
        sql_has_attr, NULL, NULL );

    sqlite3_create_function( db, "get_attr", 1, SQLITE_UTF8, NULL,
        sql_get_attr, NULL, NULL );

    sqlite3_create_module( db, MODULE_NAME, &module_definition, NULL );

    return SQLITE_OK;
}
