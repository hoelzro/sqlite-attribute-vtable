#include "sqlite3ext.h"

SQLITE_EXTENSION_INIT1;

#include <stdlib.h>
#include <string.h>

#define MODULE_NAME "attributes"
#define MODULE_VERSION 1

#define SCHEMA_PREFIX            "CREATE TABLE t ("
#define SCHEMA_SUFFIX            ")"
#define DEFAULT_ATTRIBUTE_COLUMN "attributes TEXT"

#define SCHEMA_PREFIX_SIZE            (sizeof(SCHEMA_PREFIX) - 1)
#define SCHEMA_SUFFIX_SIZE            (sizeof(SCHEMA_SUFFIX) - 1)
#define DEFAULT_ATTRIBUTE_COLUMN_SIZE (sizeof(DEFAULT_ATTRIBUTE_COLUMN) - 1)

#include <stdio.h>

#define diag(fmt, args...)\
    fprintf(stderr, "# " fmt "\n", ##args);

struct attribute_vtab {
    sqlite3_vtab vtab;
    sqlite3 *db;
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

static int attributes_create( sqlite3 *db, void *udp, int argc,
    char const * const *argv, sqlite3_vtab **vtab, char **errMsg )
{
    char *schema;
    struct attribute_vtab *avtab;

    *vtab   = NULL;
    *errMsg = NULL;

    avtab = (struct attribute_vtab *) sqlite3_malloc(sizeof(struct attribute_vtab));
    if(! avtab) {
        return SQLITE_NOMEM;
    }
    avtab->db = db;

    schema = _build_schema(argc - 3, argv + 3);
    if(! schema) {
        sqlite3_free(avtab);
        return SQLITE_NOMEM;
    }

    // XXX error checking?
    sqlite3_declare_vtab( db, schema );
    sqlite3_free(schema);

    *vtab = (sqlite3_vtab *) avtab;

    return SQLITE_OK;
}

static int attributes_destroy( sqlite3_vtab *_vtab )
{
    sqlite3_free(_vtab);
    return SQLITE_OK;
}

static sqlite3_module module_definition = {
    .iVersion    = MODULE_VERSION,
    .xCreate     = attributes_create,
    .xConnect    = attributes_create,
    .xDisconnect = attributes_destroy,
    .xDestroy    = attributes_destroy
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
