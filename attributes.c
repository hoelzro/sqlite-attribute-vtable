#include "sqlite3ext.h"

SQLITE_EXTENSION_INIT1;

#include <stdlib.h>

#define MODULE_NAME "attributes"
#define MODULE_VERSION 1

static void sql_has_attr( sqlite3_context *ctx, int nargs,
    sqlite3_value **values )
{
}

static void sql_get_attr( sqlite3_context *ctx, int nargs,
    sqlite3_value **values )
{
}

static int attributes_create( sqlite3 *db, void *udp, int argc,
    char const * const *argv, sqlite3_vtab **vtab, char **errMsg )
{
    return SQLITE_OK;
}

static int attributes_destroy( sqlite3_vtab *_vtab )
{
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
