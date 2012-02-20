# SQLite Attributes Extension

This extension provides a virtual table implementation that allows you to
create tables that store arbitrary key-value pairs in a row.  This comes
in handy if you have data that have relational properties, but can also
have associated properties that don't cleanly fall into a static schema.

# Overview

    -- creation
    CREATE VIRTUAL TABLE my_attributes USING attributes;

    -- select all rows from attributes table
    SELECT id, attributes FROM my_attributes;

    -- select row ID and width attribute.
    SELECT id, get_attr(attributes, 'color') FROM my_attributes;

    -- select all widgets with a color of 'blue'
    SELECT w.name FROM widgets AS w
    INNER JOIN my_attributes AS a
    ON w.aid = a.id
    WHERE a.attributes MATCH 'color\037fblue'

# Usage

Each instance of the attributes virtual table has two columns: **id**, which
is an integer and the table's primary key, and **attributes**, which contains
the key-value pairs for the given row.  The keys and values are separated by
the unit separator character (ASCII 0x1f).

You retrieve individual attributes from the attributes string using the
**get_attr** function, which takes an attribute string and a key; the function
returns the associated value if found, or NULL if not.  However, you should
**not** use get\_attr in a WHERE clause; this will result in a table scan,
which could be fairly slow.  Instead, this extension provides a custom
MATCH implementation that consults an index, providing quick lookups.

Duplicate attributes are not allowed on an individual row, and will result
in a constraint violation.

# Ideas for future improvement

This extension was created to scratch a particular itch, and I realize that
there's plenty of room for improvment.  Here's a list of ideas that I've had:

  * Implement transactional support
  * Implement table renaming
  * A different separator character for key-value pairs
  * Add the ability to have additional columns other than just id and attributes
  * Add the ability to have application-specific separators (ex. using '=' instead of 0x1f)
  * Improve the test suite to check memory safety using Valgrind
  * Add the ability to allow duplicate attributes (by using the most recent value)
  * Add a more advanced query language for attributes (right now you're restricted to checking equality)
