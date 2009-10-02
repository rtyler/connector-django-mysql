from base import DatabaseOperations
from mysql.connector.errors import ProgrammingError, OperationalError
from mysql.connector.constants import FieldType
import re

quote_name = DatabaseOperations().quote_name
foreign_key_re = re.compile(r"\sCONSTRAINT `[^`]*` FOREIGN KEY \(`([^`]*)`\) REFERENCES `([^`]*)` \(`([^`]*)`\)")

def get_table_list(cursor):
    "Returns a list of table names in the current database."
    cursor.execute("SHOW TABLES")
    return [row[0] for row in cursor.fetchall()]

def get_table_description(cursor, table_name):
    "Returns a description of the table, with the DB-API cursor.description interface."
    cursor.execute("SELECT * FROM %s LIMIT 1" % quote_name(table_name))
    return cursor.description

def _name_to_index(cursor, table_name):
    """
    Returns a dictionary of {field_name: field_index} for the given table.
    Indexes are 0-based.
    """
    return dict([(d[0], i) for i, d in enumerate(get_table_description(cursor, table_name))])

def get_relations(cursor, table_name):
    """
    Returns a dictionary of {field_index: (field_index_other_table, other_table)}
    representing all relationships to the given table. Indexes are 0-based.
    """
    my_field_dict = _name_to_index(cursor, table_name)
    constraints = []
    relations = {}
    try:
        # This should work for MySQL 5.0.
        cursor.execute("""
            SELECT column_name, referenced_table_name, referenced_column_name
            FROM information_schema.key_column_usage
            WHERE table_name = %s
                AND table_schema = DATABASE()
                AND referenced_table_name IS NOT NULL
                AND referenced_column_name IS NOT NULL""", [table_name])
        constraints.extend(cursor.fetchall())
    except (ProgrammingError, OperationalError):
        # Fall back to "SHOW CREATE TABLE", for previous MySQL versions.
        # Go through all constraints and save the equal matches.
        cursor.execute("SHOW CREATE TABLE %s" % quote_name(table_name))
        for row in cursor.fetchall():
            pos = 0
            while True:
                match = foreign_key_re.search(row[1], pos)
                if match == None:
                    break
                pos = match.end()
                constraints.append(match.groups())

    for my_fieldname, other_table, other_field in constraints:
        other_field_index = _name_to_index(cursor, other_table)[other_field]
        my_field_index = my_field_dict[my_fieldname]
        relations[my_field_index] = (other_field_index, other_table)

    return relations

def get_indexes(cursor, table_name):
    """
    Returns a dictionary of fieldname -> infodict for the given table,
    where each infodict is in the format:
        {'primary_key': boolean representing whether it's the primary key,
         'unique': boolean representing whether it's a unique index}
    """
    cursor.execute("SHOW INDEX FROM %s" % quote_name(table_name))
    indexes = {}
    for row in cursor.fetchall():
        indexes[row[4]] = {'primary_key': (row[2] == 'PRIMARY'), 'unique': not bool(row[1])}
    return indexes

DATA_TYPES_REVERSE = {
    FieldType.BLOB: 'TextField',
    FieldType.STRING: 'CharField',
    FieldType.DECIMAL: 'DecimalField',
    FieldType.DATE: 'DateField',
    FieldType.DATETIME: 'DateTimeField',
    FieldType.DOUBLE: 'FloatField',
    FieldType.FLOAT: 'FloatField',
    FieldType.INT24: 'IntegerField',
    FieldType.LONG: 'IntegerField',
    FieldType.LONGLONG: 'IntegerField',
    FieldType.SHORT: 'IntegerField',
    FieldType.STRING: 'CharField',
    FieldType.TIMESTAMP: 'DateTimeField',
    FieldType.TINY: 'IntegerField',
    FieldType.TINY_BLOB: 'TextField',
    FieldType.MEDIUM_BLOB: 'TextField',
    FieldType.LONG_BLOB: 'TextField',
    FieldType.VAR_STRING: 'CharField',
}
