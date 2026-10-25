hbase shell

#CREATE
create 'employees', 'info', 'work'
list
describe 'employees'

#Read
# Read an entire single row
get 'employees', '101'

# Read a specific column family or column for a row
get 'employees', '101', {COLUMN => 'info'}
get 'employees', '101', {COLUMN => 'info:name'}

# Scan the entire table
scan 'employees'

# Scan with row key range (start row inclusive, stop row exclusive)
scan 'employees', {STARTROW => '101', STOPROW => '105'}

# Count rows in a table
count 'employees'

#UPDATE
# Syntax: put 'table_name', 'row_key', 'family:column', 'value'

# Insert row with key '101'
put 'employees', '101', 'info:name', 'raghu'
put 'employees', '101', 'info:age', '21'
put 'employees', '101', 'work:role', 'sde'
put 'employees', '101', 'work:salary', '88000'

# Update (re-running put on the same row_key & column overwrites/versions it)
put 'employees', '101', 'work:salary', '96800'

#DELETE
# Delete a specific cell/column value
delete 'employees', '101', 'work:salary'

# Delete an ENTIRE row (all column families & columns)
deleteall 'employees', '101'