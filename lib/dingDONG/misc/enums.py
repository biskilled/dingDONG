
class eConn (object):
    class props (object):
        VERSION     = 'versionManager'
        DEFAULTS    = 'defaults'
        DATA_TYPES  = 'dataTypes'
        TYPE        = 'connType'
        NAME        = 'connName'
        UPDATE      = 'connUpdateMethod'
        IS_SOURCE   = 'connIsSrc'
        IS_TARGET   = 'connIsTar'
        IS_SQL      = 'connIsSql'
        FOLDER      = 'connFolder'
        SQL_FILE    = 'connFile'
        URL         = 'connUrl'
        TBL         = 'connTbl'
        FILTER      = 'connFilter'

        DB_NAME         = 'dbName'
        DB_PYODBC       = 'py'
        DB_INDEX_COLUMS = 'c'
        DB_INDEX_CLUSTER= 'ic'
        DB_INDEX_UNIQUE = 'iu'

        eDict = {
            NAME:       [NAME, 'name'],
            TYPE:       [TYPE, 'type'],
            TBL:        [TBL, 'tableName','table','tables'],
            FILTER:     [FILTER, 'filter'],
            URL:        [URL,'url'],
            FOLDER:     [FOLDER, 'files', 'folder'],
            SQL_FILE:   [SQL_FILE, 'file'],
            UPDATE:     [UPDATE,'change'],
            DB_NAME:    [DB_NAME, 'dbname'],
        }

    class types (object):
        SQLSERVER = "sql"
        ORACLE = "oracle"
        VERTICA = "vertica"
        ACCESS = "access"
        MYSQL = "mysql"
        LITE = "sqlite"
        MONGO = 'mongo'
        FILE = "file"
        FOLDER='folder'
        POSTGESQL = 'postgresql'
        SALESFORCE = 'sf'
        NONO = 'nono'

    class defaults (object):
        DEFAULT_TYPE = 'dt'
        BATCH_SIZE = 'bs'
        COLUMN_TYPE = 't'
        TABLE_SCHEMA = 's'
        COLUMNS_NULL = 'e'
        COLUMN_FRAME = 'cf'
        SP = 'sp'
        UPDATABLE = 'up'

        FILE_MIN_SIZE = 'min'
        FILE_DEF_COLUMN_PREF = 'defCol'
        FILE_ENCODING = 'encoding'
        FILE_DELIMITER = 'delimiter'
        FILE_ROW_HEADER = 'headerline'
        FILE_END_OF_LINE = "eol"
        FILE_MAX_LINES_PARSE = 'maxlines'
        FILE_LOAD_WITH_CHAR_ERR = 'charerror'
        FILE_APPEND = 'append'
        FILE_REPLACE_TO_NONE = 'replace'
        FILE_CSV = 'csv'

    class connString (object):
        URL_USER = 'user'
        URL_PASS = 'pass'
        URL_DSN = 'dsn'
        URL_NLS = 'nls'
        URL_SF = 'url'
        URL_HOST = 'host'
        URL_DB   = 'db'

    class dataTypes (object):
        B_STR       = 'str'
        B_INT       = 'int'
        B_FLOAT     = 'float'
        B_LONG_STR  = 'text'
        B_BLOB      = 'blob'
        B_DEFAULT   = 'default'

        DB_VARCHAR      = 'varchar'
        DB_VARCHAR_MAX  = 'varchar(MAX)'
        DB_NVARCHAR     = 'nvarchar'
        DB_CHAR         = 'char'
        DB_BLOB         = 'blob'
        DB_CLOB         = 'clob'
        DB_INT          = 'int'
        DB_BIGINT       = 'bigint'
        DB_FLOAT        = 'float'
        DB_NUMERIC      = 'numeric'
        DB_DECIMAL      = 'decimal'
        DB_DATE         = 'datetime'

    class updateMethod(object):
        DROP        = 1
        UPDATE      = 2
        NO_UPDATE   = 3

class eSql (object):
    RENAME          = 'rename'
    DROP            = 'drop'
    TRUNCATE        = 'truncate'
    STRUCTURE       = 'structure'
    MERGE           = 'merge'
    ISEXISTS        = 'isexists'
    DELETE          = 'delete'
    INDEX           = 'index'
    INDEX_EXISTS    = 'indexexists'
    COLUMN_UPDATE   = 'columnupdate'
    COLUMN_DELETE   = 'columndelete'
    COLUMN_ADD      = 'columnadd'
    CREATE_FROM     = 'createfrom'
    ALL_TABLES      = 'all'

    TABLE_COPY_BY_COLUMN = 'copy'

class eJson (object):
    TARGET      = 't'
    SOURCE      = 's'
    MERGE       = 'm'
    QUERY       = 'q'
    SEQUENCE    = 'seq'
    STT         = 'sttappend'
    STTONLY     = 'stt'
    MAP         = 'map'
    COLUMNS     = 'col'
    INDEX       = 'index'
    PARTITION   = 'par'           # not inplemented
    INC         = 'inc'           # not implemented
    NONO        = 'internal',
    CREATE      = 'create'

    eDict = {
        TARGET: [TARGET, 'target'],  # Target
        SOURCE: [SOURCE, 'source'],  # Source
        QUERY: [QUERY, 'query'],
        MERGE: [MERGE, 'merge'],
        SEQUENCE: [SEQUENCE, 'sequence'],
        STT: [STT, 'sourcetotarget'],
        STTONLY: [STTONLY, 'only'],
        MAP: [MAP, 'mapping'],
        COLUMNS: [COLUMNS, 'columns', 'column'],
        INDEX: [INDEX, 'i'],
        PARTITION: [PARTITION, 'partition'],
        INC: [INC, 'incremental'],
        CREATE:[CREATE,'c']
    }

    class stt(object):
        SOURCE  = 's'
        TYPE    = 't'
        ALIACE  = 'a'
        FUNCTION= 'f'
        EXECFUNC= 'e'
        INDEX   = 'i'

        DIC     = {SOURCE: None, TYPE: None, ALIACE:None}

    class merge(object):
        SOURCE  = 'merge_source'
        TARGET  = 'merge_target'
        MERGE   = 'merge_keys'
        DIC     = {SOURCE:None, TARGET:None, MERGE:None}

    class index(object):
        COLUMNS = 'c'
        CLUSTER = 'ic'
        UNIQUE  = 'iu'

        eDict = {
            COLUMNS: [COLUMNS, 'col'],
            CLUSTER: [CLUSTER, 'cluster'],
            UNIQUE:  [UNIQUE, 'unique']
        }

class eObj (object):
    DB_TBL_SCHEMA   = 'schema'
    DB_QUERY        = 'query'

    FILE_FOLDER     = 'folder'
    FILE_FULL_PATH  = 'fullFileName'