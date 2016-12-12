// ============================================================================
//
// Copyright (C) 2006-2016 Talend Inc. - www.talend.com
//
// This source code is available under agreement available at
// %InstallDIR%\features\org.talend.rcp.branding.%PRODUCTNAME%\%PRODUCTNAME%license.txt
//
// You should have received a copy of the agreement
// along with this program; if not, write to Talend SA
// 9 rue Pages 92150 Suresnes, France
//
// ============================================================================
package routines.system;

import java.util.HashMap;
import java.util.Map;

public class DBManagerFactory {

    private static Map<String, DBManager> managerMap = new HashMap<String, DBManager>();

    public static DBManager getDBManager(String dbmsId) {
        DBManager manager = managerMap.get(dbmsId);

        if (manager == null) {
            DBMSConstants curDBMS = null;
            for (DBMSConstants dbms : DBMSConstants.values()) {
                if (dbmsId.equalsIgnoreCase(dbms.getDBmsId())) {
                    curDBMS = dbms;
                    break;
                }
            }
            if (curDBMS == DBMSConstants.AS400) {
                manager = new AS400Manager(curDBMS.getDBmsId());
            } else if (curDBMS == DBMSConstants.ACCESS) {
                manager = new AccessManager(curDBMS.getDBmsId());
            } else if (curDBMS == DBMSConstants.DB2) {
                manager = new DB2Manager(curDBMS.getDBmsId());
            } else if (curDBMS == DBMSConstants.FIREBIRD) {
                manager = new FirebirdManager(curDBMS.getDBmsId());
            } else if (curDBMS == DBMSConstants.HSQLDB) {
                manager = new HSQLDBManager(curDBMS.getDBmsId());
            } else if (curDBMS == DBMSConstants.INFORMIX) {
                manager = new InformixManager(curDBMS.getDBmsId());
            } else if (curDBMS == DBMSConstants.INGRES) {
                manager = new IngresManager(curDBMS.getDBmsId());
            } else if (curDBMS == DBMSConstants.VECTORWISE) {
                manager = new VectorWiseManager(curDBMS.getDBmsId());
            } else if (curDBMS == DBMSConstants.INTERBASE) {
                manager = new InterbaseManager(curDBMS.getDBmsId());
            } else if (curDBMS == DBMSConstants.JAVADB) {
                manager = new JavaDBManager(curDBMS.getDBmsId());
            } else if (curDBMS == DBMSConstants.MAXDB) {
                manager = new MaxDBManager(curDBMS.getDBmsId());
            } else if (curDBMS == DBMSConstants.MSSQL) {
                manager = new MSSQLManager(curDBMS.getDBmsId());
            } else if (curDBMS == DBMSConstants.MYSQL) {
                manager = new MysqlManager(curDBMS.getDBmsId());
            } else if (curDBMS == DBMSConstants.NETEZZA) {
                manager = new NetezzaManager(curDBMS.getDBmsId());
            } else if (curDBMS == DBMSConstants.ORACLE) {
                manager = new OracleManager(curDBMS.getDBmsId());
            } else if (curDBMS == DBMSConstants.POSTGREPLUS) {
                manager = new PostgrePlusManager(curDBMS.getDBmsId());
            } else if (curDBMS == DBMSConstants.POSTGRESQL) {
                manager = new PostgreManager(curDBMS.getDBmsId());
            } else if (curDBMS == DBMSConstants.SQLITE) {
                manager = new SQLiteManager(curDBMS.getDBmsId());
            } else if (curDBMS == DBMSConstants.SYBASE) {
                manager = new SybaseManager(curDBMS.getDBmsId());
            } else if (curDBMS == DBMSConstants.TERADATA) {
                manager = new TeradataManager(curDBMS.getDBmsId());
            } else if (curDBMS == DBMSConstants.VERTICA) {
                manager = new VerticaManager(curDBMS.getDBmsId());
            } else if (curDBMS == DBMSConstants.SAPHANA) {
                manager = new SAPHanaManager(curDBMS.getDBmsId());
            } else if (curDBMS == DBMSConstants.ODBC) {
                manager = new ODBCManager(curDBMS.getDBmsId());
            } else if (curDBMS == DBMSConstants.REDSHIFT) {
                manager = new RedshiftManager(curDBMS.getDBmsId());
            }
        }
        managerMap.put(dbmsId, manager);
        return manager;
    }
}

abstract class DBManager {

    protected abstract String getLProtectedChar();

    protected abstract String getRProtectedChar();

    protected abstract String getDBMSId();

    protected String getLProtectedChar(String columName) {
        return getLProtectedChar();
    }

    protected String getRProtectedChar(String columName) {
        return getRProtectedChar();
    }

    private String getSqlStmt(DynamicMetadata column) {
        if (column != null) {
            if (column.getType().equals("id_Geometry"))
                return "GeomFromText(?, ?)"; // For PostGIS
            else
                return "?";
        } else {
            return "?";
        }
    }

    public String getUpdateSetSQL(Dynamic dynamic) {
        StringBuilder updateSetStmt = new StringBuilder();
        String ending = ",";
        for (int i = 0; i < dynamic.getColumnCount(); i++) {
            DynamicMetadata column = dynamic.getColumnMetadata(i);
            updateSetStmt.append(getLProtectedChar(column.getName()) + column.getName() + getRProtectedChar(column.getName())
                    + " = " + getSqlStmt(column));
            if (i >= dynamic.getColumnCount() - 1) {
                ending = "";
            }
            updateSetStmt.append(ending);
        }

        return updateSetStmt.toString();
    }

    public String getInsertTableSQL(Dynamic dynamic) {

        StringBuilder createSQL = new StringBuilder();
        String ending = ",";

        for (int i = 0; i < dynamic.getColumnCount(); i++) {
            DynamicMetadata column = dynamic.getColumnMetadata(i);
            createSQL.append(getLProtectedChar(column.getName()) + column.getName() + getRProtectedChar(column.getName()) + " ");
            if (i >= dynamic.getColumnCount() - 1) {
                ending = "";
            }
            createSQL.append(ending);
        }

        return createSQL.toString();
    }

    public void appendDBType(StringBuilder createSQL,String dataType) {
        createSQL.append(dataType);
    }
    
    public void appendDBTypeTail(StringBuilder createSQL,String dataType) {
        //do nothing
    }
    
    public boolean checkPrecision(Integer precision) {
        return precision.intValue() < 0;
    }
    
    public boolean ignoreLengthAndPrecision(String dataType,Integer length,Integer precision) {
        return false;
    }
    
    public boolean ignoreLength(Integer length) {
        return false;
    }
    
    protected String correctDBType(DynamicMetadata column) {
    	return column.getDbType();
    }
    
    private String getDBTypeFromDynamic(DynamicMetadata column) {
		String dataType = getDefaultSelectedDbType(column.getType());
		String orignalDBType = correctDBType(column);
		java.util.List<String> outputDBTypelist = MetadataTalendType.getTalendToDBList(this.getDBMSId(), column.getType());
		if (outputDBTypelist != null && outputDBTypelist.size() > 0) {
			for (String dbType : outputDBTypelist) {
				if (orignalDBType.equalsIgnoreCase(dbType)) {
					dataType = dbType;
					break;
				}
			}
		}
		if (dataType == null || ("").equals(dataType)) {
			dataType = orignalDBType;
		}
		return dataType;
	}
    
	private String getDefaultSelectedDbType(String talendType) {
	    return MetadataTalendType.getDefaultSelectedDbType(this.getDBMSId(), talendType, -1, -1);
	}
    
    public String getCreateTableSQL(Dynamic dynamic) {

        StringBuilder createSQL = new StringBuilder();
        String ending = ",";

        for (int i = 0; i < dynamic.getColumnCount(); i++) {
            DynamicMetadata column = dynamic.getColumnMetadata(i);
            createSQL.append(getLProtectedChar(column.getName()) + column.getName() + getRProtectedChar(column.getName()) + " ");
            String dataType = this.getDBTypeFromDynamic(column);
           
            appendDBType(createSQL,dataType);

            boolean defaultLengthIgnored = ("true").equals(MetadataTalendType.getDefaultDBTypes(getDBMSId(), dataType,
                    MetadataTalendType.IGNORE_LEN));
            Integer length = column.getLength();
            if (length.intValue() <= 0) {
                String defaultLen = MetadataTalendType.getDefaultDBTypes(getDBMSId(), dataType,
                        MetadataTalendType.DEFAULT_LENGTH);
                if (defaultLen != null && !("").equals(defaultLen)) {
                    length = Integer.parseInt(defaultLen);
                }
            }
            boolean defaultPrecisionIgnored = ("true").equals(MetadataTalendType.getDefaultDBTypes(getDBMSId(), dataType,
                    MetadataTalendType.IGNORE_PRE));
            Integer precision = column.getPrecision();
            if (checkPrecision(precision)) {
                String strPrecision = MetadataTalendType.getDefaultDBTypes(getDBMSId(), dataType,
                        MetadataTalendType.DEFAULT_PRECISION);
                if (strPrecision != null && !("").equals(strPrecision)) {
                    precision = Integer.parseInt(strPrecision);
                }
            }
            String prefix = "";
            String suffix = "";
            String comma = "";
            if (!ignoreLengthAndPrecision(dataType,length,precision)) {

                if (!defaultLengthIgnored) {
                    if (!ignoreLength(length)) {
                        prefix = "(";
                        suffix = ") ";
                        createSQL.append(prefix + length);
                    }
                }
                if (!defaultPrecisionIgnored) {
                    prefix = (prefix.equals("") ? "(" : prefix);
                    suffix = (suffix.equals("") ? ") " : suffix);
                    if (defaultLengthIgnored) {
                        createSQL.append(prefix);
                        comma = "";
                    } else {
                        comma = ",";
                    }
                    createSQL.append(comma + precision);
                }
                if (!ignoreLength(length)) {
                    createSQL.append(suffix);
                }
                appendDBTypeTail(createSQL,dataType);
            }
            createSQL.append(setNullable(column.isNullable()));
            if (i >= dynamic.getColumnCount() - 1) {
                ending = "";
            }
            createSQL.append(ending);
        }

        return createSQL.toString();
    }

    protected String setNullable(boolean nullable) {
        if (!nullable) {
            return " not null ";
        } else {
            return "";
        }
    }

}

class AccessManager extends DBManager {

    private String dbmsId;

    public AccessManager(String dbmsId) {
        super();
        this.dbmsId = dbmsId;
    }

    protected String getDBMSId() {

        return this.dbmsId;
    }

    protected String getLProtectedChar() {
        return "[";
    }

    protected String getRProtectedChar() {
        return "]";
    }

}

class AS400Manager extends DBManager {

    private String dbmsId;

    public AS400Manager(String dbmsId) {
        super();
        this.dbmsId = dbmsId;
    }

    protected String getDBMSId() {
        return this.dbmsId;
    }

    protected String getLProtectedChar() {
        return "";
    }

    protected String getRProtectedChar() {
        return "";
    }

}

class DB2Manager extends DBManager {

    private String dbmsId;

    public DB2Manager(String dbmsId) {
        super();
        this.dbmsId = dbmsId;
    }

    protected String getDBMSId() {
        // TODO Auto-generated method stub
        return this.dbmsId;
    }

    protected String getLProtectedChar() {
        return "\"";
    }

    protected String getRProtectedChar() {
        return "\"";
    }
}

class FirebirdManager extends DBManager {

    private String dbmsId;

    public FirebirdManager(String dbmsId) {
        super();
        this.dbmsId = dbmsId;
    }

    protected String getDBMSId() {
        // TODO Auto-generated method stub
        return this.dbmsId;
    }

    protected String getLProtectedChar() {
        return " ";
    }

    protected String getRProtectedChar() {
        return " ";
    }

}

class HSQLDBManager extends DBManager {

    private String dbmsId;

    String[] hsqldbKeyWords = {};

    public HSQLDBManager(String dbmsId) {
        super();
        this.dbmsId = dbmsId;
    }

    protected String getDBMSId() {
        // TODO Auto-generated method stub
        return this.dbmsId;
    }

    protected String getLProtectedChar() {
        return "";
    }

    protected String getRProtectedChar() {
        return "";
    }

    protected boolean isHSQLDBKeyword(String keyword) {
        for (int i = 0; i < hsqldbKeyWords.length; i++) {
            if (hsqldbKeyWords[i].equalsIgnoreCase(keyword)) {
                return true;
            }
        }
        return false;
    }

    protected String getLProtectedChar(String keyword) {
        if (isHSQLDBKeyword(keyword)) {
            return "\"";
        }
        return getLProtectedChar();
    }

    protected String getRProtectedChar(String keyword) {
        if (isHSQLDBKeyword(keyword)) {
            return "\"";
        }
        return getRProtectedChar();
    }

}

class InformixManager extends DBManager {

    private String dbmsId;

    public InformixManager(String dbmsId) {
        super();
        this.dbmsId = dbmsId;
    }

    /*
     * (non-Javadoc)
     * 
     * @see routines.system.dbmanager.DBManager#getDBMSId()
     */
    @Override
    protected String getDBMSId() {
        // TODO Auto-generated method stub
        return this.dbmsId;
    }

    protected String getLProtectedChar() {
        return "";
    }

    protected String getRProtectedChar() {
        return "";
    }

}

class IngresManager extends DBManager {

    private String dbmsId;

    public IngresManager(String dbmsId) {
        super();
        this.dbmsId = dbmsId;
    }

    protected String getDBMSId() {
        // TODO Auto-generated method stub
        return this.dbmsId;
    }

    protected String getLProtectedChar() {
        return "\"";
    }

    protected String getRProtectedChar() {
        return "\"";
    }

}

class InterbaseManager extends DBManager {

    private String dbmsId;

    public InterbaseManager(String dbmsId) {
        super();
        this.dbmsId = dbmsId;
    }

    protected String getDBMSId() {
        // TODO Auto-generated method stub
        return this.dbmsId;
    }

    protected String getLProtectedChar() {
        return "\"";
    }

    protected String getRProtectedChar() {
        return "\"";
    }

}

class JavaDBManager extends DBManager {

    private String dbmsId;

    public JavaDBManager(String dbmsId) {
        super();
        this.dbmsId = dbmsId;
    }

    protected String getDBMSId() {
        // TODO Auto-generated method stub
        return this.dbmsId;
    }

    protected String getLProtectedChar() {
        return "\"";
    }

    protected String getRProtectedChar() {
        return "\"";
    }

}

class JDBCManager extends DBManager {

    private String dbmsId;

    public JDBCManager(String dbmsId) {
        super();
        this.dbmsId = dbmsId;
    }

    protected String getDBMSId() {
        // TODO Auto-generated method stub
        return this.dbmsId;
    }

    protected String getLProtectedChar() {
        return "";
    }

    protected String getRProtectedChar() {
        return "";
    }

}

class MaxDBManager extends DBManager {

    private String dbmsId;

    public MaxDBManager(String dbmsId) {
        super();
        this.dbmsId = dbmsId;
    }

    protected String getDBMSId() {
        // TODO Auto-generated method stub
        return this.dbmsId;
    }

    protected String getLProtectedChar() {
        return "";
    }

    protected String getRProtectedChar() {
        return "";
    }

}

class MSSQLManager extends DBManager {

    private String dbmsId;

    public MSSQLManager(String dbmsId) {
        super();
        this.dbmsId = dbmsId;
    }

    protected String getDBMSId() {
        // TODO Auto-generated method stub
        return this.dbmsId;
    }

    protected String getLProtectedChar() {
        return "[";
    }

    protected String getRProtectedChar() {
        return "]";
    }

}

class MysqlManager extends DBManager {

    private String dbmsId;

    public MysqlManager(String dbmsId) {
        super();
        this.dbmsId = dbmsId;
    }

    protected String getDBMSId() {
        // TODO Auto-generated method stub
        return this.dbmsId;
    }

    protected String getLProtectedChar() {
        return "`";
    }

    protected String getRProtectedChar() {
        return "`";
    }
    
    public void appendDBType(StringBuilder createSQL,String dataType) {
        if (dataType.endsWith("UNSIGNED")) {
            createSQL.append(dataType.substring(0, dataType.indexOf("UNSIGNED")));
        } else {
            createSQL.append(dataType);
        }
    }
    
    public void appendDBTypeTail(StringBuilder createSQL,String dataType) {
        if (dataType.endsWith("UNSIGNED")) {
            createSQL.append("UNSIGNED");
        }
    }
    
    public boolean ignoreLengthAndPrecision(String dataType,Integer length,Integer precision) {
        return (("DECIMAL".equalsIgnoreCase(dataType)) || ("NUMERIC".equalsIgnoreCase(dataType)))
        && (length == null || length <= 0) && (precision == null || precision <= 0);
    }
    
    public String correctDBType(DynamicMetadata column) {
    	if((java.sql.Types.LONGVARCHAR == column.getDbTypeId()) && "VARCHAR".equals(column.getDbType())) {
    		int length = column.getLength();
    		if(length<256) {
    			return "TINYTEXT";
    		} else if(length<65536) {
    			return "TEXT";
    		} else if(length<16777216) {
    			return "MEDIUMTEXT";
    		} else {
    			return "LONGTEXT";
    		}
    	}
    	return column.getDbType();
    }

}

class NetezzaManager extends DBManager {

    private String dbmsId;

    public NetezzaManager(String dbmsId) {
        super();
        this.dbmsId = dbmsId;
    }

    protected String getDBMSId() {
        // TODO Auto-generated method stub
        return this.dbmsId;
    }

    protected String getLProtectedChar() {
        return "";
    }

    protected String getRProtectedChar() {
        return "";
    }

}

class ODBCManager extends DBManager {

    private String dbmsId;

    public ODBCManager(String dbmsId) {
        super();
        this.dbmsId = dbmsId;
    }

    protected String getDBMSId() {
        // TODO Auto-generated method stub
        return this.dbmsId;
    }

    protected String getLProtectedChar() {
        return "";
    }

    protected String getRProtectedChar() {
        return "";
    }

}

class OracleManager extends DBManager {

    private String[] oracleKeyWords = { "ACCESS", "AUDIT", "COMPRESS", "DESC", "ADD", "CONNECT", "DISTINCT", "ALL", "BY",
            "CREATE", "DROP", "ALTER", "CHAR", "CURRENT", "ELSE", "AND", "CHECK", "DATE", "EXCLUSIVE", "ANY", "CLUSTER",
            "DECIMAL", " EXISTS", "AS", "COLUMN", "DEFAULT", "FILE", "ASC", "COMMENT", "DELETE", "FLOAT", "FOR", "LONG",
            "PCTFREE", "SUCCESSFUL", "FROM", "MAXEXTENTS", "PRIOR", "SYNONYM", "GRANT", "MINUS", "PRIVILEGES", "SYSDATE",
            "GROUP", "MODE", "PUBLIC", "TABLE", "HAVING", "MODIFY", "RAW", "THEN", "IDENTIFIED", "NETWORK", "RENAME", "TO",
            "IMMEDIATE", "NOAUDIT", "RESOURCE", "TRIGGER", "IN", "NOCOMPRESS", "REVOKE", "UID", "INCREMENT", "NOT", "ROW",
            "UNION", "INDEX", "NOWAIT", "ROWID", "UNIQUE", "INITIAL", "NULL", "ROWNUM", "UPDATE", "INSERT", "NUMBER", "ROWS",
            "USER", "INTEGER", "OF", "SELECT", "VALIDATE", "INTERSECT", "OFFLINE", "SESSION", "VALUES", "INTO", "ON", "SET",
            "VARCHAR", "IS", "ONLINE", "SHARE", "VARCHAR2", "LEVEL", "OPTION", "SIZE", "VIEW", "LIKE", "OR", "SMALLINT",
            "WHENEVER", "LOCK", "ORDER", "START", "WHERE", "WITH" };

    private String dbmsId;

    public OracleManager(String dbmsId) {
        super();
        this.dbmsId = dbmsId;
    }

    protected String getLProtectedChar() {
        return "";
    }

    protected String getRProtectedChar() {
        return "";
    }

    protected String getDBMSId() {
        return dbmsId;
    }

    protected boolean isOracleKeyword(String keyword) {
        for (int i = 0; i < oracleKeyWords.length; i++) {
            if (oracleKeyWords[i].equalsIgnoreCase(keyword)) {
                return true;
            }
        }
        return false;
    }

    protected boolean contaionsSpaces(String columnName) {
        if (columnName != null) {
            // bug0016837 when use Additional column the coulmn name like: " + "columnNmae" + "
            if (columnName.startsWith("\" + ") && columnName.endsWith(" + \"")) {
                return false;
            }

            if (columnName.contains(" ")) {
                return true;
            }
        }
        return false;
    }

    protected String getLProtectedChar(String keyword) {
        if (isOracleKeyword(keyword) || contaionsSpaces(keyword)) {
            return "\"";
        }
        return getLProtectedChar();
    }

    protected String getRProtectedChar(String keyword) {
        if (isOracleKeyword(keyword) || contaionsSpaces(keyword)) {
            return "\"";
        }
        return getRProtectedChar();
    }
    
    public boolean ignoreLengthAndPrecision(String dataType,Integer length,Integer precision) {
        if (("NUMBER".equalsIgnoreCase(dataType)) 
                && (length == null || length <= 0) && (precision == null || precision < 0)) {
            return true;
        } else if((("CHAR".equalsIgnoreCase(dataType)) || ("NCHAR".equalsIgnoreCase(dataType))) 
                && (length == null || length <= 0) && (precision == null || precision <= 0)) {
            return true;
        }
        
        return false;
    }
    
}

class PostgreManager extends DBManager {

    private String dbmsId;

    public PostgreManager(String dbmsId) {
        super();
        this.dbmsId = dbmsId;
    }

    protected String getDBMSId() {
        // TODO Auto-generated method stub
        return dbmsId;
    }

    protected String getLProtectedChar() {
        return "\"";
    }

    protected String getRProtectedChar() {
        return "\"";
    }

    public boolean ignoreLength(Integer length) {
        return length < 0;
    }
    
}

class PostgrePlusManager extends DBManager {

    private String dbmsId;

    public PostgrePlusManager(String dbmsId) {
        super();
        this.dbmsId = dbmsId;
    }

    protected String getDBMSId() {
        // TODO Auto-generated method stub
        return this.dbmsId;
    }

    protected String getLProtectedChar() {
        return "\"";
    }

    protected String getRProtectedChar() {
        return "\"";
    }
    
    public boolean ignoreLength(Integer length) {
        return length < 0;
    }

}

class RedshiftManager extends DBManager {

    private String dbmsId;

    public RedshiftManager(String dbmsId) {
        super();
        this.dbmsId = dbmsId;
    }

    protected String getDBMSId() {
        // TODO Auto-generated method stub
        return this.dbmsId;
    }

    protected String getLProtectedChar() {
        return "\"";
    }

    protected String getRProtectedChar() {
        return "\"";
    }
    
    public boolean ignoreLength(Integer length) {
        return length < 0;
    }

}

class SQLiteManager extends DBManager {

    private String dbmsId;

    public SQLiteManager(String dbmsId) {
        super();
        this.dbmsId = dbmsId;
    }

    protected String getDBMSId() {
        // TODO Auto-generated method stub
        return this.dbmsId;
    }

    protected String getLProtectedChar() {
        return "\"";
    }

    protected String getRProtectedChar() {
        return "\"";
    }

}

class SybaseManager extends DBManager {

    private String dbmsId;

    private String[] sybaseKeyWords = { "ACCESS", "AUDIT", "COMPRESS", "DESC", "ADD", "CONNECT", "COUNT", "DISTINCT", "ALL",
            "BY", "CREATE", "DROP", "ALTER", "CHAR", "CURRENT", "ELSE", "AND", "CHECK", "DATE", "EXCLUSIVE", "ANY", "CLUSTER",
            "DECIMAL", " EXISTS", "AS", "COLUMN", "DEFAULT", "FILE", "ASC", "COMMENT", "DELETE", "FLOAT", "FOR", "LONG",
            "PCTFREE", "SUCCESSFUL", "FROM", "FALSE", "MAXEXTENTS", "PRIOR", "SYNONYM", "GRANT", "MINUS", "PRIVILEGES",
            "SYSDATE", "GROUP", "MODE", "PUBLIC", "TABLE", "HAVING", "MODIFY", "RAW", "THEN", "IDENTIFIED", "NETWORK", "RENAME",
            "TO", "IMMEDIATE", "NOAUDIT", "RESOURCE", "TRIGGER", "IN", "NOCOMPRESS", "REVOKE", "UID", "INCREMENT", "NOT", "ROW",
            "UNION", "INDEX", "NOWAIT", "ROWID", "UNIQUE", "INITIAL", "NULL", "ROWNUM", "UPDATE", "INSERT", "NUMBER", "ROWS",
            "USER", "INTEGER", "OF", "SELECT", "VALIDATE", "INTERSECT", "OFFLINE", "SESSION", "VALUES", "INTO", "ON", "SET",
            "VARCHAR", "IS", "ONLINE", "SHARE", "LEVEL", "OPTION", "SIZE", "VIEW", "LIKE", "OR", "SMALLINT", "WHENEVER", "LOCK",
            "ORDER", "START", "WHERE", "WITH"

    };

    public SybaseManager(String dbmsId) {
        super();
        this.dbmsId = dbmsId;
    }

    protected String getDBMSId() {
        // TODO Auto-generated method stub
        return this.dbmsId;
    }

    protected String getLProtectedChar() {
        return "";
    }

    protected String getRProtectedChar() {
        return "";
    }

    protected boolean isSybaseKeyword(String keyword) {
        for (int i = 0; i < sybaseKeyWords.length; i++) {
            if (sybaseKeyWords[i].equalsIgnoreCase(keyword)) {
                return true;
            }
        }
        return false;
    }

    protected String getLProtectedChar(String keyword) {
        if (isSybaseKeyword(keyword)) {
            return "\"";
        }
        return getLProtectedChar();
    }

    protected String getRProtectedChar(String keyword) {
        if (isSybaseKeyword(keyword)) {
            return "\"";
        }
        return getRProtectedChar();
    }

}

class TeradataManager extends DBManager {

    private String dbmsId;

    public TeradataManager(String dbmsId) {
        super();
        this.dbmsId = dbmsId;
    }

    protected String getDBMSId() {
        // TODO Auto-generated method stub
        return dbmsId;
    }

    protected String getLProtectedChar() {
        return "\"";
    }

    protected String getRProtectedChar() {
        return "\"";
    }

}

class VectorWiseManager extends IngresManager {

    private String dbmsId;

    public VectorWiseManager(String dbmsId) {
        super(dbmsId);
        this.dbmsId = dbmsId;
    }

    protected String getDBMSId() {
        // TODO Auto-generated method stub
        return dbmsId;
    }

}

class VerticaManager extends DBManager {

    private String dbmsId;

    public VerticaManager(String dbmsId) {
        super();
        this.dbmsId = dbmsId;
    }

    protected String getDBMSId() {
        return this.dbmsId;
    }

    protected String getLProtectedChar() {
        return "";
    }

    protected String getRProtectedChar() {
        return "";
    }

}
class SAPHanaManager extends DBManager {

    private String dbmsId;
    private String[] sapHanaReservedWords = { "ALL", "ALTER", "AS", "BEFORE", "BEGIN", "BOTH", "CASE", "CHAR", "CONDITION",
            "CONNECT", "CROSS", "CUBE", "CURRENT_CONNECTION", "CURRENT_DATE", "CURRENT_SCHEMA", "CURRENT_TIME",
            "CURRENT_TIMESTAMP", "CURRENT_USER", "CURRENT_UTCDATE", "CURRENT_UTCTIME", "CURRENT_UTCTIMESTAMP", "CURRVAL",
            "CURSOR", "DECLARE", "DISTINCT", "ELSE", "ELSEIF", "ELSIF", "END", "EXCEPT", "EXCEPTION", "EXEC", "FOR", "FROM",
            "FULL", "GROUP", "HAVING", "IF", "IN", "INNER", "INOUT", "INTERSECT", "INTO", "IS", "JOIN", "LEADING", "LEFT",
            "LIMIT", "LOOP", "MINUS", "NATURAL", "NEXTVAL", "NULL", "ON", "ORDER", "OUT", "PRIOR", "RETURN", "RETURNS",
            "REVERSE", "RIGHT", "ROLLUP", "ROWID", "SELECT", "SET", "SQL", "START", "SYSDATE", "SYSTIME", "SYSTIMESTAMP",
            "SYSUUID", "TOP", "TRAILING", "UNION", "USING", "UTCDATE", "UTCTIME", "UTCTIMESTAMP", "VALUES", "WHEN", "WHERE",
            "WHILE", "WITH" };

   

    public SAPHanaManager(String dbmsId) {
        super();
        this.dbmsId = dbmsId;
    }

    protected String getDBMSId() {
        // TODO Auto-generated method stub
        return this.dbmsId;
    }

    protected String getLProtectedChar() {
        return "";
    }

    protected String getRProtectedChar() {
        return "";
    }

    protected boolean isSAPHanaKeyword(String keyword) {
        for (int i = 0; i < sapHanaReservedWords.length; i++) {
            if (sapHanaReservedWords[i].equalsIgnoreCase(keyword)) {
                return true;
            }
        }
        return false;
    }

    protected String getLProtectedChar(String keyword) {
        if (isSAPHanaKeyword(keyword)) {
            return "\"";
        }
        return getLProtectedChar();
    }

    protected String getRProtectedChar(String keyword) {
        if (isSAPHanaKeyword(keyword)) {
            return "\"";
        }
        return getRProtectedChar();
    }

}
