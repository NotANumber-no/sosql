package no.notanumber.sosql;

import org.apache.commons.lang.StringUtils;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.LocalDate;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toList;
import static org.apache.commons.lang.StringUtils.join;

public class CodeGenerator {

    public static void generateTableConstants(String tableName, DBFunctions dbFunctions) throws Exception {


        Connection connection = dbFunctions.getConnection();
        PreparedStatement stmt = connection.prepareStatement("SELECT * FROM \""+ tableName + "\"");
        ResultSet rs = stmt.executeQuery();
        
        for (int i = 0; i < rs.getMetaData().getColumnCount(); i++) {
            String col = rs.getMetaData().getColumnName(i+1);
            System.out.println(col + "(no.notanumber.sosql.Table." + tableName + "),");
        }
        
    }
    public static void generateDO(String tableName, DBFunctions dbFunctions) throws Exception {

        System.out.println("import no.kommune.drammen.database.*;");
        System.out.println("import java.time.*;");
        Connection connection = dbFunctions.getConnection();
        PreparedStatement stmt = connection.prepareStatement("SELECT * FROM \"" + tableName + "\"");
        ResultSet rs = stmt.executeQuery();
        @SuppressWarnings("serial")
        Map<String, String> types = new HashMap<String, String>() {{
            put("int8", "Long");
            put("char", "Boolean");
            put("varchar", "String");
            put("datetime2", "LocalDateTime");
            put("Date", "LocalDate");
            put("date", "LocalDate");
            put("timestamp", "LocalDateTime");
            put("numeric", "Double");
        }};
        for (int i = 0; i < rs.getMetaData().getColumnCount(); i++) {
            String col = rs.getMetaData().getColumnName(i+1);
            String type = rs.getMetaData().getColumnTypeName(i+1);
            System.out.println("@no.notanumber.sosql.Column(column=DatabaseColumns." + col + ")");
            String thisType = types.get(type);
            if (thisType == null) {
                thisType = type;
            }
            System.out.println("private " + thisType + " " + col + ";");
            System.out.println("");
        }
        
    }

    public static void createDB(List<DatabaseColumn> columns) {

        for (String t : ColumnHelper.tables) {
            System.out.println("CREATE TABLE " + t + " (");
            List<String> colDefs = ColumnHelper.getColumnsFor(t).stream().map(col -> StringUtils.join(asList(col.columnName, getDbType(col), getConstraints(col.type)), " ")).collect(toList());
            System.out.println(join(colDefs, ",\n"));
            System.out.println(");");
            System.out.println("");
            System.out.println("ALTER TABLE public." + t + " OWNER TO postgres;");
            System.out.println("");
            Optional<DatabaseColumn> pk = ColumnHelper.getPrimaryKey(t);
            if (pk.isPresent()) {
                String seq = t + "_" + pk.get().columnName + "_SEQ";
                System.out.println("CREATE SEQUENCE " + seq);
                System.out.println("START WITH 1");
                System.out.println("INCREMENT BY 1");
                System.out.println("NO MINVALUE");
                System.out.println("NO MAXVALUE");
                System.out.println("CACHE 1;");
                System.out.println("");
                System.out.println("ALTER TABLE public." + seq + " OWNER TO postgres;");
                System.out.println("ALTER SEQUENCE " + seq + " OWNED BY " + t + "." + pk.get().columnName + ";");
                System.out.println("ALTER TABLE ONLY " + t + " ALTER COLUMN " + pk.get().columnName + " SET DEFAULT nextval('" + seq + "'::regclass);");
                System.out.println("ALTER TABLE ONLY " + t + " ADD CONSTRAINT " + pk.get().columnName + " PRIMARY KEY (" + pk.get().columnName + ");");
            }else {
                System.out.println("ALTER TABLE ONLY " + t + " ADD CONSTRAINT " + t + "_pkey PRIMARY KEY (" + StringUtils.join(ColumnHelper.getColumnsFor(t).stream().filter(col -> col.type == ColumnType.ForeignKey).collect(toList()), ", ") + ");");
            }
            System.out.println("");
        }
        System.out.println("");
        for (DatabaseColumn col : columns) {
            if (col.type == ColumnType.ForeignKey){
                System.out.println("CREATE INDEX FKI_" + col.columnName + "FK ON " + col.table + " USING btree (" + col.columnName + ");");
                System.out.println("ALTER TABLE ONLY " + col.table + " ADD CONSTRAINT " + col.columnName + "FK FOREIGN KEY (" + col.columnName + ") REFERENCES " + col.joinedTo + "(" + ColumnHelper.getPrimaryKey(col.joinedTo).get().columnName + ");");
            }
        }
    }

    private static String getDbType(DatabaseColumn column) {
        if (String.class.equals(column.clazz))
            return "character varying(" + column.length + ")";
        if (asList(Integer.class, Integer.TYPE, Long.class, Long.TYPE).contains(column.clazz))
            return "bigint";
        if (LocalDate.class.equals(column.clazz))
            return "bigint";
        if (byte[].class.equals(column.clazz))
            return "bytea";
        return "";

    }

    private static String getConstraints(ColumnType type) {
        switch(type){
            case PrimaryKey: return "NOT NULL";
            case Version: return "DEFAULT 0 NOT NULL";
            default:
                return  "";
        }
    }


    public static void main(String[] args) throws Exception {
        DBFunctions dbFunctions = new DBFunctions("jdbc:postgresql://localhost/sommerskole?useUnicode=true&characterEncoding=utf8", "postgres", "baxter", 3);
        generateDO("Trinn", dbFunctions);
        //createDB();
    }
}

