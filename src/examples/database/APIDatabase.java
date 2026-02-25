package database;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.concurrent.locks.ReentrantLock;

/**
 * API for the database. All procedures have to use it to simulate the database behaviour.
 * This is NOT a real database. There is no actual write/read/lock effect.
 * However, Transactional JPF can detect specific calls to this API and write/read the
 * desired values. A code executing this functions without Transactional JPF will fail.
 *
 * Basic primitives: BEGIN, READ, WRITE, COMMIT, ABORT, ASSERT.
 * Other primitives: readRow, writeRow, insertRow... (based on the previous ones).
 */
public class APIDatabase {

    protected static APIDatabase databaseInstance;

    protected int r;

    private APIDatabase(){
        r = 0;
    }

    public static APIDatabase getDatabase(){
        if(databaseInstance == null){
            databaseInstance = new APIDatabase();
        }
        return databaseInstance;
    }


    /**
     * Abuse of JPF to break transitions.
     */
    private void breakTransition(){
        for(int  i = 0; i < 1; ++i){}

    }

    private String readInstruction(String variable){
        breakTransition();
        //This value will be overwritten when the actual instruction is executed.
        return "NON-VALID-VALUE";

    }
    private void writeInstruction(String variable, String value){
        breakTransition();
    }
    private void beginInstruction(String name){
        breakTransition();
    }
    private void beginInstruction(){
        breakTransition();
    }
    private void commitInstruction(){
        breakTransition();
    }

    private void abortInstruction(){
        breakTransition();
    }
    private void assertInstruction(String value1, String op, String value2){
        breakTransition();
    }

    public String read(String variable){
        return readInstruction(variable);
    }

    public void write(String variable, String value){
        if(value == null) value = "null";
        writeInstruction(variable, value);
    }

    public void begin(){
        beginInstruction("undefined");
    }

    public void begin(String name){
        beginInstruction(name);
    }
    public void commit(){
        commitInstruction();
    }

    public void abort() throws AbortDatabaseException {
        abortInstruction();
        throw new AbortDatabaseException("Abort database.");
    }

    public String readRow(String table, String row){
        var t = read(table);
        if(t == null) return null;
        var tList = TRUtility.generateArrayList(t);
        for(var e: tList){
            if(e.equals(row)){
                var r = read(table+":"+row);
                if(r.equals("null")) return null;
                else return r;
            }
        }
        return null;
    }

    public ArrayList<String> readAll(String table){
        return readIfIDStartsWith(table, "");
    }

    public ArrayList<String> readIfIsIn(String table, HashSet<String> idSet){
        var t = read(table);
        if(t == null) return null;
        var tList = TRUtility.generateArrayList(t);
        var results = new ArrayList<String>();
        for(var e: tList) {
            if (idSet.contains(e)) {
                var r = read(table + ":" + e);
                if(r.equals("null")) r = null;
                results.add(r);
            }
        }
        return results;
    }

    public ArrayList<String> readIfIDStartsWith(String table,
                                                String prefix){
        var t = read(table);
        if(t == null) return null;
        var tList = TRUtility.generateArrayList(t);
        var results = new ArrayList<String>();

        for(var e: tList) {
            if (e.startsWith(prefix)) {
                var r = read(table + ":" + e);
                if(r.equals("null")) r = null;
                results.add(r);
            }
        }
        return results;
    }

    public ArrayList<String> readIfIDEndsWith(String table,
                                                String suffix){
        var t = read(table);
        if(t == null) return null;
        var tList = TRUtility.generateArrayList(t);
        var results = new ArrayList<String>();

        for(var e: tList) {
            if (e.endsWith(suffix)) {
                var r = read(table + ":" + e);
                if(r.equals("null")) r = null;
                results.add(r);
            }
        }
        return results;
    }

    public ArrayList<String> readAllIDs(String table){
        var t = read(table);
        if(t == null) return null;
        return TRUtility.generateArrayList(t);
    }

    public void insertRow(String table, String row, String value){
        insertRow(table, row);
        if(value == null) value = "null";
        write(table+":"+row, value);
    }

    public void insertRow(String table, String row){
        var t = read(table);
        var tList = TRUtility.generateArrayList(t);
        tList.add(row);
        write(table, tList.toString());
    }

    public void writeRow(String table, String row, String value){

        var t = read(table);
        var tList = TRUtility.generateArrayList(t);

        if(value == null) value = "null";
        for(var e: tList){
            if(e.equals(row)){
                write(table+":"+row, value);
                return;
            }
        }
    }

    public void deleteRow(String table, String row){
        var t = read(table);
        var tList = TRUtility.generateArrayList(t);
        tList.remove(row);
        write(table, tList.toString());
    }

    public void assertDB(String value1, String op, String value2){
        assertInstruction(value1,op,value2);
    }

    public void assertDB(boolean b){
        String s2 = "=";
        String s3 = "true";
        assertInstruction(String.valueOf(b), s2, s3);
    }
}


