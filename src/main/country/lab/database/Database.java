package country.lab.database;

import com.rits.cloning.Cloner;
import country.lab.events.*;
import country.lab.histories.History;
import gov.nasa.jpf.Config;
import gov.nasa.jpf.util.Pair;
import gov.nasa.jpf.vm.Transition;

import java.awt.geom.IllegalPathStateException;
import java.util.*;

public abstract class Database {

    protected Oracle oracle;

    protected ArrayList<Transaction> events;
    protected HashMap<EventData, Integer> backtrackPoints;

    //It points to the event w in writeEventsPerVariable.get(e.getVariable()) such that
    //when e it was added, w was the last event in that list.
    //protected HashMap<EventData, EventData> maximalWriteEventIndexes;

    protected HashMap<String, ArrayList<WriteTransactionalEvent>> writeEventsPerVariable;

    protected HashMap<String, ArrayList<ReadTransactionalEvent>> readEventsPerVariable;

    protected HashMap<Integer, ArrayList<Integer>> sessionOrder;

    protected HashMap<Integer, Integer> programExtendedOrder;

    protected HashMap<EventData, TransactionalEvent> instructionsMapped;

    protected HashMap<String, Integer> timesPathExecuted;

    protected LinkedList<TransactionalEvent> deletedOnSwap;
    protected boolean mockAccess;
    protected GuideInfo guideInfo;

    protected History history;
    protected History trueHistory;
    protected static Database databaseInstance;
    protected String mockPath;

    protected Config config;

    protected Database(Config config) {
        events = new ArrayList<>();
        backtrackPoints = new HashMap<>();
        writeEventsPerVariable = new HashMap<>();
        readEventsPerVariable = new HashMap<>();
        history = config.getEssentialInstance("db.database_isolation_level.class", History.class,
                new Class[]{Config.class},
                new Object[]{config});
        guideInfo = new GuideInfo();
        sessionOrder = new HashMap<>();
        instructionsMapped = new HashMap<>();
        oracle = Oracle.getOracle();
        mockAccess = true;
        programExtendedOrder = new HashMap<>();
        timesPathExecuted = new HashMap<>();
        deletedOnSwap = new LinkedList<>();
        this.config = config;
    }
    private ArrayList<String> getTransactionNames(){
    	var res =new ArrayList<String>();
    	for(Transaction transaction:events) {
    		res.add(((BeginTransactionalEvent)transaction.getFirst()).name);
    	}
    	return res;
    }
    public void addTransactionNames(History h) {
    	h.names=getTransactionNames();
    }
    public static Database getDatabase(Config config) {
        if (databaseInstance == null) {
            databaseInstance = config.getEssentialInstance("db.database_model.class", Database.class,
                    new Class[]{Config.class},
                    new Object[]{config});
        }
        return databaseInstance;
    }

    public static Database getDatabase() {
        if (databaseInstance == null) {
            throw new IllegalPathStateException();
        }
        return databaseInstance;
    }

    //Use it carefully.
    public Database cloneDatabase(){

        //TODO: it does not work!
        Cloner cloner = new Cloner();

        var clone = cloner.deepClone(this);
        //This variable has to be shared.
        clone.backtrackPoints = backtrackPoints;
        return clone;
    }


    public void setDatabaseInstance(Database db){
        databaseInstance = db;
    }
    
    public boolean isGuided(){
        return guideInfo.getDatabaseBacktrackMode() == GuideInfo.BacktrackTypes.SWAP ||
                guideInfo.getDatabaseBacktrackMode() == GuideInfo.BacktrackTypes.RESTORE;
    }

    public abstract void addEvent(TransactionalEvent t);

    public boolean isLastReadEventBacktrackable(){
        var e = getLastEvent();
        if(e == null) return false;
        if(e.getType() != TransactionalEvent.Type.READ) return false;
        WriteTransactionalEvent w = ((ReadTransactionalEvent) e).getWriteEvent();
        return !swapped(e) && w.getWriteIndex() != 0;
    }

    /*
    public boolean isLastReadEventReadingCausallyLatest(){
        var e = getLastEvent();
        if(e == null) return true;
        if(e.getType() != TransactionalEvent.Type.READ) return true;
        return maximalWriteEventIndexes.containsKey(e.getEventData());
    }
    */


    public boolean isExecutingTransactionalEvent(Transition t, TransactionalEvent e){
        if(events.isEmpty() || e == null) return false;
        String s = TrEventRegister.getEventRegister().getStackTrace(t.getThreadInfo());
        ArrayList<String> lines = new ArrayList<>(Arrays.asList(s.split("\\n")));
        if(lines.size() >= 2) {
            lines.remove(lines.size() - 1);
            lines.remove(lines.size() - 1); //This is lines.size() - 2 in the original array.
        }//There is one level of extra nesting before the end of a transaction.
        s = String.join("\n", lines);

        EventData ed = e.getEventData();
        return ed!=null && ed.equals(new EventData(s, timesPathExecuted.get(ed.getPath()), ed.getBeginEvent()));
    }


    //TODO
    public abstract void backtrackDatabase();

    public int getTimesPathExecuted(String s){
        Integer n = timesPathExecuted.get(s);
        if(n == null) return -1;
        else return n;
    }

    public int getNumberEvents() {
        return events.isEmpty() ? 0 : events.get(events.size() - 1).getLast().getObservationSequenceIndex() + 1;

    }

    public Integer getNumberOfWrites(String variable) {
        var list = writeEventsPerVariable.getOrDefault(variable, new ArrayList<>());
        if(list.isEmpty()) return 0;
        else if(list.get(list.size() -1).getTransactionId() == getTransactionalId()) return list.size() - 1;
        else return list.size();

    }

    //TODO
    public boolean swapped(TransactionalEvent e) {
        if (e.getType() != TransactionalEvent.Type.READ) return false;
        ReadTransactionalEvent r = (ReadTransactionalEvent) e;
        WriteTransactionalEvent w = r.getWriteEvent();
        if (oracle.compareEvents(r, w) > 0) return false;

        //This should be equivalent to w.getTransactionId() + 1 == r.getTransactIonID()
        for(int i = 0; i < r.getTransactionId(); ++i){
            if(history.areWRSO_plusRelated(w.getTransactionId(),i))
                return false;
        }
        //TODO: optimize with a segement tree?
        for(var ei: events.get(e.getTransactionId())){
            if(ei.getObservationSequenceIndex() == e.getObservationSequenceIndex()) break;
            if (ei.getType() == TransactionalEvent.Type.READ &&
                    ((ReadTransactionalEvent) ei).getWriteEvent().getTransactionId() == w.getTransactionId())
                return false;
        }

        return true;
        //return databaseRelations.numberWR(w.getTransactionId(), e.getTransactionId()) == 1;

    }

    public GuideInfo.BacktrackTypes getDatabaseBacktrackMode() {
        return guideInfo.getDatabaseBacktrackMode();
    }

    public void setDatabaseBacktrackMode(GuideInfo.BacktrackTypes databaseBacktrackMode) {
        guideInfo.setDatabaseBacktrackMode(databaseBacktrackMode);
    }

    public GuideInfo getGuideInfo() {
        return guideInfo;
    }

    public void resetGuidedInfo() {
      guideInfo.resetPath();
    }

    public void fullResetGuidedInfo(){
        guideInfo.fullResetPath();
    }

    public TransactionalEvent getEventFromEventData(EventData eventData) {
        return instructionsMapped.get(eventData);
    }

    protected void setWriteRead(WriteTransactionalEvent w,ReadTransactionalEvent r){
        r.setWriteEvent(w);

        history.setWR(r.getVariable(), w.getTransactionId(), r.getTransactionId(), r.getPoId());
        trueHistory = null;
    }

    public void changeWriteRead(WriteTransactionalEvent w, ReadTransactionalEvent r){
        eraseWriteRead(r);
        setWriteRead(w, r);
    }

    protected void eraseWriteRead(ReadTransactionalEvent r){
        WriteTransactionalEvent w = r.getWriteEvent();
        history.removeWR(r.getVariable(), w.getTransactionId(), r.getTransactionId(), r.getPoId());
        trueHistory = null;
    }

    public boolean isMockAccess() {
        return mockAccess;
    }

    public void setMockAccess(boolean mockAccess) {
        this.mockAccess = mockAccess;
    }

    public boolean isAssertionViolated(){
        if(events.isEmpty()) return false;
        TransactionalEvent t = getLastEvent();
        if(t.getType()== TransactionalEvent.Type.ASSERT){
            return  !((AssertTransactionalEvent) t).checkAssertion();
        }
        else return false;
    }

    public boolean isConsistent(){
    	addTransactionNames(history);
        return !isAssertionViolated() && history.isConsistent();
    }

    /*
    protected void confirmMaximumConsistency(){
        if(isMockAccess()) return;
        if(events.isEmpty() || guideInfo.getDatabaseBacktrackMode() == GuideInfo.BacktrackTypes.SWAP) return;
        TransactionalEvent t = events.get(events.size() - 1).getLast();
        if(t.getType() == TransactionalEvent.Type.READ){
            ReadTransactionalEvent r = (ReadTransactionalEvent) t;
            var w = r.getWriteEvent();

            //r is the last event in the history!
            history.removeWR(r.getVariable(), w.getTransactionId(), r.getTransactionId());
            if(history.areWRSO_starRelated(w.getTransactionId(), r.getTransactionId())){
                //TODO: before there was putIfAbsent
                maximalWriteEventIndexes.putIfAbsent(r.getEventData(), r.getWriteEvent().getEventData());
            }
            history.setWR(r.getVariable(), w.getTransactionId(), r.getTransactionId(), r.getPoId());
        }
    }
     */

    public int getTransactionalId() {
        return events.size() - 1;
    }

    public int getTransactionalS0Id(int threadId){
        if(!sessionOrder.containsKey(threadId)) return -1;
        return sessionOrder.get(threadId).size() - 1;
    }

    public int getPOId(int threadId){
        if(!programExtendedOrder.containsKey(threadId)) return 0;
        return programExtendedOrder.get(threadId);
    }

    public TransactionalEvent getLastEvent(){
        return events.isEmpty() ? null : events.get(events.size() - 1).getLast();
    }

    //Only for printing reasons. Hack.
    public WriteTransactionalEvent getWriteEvent(String var, int idx){
        return writeEventsPerVariable.get(var).get(idx);
    }

    public WriteTransactionalEvent getLastWriteEvent(String var){
        if(!writeEventsPerVariable.containsKey(var) ||
                writeEventsPerVariable.get(var).isEmpty()) return null;
        else return writeEventsPerVariable.get(var).get(writeEventsPerVariable.get(var).size() - 1);
    }

    public EventData getLastBeginData(){
        if(events.isEmpty()) return null;
        else return events.get(events.size() - 1).getFirst().getEventData();

    }

    public String getDatabaseState(){
        StringBuilder sb = new StringBuilder();
        for(var t: events){
            for(var e: t) {
                sb.append(e.toWRString()).append("\n");
            }
        }
        sb.append("Consistency: ").append(!isAssertionViolated()).append("\n");
        return sb.toString();
    }


    public boolean isTrulyConsistent(){
        if(config.getString("db.database_true_isolation_level.class") != null) {
            if(trueHistory == null) {
                trueHistory = config.getEssentialInstance("db.database_true_isolation_level.class", History.class,
                        new Class[]{History.class},
                        new Object[]{history});
            }
        	addTransactionNames(trueHistory);
            boolean res = !isAssertionViolated() && trueHistory.isConsistent();
            if(res)
            	trueHistory.toFileChance("Cons");
            else
            	trueHistory.toFileChance("Incons");
            return res;
        }
        else return isConsistent();
    }

}
