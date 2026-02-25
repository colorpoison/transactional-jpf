package country.lab.database;

import com.rits.cloning.Cloner;
import country.lab.events.*;
import country.lab.events.*;
import country.lab.histories.History;
import gov.nasa.jpf.Config;
import gov.nasa.jpf.util.Pair;

import java.util.*;

public class TrDatabase extends Database{

    protected History restoreHistory;

    protected RestoreTranslator restoreTranslator;

    public TrDatabase(Config config) {
        super(config);
    }

    protected Pair<LinkedList<Transaction>, WriteTransactionalEvent> computeRestoreInformation
            (ReadTransactionalEvent r, TransactionalEvent w, int pi_idx){

        LinkedList<Transaction> restorePath = new LinkedList<>();

        EventData orac_idx = new EventData("", -1, null);
        orac_idx = oracle.getNextData(orac_idx);
        WriteTransactionalEvent writeBack = null;

        while(true){

            if(!instructionsMapped.containsKey(orac_idx)){
                //TODO: remove the unknown end (it may be commit or abort)
                EventData endData = oracle.getCommit(orac_idx);
                restorePath.add(new Transaction(new UnknownEvent(orac_idx), new UnknownEvent(endData)));
            }
            else{
                TransactionalEvent begin = instructionsMapped.get(orac_idx);
                LinkedList<TransactionalEvent> trans = new LinkedList<>();

                if(begin.getTransactionId() == r.getTransactionId()) {
                    for (var ei : events.get(begin.getTransactionId())) {
                        if(ei.getObservationSequenceIndex() == r.getObservationSequenceIndex()) break;
                        trans.add(ei);
                    }

                    writeBack = (WriteTransactionalEvent)
                            instructionsMapped.get(r.getBacktrackWriteEvent());

                    //r.setWriteEvent((WriteTransactionalEvent) wMCW);
                    trans.add(r);
                    var endData = oracle.getCommit(begin.getEventData());
                    trans.add(new UnknownEvent(endData));
                    restorePath.add(new Transaction(trans));
                }
                else {
                    //We discard every transaction before r.
                    while (pi_idx <= begin.getTransactionId()) {
                        for(var t : events.get(pi_idx)){
                            trans.add(t);
                            if(t.getType() == TransactionalEvent.Type.COMMIT ||
                                    t.getType() == TransactionalEvent.Type.ABORT) {
                                restorePath.add(new Transaction(trans));
                                trans = new LinkedList<>();
                            }
                        }
                        pi_idx++;
                    }
                }

                //e.equals(w)?
                //end will also have the same trId, but begin is always not null
                if(begin.getTransactionId() == w.getTransactionId()){
                    break;
                }
            }
            orac_idx = oracle.getNextData(orac_idx);

        }
        return new Pair<>(restorePath, writeBack);
    }

    protected void generateRestoreHistory(ReadTransactionalEvent r, TransactionalEvent w, TransactionalEvent e){

        restoreHistory = config.getEssentialInstance("db.database_isolation_level.class", History.class,
                new Class[]{History.class},
                new Object[]{history});

        restoreTranslator = new RestoreTranslator(sessionOrder);

        //We remove all events until the backtracked transaction
        truncateHistoryTransaction(restoreHistory, e.getTransactionId(), restoreTranslator,true);

        //We add all the events in the causal dependency of w
        addCausalDependentTransactions(restoreHistory, e.getTransactionId() + 1,
                w.getTransactionId(), restoreTranslator);

        //We add all events from trans(r) until r
        addFromTransactionUntilEvent(restoreHistory, r, restoreTranslator, false);
    }

    protected void generateRestorePath(ReadTransactionalEvent r){
        EventData backEvent = r.getBacktrackEvent();
        TransactionalEvent e = instructionsMapped.get(backEvent);
        int pi_idx = e.getTransactionId()+1;

        TransactionalEvent w = r.getWriteEvent();

        //TODO: fix the writeBack.
        WriteTransactionalEvent writeBack = null;

        generateRestoreHistory(r, w, e);

        var restoreInformation =
                computeRestoreInformation(r, w, pi_idx);

        guideInfo.addGuide(restoreInformation._1, e, restoreInformation._2, new HashSet<>());
    }

    protected void generateBacktrackPath(WriteTransactionalEvent w, ReadTransactionalEvent r){
        LinkedList<Transaction> backtrackPath = new LinkedList<>();
        LinkedList<TransactionalEvent> transaction = new LinkedList<>();
        HashSet<BeginTransactionalEvent> deleted = new HashSet<>();

        //We need to start in a complete transaction, i.e. after the read.
        //int i = events.get(r.getTransactionId()).getFirst().getObservationSequenceIndex();
        var trRead = events.get(r.getTransactionId());


        for(int i = r.getTransactionId() + 1; i < events.size(); ++i){
            if(history.areWRSO_starRelated(i, w.getTransactionId())){
                for(var e: events.get(i)){
                    transaction.add(e);
                }
                backtrackPath.add(new Transaction(transaction));
                transaction = new LinkedList<>();
            }
            else{
                deleted.add((BeginTransactionalEvent) events.get(i).getFirst());
                for(var e: events.get(i)){
                    //if(e.getType() == TransactionalEvent.Type.READ)
                    deletedOnSwap.add(e);
                }
            }

        }
        var deletedOnSwapR = new LinkedList<TransactionalEvent>();
        for(var e: trRead){
            if(e.getObservationSequenceIndex() <= r.getObservationSequenceIndex()) {
                transaction.add(e);
            }
            else {
                deletedOnSwapR.addFirst(e);
            }
        }
        if(!deletedOnSwapR.isEmpty()) {
            for (TransactionalEvent transactionalEvent : deletedOnSwapR) {
                deletedOnSwap.addFirst(transactionalEvent);
            }
        }

        backtrackPath.add(new Transaction(transaction));

        guideInfo.addGuide(backtrackPath, events.get(r.getTransactionId()-1).getLast(), w, deleted);
    }



    @Override
    public void backtrackDatabase() {

        if(isMockAccess()){
            if(mockPath != null) {
                int n = timesPathExecuted.get(mockPath);
                if (n == -1) timesPathExecuted.remove(mockPath);
                else timesPathExecuted.put(mockPath, n - 1);
                mockPath = null;
            }
            return;
        }
        var tr = events.get(events.size() - 1);
        TransactionalEvent e = tr.getLast();
        switch (e.getType()) {
            case READ:
                ReadTransactionalEvent r = (ReadTransactionalEvent) e;
                WriteTransactionalEvent w = ((ReadTransactionalEvent) e).getWriteEvent();

                var localRead = w.getTransactionId() == r.getTransactionId();

                if (!swapped(e) && w.getWriteIndex() != 0 && !isGuided() && !localRead) {
                    WriteTransactionalEvent nw = writeEventsPerVariable.get(e.getVariable()).get(w.getWriteIndex() - 1);
                    changeWriteRead(nw, r);
                    guideInfo.setDatabaseBacktrackMode(GuideInfo.BacktrackTypes.READ);
                    return;

                } else if (!swapped(e) || isGuided()) {
                    readEventsPerVariable.get(e.getVariable()).remove(readEventsPerVariable.get(e.getVariable()).size() - 1);
                    eraseWriteRead(r);

                } else {
                    generateRestorePath(r);
                    guideInfo.setDatabaseBacktrackMode(GuideInfo.BacktrackTypes.RESTORE);
                    return;
                }
                break;
            case WRITE:
                ArrayList<WriteTransactionalEvent> writeEvents = writeEventsPerVariable.get(e.getVariable());

                if(!writeEvents.isEmpty() && writeEvents.get(writeEvents.size()-1).getTransactionId() == e.getTransactionId()){
                    writeEvents.remove(writeEvents.size() -1);

                }
                if(!deletedOnSwap.isEmpty() && deletedOnSwap.getLast() == e) {
                    backtrackPoints.remove(e.getEventData());
                }
                history.removeWrite(e.getVariable(), e.getTransactionId());
                break;
            case COMMIT:

                if(getDatabaseBacktrackMode() == GuideInfo.BacktrackTypes.SWAP &&
                                guideInfo.getDeleted().contains(tr.getFirst())) {
                    for(var ei : tr){
                        if (ei.getType() == TransactionalEvent.Type.WRITE) {
                            backtrackPoints.remove(ei.getEventData());
                        }
                    }
                    break;
                }
                else if(isGuided()){
                    break;
                }

                Pair<WriteTransactionalEvent, ReadTransactionalEvent> p = nextSwap();
                if (p == null) {
                    guideInfo.setDatabaseBacktrackMode(GuideInfo.BacktrackTypes.JPF);
                    //remove all backtrack points
                    for(var ei : tr){
                        if (ei.getType() == TransactionalEvent.Type.WRITE) {
                            backtrackPoints.remove(ei.getEventData());
                        }
                    }
                }
                else {
                    guideInfo.setDatabaseBacktrackMode(GuideInfo.BacktrackTypes.SWAP);
                    WriteTransactionalEvent wSwap = p._1;
                    ReadTransactionalEvent rSwap = p._2;
                    generateBacktrackPath(wSwap, rSwap);

                    var previousEvent = events.get(rSwap.getTransactionId()-1).getLast();
                    var backtrackPair = new Pair<>(previousEvent.getEventData(), rSwap.getWriteEvent().getEventData());
                    rSwap.setBacktrackEvent(backtrackPair);
                    return;
                }
                break;
            case ABORT:
                //If T is not longer aborted, the writes have to be added to the history: if a read change its wr, it
                //may be committed.
                for(var ea: events.get(events.size() - 1)){
                    if(ea.getType() == TransactionalEvent.Type.WRITE) {

                        var wa = (WriteTransactionalEvent) ea;
                        var writes = writeEventsPerVariable.get(ea.getVariable());

                        //Transactions have at most one visible write per variable.
                        if (!writes.isEmpty() && writes.get(writes.size() - 1).getTransactionId() == ea.getTransactionId()) {
                            writes.remove(writes.size() - 1);
                        }
                        writes.add(wa);
                        history.addWrite(wa.getVariable(), wa.getTransactionId(), wa.getPoId());
                    }
                }
                break;
            case BEGIN:
                history.removeLastTransaction();
                sessionOrder.get(e.getThreadId()).remove(sessionOrder.get(e.getThreadId()).size() - 1);
                break;


        }

        trueHistory = null;

        if(!isGuided()) {
            guideInfo.setDatabaseBacktrackMode(GuideInfo.BacktrackTypes.JPF);
        }
        programExtendedOrder.put(e.getThreadId(), programExtendedOrder.get(e.getThreadId()) - 1);

        if(!deletedOnSwap.isEmpty() && deletedOnSwap.getLast() == e) {
            deletedOnSwap.removeLast();
        }

        EventData ed = e.getEventData();

        instructionsMapped.remove(ed);
        int n = timesPathExecuted.get(ed.getPath());
        if(n == -1) timesPathExecuted.remove(ed.getPath());
        else timesPathExecuted.put(ed.getPath(), n-1);

        tr.removeLast();
        if(tr.isEmpty()) events.remove(events.size() - 1);
        if(e.getType() == TransactionalEvent.Type.WRITE){
            var newWE = tr.getLastWriteEvent(e.getVariable());
            if(newWE != null){
                writeEventsPerVariable.get(e.getVariable()).add(newWE);
            }
        }

    }

    @Override
    public void addEvent(TransactionalEvent t){

        EventData ed = t.getEventData();
        timesPathExecuted.put(ed.getPath(), ed.getTime()); //before mockAccess, we need it to check path of alternatives

        if(isMockAccess()){
            mockPath = ed.getPath();
            return;
        }

        instructionsMapped.put(ed, t);
        timesPathExecuted.put(ed.getPath(), ed.getTime());


        switch (t.getType()) {
            case WRITE:
                if(!isGuided()) {
                    readEventsPerVariable.putIfAbsent(t.getVariable(), new ArrayList<>());
                }
                writeEventsPerVariable.putIfAbsent(t.getVariable(), new ArrayList<>());


                var writesOfX = writeEventsPerVariable.get(t.getVariable());
                if(!writesOfX.isEmpty() && writesOfX.get(writesOfX.size()-1).getTransactionId() == t.getTransactionId()){
                    writesOfX.remove(writesOfX.size() -1);

                }
                writesOfX.add((WriteTransactionalEvent) t);

                history.addWrite(t.getVariable(),t.getTransactionId(), t.getPoId());
                if(getDatabaseBacktrackMode() == GuideInfo.BacktrackTypes.RESTORE){
                    var begT = events.get(t.getTransactionId()).getFirst();
                    if(!restoreHistory.isWrittingVariable(t.getVariable(),
                            restoreTranslator.translate(begT), t.getPoId() + 1) &&
                            restoreHistory.isCommitted(restoreTranslator.translate(begT))
                        )
                        restoreHistory.addWrite(t.getVariable(), restoreTranslator.translate(begT), t.getPoId());
                }

                break;
            case READ:
                ArrayList<WriteTransactionalEvent> writeEvents = writeEventsPerVariable.get(t.getVariable());
                ReadTransactionalEvent r = (ReadTransactionalEvent) t;
                WriteTransactionalEvent w = null;

                if(writeEventsPerVariable.get(t.getVariable()) == null){
                    //If the event does not exist we create a new event out
                    //of the blue that writes null and that would be theoretically
                    //placed just before the read.
                    var edw = new EventData(ed.getPath()+"---"+"FAKE WRITE EVENT", ed.getTime(), ed.getBeginEvent());
                    w = new WriteTransactionalEvent(edw,
                            new ArrayList<>(Arrays.asList(r.getVariable(), "null")),
                            0, r.getObservationSequenceIndex(),
                            r.getThreadId(), r.getTransactionId(),
                            r.getTransactionalSessionId(), r.getPoId()-1);
                    instructionsMapped.put(edw, w);
                }
                else {
                    w = writeEvents.get(writeEvents.size() - 1);
                }

                //If we are under RESTORE case, we assign the latest causal value (under the restored history).
                if(getDatabaseBacktrackMode() == GuideInfo.BacktrackTypes.RESTORE){
                    w = getCausalLatest(r);
                    var begR = events.get(r.getTransactionId()).getFirst();
                    var begW = events.get(w.getTransactionId()).getFirst();


                    if(!restoreHistory.areWR(r.getVariable(), restoreTranslator.translate(begW),
                            restoreTranslator.translate(begR), r.getPoId()))
                        restoreHistory.setWR(r.getVariable(), restoreTranslator.translate(begW),
                            restoreTranslator.translate(begR), r.getPoId());
                }



                setWriteRead(w, r);
                readEventsPerVariable.putIfAbsent(t.getVariable(), new ArrayList<>());
                readEventsPerVariable.get(t.getVariable()).add((ReadTransactionalEvent) t);
                break;
            case BEGIN:
                sessionOrder.putIfAbsent(t.getThreadId(), new ArrayList<>());
                history.addTransaction(t.getTransactionId(), t.getThreadId(), sessionOrder.get(t.getThreadId()));
                sessionOrder.get(t.getThreadId()).add(t.getTransactionId());
                programExtendedOrder.putIfAbsent(t.getThreadId(), 0);
                oracle.addBegin(ed);
                events.add(new Transaction(new LinkedList<>()));

                if(getDatabaseBacktrackMode() == GuideInfo.BacktrackTypes.RESTORE &&
                        !restoreTranslator.containsID(t)){
                    restoreTranslator.putID(t,restoreHistory.getNumberTransactions());
                    restoreHistory.addTransaction(restoreHistory.getNumberTransactions(),
                            t.getThreadId(), restoreTranslator.getSO(t));
                    restoreTranslator.putSO(t);
                }

                break;
            case COMMIT:
                if(getDatabaseBacktrackMode() != GuideInfo.BacktrackTypes.SWAP) {
                    var begin = events.get(events.size() -1).getFirst();
                    oracle.addCommit(begin.getEventData(), ed);
                    for(var e: events.get(events.size()-1)){
                        if (e.getType() == TransactionalEvent.Type.WRITE) {
                            var writes = writeEventsPerVariable.get(e.getVariable());
                            if(writes.get(writes.size() - 1).getObservationSequenceIndex() == e.getObservationSequenceIndex()) {
                                backtrackPoints.putIfAbsent(e.getEventData(), readEventsPerVariable.get(e.getVariable()).size() - 1);
                            }
                            else{
                                //TODO: remove this
                                backtrackPoints.putIfAbsent(e.getEventData(), -1);
                            }
                        }
                    }
                }
                break;
            case ABORT:
                var begT = events.get(events.size() - 1).getFirst();
                history.setCommitted(false, events.size() - 1);

                for(var e: events.get(events.size() - 1)){
                    if(e.getType() == TransactionalEvent.Type.WRITE) {
                        var writes = writeEventsPerVariable.get(e.getVariable());
                        if (!writes.isEmpty() && writes.get(writes.size() - 1).getTransactionId() == e.getTransactionId()) {
                            writes.remove(writes.size() - 1);
                        }
                        history.removeWrite(e.getVariable(), e.getTransactionId());

                        if(getDatabaseBacktrackMode() == GuideInfo.BacktrackTypes.RESTORE &&
                                restoreHistory.isWrittingVariable(e.getVariable(),restoreTranslator.translate(begT))){
                            restoreHistory.removeWrite(e.getVariable(), restoreTranslator.translate(begT));
                            restoreHistory.setCommitted(false, restoreTranslator.translate(begT));
                        }
                    }
                }
                break;
        }

        trueHistory = null;

        events.get(events.size()-1).addEvent(t);
        programExtendedOrder.put(t.getThreadId(), programExtendedOrder.get(t.getThreadId()) + 1);

    }

    protected WriteTransactionalEvent getCausalLatest(ReadTransactionalEvent r) {
        var beg = events.get(r.getTransactionId()).getFirst();
        var rIDTranslated = restoreTranslator.translate(beg);
        for(int i = rIDTranslated; i >= 0; --i ){
            if(restoreHistory.areWRSO_starRelated(i, rIDTranslated)){
                if(i == rIDTranslated && !restoreHistory.isWrittingVariable(r.getVariable(), i, r.getPoId()))
                    continue;
                else if(i != rIDTranslated && !restoreHistory.isWrittingVariable(r.getVariable(), i))
                    continue;

                if(!restoreHistory.areWR(r.getVariable(), i, rIDTranslated, r.getPoId()))
                    restoreHistory.setWR(r.getVariable(), i, rIDTranslated, r.getPoId());
            	addTransactionNames(restoreHistory);
                if(!restoreHistory.isConsistent()){
                    restoreHistory.removeWR(r.getVariable(), i, rIDTranslated, r.getPoId());
                    continue;
                }

                var begWED = restoreTranslator.translate(i);

                var begID = instructionsMapped.get(begWED).getTransactionId();
                //begID contains the id of the transaction that has the write we need to. Loop on the transaction.
                var wTrans = events.get(begID);
                var wTransIt = wTrans.descendingIterator();

                while(wTransIt.hasNext()){
                    var w = wTransIt.next();
                    if(w.getType() == TransactionalEvent.Type.WRITE && w.getVariable().equals(r.getVariable())){
                        return (WriteTransactionalEvent) w;
                    }
                }


            }
        }
        //This should never happen!
        return null;
    }

    @Override
    public void changeWriteRead(WriteTransactionalEvent w, ReadTransactionalEvent r) {
        var wPast = r.getWriteEvent();
        super.changeWriteRead(w, r);
        if(getDatabaseBacktrackMode() == GuideInfo.BacktrackTypes.RESTORE){

            var wPastBeg = events.get(wPast.getTransactionId()).getFirst();
            var rBeg = events.get(r.getTransactionId()).getFirst();
            var wBeg = events.get(w.getTransactionId()).getFirst();

            restoreHistory.removeWR(r.getVariable(), restoreTranslator.translate(wPastBeg),
                    restoreTranslator.translate(rBeg), r.getPoId());
            if(!restoreHistory.areWR(r.getVariable(), restoreTranslator.translate(wBeg),
                    restoreTranslator.translate(rBeg), r.getPoId()))
                restoreHistory.setWR(r.getVariable(),restoreTranslator.translate(wBeg),
                    restoreTranslator.translate(rBeg), r.getPoId());

        }
    }

    //write, read
    protected Pair<WriteTransactionalEvent, ReadTransactionalEvent> nextSwap(){
        var tr = events.get(events.size()- 1);
        var trListIterator = tr.getListIterator(tr.size() - 1);
        while(trListIterator.hasPrevious()){
            var e = trListIterator.previous();
            if(e.getType() == TransactionalEvent.Type.WRITE){
                WriteTransactionalEvent w = (WriteTransactionalEvent) e;
                for (int j = backtrackPoints.get(w.getEventData()); j >= 0; --j) {
                    //TODO: revisar
                    ReadTransactionalEvent r = readEventsPerVariable.get(w.getVariable()).get(j);

                    //Local read
                    if(r.getWriteEvent().getTransactionId() == r.getTransactionId()) continue;

                    //Cycle
                    if(history.areWRSO_starRelated(r.getTransactionId(), w.getTransactionId())) continue;

                    var p = constructNextHistory(w, r);
                    var h = p._1; var translator = p._2;

                    //We add "r" in a causal manner just to check the property.
                    var fakeWriteID = addCausalEventInHistory(h, r, translator, false);
                    boolean allReads = !swapped(r) &&
                            translator.getID(r.getWriteEvent()).equals(fakeWriteID);

                    if(!allReads) continue;

                    //This is the actual read
                    h.removeWR(r.getVariable(), fakeWriteID, translator.getID(r), r.getPoId());
                    h.setWR(r.getVariable(), translator.getID(w),
                            translator.getID(r), r.getPoId());

                	addTransactionNames(h);
                    if (!h.isConsistent()) continue;

                    //We re-add the read in a causal manner to check consistency for the rest of the reads.
                    h.removeWR(r.getVariable(), translator.getID(w),
                            translator.getID(r), r.getPoId());
                    h.setWR(r.getVariable(), fakeWriteID, translator.getID(r), r.getPoId());


                    var trRead = events.get(r.getTransactionId());
                    var trIteratorR =
                            trRead.getListIterator(r.getObservationSequenceIndex() -
                                    trRead.getFirst().getObservationSequenceIndex() + 1);





                    //events in trRead
                    while(trIteratorR.hasNext()){
                        if(!allReads) break;
                        var evI = trIteratorR.next();
                        //It should be always != -1 !
                        var abortedR = trRead.getLast().getType() == TransactionalEvent.Type.ABORT;
                        var writeIndex = addCausalEventInHistory(h, evI, translator, abortedR);
                        if(evI.getType() == TransactionalEvent.Type.READ){
                            var readI = (ReadTransactionalEvent) evI;
                            allReads = !swapped(readI) &&
                                    translator.getID(readI.getWriteEvent()).equals(writeIndex);
                        }

                    }

                    // Then all the rest deleted events.
                    for(int i = r.getTransactionId() + 1; i < events.size() && allReads; ++i){
                        if(translator.containsID(i)) continue;
                        var transI = events.get(i);
                        var begI = transI.getFirst();
                        var aborted = transI.getLast().getType() == TransactionalEvent.Type.ABORT;
                        translator.putID(begI, h.getNumberTransactions());
                        translator.putSO(begI);
                        h.addTransaction(h.getNumberTransactions(), begI.getThreadId(), translator.getSO(begI));

                        //Every deleted read has to read from latest causal value.
                        for(var evI: transI){
                            //It should be always != -1 !
                            var writeIndex = addCausalEventInHistory(h, evI, translator, aborted);
                            if(evI.getType() == TransactionalEvent.Type.READ){
                                var readI = (ReadTransactionalEvent) evI;
                                allReads = !swapped(readI) && translator.getID(readI.getWriteEvent()).equals(writeIndex);
                            }
                            if(!allReads) break;

                        }
                    }

                    if(allReads){
                        backtrackPoints.put(e.getEventData(),j-1);
                        return new Pair<>(w,r);
                    }
                }
            }
        }

        return null;
    }

    protected Pair<History, TransactionTranslator> constructNextHistory
            (WriteTransactionalEvent w, ReadTransactionalEvent r){
        var h = config.getEssentialInstance("db.database_isolation_level.class", History.class,
                new Class[]{History.class},
                new Object[]{history});

        var trTranslator = new TransactionTranslator(sessionOrder);

        truncateHistoryTransaction(h, r.getTransactionId(), trTranslator, false);

        addCausalDependentTransactions(h, r.getTransactionId() + 1, w.getTransactionId(), trTranslator);

        addFromTransactionUntilEvent(h, r, trTranslator, false);

        return new Pair<>(h, trTranslator);
    }

    protected void truncateHistoryTransaction(History h, int transactionID, TransactionTranslator trTranslator,
                                              boolean include){
        var lastTransaction = include ? transactionID : transactionID - 1;
        for(int i = events.size() -1; i > lastTransaction; --i){
            var threadID = events.get(i).getFirst().getThreadId();
            h.removeLastTransaction();
            trTranslator.removeLastSO(threadID);
        }

        for(int i = 0; i <= lastTransaction; ++i){
            var beg = events.get(i).getFirst();
            trTranslator.putID(beg, beg.getTransactionId());
            //We do not need to add SO!
        }
    }

    protected void addFromTransactionUntilEvent(History h, TransactionalEvent r,
                                                TransactionTranslator trTranslator,
                                                boolean aborted){

        var rTID = r.getTransactionId();

        var beg = events.get(rTID).getFirst();
        trTranslator.putID(beg, h.getNumberTransactions());
        h.addTransaction(h.getNumberTransactions(), beg.getThreadId(),
                trTranslator.getSO(beg));
        trTranslator.putSO(beg);
        for(var e: events.get(rTID)){
            if(e == r) {
                break;
            }
            else{
                addEventInHistory(h, e, trTranslator, aborted);
            }
        }
    }

    protected void addCausalDependentTransactions(History h, int startTransactionID, int writeTransactionID,
                                                  TransactionTranslator trTranslator){
        for(int i = startTransactionID; i < events.size(); ++i){
            var beg = events.get(i).getFirst();
            var thID = beg.getThreadId();
            if(history.areWRSO_starRelated(i, writeTransactionID)){
                trTranslator.putID(beg, h.getNumberTransactions());
                trTranslator.putSO(beg);
                h.addTransaction(h.getNumberTransactions(), thID,
                        trTranslator.getSO(beg));
                var aborted = events.get(i).getLast().getType() ==
                        TransactionalEvent.Type.ABORT;
                h.setCommitted(!aborted, h.getNumberTransactions() - 1);
                for(var e: events.get(i)){
                    addEventInHistory(h, e, trTranslator, aborted);
                }
            }
        }
    }

    protected Integer addCausalEventInHistory(History h, TransactionalEvent e,
                                              TransactionTranslator trTranslator,
                                              boolean aborted){

        switch(e.getType()) {
            case READ:
                var readID = trTranslator.getID(e);
                var r = (ReadTransactionalEvent) e;

                for(int i = h.getNumberTransactions() - 1; i>= 0; --i){
                    if(h.areWRSO_starRelated(i, readID)){

                        if(i == readID && !h.isWrittingVariable(r.getVariable(), i, r.getPoId()))
                            continue;
                        else if (i != readID && !h.isWrittingVariable(r.getVariable(), i))
                            continue;

                        h.setWR(r.getVariable(),
                                i, readID, r.getPoId());
                        if(h.isConsistent()){
                            return i;
                        }
                        else{
                            h.removeWR(r.getVariable(), i, readID, r.getPoId());
                        }
                    }
                }
                return -1;
            case ABORT:
                for(var key : writeEventsPerVariable.keySet()){
                    if(h.isWrittingVariable(key, trTranslator.getID(e)))
                        h.removeWrite(key, trTranslator.getID(e));
                }
            default:
                addEventInHistory(h, e, trTranslator, aborted);
                return trTranslator.getID(e);

        }
    }

    protected void addEventInHistory(History h, TransactionalEvent e,
                                     TransactionTranslator trTranslator,
                                     boolean aborted){
        switch(e.getType()){
            case READ:
                var r = (ReadTransactionalEvent) e;
                h.setWR(r.getVariable(),
                        trTranslator.getID(r.getWriteEvent()),
                        trTranslator.getID(r), r.getPoId());
                break;
            case WRITE:
                if(!aborted) {
                    var w = (WriteTransactionalEvent) e;
                    h.addWrite(w.getVariable(), trTranslator.getID(w), w.getPoId());
                }
            default:
                break;
        }
    }




}
