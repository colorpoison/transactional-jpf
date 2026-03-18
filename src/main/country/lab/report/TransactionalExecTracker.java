package country.lab.report;

import country.lab.database.Database;
import country.lab.events.ReadTransactionalEvent;
import country.lab.events.TrEventRegister;
import country.lab.events.TransactionalEvent;
import country.lab.events.WriteTransactionalEvent;
import country.lab.search.TrDFSearch;
import gov.nasa.jpf.Config;
import gov.nasa.jpf.listener.ExecTracker;
import gov.nasa.jpf.search.Search;
import gov.nasa.jpf.vm.*;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;

public class TransactionalExecTracker extends ExecTracker {

    protected TrEventRegister trEventRegister;

    protected PrintWriter out;

    protected boolean actualFile;

    protected int numTotalBranches;

    public TransactionalExecTracker(Config config) {
        super(config);
        trEventRegister = TrEventRegister.getEventRegister();
        String path = config.getString("db.trTracker.out", null);
        if (path == null) {
            out = new PrintWriter(System.out, true);
            actualFile = false;
        } else {
            actualFile = true;
            File file = new File(path);
            try {
                out = new PrintWriter(file);
            } catch (FileNotFoundException e) {
                throw new IllegalArgumentException("File not found");
            }
        }


        numTotalBranches = 0;
        //out = new PrintWriter(System.out, true);


    }

    @Override
    public void gcEnd(VM vm) {
        //super.gcEnd(vm);
    }

    @Override
    public void choiceGeneratorAdvanced(VM vm, ChoiceGenerator<?> currentCG) {
        //super.choiceGeneratorAdvanced(vm, currentCG);
    }

    @Override
    public void threadStarted(VM vm, ThreadInfo ti) {
        //super.threadStarted(vm, ti);
    }

    @Override
    public void threadTerminated(VM vm, ThreadInfo ti) {
        //super.threadTerminated(vm, ti);
    }

    @Override
    public void objectExposed(VM vm, ThreadInfo currentThread, ElementInfo fieldOwnerObject, ElementInfo exposedObject) {
        //super.objectExposed(vm, currentThread, fieldOwnerObject, exposedObject);
    }

    @Override
    public void objectShared(VM vm, ThreadInfo currentThread, ElementInfo sharedObject) {
        //super.objectShared(vm, currentThread, sharedObject);
    }

    @Override
    public void stateProcessed(Search search) {
        if(search instanceof TrDFSearch){
            TrDFSearch trDFSearch = (TrDFSearch) search;
            String msg = trDFSearch.getMessage();
            if(msg != null)
                out.println(msg);
        }
        //super.stateProcessed(search);
        int id = search.getStateId();
        out.println("----------------------------------- [" +
                search.getDepth() + "] done: " + id);
        if(search.isEndState() && Database.getDatabase().isTrulyConsistent()){
            out.println("Branch #"+numTotalBranches+" ended.");
            ++numTotalBranches;
        }
    }

    @Override
    public void stateAdvanced(Search search) {
        var frame = search.getTransition().getThreadInfo().getTopFrame();
        //t.getThreadInfo().getTopFrame()
        var database = Database.getDatabase();
        if(frame != null && trEventRegister.isTransactionalBreakTransition(frame)
                && !database.isMockAccess()){
            TransactionalEvent t = database.getLastEvent();
            out.println(t+ " executed.");
            if(t.getType() == TransactionalEvent.Type.READ){
                if(!TrEventRegister.getEventRegister().isFakeRead()) {
                    WriteTransactionalEvent w = ((ReadTransactionalEvent) t).getWriteEvent();
                    out.println("\t reading from " + t.getVariable() + " = " + w.getValue()+" ("+ w.getBaseName()+").");
                }
            }
        }
        //super.stateAdvanced(search);
        int id = search.getStateId();

        out.print("----------------------------------- [" +
                search.getDepth() + "] forward: " + id);
        if (search.isNewState()) {
            out.print(" new");
        } else {
            out.print(" visited");
        }

        if (search.isEndState()) {
            out.print(" end");
        }

        out.println();
    }

    @Override
    public void searchFinished(Search search) {
        //super.searchFinished(search);
        out.println("----------------------------------- search finished");
        if(actualFile)
            out.close();
    }

    @Override
    public void searchStarted(Search search) {
        //super.searchStarted(search);
        out.println("----------------------------------- search started");
        /*if (skipInit) {
            ThreadInfo tiCurrent = ThreadInfo.getCurrentThread();
            miMain = tiCurrent.getEntryMethod();

            out.println("      [skipping static init instructions]");
        }
         */
    }

    @Override
    public void stateBacktracked(Search search) {
        //super.stateBacktracked(search);

        int id = search.getStateId();
        out.println("----------------------------------- [" +
                search.getDepth() + "] backtrack: " + id);
    }
    
    @Override
    public void exceptionThrown (VM vm, ThreadInfo ti, ElementInfo ei) {
        MethodInfo mi = ti.getTopFrameMethodInfo();
        if(!mi.getFullName().equals("database.APIDatabase.abort()V")){
            super.exceptionThrown(vm,ti,ei);
        }
    }
}
