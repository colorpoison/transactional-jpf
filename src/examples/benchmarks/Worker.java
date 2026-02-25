package benchmarks;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Class representing a session. A session execute several transactions, one after another.
 * In its construction receives the file with the specific transaction's names. Then, it runs
 * them one after the other invoking the homonymous method in the bencharkModule.
 * @param <T>
 */
public class Worker<T extends BenchmarkModule> implements Runnable{

    protected List<List<String>> listTransactions;

    protected final T benchmarkModule;

    private final int maxBufferSize = 128;

    public Worker(T benchmarkModule, String filename) throws IOException {
        this.benchmarkModule = benchmarkModule;
        listTransactions = prepareInfo(filename);
    }

    /**
     * Read all transactions' names and args from the file.
     * @param filename
     * @return
     * @throws IOException
     */
    protected List<List<String>> prepareInfo(String filename) throws IOException {

        var info = new ArrayList<List<String>>();
        var file = new File(filename);

        FileInputStream fis = new FileInputStream(file);
        FileChannel fis_fc = fis.getChannel();
        var totalBytes = file.length();
        StringBuilder sb = new StringBuilder();
        while(totalBytes > maxBufferSize){
            ByteBuffer buf_read = ByteBuffer.allocate(maxBufferSize - 1);
            totalBytes -= (maxBufferSize - 1);
            fis_fc.read(buf_read);
            var s = new String(buf_read.array());

            sb.append(s);
        }
        ByteBuffer buf_read = ByteBuffer.allocate((int) totalBytes);
        fis_fc.read(buf_read);
        var s = new String(buf_read.array());
        sb.append(s);
        fis.close();

        var lines = sb.toString().split("\n");

        for(var line: lines) {
            //One level of nesting
            //name of methods dont have " character, so if it appears it's in a name.
            //Therefore, every odd index it's a real string while the rest is unknown.
            var stringDetected = line.split("\"");
            ArrayList<String> tr = new ArrayList<>();
            for(int i = 0; i < stringDetected.length; ++i){
                if(i%2 == 1){
                    tr.add(stringDetected[i]);
                }
                else{
                    var trSplit = stringDetected[i].trim().split("( )(?![^\\[]*\\])");
                    Collections.addAll(tr, trSplit);
                }
            }

            //var tr = line.split("( )(?![^\\[]*\\])");
            info.add(tr);
        }
        return info;

    }

    @Override
    public void run() {

    	System.out.println(listTransactions.toString());
        var benchmarkInfo = benchmarkModule.getAllMethods();
        try {
            for(var t : listTransactions){
                if(t.size() == 0) {
                    throw new NoSuchMethodException("Instruction with no transaction");
                }
                else if(!benchmarkInfo.containsKey(t.get(0))){
                    throw new NoSuchMethodException("Method "+ t.get(0)+" not defined for benchmark "+ benchmarkModule);
                }
                var args = t.subList(1, t.size());
                benchmarkModule.executeTransaction(t.get(0), args);
            }
        }catch (NoSuchMethodException e) {
            e.printStackTrace();
        } catch (InvocationTargetException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (InstantiationException e) {
            e.printStackTrace();
        } catch (Exception e){
            e.printStackTrace();
        }
    }
}
