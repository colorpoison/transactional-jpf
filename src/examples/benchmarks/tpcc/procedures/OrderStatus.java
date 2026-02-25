package benchmarks.tpcc.procedures;

import benchmarks.tpcc.TPCC;
import benchmarks.tpcc.TPCCUtility;
import benchmarks.tpcc.objects.Customer;
import benchmarks.tpcc.objects.Order;
import database.APIDatabase;
import database.AbortDatabaseException;

import java.util.ArrayList;
import java.util.List;

public class OrderStatus extends BasicTPCCProcedure{

    public OrderStatus(APIDatabase db){
        super(db);
    }

    //all but warehouseID are random!!!
    public String orderStatus(int warehouseID, int districtID, int customerID,
                                     String customerName, boolean customerIDSearch){
        try {
            db.begin("OrderStatus");


            var c = getCustomer
                    (warehouseID, districtID, customerID, customerName, customerIDSearch);


            Order o = getOrderDetails(warehouseID, districtID, c);

            var oll = getOrderLines(warehouseID, districtID, o);

            db.commit();


            StringBuilder sb = new StringBuilder();
            sb.append("ORDER STATUS:");
            sb.append("Warehouse: ").append(warehouseID);
            sb.append("District: ").append(districtID);


            sb.append("Client: ").append(c);

            if(o != null) {
                sb.append("Order: ").append(o);
                if(oll != null){
                    sb.append("Number of order lines: ").append(oll.size());
                    for (int i = 0; i < oll.size(); ++i) {
                        sb.append("Order line ").append(i).append(": ").append(oll.get(i));
                    }
                }
            }
            else{
                sb.append("No open orders available");
            }


            return sb.toString();
        } catch (AbortDatabaseException ignored) {

        }
        return null;
    }

    private Order getOrderDetails(int warehouseID, int districtID, Customer c) throws AbortDatabaseException {

        var oLSt = db.readIfIDStartsWith(TPCC.OPENORDER, warehouseID+":"+districtID);
        var orders = new ArrayList<Order>();
        for(var oSt : oLSt){
            if(oSt == null) db.abort();
            orders.add(new Order(oSt));
        }

        if(c == null) return null; //The transaction must always be executed.

        orders.sort((a, b)->(int) (b.getID() - a.getID()));
        for(var o: orders){
            if(o.getCustomerID() == c.getID()) return o;
        }
        db.abort();
        return null;

    }

    private List<String> getOrderLines(int warehouseID, int districtID, Order o){

        return db.readIfIDStartsWith(TPCC.ORDERLINE, warehouseID+":"+districtID+":"+o.getID());

    }


}
