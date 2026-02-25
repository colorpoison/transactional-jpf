package benchmarks.wikipedia.procedures;

import benchmarks.wikipedia.Wikipedia;
import database.APIDatabase;

public class RemoveWatchList extends WikipediaProcedure {
    public RemoveWatchList(APIDatabase db) {
        super(db);
    }

    public void removeWatchList(int userID, int nameSpace, String pageTitle){
        db.begin("RemoveWatchList");

        if(userID > 0){
            deleteWatchList(userID, nameSpace, pageTitle);

            if(nameSpace == 0){
                deleteWatchList(userID, 1, pageTitle);
            }

            updateUserTouched(userID);
        }

        db.commit();
    }

    private void deleteWatchList(int userID, int nameSpace, String pageTitle){

        db.deleteRow(Wikipedia.WATCHLIST, userID +":"+nameSpace+":"+pageTitle);
    }
}
