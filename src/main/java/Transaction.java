import java.sql.Timestamp;
import java.util.Properties;

import com.mysql.jdbc.exceptions.jdbc4.MySQLTransactionRollbackException;

public class Transaction {
	private Seats dao;
	private int insID;
	private Properties p;

	// constructor
	public Transaction(int insID, Properties p) {
		this.insID = insID;
		dao = new Seats(insID);
		this.p = p;

	}

	// run the transaction wrapper
	public void run(String opType) {

		try {
			if (opType.equals("updateReservation")) {
				int c_id = Integer.valueOf(p.getProperty("c_id"));
				dao.updateReservation(c_id * 1111, 1, c_id, 13, 1, 666);
			} else if (opType.equals("deleteReservation"))
				dao.deleteReservation(1001,null,"kiarash",null,22L);
			else if (opType.equals("init"))
				dao.initialize();
			else if (opType.equals("updateCustomer")) {
				int c_id = Integer.valueOf(p.getProperty("c_id"));
				dao.updateCustomer(null, "kiarash", 10L, c_id, c_id);
			} else if (opType.equals("newReservation")) {
				long attrs[] = { 666L, 666L, 666L, 666L, 666L, 666L, 666L, 666L, 666L };
				int c_id = Integer.valueOf(p.getProperty("c_id"));
				dao.newReservation(c_id*3017, c_id, 1001, c_id*50, 540, attrs);
			} else if (opType.equals("findOpenSeats"))
				dao.findOpenSeats(1001);
			else if (opType.equals("findFlights"))
				dao.findFlights(10, 20, 1000, 2000, 20);
			else
				System.err.println("Unknown Operaition Type");
		} catch (MySQLTransactionRollbackException e) {
			// retry
			try {
				Thread.sleep(10);
				// System.out.println(".");
			} catch (InterruptedException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			run(opType);
		} catch (Exception e) {
			System.out.println(e);
			// TODO Auto-generated catch block

		}
	}
}
