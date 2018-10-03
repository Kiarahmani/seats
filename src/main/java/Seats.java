import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map.Entry;
import java.util.Properties;

import api.Printer;
import api.Row;

import org.omg.CORBA.COMM_FAILURE;

public class Seats {
	private Connection connect = null;
	private Statement statement = null;
	private PreparedStatement preparedStatement = null;
	private ResultSet resultSet = null;
	private int _ISOLATION = Connection.TRANSACTION_REPEATABLE_READ;
	private int insID;
	Properties p;

	public Seats(int insID) {
		this.insID = insID;
		p = new Properties();
		p.setProperty("ID", String.valueOf(insID));
		p.setProperty("dbName", "seats");
	}

	// ----------------------------------------------------------------------------------------------------------------------
	// ----------------------------------------------------------------------------------------------------------------------
	public void initialize() throws Exception {

		try {

			Object o = Class.forName("MyDriver").newInstance();
			DriverManager.registerDriver((Driver) o);
			Driver driver = DriverManager.getDriver("jdbc:mydriver://");
			connect = driver.connect(null, p);
			connect.setAutoCommit(false);
			connect.setTransactionIsolation(_ISOLATION);

			//
			//
			//
			//
			//

			connect.commit();
		} catch (Exception e) {
			throw e;
		} finally {
			close();
		}

	}

	// ----------------------------------------------------------------------------------------------------------------------
	// ----------------------------------------------------------------------------------------------------------------------

	public void updateReservation(long r_id, long f_id, long c_id, long seatnum, long attr_idx, long attr_val)
			throws Exception {

		try {
			Object o = Class.forName("MyDriver").newInstance();
			List<Row> table = new ArrayList<Row>();
			DriverManager.registerDriver((Driver) o);
			Driver driver = DriverManager.getDriver("jdbc:mydriver://");
			connect = driver.connect(null, p);
			connect.setAutoCommit(false);
			connect.setTransactionIsolation(_ISOLATION);
			assert (attr_idx >= 0);
			boolean found;
			PreparedStatement stmt = null;
			ResultSet results = null;
			stmt = connect.prepareStatement(("SELECT R_ID " + "  FROM " + SEATSConstants.TABLENAME_RESERVATION
					+ " WHERE R_F_ID = ? and R_SEAT = ?"));
			stmt.setLong(1, f_id);
			stmt.setLong(2, seatnum);
			results = stmt.executeQuery();
			found = results.next();
			results.close();
			if (found)
				throw new Exception(String.format(" Seat %d is already reserved on flight #%d", seatnum, f_id));
			stmt = connect.prepareStatement("SELECT R_ID " + "  FROM " + SEATSConstants.TABLENAME_RESERVATION
					+ " WHERE R_F_ID = ? AND R_C_ID = ?");
			stmt.setLong(1, f_id);
			stmt.setLong(2, c_id);
			results = stmt.executeQuery();
			found = results.next();
			results.close();
			if (found == false)
				throw new Exception(
						String.format(" Customer %d does not have an existing reservation on flight #%d", c_id, f_id));

			String BASE_SQL = "UPDATE " + SEATSConstants.TABLENAME_RESERVATION + "   SET R_SEAT = ?, %s = ? "
					+ " WHERE R_ID = ? AND R_C_ID = ? AND R_F_ID = ?";
			final String ReserveSeat0 = "R_IATTR00";
			final String ReserveSeat1 = "R_IATTR01";
			final String ReserveSeat2 = "R_IATTR02";
			final String ReserveSeat3 = "R_IATTR03";
			String ReserveSeats[] = { ReserveSeat0, ReserveSeat1, ReserveSeat2, ReserveSeat3 };
			stmt = connect.prepareStatement(String.format(BASE_SQL, ReserveSeats[(int) attr_idx]));
			stmt.setLong(1, seatnum);
			stmt.setLong(2, attr_val);
			stmt.setLong(3, r_id);
			stmt.setLong(4, c_id);
			stmt.setLong(5, f_id);
			int updated = stmt.executeUpdate();
			if (updated != 1)
				throw new Exception(
						String.format("Failed to update reservation on flight %d for customer #%d - Updated %d records",
								f_id, c_id, updated));

			connect.commit();
		} catch (Exception e) {
			throw e;
		} finally {
			close();
		}

	}

	// ----------------------------------------------------------------------------------------------------------------------
	// ----------------------------------------------------------------------------------------------------------------------
	public void findFlights() throws Exception {
		try {
			Object o = Class.forName("MyDriver").newInstance();
			DriverManager.registerDriver((Driver) o);
			Driver driver = DriverManager.getDriver("jdbc:mydriver://");
			connect = driver.connect(null, p);
			connect.setAutoCommit(false);
			connect.setTransactionIsolation(_ISOLATION);

			//
			//
			//
			//
			//

			connect.commit();
		} catch (Exception e) {
			throw e;
		} finally {
			close();
		}

	}

	// ----------------------------------------------------------------------------------------------------------------------
	// ----------------------------------------------------------------------------------------------------------------------
	public void findOpenSeats() throws Exception {
		try {
			Object o = Class.forName("MyDriver").newInstance();
			DriverManager.registerDriver((Driver) o);
			Driver driver = DriverManager.getDriver("jdbc:mydriver://");
			connect = driver.connect(null, p);
			connect.setAutoCommit(false);
			connect.setTransactionIsolation(_ISOLATION);

			//
			//
			//
			//

			connect.commit();
		} catch (Exception e) {
			throw e;
		} finally {
			close();
		}

	}

	// ----------------------------------------------------------------------------------------------------------------------
	// ----------------------------------------------------------------------------------------------------------------------
	public void newReservation() throws Exception {
		try {
			Object o = Class.forName("MyDriver").newInstance();
			DriverManager.registerDriver((Driver) o);
			Driver driver = DriverManager.getDriver("jdbc:mydriver://");
			connect = driver.connect(null, p);
			connect.setAutoCommit(false);
			connect.setTransactionIsolation(_ISOLATION);

			//
			//
			//
			//

			connect.commit();
		} catch (Exception e) {
			throw e;
		} finally {
			close();
		}

	}

	// ----------------------------------------------------------------------------------------------------------------------
	// ----------------------------------------------------------------------------------------------------------------------
	public void updateCustomer(Long c_id, String c_id_str, Long update_ff, long attr0, long attr1) {
		try {

			Object o = Class.forName("MyDriver").newInstance();
			DriverManager.registerDriver((Driver) o);
			Driver driver = DriverManager.getDriver("jdbc:mydriver://");
			connect = driver.connect(null, p);
			connect.setAutoCommit(false);
			connect.setTransactionIsolation(_ISOLATION);
			PreparedStatement stmt;
			ResultSet rs;
			// Use C_ID_STR to get C_ID

			if (c_id == null) {

				assert (c_id_str != null);
				assert (c_id_str.isEmpty() == false);

				stmt = connect.prepareStatement(
						"SELECT C_ID " + "  FROM " + SEATSConstants.TABLENAME_CUSTOMER + " WHERE C_ID_STR = ? ");
				stmt.setString(1, c_id_str);
				rs = stmt.executeQuery();
				if (rs.next()) {
					c_id = rs.getLong(1);
				} else {
					rs.close();
					throw new Exception(
							String.format("No Customer information record found for string '%s'", c_id_str));
				}
				rs.close();
			}
			assert (c_id != null);

			stmt = connect
					.prepareStatement("SELECT * " + "  FROM " + SEATSConstants.TABLENAME_CUSTOMER + " WHERE C_ID = ? ");
			stmt.setLong(1, c_id);
			rs = stmt.executeQuery();
			if (rs.next() == false) {
				rs.close();
				throw new Exception(String.format("No Customer information record found for id '%d'", c_id));
			}
			assert (c_id == rs.getLong(1));
			long base_airport = rs.getLong(3);
			rs.close();

			// Get their airport information
			stmt = connect.prepareStatement("SELECT * " + "  FROM " + SEATSConstants.TABLENAME_AIRPORT + ", "
					+ SEATSConstants.TABLENAME_COUNTRY + " WHERE AP_ID = ? AND AP_CO_ID = CO_ID ");
			stmt.setLong(1, base_airport);
			ResultSet airport_results = stmt.executeQuery();
			boolean adv = airport_results.next();
			airport_results.close();
			assert (adv);

			if (update_ff != null) {
				stmt = connect.prepareStatement(
						"SELECT * FROM " + SEATSConstants.TABLENAME_FREQUENT_FLYER + " WHERE FF_C_ID = ?");
				stmt.setLong(1, c_id);
				ResultSet ff_results = stmt.executeQuery();
				while (ff_results.next()) {
					long ff_al_id = ff_results.getLong(2);
					stmt = connect.prepareStatement(
							"UPDATE " + SEATSConstants.TABLENAME_FREQUENT_FLYER + "   SET FF_IATTR00 = ?, "
									+ "       FF_IATTR01 = ? " + " WHERE FF_C_ID = ? " + "   AND FF_AL_ID = ? ");
					stmt.setLong(1, attr0);
					stmt.setLong(2, attr1);
					stmt.setLong(3, c_id);
					stmt.setLong(4, ff_al_id);
					stmt.executeUpdate();
				} // WHILE
				ff_results.close();
				stmt = connect.prepareStatement("UPDATE " + SEATSConstants.TABLENAME_CUSTOMER + "   SET C_IATTR00 = ?, "
						+ "       C_IATTR01 = ? " + " WHERE C_ID = ?");
				stmt.setLong(1, attr0);
				stmt.setLong(2, attr1);
				stmt.setLong(3, c_id);
				int updated = stmt.executeUpdate();
				if (updated != 1) {
					String msg = String.format("Failed to update customer #%d - Updated %d records", c_id, updated);
					throw new Exception(msg);
				}
			}

			connect.commit();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			close();
		}
	}

	// ----------------------------------------------------------------------------------------------------------------------
	// ----------------------------------------------------------------------------------------------------------------------
	public void deleteReservation() throws Exception {
		try {
			Object o = Class.forName("MyDriver").newInstance();
			DriverManager.registerDriver((Driver) o);
			Driver driver = DriverManager.getDriver("jdbc:mydriver://");
			connect.setAutoCommit(false);
			connect = driver.connect(null, p);

			//
			//
			//
			//
			//

			connect.commit();
		} catch (Exception e) {
			throw e;
		} finally {
			close();
		}

	}

	private void close() {
		try {
			if (resultSet != null)
				resultSet.close();
			if (statement != null)
				statement.close();
			if (connect != null)
				connect.close();
		} catch (Exception e) {

		}
	}

}
