/*
 * @author KB5059630
 */

import java.sql.*;

import oracle.jdbc.rowset.OracleCachedRowSet;
public class ProjectCRUD2 {

	public static void main(String[] args) {
		
		System.out.println("===Create===");
		insertRow("P1", "ProjectPapa", 500);
		
		System.out.println("===Delete===");
		deleteRow("P1");
		
		System.out.println("===Create (mass)===");
		String[][] pjcts = {{"P1", "ProjectPapa", "500"},
				{"P2", "SeizureKill", "1500"},
				{"P3", "SukiNewName", "700"},
				{"P4", "OciCommission", "75"},
				{"P5", "Project GEM", "500"}};		
		insertRows(pjcts);
		
		System.out.println("===Retrieve===");
		getRow("P1");
		
		System.out.println("===Update===");
		updateRow("P1", "Project Papa", 2500);
		
		System.out.println("===Retrieve (partial)===");
		getName("P1");
		
		System.out.println("===Update (mass)===");
		String[][] pjctss = {{"P2", "SeizureKill", "500"},
				{"P5", "Project GEM", "1500"}};	
		updateRows(pjctss);
		
		System.out.println("===Retrieve (mass)===");
		getAllRows();
		
		System.out.println("===Delete (all)===");
		deleteRows(new String[] {"P1", "P2"});
		
		System.out.println("===Delete (all)===");
		deleteAllRows();
		
	}

	/* Connection generation
	 * @return a connection object to the hr database
	 */
	public static Connection makeConnection() {
		try {
		    //Class.forName("oracle.jdbc.driver.OracleDriver");
			DriverManager.registerDriver (new oracle.jdbc.driver.OracleDriver());
			Connection con = DriverManager.getConnection("jdbc:oracle:thin:@localhost:1521:XE","hr","hr");
		    return con;
		} catch (Exception ex) {
            System.out.println(ex);
            return null;
       }
	}
	
	/* Safe? command execution, closing all resources
	 * @param sql - a string containing the SQL statement to be executed
	 * @param message - a string to be printed after the statement is executed but before results are printed
	 * @param printRecord - boolean indicating if the result set is to be printed
	 */
	public static void execCommand(String sql, String message, boolean printRecord) {
		try {
			Connection con = makeConnection();
			Statement st = con.createStatement();
			ResultSet rs = st.executeQuery(sql);
			
			System.out.println(message);
			
			if(printRecord) {
			  while(rs.next()) 
				  System.out.println(rs.getString("project_id")+ "            "+ rs.getString(2) + "      " + rs.getDouble(3));
			} 			
			
            rs.close();
			st.close();
	    	con.close();
		} catch (Exception ex) {
            System.out.println(ex);
		}
		
	}
		
	/* Insert a single row
	 * @param projectID - value for project_id column
	 * @param projectName - value for project_name column
	 * @param resources - value for resources column
	 */
	public static void insertRow(String projectID, String projectName, int resources) {  //create
		try {
			Connection con = makeConnection(); 
			CallableStatement cs = con.prepareCall("{call sp_addProject(?,?,?)}");
			
			cs.setString(1, projectID);
			cs.setString(2, projectName);
			cs.setInt(3, resources);
			
			cs.execute();
			cs.close();
			con.close();
			
			System.out.println("Record for project " + projectID + " created.");
		} catch(Exception e) {System.out.println(e);}
	}
	
	/* Insert multiple rows
	 * @param projects - Array of arrays describing projects. Each interior array is [project_id, project_name, STRING OF revenue]
	 */
	public static void insertRows(String[][] projects) {
		for(String[] pjct : projects) {
			insertRow(pjct[0], pjct[1], Integer.parseInt(pjct[2]));
		}
	}
	
	//TODO make funcation not SP	
	public static void getName(String projectID) {  //read
		try {
			Connection con = makeConnection(); 
			CallableStatement cs = con.prepareCall("{call sp_getProjectName(?, ?)}");
			
			cs.setString(1, projectID);
			cs.registerOutParameter(2, java.sql.Types.VARCHAR);
			
			cs.execute();
			System.out.println("project ID " + projectID + " has name " + cs.getString(2));
			
			cs.close();
			con.close();
		
		} catch(Exception e) {System.out.println(e);}
	}
	
	/* Print single row
	 * @param projectID - the target project_id
	 */
	public static void getRow(String projectID) {  //read
		try {
			Connection con = makeConnection(); 
			CallableStatement cs = con.prepareCall("{call sp_getProject(?, ?, ?)}");
			
			cs.setString(1, projectID);
			cs.registerOutParameter(2, java.sql.Types.VARCHAR);
			cs.registerOutParameter(3, java.sql.Types.INTEGER);
			
			cs.execute();
			System.out.println("project ID " + projectID + " has name " + cs.getString(2) + " and resources " + cs.getInt(3));
			
			cs.close();
			con.close();
		
		} catch(Exception e) {System.out.println(e);}
	}
	
	/* Print all rows
	 * Retrieves and prints all records from the DB
	 */
	public static void getAllRows() {
		OracleCachedRowSet crs;

	    try{
		    crs = new OracleCachedRowSet();
	
		    crs.setUrl("jdbc:oracle:thin:@localhost:1521:XE");
		    crs.setUsername("hr");
		    crs.setPassword("hr");
		    crs.setReadOnly(true);
		    crs.setCommand("select * from project");
	       
		    crs.execute();
		    System.out.println("Selected Records: ");
		    while (crs.next()) {
		    System.out.println(crs.getString(1)+"      "+crs.getString(2)+"      "+crs.getInt(3));
		    }
	    }catch(Exception e) {System.out.println(e);} 
	    
		//String query = "SELECT * FROM project ORDER BY project_id";
		//String message = "Project ID    Project Name     Resources\n________________________________________________";
	}
	
	//TODO - get specific rows? Using WHERE?
	
	/* Update single row
	 * @param projectID - value for project_id column
	 * @param projectName - value for project_name column
	 * @param resources - value for resources column
	 */
	public static void updateRow(String projectID, String projectName, int resources) { //update
		try {
			Connection con = makeConnection(); 
			CallableStatement cs = con.prepareCall("{call sp_updateProject(?,?,?)}");
			
			cs.setString(1, projectID);
			cs.setString(2, projectName);
			cs.setInt(3, resources);
			
			cs.execute();
			cs.close();
			con.close();
			
			System.out.println("Record for project " + projectID + " updated.");
		} catch(Exception e) {System.out.println(e);}
	}
	
	/* Insert multiple rows
	 * @param projects - Array of arrays describing projects. Each interior array is [project_id, project_name, STRING OF revenue]
	 */
	public static void updateRows(String[][] projects) {
		for(String[] pjct : projects) {
			updateRow(pjct[0], pjct[1], Integer.parseInt(pjct[2]));
		}
	}
		
	/* Delete single row
	 * @param projectID - the target project_id
	 */
	public static void deleteRow(String projectID) { //delete
		try {
			Connection con = makeConnection(); 
			CallableStatement cs = con.prepareCall("{call sp_deleteProject(?)}");
			
			cs.setString(1, projectID);
			
			cs.execute();
			cs.close();
			con.close();
			
			System.out.println("Record for project " + projectID + " deleted.");
		} catch(Exception e) {System.out.println(e);}
	}
	
	/* Delete some rows in the table
	 * @param projectIDs - the project_ids to delete
	 */
	public static void deleteRows(String[] projectIDs) {
		for(String pid : projectIDs) {
			deleteRow(pid);
		}
	}

	/*
	 * Deletes all rows in the table
	 */
	public static void deleteAllRows() {
		try {
			Connection con = makeConnection();
			Statement st = con.createStatement();
			ResultSet rs = st.executeQuery("TRUNCATE TABLE project");
			
			System.out.println("project table truncated");
			
            rs.close();
			st.close();
	    	con.close();
		} catch (Exception ex) {
            System.out.println(ex);
		}
	}
}

/* SQL to make table:
CREATE TABLE hr.project (
  project_id    VARCHAR2(5) PRIMARY KEY,
  project_name  VARCHAR2(25),
  resources     NUMBER
);
 */
