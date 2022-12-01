package com.wasacoder;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;

public class DataStreamJob {

	public static void main(String[] args) throws Exception {
		EnvironmentSettings settings = EnvironmentSettings
		.newInstance()
		// .inStreamingMode()
		.inBatchMode()
		.build();

		TableEnvironment tEnv = TableEnvironment.create(settings);
		String createAqcProject = "CREATE TABLE AqcProject (id VARCHAR, aqc_no VARCHAR, PRIMARY KEY(id) NOT ENFORCED) WITH \n"+
			" ('connector' = 'filesystem'\n" +
			" ,'path' = 'file:///Users/wangxq/csv/aqc_project.csv'\n" +
			" ,'format' = 'csv')";
		tEnv.executeSql(createAqcProject);

		String createAqcProjectLoan = "CREATE TABLE AqcProjectLoan (id VARCHAR, aqc_project_id VARCHAR, loan_no VARCHAR, PRIMARY KEY(id) NOT ENFORCED) WITH \n"+
			" ('connector' = 'filesystem'\n" +
			" ,'path' = 'file:///Users/wangxq/csv/aqc_project_loan.csv'\n" +
			" ,'format' = 'csv')";
		tEnv.executeSql(createAqcProjectLoan);

		String createLoanAccount = "CREATE TABLE LoanAccount (id VARCHAR, loan_no VARCHAR, balance_amt DOUBLE, PRIMARY KEY(id) NOT ENFORCED) WITH \n"+
			" ('connector' = 'filesystem'\n" +
			" ,'path' = 'file:///Users/wangxq/csv/loan_account.csv'\n" +
			" ,'format' = 'csv')";
		tEnv.executeSql(createLoanAccount);


		TableResult AqcProject = tEnv.sqlQuery("SELECT * FROM AqcProject").execute();
		TableResult AqcProjectLoan = tEnv.sqlQuery("SELECT * FROM AqcProjectLoan").execute();
		TableResult LoanAccount = tEnv.sqlQuery("SELECT * FROM LoanAccount").execute();

		String joinQuery = "SELECT p.aqc_no, sum(loan.balance_amt) as balance FROM AqcProject p \n" +
		" join AqcProjectLoan pl on p.id = pl.aqc_project_id \n" +
		" join LoanAccount loan on pl.loan_no = loan.loan_no \n" +
		" group by p.aqc_no";
		TableResult AqcProjectLoanRelation = tEnv.sqlQuery(joinQuery).execute();

		AqcProject.print();
		AqcProjectLoan.print();
		LoanAccount.print();
		AqcProjectLoanRelation.print();



		String createAqcProjectBalance = "CREATE TABLE AqcProjectBalance (aqc_project_no VARCHAR, balance_amt DOUBLE) WITH \n"+
			" ('connector' = 'filesystem'\n" +
			" ,'path' = 'file:///Users/wangxq/csv'\n" +
			" ,'format' = 'csv')";
		tEnv.executeSql(createAqcProjectBalance);
		tEnv.sqlQuery(joinQuery).executeInsert("AqcProjectBalance");
	}
}
