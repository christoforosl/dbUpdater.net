﻿
using Microsoft.VisualBasic;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.IO;
using System.Reflection;
using System.Threading;
using System.Linq;

class ORADBUpdater : DBUpdater {

	protected internal override string getSQLCommandSeparator() {
		return "\n/\n";
	}

	protected internal override string getSQLCommandFileName(int iVersion) {
		return "dbUpdate_" + Convert.ToString(iVersion) + "_ORA.SQL";
	}

	protected internal override Int32 getVersionAndLockTable() {

		IDataReader rs = default(IDataReader);
		Int32 dbversion = default(Int32);

		rs = dbconn.getDataReader("SELECT version from DatabaseVersion ORDER BY VERSION DESC FOR UPDATE NOWAIT");
		//lock table exclusively
		if (rs.Read()) {
			dbversion = Convert.ToInt32(rs[0]);
		} else {
			throw new ApplicationException("No Database file version info found.");
		}

		dbconn.closeDataReader(rs);
		return dbversion;

	}

}

internal class MSSQLDBUpdater : DBUpdater {

	protected internal override string getSQLCommandSeparator() {
		return  "\ngo\n";
	}

	protected internal override string getSQLCommandFileName(int iVersion) {

		return "dbUpdate_" + Convert.ToString(iVersion) + "_MS.SQL";

	}

	protected internal override Int32 getVersionAndLockTable() {

		IDataReader rs = default(IDataReader);
		Int32 dbversion = default(Int32);

		// the following creates the DatabaseVersion table if it does not exist
		dbconn.executeSQL("if not exists(select * from sysobjects where id=object_id('DatabaseVersion'))CREATE TABLE [dbo].[DatabaseVersion]([version] [int] NOT NULL,[VersionDate] [datetime] NOT NULL DEFAULT (getdate()))");

		rs = dbconn.getDataReader("SELECT isnull(max(version), 0) from DatabaseVersion WITH (TABLOCKX)");
		//lock table exclusively
		if (rs.Read()) {
			dbversion = Convert.ToInt32(rs[0]);
		} else {
			throw new ApplicationException("No Database file version info found.");
		}

		dbconn.closeDataReader(rs);
		return dbversion;

	}

}


/// <summary>
/// Class to fascilitate easy upgrade of a database.
/// </summary>
/// <remarks>
/// Requirements:
/// <ol>
/// <li>Create a table in your database called <b>sysDatabaseVersion</b>
/// with 2 columns: 
/// <ul><li>DatabaseVersion, integer</li>
/// <li>VersionDate, datetime, default: current date (getDate() in MSSQL, 
/// sysdate in Oracle.</li>
/// </ul>
/// </li>
/// <li>
/// Define a constant in your application that defines the database version
/// that your code expects.
/// </li>
/// <li>
/// In your assembly, create a directory where you will place the script files that contain 
/// the sql statements to execute to bring the database to the required version.  The sql statements 
/// must be separated by the "new line" + go + "new Line", ie  the word "go" or "GO" in a line by itself.
/// For oracle, the command separator is "/" on a line by it self.
/// <b>Important: all your sql script files should be marked as <b>embedded resource</b>.</b>
/// Naming conventions: 
/// <ul><li>For MS SQL files: "dbUpdate_&lt;version&gt;_MS.SQL".<br/>For example: dbUpdate_400_MS.SQL</li>
/// <li>For Oracle files: dbUpdate_&lt;version&gt;_ORA.SQL.<br/>For example: dbUpdate_400_ORA.SQL</li>
/// </ul>
/// </li>
/// </ol>
/// 
/// At your system startup, create a new DBUtils object, <see cref="org.model.lib.db.DBUtils">DBUtils object</see>, 
/// and call public shared sub DBUpdater.dbUpdateVersion. See also <seealso cref="DBUpdater.dbUpdateVersion">dbUpdateVersion</seealso>
/// dbUpdateVersion works by comparing the application database version with the database version stored in
/// the sysDatabaseVersion table.  If the application database version is greater than the latest version number 
/// in sysDatabaseVersion, then a loop is executed while  [sysDatabaseVersion] &lt; [application database version]
/// opening sql script files and executing the commands for each version.
/// After each version sql file is done, a new row is inserted in table sysDatabaseVersion, updating the version,
/// until the versions come to the same lebel.
/// </remarks>
public abstract class DBUpdater {

	protected IDbConnection dbconn;
	protected int codeDatabaseVersion;
	protected Assembly assemblyName;
	public System.Text.Encoding encoding { get; set; }

	protected internal abstract Int32 getVersionAndLockTable();

	//the character or string that the sql commands are separated with
	protected internal abstract string getSQLCommandSeparator();
	protected internal abstract string getSQLCommandFileName(int iVersion);

	public event VersionUpgradeCompletedEventHandler VersionUpgradeCompleted;
	public delegate void VersionUpgradeCompletedEventHandler(int iversion);

	/// <summary>
	/// Command to execute for backing up the database.  Only applies for sql server.
	/// </summary>
	public string backupSQLStatement { get; set; }

	private static Stream getResourceStream(string resname, Assembly assembly) {

		string resourceName = assembly.GetManifestResourceNames().FirstOrDefault(xc => { return xc.EndsWith("." + resname); });


		return assembly.GetManifestResourceStream(resourceName);

	}

	private string getResourceFileText(string resname, Assembly assembly) {

		string templ = string.Empty;
		Stream d = getResourceStream(resname, assembly);
		using (StreamReader ds = new StreamReader(d, this.encoding)) {
			string tline = null;
			do {
				tline = ds.ReadLine();
				templ += tline + "\n";

			} while (!(tline == null));

			return templ;

		}

	}


	private void upgradeDatabase() {
		
		string myerrprefix = null;
		int i = 0;
		
		object oLock = new object();
		Int32 dbversion = default(Int32);
		string dbName = null;

		System.Threading.Monitor.Enter(oLock);

		try {
			dbversion = this.getVersionAndLockTable();

			if (dbversion == codeDatabaseVersion) {
				//good!

			} else if (dbversion > codeDatabaseVersion) {
				//Throw New ApplicationException("Bad File Version: " & dbversion & ".  Expected version less than " & codeDatabaseVersion)
				//ErrorLogging.addError("Newer Database Version: " & dbversion & ".  Expected version less than " & codeDatabaseVersion, "", "", ErrorLogging.enumErrType.ERR_INFO)

			} else {
				if (string.IsNullOrEmpty(this.backupSQLStatement) == false) {
					string sqlBackup = string.Format(this.backupSQLStatement, Convert.ToString(dbversion), Convert.ToString(codeDatabaseVersion));
					IDbCommand cmd = dbconn.CreateCommand();
					cmd.CommandText=sqlBackup;
					cmd.ExecuteNonQuery();
					
				}

				myerrprefix = "Error upgrading to version [" + codeDatabaseVersion + "]: ";

				//we have the codeDatabaseVersion constant
				//and we compare it with dbversion. the version stored
				//in the database.  If dbversion is less than codeDatabaseVersion

				while (dbversion < codeDatabaseVersion) {
					string scriptFile = this.getSQLCommandFileName(Convert.ToInt16(dbversion));
					string sqlFile = getResourceFileText(scriptFile, this.assemblyName);
					string[] arrSQL = sqlFile.Split(new[] { this.getSQLCommandSeparator() }, StringSplitOptions.RemoveEmptyEntries);

					
					dbName = dbconn.Database;
					for (i = 0; i <= arrSQL.Length; i++) {
						
						string execSQL = arrSQL[i].Trim();

						if (!string.IsNullOrEmpty(arrSQL[i].Trim().Replace("\n", ""))) {
							
							IDbCommand cmd = dbconn.CreateCommand();
							cmd.CommandText = execSQL;
							cmd.ExecuteNonQuery();
						}
					}

					if (VersionUpgradeCompleted != null) {
						VersionUpgradeCompleted(dbversion);
					}
					dbversion = dbversion + 1;

					dbconn.executeSQL("INSERT INTO DatabaseVersion (version) VALUES ('" + dbversion + "') ");
	
				}
			}


		} catch (Exception ex) {
			string errMsg = scriptFile + Constants.vbCrLf + "Error Updating database \"" + dbName + "\" to version " + dbversion + Constants.vbCrLf + ex.Message + Constants.vbCrLf + ex.StackTrace;
			//ErrorLogging.addError(errMsg, "", "", ErrorLogging.enumErrType.ERR_INFO)
			throw new ApplicationException(errMsg);

		} finally {
			
			System.Threading.Monitor.Exit(oLock);
		}

	}

	#region "Public class interface"

	/// <summary>
	/// Creates an updater class instance and brings the database to the target version
	/// </summary>
	/// <param name="dbconn">Database connection to your database</param>
	/// <param name="_dbversion">The target version</param>
	/// <param name="_backupSQLStatement">SQL to execute before the upgrade to backup database</param>
	/// <param name="_assembly">the assembly that contains the embedded resource sql files</param>

	public static void dbUpdateVersion(IDbConnection dbconn, int _dbversion, Assembly _assembly, System.Text.Encoding encoding = null, string _backupSQLStatement = "") {
		DBUpdater dbu = null;

		dbu.dbconn = dbconn;
		dbu.codeDatabaseVersion = _dbversion;
		dbu.assemblyName = _assembly;
		dbu.encoding = (System.Text.Encoding)(encoding == null ? System.Text.Encoding.UTF8 : encoding);
		
		dbu.upgradeDatabase();

	}

	#endregion



}
