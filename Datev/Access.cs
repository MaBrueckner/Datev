using Microsoft.Win32;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.OleDb;
using System.IO;
using System.Linq;
using ErrorHandler;
using System.Data.SqlClient;

namespace Datev
{
    public class Access
    {
        #region Variables
        private string _path;
        private string _oleDB;
        private string _version;
        private string _provider = "Provider=Microsoft.ACE.OLEDB.";
        private readonly string _source = "Data Source=";

        private bool _isConnected;

        private OleDbConnection _con;
        private ErrHndl errHndl = new ErrHndl();
        #endregion //Variables

        #region Constructors
        public Access(string path)
        {
            _version = GetDBversion();

            if (_version == "")
            {
                _provider = "Provider=Microsoft.Jet.OLEDB.";
                _version = "4.0";
            }

            _isConnected = false;
            _path = path;
            _oleDB = $"{_provider}{_version}; {_source}{_path}";
        }

        public Access(string path, string version)
        {
            _version = version;
            _isConnected = false;
            _path = path;
            _oleDB = $@"{_provider}{_version}; {_source}{_path}";
        }
        #endregion //Constructors

        #region Functions
        #region Connect
        /// <summary>
        /// Öffnet die Verbindung zu einer Access-Datenbank
        /// </summary>
        /// <returns>Fehler</returns>
        public ErrHndl.Error Connect()
        {
            ErrHndl.Error error = errHndl.Init_Error();

            try
            {
                if (File.Exists(_path))
                {
                    if (_con == null)
                    {
                        _con = new OleDbConnection(_oleDB);
                        _con.Open();
                    }
                    else if (_con != null && _con.State != ConnectionState.Open)
                        _con.Open();

                    if (_con != null)
                        _isConnected = _con.State == ConnectionState.Open;
                }
                else
                {
                    error.bError = true;
                    error.iCode = -1;
                    error.sMessage = $"{DateTime.Now} - DatevDLL_Access-Connect - Datenbankdatei existiert nicht";
                }
            }
            catch(Exception ex)
            {
                error.bError = true;
                error.iCode = ex.HResult;
                error.sMessage = ex.Message;
            }
            finally
            {
                if(!_isConnected && !error.bError)
                {
                    error.bError = true;
                    error.iCode = -1;
                    error.sMessage = $"{DateTime.Now} - DatevDLL_Access-Connect - Verbindung wurde nicht hergestellt";
                }
            }

            return error;
        }

        /// <summary>
        /// Öffnet die Verbindung zu einer Access-Datenbank
        /// </summary>
        /// <param name="password">Passwort</param>
        /// <returns>Fehler</returns>
        public ErrHndl.Error Connect(string password)
        {
            ErrHndl.Error error = errHndl.Init_Error();

            _oleDB += $"; Jet OLEDB:Database Password={password}";

            try
            {
                if (File.Exists(_path))
                {
                    if (_con == null)
                    {
                        _con = new OleDbConnection(_oleDB);
                        _con.Open();
                    }
                    else if (_con != null && _con.State != ConnectionState.Open)
                        _con.Open();

                    if (_con != null)
                        _isConnected = _con.State == ConnectionState.Open;
                }
                else
                {
                    error.bError = true;
                    error.iCode = -1;
                    error.sMessage = $"{DateTime.Now} - DatevDLL_Access-Connect - Datenbankdatei existiert nicht";
                }
            }
            catch (OleDbException ex)
            {
                error.bError = true;
                error.iCode = ex.HResult;
                error.sMessage = ex.Message;
            }
            catch (Exception ex)
            {
                error.bError = true;
                error.iCode = ex.HResult;
                error.sMessage = ex.Message;
            }
            finally
            {
                if (!_isConnected && !error.bError)
                {
                    error.bError = true;
                    error.iCode = -1;
                    error.sMessage = $"{DateTime.Now} - DatevDLL_Access-Connect - Verbindung wurde nicht hergestellt";
                }
            }

            return error;
        }
        #endregion //Connect

        #region Disconnect
        /// <summary>
        /// Trennt die Verbindung zur Access-Datenbank
        /// </summary>
        /// <returns>Fehler</returns>
        public ErrHndl.Error Disconnect()
        {
            ErrHndl.Error error = errHndl.Init_Error();

            try
            {
                if (_con != null && _con.State == ConnectionState.Open)
                {
                    _con.Close();
                    _con.Dispose();
                }

                _isConnected = false;
            }
            catch (OleDbException ex)
            {
                error.bError = true;
                error.iCode = ex.HResult;
                error.sMessage = ex.Message;
            }
            catch (Exception ex)
            {
                error.bError = true;
                error.iCode = ex.HResult;
                error.sMessage = ex.Message;
            }

            return error;
        }
        #endregion //Disconnect

        #region State
        /// <summary>
        /// Abrufen des aktuellen Verbindungsstatus zur Access Datenbank
        /// </summary>
        /// <returns>Verbunden?</returns>
        public bool GetConState()
        {
            if (_con != null)
                _isConnected = _con.State == ConnectionState.Open;
            else
                _isConnected = false;

            return _isConnected;
        }
        #endregion //State

        #region Querys
        /// <summary>
        /// Führt eine SQL Leseanweisung auf die Access Datenbank aus
        /// </summary>
        /// <param name="sqlQuery">SQL Anweisung</param>
        /// <param name="dt">Ergebnisdaten</param>
        /// <returns>Fehler</returns>
        public ErrHndl.Error QueryDB(string sqlQuery, out DataTable dt)
        {
            ErrHndl.Error error = errHndl.Init_Error();
            dt = new DataTable();

            try
            {
                if (_con != null && _con.State == ConnectionState.Open)
                {
                    OleDbCommand cmd = new OleDbCommand(sqlQuery, _con);
                    OleDbDataReader reader = cmd.ExecuteReader();

                    dt.Load(reader);

                    reader.Close();
                    cmd.Dispose();
                }
            }
            catch (OleDbException ex)
            {
                error.bError = true;
                error.iCode = ex.HResult;
                error.sMessage = ex.Message;
            }
            catch (Exception ex)
            {
                error.bError = true;
                error.iCode = ex.HResult;
                error.sMessage = ex.Message;
            }

            return error;
        }

        /// <summary>
        /// Führt eine SQL Schreibanweisung auf die Access Datenbank aus
        /// </summary>
        /// <param name="sqlQuery">SQL Anweisung</param>
        /// <returns>Fehler</returns>
        public ErrHndl.Error UpdateDB(string sqlQuery)
        {
            ErrHndl.Error error = errHndl.Init_Error();

            try
            {
                if (_con != null && _con.State == ConnectionState.Open)
                {
                    OleDbCommand cmd = new OleDbCommand(sqlQuery, _con);
                    OleDbTransaction trans = _con.BeginTransaction();

                    cmd.Transaction = trans;
                    cmd.ExecuteNonQuery();

                    trans.Commit();

                    cmd.Dispose();
                    trans.Dispose();
                }
            }
            catch (OleDbException ex)
            {
                error.bError = true;
                error.iCode = ex.HResult;
                error.sMessage = ex.Message;
            }
            catch (Exception ex)
            {
                error.bError = true;
                error.iCode = ex.HResult;
                error.sMessage = ex.Message;
            }

            return error;
        }

        /// <summary>
        /// Abrufen aller Tabellennamen in der Access Datenbank
        /// </summary>
        /// <param name="tables">Tabellennamen</param>
        /// <returns>Fehler</returns>
        public ErrHndl.Error GetAllTables(out List<string> tables)
        {
            ErrHndl.Error error = errHndl.Init_Error();
            tables = new List<string>();

            try
            {
                if (_con != null && _con.State == ConnectionState.Open)
                {
                    foreach(DataRow row in _con.GetOleDbSchemaTable(OleDbSchemaGuid.Tables, new object[] { null, null, null, null }).Rows)
                    {
                        if (row.Field<string>("TABLE_TYPE") == "TABLE")
                            tables.Add(row.Field<string>("TABLE_NAME"));
                    }
                }
            }
            catch (OleDbException ex)
            {
                error.bError = true;
                error.iCode = ex.HResult;
                error.sMessage = ex.Message;
            }
            catch (Exception ex)
            {
                error.bError = true;
                error.iCode = ex.HResult;
                error.sMessage = ex.Message;
            }

            return error;
        }
        #endregion //Querys

        /// <summary>
        /// Ermitteln der höchsten installierten OleDB Version
        /// </summary>
        /// <returns>Version</returns>
        public string GetDBversion()
        {
            string AccDBs = string.Empty;
            List<double> versions = new List<double>();
            RegistryKey key = Registry.LocalMachine.OpenSubKey(@"SOFTWARE\Classes");

            if(key!=null)
            {
                foreach(string sub in key.GetSubKeyNames())
                {
                    if(sub.Contains("Microsoft.ACE.OLEDB"))
                        versions.Add(double.Parse($"{sub.Split('.')[sub.Split('.').Length-2]},{sub.Split('.')[sub.Split('.').Length - 1]}"));
                }
            }

            if (versions.Count > 0)
                return Math.Round(versions.Max(), 1).ToString("#.0").Replace(',', '.');
            else
                return "";
        }

        /// <summary>
        /// Abrufen des aktuellen Connection String, zur Externen Fehlerprüfung
        /// </summary>
        /// <returns>Connection String</returns>
        public string GetConString()
        {
            return _oleDB;
        }

        /// <summary>
        /// Abrufen aller Spaltennamen einer Tabelle
        /// </summary>
        /// <param name="TableName"></param>
        /// <returns></returns>
        public List<string> GetColumns(string TableName)
        {
            List<string> columns = new List<string>();
            ErrHndl.Error error = errHndl.Init_Error();

            try
            {
                if (_con != null && _con.State == ConnectionState.Open)
                {
                    string sql = "select column_name as Spalte\r\n" +
                                 "from all_tab_columns\r\n" +
                                $"where table_name = '{TableName}'";

                    error = QueryDB(sql, out DataTable dt);

                    if (!error.bError && dt.Rows.Count > 0)
                    {
                        foreach (DataRow dr in dt.Rows)
                            columns.Add(dr.Field<string>("Spalte"));
                    }
                }
            }
            catch (SqlException ex)
            {
                error.bError = true;
                error.iCode = ex.HResult;
                error.sMessage = ex.Message;
            }
            catch (Exception ex)
            {
                error.bError = true;
                error.iCode = ex.HResult;
                error.sMessage = ex.Message;
            }

            return columns;
        }
        #endregion //Functions
    }
}