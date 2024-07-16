using System;
using System.Collections.Generic;
using System.Data;
using FirebirdSql.Data.FirebirdClient;
using ErrorHandler;
using System.Data.SqlClient;

namespace Datev
{
    public class Firebird
    {
        #region Variables
        private bool _isConnected;

        private string _path;
        private string _user;
        private string _pwd;
        private string _ip;

        private int _timeout;

        private FbConnection _con;
        private ErrHndl errHndl = new ErrHndl();
        #endregion //Variables

        #region Constructors
        public Firebird()
        {
            _path = @"c:\Temp\Test.fdb";
            _user = "SYSDBA";
            _pwd = "masterkey";
            _ip = "localhost";
            _timeout = 2;
        }

        public Firebird(string path, string user, string pwd, string ip, string catalog, int timeout = 2)
        {
            _path = path + catalog;
            _user = user;
            _pwd = pwd;
            _ip = ip;
            _timeout = timeout;
        }
        #endregion //Constructors

        #region Functions
        #region Connect
        /// <summary>
        /// Stellt die Verbindung mit der Firebird-Datenbank her.
        /// </summary>
        /// <returns>Fehler</returns>
        public ErrHndl.Error Connect()
        {
            ErrHndl.Error error = errHndl.Init_Error();

            try
            {
                if(_con == null)
                {
                    FbConnectionStringBuilder fsb = new FbConnectionStringBuilder();
                    fsb.Database = _ip + ":" + _path;
                    fsb.ServerType = FbServerType.Default;
                    fsb.UserID = _user;
                    fsb.Password = _pwd;
                    fsb.ConnectionTimeout = _timeout;
                    fsb.Charset = FbCharset.Utf8.ToString();

                    _con = new FbConnection(fsb.ToString());
                    _con.Open();
                }
                else if(_con != null && _con.State != ConnectionState.Open)
                    _con.Open();

                if (_con != null)
                    _isConnected = _con.State == ConnectionState.Open;
            }
            catch(FbException ex)
            {
                error.bError = true;
                error.iCode = ex.HResult;
                error.sMessage = ex.Message;
            }
            catch(Exception ex)
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
                    error.sMessage = $"{DateTime.Now} - DatevDLL_Firebird-Connect - Verbindung wurde nicht hergestellt";
                }
            }

            return error;
        }
        #endregion //Connect

        #region Disconnect
        /// <summary>
        /// Trennt die Verbindung zur Firebird Datenbank.
        /// </summary>
        /// <returns>Fehler</returns>
        public ErrHndl.Error Disconnect()
        {
            ErrHndl.Error error = errHndl.Init_Error();

            try
            {
                if(_con != null && _con.State == ConnectionState.Open)
                {
                    _con.Close();
                    _con.Dispose();
                }

                _con = null;
                _isConnected = false;
            }
            catch (FbException ex)
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
                    error.sMessage = $"{DateTime.Now} - DatevDLL_Firebird-Disconnect - Fehler beim trennen der Verbindung";
                }
            }

            return error;
        }
        #endregion //Disconnect

        #region State
        /// <summary>
        /// Abrufen des aktuellen Verbindungsstatus zur Firebird Datenbank
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
        /// Führt eine SQL Leseanweisung auf die Firebird Datenbank aus
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
                    FbCommand cmd = new FbCommand(sqlQuery, _con);
                    FbDataAdapter da = new FbDataAdapter(cmd);
                    FbCommandBuilder cb = new FbCommandBuilder(da);

                    da.Fill(dt);

                    da.Dispose();
                    cb.Dispose();
                    cmd.Dispose();
                }
            }
            catch (FbException ex)
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
        /// Führt eine SQL Schreibanweisung auf die Firebird Datenbank aus
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
                    FbCommand cmd = new FbCommand(sqlQuery, _con);
                    FbTransaction trans = _con.BeginTransaction();

                    cmd.Transaction = trans;
                    cmd.ExecuteNonQuery();

                    trans.Commit();

                    cmd.Dispose();
                    trans.Dispose();
                }
            }
            catch (FbException ex)
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
        /// Abrufen aller Tabellennamen in der Firebird Datenbank
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
                    string sql = "select RDB$RELATION_NAME as Name\r\n" +
                                 "from RDB$RELATIONS\r\n" +
                                 "where RDB$SYSTEM_FLAG = 0\r\n" +
                                 "order by RDB$RELATION_NAME asc";

                    error = QueryDB(sql, out DataTable dt);

                    if (!error.bError)
                    {
                        foreach (DataRow dr in dt.Rows)
                        {
                            string name = dr["Name"] != DBNull.Value ? dr.Field<string>("Name").Trim() : "-";

                            if (name != "-")
                                tables.Add(name);
                        }
                    }
                }
            }
            catch (FbException ex)
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
        /// Ermitteln der aktuell installierten Firebird DB Version
        /// </summary>
        /// <returns>Version</returns>
        public string GetDBversion()
        {
            ErrHndl.Error error = errHndl.Init_Error();
            string version = "";

            try
            {
                if (_con != null && _con.State == ConnectionState.Open)
                {
                    string sql = "select rdb$get_context('SYSTEM','ENGINE_VERSION') as version from RDB$DATABASE";

                    error = QueryDB(sql, out DataTable dt);

                    if (!error.bError)
                    {
                        version = dt.Rows[0].Field<string>("version");
                    }
                }
            }
            catch (FbException ex)
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

            return version;
        }

        /// <summary>
        /// Abrufen des aktuellen Connection String, zur Externen Fehlerprüfung
        /// </summary>
        /// <returns>Connection String</returns>
        public string GetConString()
        {
            return _con.ConnectionString;
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
                    string sql = "select f.rdb$field_name as Spalte\r\n" +
                                 "from rdb$relation_fields f\r\n" +
                                $"where f.rdb$relation_name = '{TableName}'";

                    error = QueryDB(sql, out DataTable dt);

                    if (!error.bError && dt.Rows.Count > 0)
                    {
                        foreach (DataRow dr in dt.Rows)
                            columns.Add(dr.Field<string>("Spalte"));
                    }
                }
            }
            catch (FbException ex)
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

        /// <summary>
        /// Fügt eine neue Spalte an eine bereits bestehende Tabelle an.
        /// 
        /// Besonderheiten bei String Datentypen:
        /// - Ein String wird in der SQL Tabelle als VARCHAR(xxx) angelegt die maximale Anzahl von Zeichen lässt sich von außen beeinflussen
        ///   a. Leerer String: Die Spalte kann maximal 100 Zeichen aufnehmen
        ///   b. String mit einer Zahl: Die Spalte wird so groß wie die Zahl in dem String
        ///   c. String mit Mustertext: Die Spalte wird so groß wie der Übergebene Text
        /// - Wird direkt Char verwendet, auch in dem Fall wird die Spalte als VARCHAR(xxx) angelegt
        ///   a. Bei einem Array of Char: Die Spalte wird so groß wie das übergebene Array
        ///   b. Bei einem einzelonen Char kann die Spalte maximal ein Zeichen aufnehmen
        /// </summary>
        /// <param name="TableName">Name der Tabelle, in die eine Spalte eingefügt werden soll</param>
        /// <param name="ColumnName">Name der Spalte, die erstellt werden soll</param>
        /// <param name="DataType">Hier ist eine Variable mit dem Zieldatentyp anzuschließen.</param>
        public void AddColumn(string TableName, string ColumnName, object DataType)
        {
            //Datentyp ermitteln und in SQL-Datentyp übersetzen
            string sType = string.Empty;

            if (DataType.GetType() == typeof(string))
            {
                string sTemp = (string)DataType;

                if (sTemp.Length == 0)
                    sType = "VARCHAR(100)";
                else
                {
                    if (!int.TryParse(sTemp, out int val))
                        val = sTemp.Length;

                    sType = $"VARCHAR({val})";
                }
            }
            else if (DataType.GetType() == typeof(char))
                sType = "CHAR";
            else if (DataType.GetType() == typeof(char[]))
                sType = $"CHAR({((char[])DataType).Length})";
            else if (DataType.GetType() == typeof(bool))
                sType = "CHAR(1)";
            else if (DataType.GetType() == typeof(byte))
                sType = "SMALLINT";
            else if (DataType.GetType() == typeof(Int16) || DataType.GetType() == typeof(UInt16))
                sType = "SMALLINT";
            else if (DataType.GetType() == typeof(int))
                sType = "INTEGER";
            else if (DataType.GetType() == typeof(Int64) || DataType.GetType() == typeof(UInt64))
                sType = "INT64";
            else if (DataType.GetType() == typeof(Single))
                sType = "DECIMAL";
            else if (DataType.GetType() == typeof(double))
                sType = "FLOAT";
            else if (DataType.GetType() == typeof(DateTime))
                sType = "TIMESTAMP";

            string sql = $"ALTER TABLE {TableName}\r\n" +
                         $"ADD {ColumnName} {sType} NULL;";

            //Neue Spalte an Tabelle anhängen
            ErrHndl.Error error = errHndl.Init_Error();

            try
            {
                if (_con != null && _con.State == ConnectionState.Open)
                {
                    error = UpdateDB(sql);
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
        }
        #endregion //Functions
    }
}