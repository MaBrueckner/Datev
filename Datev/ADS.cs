using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using Advantage.Data.Provider;
using ErrorHandler;

namespace Datev
{
    public class ADS
    {
        #region Variables
        private bool _isConnected;

        private string _path;
        private string _user;
        private string _pwd;
        private string _ip;

        private int _timeout;

        private AdsConnection _con;
        private ErrHndl errHndl = new ErrHndl();
        #endregion //Variables

        #region Constructors
        public ADS()
        {
            _path = @"\zmi\data_time\ZMITime.add";
            _user = "zmi";
            _pwd = "zmi";
            _ip = "192.168.0.106";
            _timeout = 1000;
        }

        public ADS(string path, string user, string pwd, string ip, int timeout = 1000)
        {
            _path = path;
            _user = user;
            _pwd = pwd;
            _ip = ip;
            _timeout = timeout;
        }
        #endregion //Constructors

        #region Functions
        #region Connect
        /// <summary>
        /// Stellt die Verbindung mit der Advantage Datenbank her.
        /// </summary>
        /// <returns>Fehler</returns>
        public ErrHndl.Error Connect()
        {
            ErrHndl.Error error = errHndl.Init_Error();

            try
            {
                if (_con == null)
                {
                    _con = new AdsConnection();
                    _con.ConnectionString = $@"Data Source=\\{_ip}{_path}; User ID={_user}; Password={_pwd}; TableType=ADT;ServerType=REMOTE;";
                    _con.Open();
                }
                else if (_con != null && _con.State != ConnectionState.Open)
                    _con.Open();

                if (_con != null)
                    _isConnected = _con.State == ConnectionState.Open;
            }
            catch (AdsException ex)
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
                    error.sMessage = $"{DateTime.Now} - DatevDLL_ADS-Connect - Verbindung wurde nicht hergestellt";
                }
            }

            return error;
        }
        #endregion //Connect

        #region Disconnect
        /// <summary>
        /// Trennt die Verbindung zur Advantage Datenbank.
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

                _con = null;
                _isConnected = false;
            }
            catch (AdsException ex)
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
                    error.sMessage = $"{DateTime.Now} - DatevDLL_ADS-Disconnect - Fehler beim trennen der Verbindung";
                }
            }

            return error;
        }
        #endregion //Disconnect

        #region State
        /// <summary>
        /// Abrufen des aktuellen Verbindungsstatus zur Advantage Datenbank
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
        /// Führt eine SQL Leseanweisung auf die Advantage Datenbank aus
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
                    AdsCommand cmd = new AdsCommand(sqlQuery, _con);
                    AdsDataAdapter da = new AdsDataAdapter();

                    da.SelectCommand = cmd;
                    da.Fill(dt);

                    da.Dispose();
                    cmd.Dispose();
                }
            }
            catch (AdsException ex)
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
        /// Führt eine SQL Schreibanweisung auf die Advantage Datenbank aus
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
                    AdsCommand cmd = new AdsCommand(sqlQuery, _con);
                    AdsTransaction trans = _con.BeginTransaction();

                    cmd.Transaction = trans;
                    cmd.ExecuteNonQuery();

                    trans.Commit();

                    cmd.Dispose();
                    trans.Dispose();
                }
            }
            catch (AdsException ex)
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
        /// Abrufen aller Tabellennamen in der Advantage Datenbank
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
                    tables = ((string[])_con.GetTableNames()).ToList();
            }
            catch (AdsException ex)
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
        /// Ermitteln der aktuell installierten Advantage Server Version
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
                    version = _con.ServerVersion;
                }
            }
            catch (AdsException ex)
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
        #endregion //Functions
    }
}