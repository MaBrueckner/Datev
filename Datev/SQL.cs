using ErrorHandler;
using System.Collections.Generic;
using System.Data;

namespace Datev
{
    public class SQL
    {
        #region Enums
        public enum SelectDB
        {
            AccDB,
            ADS,
            FB,
            msSql
        }
        #endregion //Enums

        #region Variables
        private SelectDB _select;
        private object _sql;
        #endregion //Variables

        #region Constructors
        public SQL(SelectDB select, string path, string user = "", string pwd = "", string ip = "", string catalog = "", int timeout = 2)
        {
            _select = select;

            switch (_select)
            {
                case SelectDB.AccDB:
                    _sql = new Access(path);
                    break;
                case SelectDB.ADS:
                    _sql = new ADS(path, user, pwd, ip, timeout);
                    break;
                case SelectDB.FB:
                    _sql = new Firebird(path, user, pwd, ip, catalog, timeout);
                    break;
                default:
                    _sql = new MS(path, user, pwd, ip, catalog, timeout);
                    break;
            }
        }
        #endregion //Constructors

        #region Functions
        #region Connect
        /// <summary>
        /// Stellt die Verbindung mit der Datenbank her.
        /// </summary>
        /// <returns>Fehler</returns>
        public ErrHndl.Error Connect()
        {
            ErrHndl.Error error;

            switch (_select)
            {
                case SelectDB.AccDB:
                    error = ((Access)_sql).Connect();
                    break;
                case SelectDB.ADS:
                    error = ((ADS)_sql).Connect();
                    break;
                case SelectDB.FB:
                    error = ((Firebird)_sql).Connect();
                    break;
                default:
                    error = ((MS)_sql).Connect();
                    break;
            }

            return error;
        }
        #endregion //Connect

        #region Disconnect
        /// <summary>
        /// Trennt die Verbindung zur Datenbank.
        /// </summary>
        /// <returns>Fehler</returns>
        public ErrHndl.Error Disconnect()
        {
            ErrHndl.Error error;

            switch (_select)
            {
                case SelectDB.AccDB:
                    error = ((Access)_sql).Disconnect();
                    break;
                case SelectDB.ADS:
                    error = ((ADS)_sql).Disconnect();
                    break;
                case SelectDB.FB:
                    error = ((Firebird)_sql).Disconnect();
                    break;
                default:
                    error = ((MS)_sql).Disconnect();
                    break;
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
            bool _isConnected;

            switch (_select)
            {
                case SelectDB.AccDB:
                    _isConnected = ((Access)_sql).GetConState();
                    break;
                case SelectDB.ADS:
                    _isConnected = ((ADS)_sql).GetConState();
                    break;
                case SelectDB.FB:
                    _isConnected = ((Firebird)_sql).GetConState();
                    break;
                default:
                    _isConnected = ((MS)_sql).GetConState();
                    break;
            }

            return _isConnected;
        }
        #endregion //State

        #region Querys
        /// <summary>
        /// Führt eine SQL Leseanweisung auf die Datenbank aus
        /// </summary>
        /// <param name="sqlQuery">SQL Anweisung</param>
        /// <param name="dt">Ergebnisdaten</param>
        /// <returns>Fehler</returns>
        public ErrHndl.Error QueryDB(string sqlQuery, out DataTable dt)
        {
            ErrHndl.Error error;

            switch (_select)
            {
                case SelectDB.AccDB:
                    error = ((Access)_sql).QueryDB(sqlQuery, out dt);
                    break;
                case SelectDB.ADS:
                    error = ((ADS)_sql).QueryDB(sqlQuery, out dt);
                    break;
                case SelectDB.FB:
                    error = ((Firebird)_sql).QueryDB(sqlQuery, out dt);
                    break;
                default:
                    error = ((MS)_sql).QueryDB(sqlQuery, out dt);
                    break;
            }

            return error;
        }

        /// <summary>
        /// Führt eine SQL Schreibanweisung auf die Datenbank aus
        /// </summary>
        /// <param name="sqlQuery">SQL Anweisung</param>
        /// <returns>Fehler</returns>
        public ErrHndl.Error UpdateDB(string sqlQuery)
        {
            ErrHndl.Error error;

            switch (_select)
            {
                case SelectDB.AccDB:
                    error = ((Access)_sql).UpdateDB(sqlQuery);
                    break;
                case SelectDB.ADS:
                    error = ((ADS)_sql).UpdateDB(sqlQuery);
                    break;
                case SelectDB.FB:
                    error = ((Firebird)_sql).UpdateDB(sqlQuery);
                    break;
                default:
                    error = ((MS)_sql).UpdateDB(sqlQuery);
                    break;
            }

            return error;
        }

        /// <summary>
        /// Abrufen aller Tabellennamen in der Datenbank
        /// </summary>
        /// <param name="tables">Tabellennamen</param>
        /// <returns>Fehler</returns>
        public ErrHndl.Error GetAllTables(out List<string> tables)
        {
            ErrHndl.Error error;

            switch (_select)
            {
                case SelectDB.AccDB:
                    error = ((Access)_sql).GetAllTables(out tables);
                    break;
                case SelectDB.ADS:
                    error = ((ADS)_sql).GetAllTables(out tables);
                    break;
                case SelectDB.FB:
                    error = ((Firebird)_sql).GetAllTables(out tables);
                    break;
                default:
                    error = ((MS)_sql).GetAllTables(out tables);
                    break;
            }

            return error;
        }
        #endregion //Querys

        /// <summary>
        /// Ermitteln der aktuell installierten Server Version
        /// </summary>
        /// <returns>Version</returns>
        public string GetDBversion()
        {
            string version;

            switch (_select)
            {
                case SelectDB.AccDB:
                    version = ((Access)_sql).GetDBversion();
                    break;
                case SelectDB.ADS:
                    version = ((ADS)_sql).GetDBversion();
                    break;
                case SelectDB.FB:
                    version = ((Firebird)_sql).GetDBversion();
                    break;
                default:
                    version = ((MS)_sql).GetDBversion();
                    break;
            }

            return version;
        }

        /// <summary>
        /// Abrufen des aktuellen Connection String, zur Externen Fehlerprüfung
        /// </summary>
        /// <returns>Connection String</returns>
        public string GetConString()
        {
            string con;

            switch (_select)
            {
                case SelectDB.AccDB:
                    con = ((Access)_sql).GetConString();
                    break;
                case SelectDB.ADS:
                    con = ((ADS)_sql).GetConString();
                    break;
                case SelectDB.FB:
                    con = ((Firebird)_sql).GetConString();
                    break;
                default:
                    con = ((MS)_sql).GetConString();
                    break;
            }

            return con;
        }
        #endregion //Functions
    }
}