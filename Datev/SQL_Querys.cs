using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Datev
{
    public class SQL_Querys
    {
        #region GFU
        #region Select
        /// <summary>
        /// SQL Anweisung, zum Abrufen Benutzerspezifischer Informationen
        /// </summary>
        /// <param name="pe_Karte">Strichcode oder Chipkarte</param>
        /// <returns>SQL Anweisung</returns>
        public static string slqSel_User(string pe_Karte)
        {
            string sql = "Select r.PE_LFDNR, r.PE_NUMMER, r.PE_NAME, r.PE_ABTLG, r.PE_UTAGE, r.PE_GEBTAG, r.PE_KBEZ, r.PE_STDKST, r.PE_GRP\r\n" +
                         "From PERSONAL r\r\n" +
                        $"Where (r.PE_KARTE = '{pe_Karte}' or r.PE_KARTE2 = '{pe_Karte}') and r.PE_ALT = 'F'";

            return sql;
        }

        /// <summary>
        /// SQL Anweisung, zum abrufen der Arbeitszeit für einen bestimmten Monat
        /// </summary>
        /// <param name="pe_nummer"></param>
        /// <param name="year"></param>
        /// <param name="month"></param>
        /// <returns>SQL Anweisung</returns>
        public static string sqlSel_Worktime(string pe_nummer, int year = 0, int month = 0)
        {
            int _year = year == 0 || year > DateTime.Today.Year ? DateTime.Now.Year : year;
            int _month = month == 0 || month > DateTime.Today.Month ? DateTime.Now.Month : month;

            DateTime start = new DateTime(_year, _month, 1);
            DateTime end = new DateTime(_year, _month, start.AddMonths(1).AddDays(-1).Day);

            string sql = "Select r.ZE_LFDNR, r.ZE_DATUM, r.ZE_START, r.ZE_ENDE, l.LA_NAME\r\n" +
                         "From ZEITERFASSUNG r\r\n" +
                         "Inner Join LOHNART l on l.LA_KNZ = r.ZE_KNZ\r\n" +
                        $"Where r.ZE_NUMMER = '{pe_nummer}' and (r.ZE_DATUM >= '{start.Date}' " +
                                                          $"and r.ZE_DATUM <= '{end.Date}') " +
                                                           "and l.LA_ART = 'F'\r\n" +
                         "order by r.ZE_DATUM asc";

            return sql;
        }

        /// <summary>
        /// SQL Anweisung, zum abrufen eines Tages aus der Zeiterfassung für einen Benutzer
        /// </summary>
        /// <param name="pe_nummer">Personalnummer</param>
        /// <param name="datum">[Optional] Wenn nicht angegeben wird Heute genommen</param>
        /// <returns>SQL Anweisung</returns>
        public static string sqlSel_Workday(string pe_nummer, DateTime? datum = null)
        {
            DateTime ze_datum = datum == null ? DateTime.Today : datum.Value;

            string sql = "Select r.ZE_LFDNR, r.ZE_DATUM, r.ZE_START, r.ZE_ENDE, l.LA_NAME\r\n" +
                         "From ZEITERFASSUNG r\r\n" +
                         "Inner Join LOHNART l on l.LA_KNZ = r.ZE_KNZ\r\n" +
                        $"Where r.ZE_NUMMER = '{pe_nummer}' and r.ZE_DATUM = '{ze_datum.Date}' and l.LA_ART = 'F'\r\n" +
                         "Order by r.ZE_DATUM asc";

            return sql;
        }

        /// <summary>
        /// SQL Anweisung, zum abrufen der Regelarbeitszeit für einen Benutzer
        /// </summary>
        /// <param name="pe_nummer">Personalnummer</param>
        /// <returns>SQL Anweisung</returns>
        public static string sqlSel_TimeProfil(string pe_nummer)
        {
            string sql = "Select r.PE_NAME,\r\n" +
                         "r.PE_KARTE, z.PR_MO, z.PR_DI, z.PR_MI, z.PR_DO, z.PR_FR, z.PR_SA, z.PR_SO,\r\n" +
                         "z.PR_MOESS, z.PR_MOEMS, z.PR_MOESE, z.PR_MOEME, z.PR_MOZSS, z.PR_MOZMS, z.PR_MOZSE, z.PR_MOZME,\r\n" +
                         "z.PR_DIESS, z.PR_DIEMS, z.PR_DIESE, z.PR_DIEME, z.PR_DIZSS, z.PR_DIZMS, z.PR_DIZSE, z.PR_DIZME,\r\n" +
                         "z.PR_MIESS, z.PR_MIEMS, z.PR_MIESE, z.PR_MIEME, z.PR_MIZSS, z.PR_MIZMS, z.PR_MIZSE, z.PR_MIZME,\r\n" +
                         "z.PR_DOESS, z.PR_DOEMS, z.PR_DOESE, z.PR_DOEME, z.PR_DOZSS, z.PR_DOZMS, z.PR_DOZSE, z.PR_DOZME,\r\n" +
                         "z.PR_FRESS, z.PR_FREMS, z.PR_FRESE, z.PR_FREME, z.PR_FRZSS, z.PR_FRZMS, z.PR_FRZSE, z.PR_FRZME,\r\n" +
                         "z.PR_SAESS, z.PR_SAEMS, z.PR_SAESE, z.PR_SAEME, z.PR_SAZSS, z.PR_SAZMS, z.PR_SAZSE, z.PR_SAZME,\r\n" +
                         "z.PR_SOESS, z.PR_SOEMS, z.PR_SOESE, z.PR_SOEME, z.PR_SOZSS, z.PR_SOZMS, z.PR_SOZSE, z.PR_SOZME\r\n" +
                         "From PERSONAL r\r\n" +
                         "Inner Join ZEITPROFILE z on r.PE_PROFIL = z.PR_PROFIL\r\n" +
                        $"Where r.PE_NUMMER = '{pe_nummer}'";

            return sql;
        }

        /// <summary>
        /// SQL Anweisung, zum abrufen der Unterbrechungen für einen bestimmten Monat
        /// </summary>
        /// <param name="pe_nummer">Personalnummer</param>
        /// <param name="year">[Optional] <> 0 dann ein anderes</param>
        /// <param name="month"></param>
        /// <returns>SQL Anweisung</returns>
        public static string sqlSel_Interrupts(string pe_nummer, int year = 0, int month = 0)
        {
            int _year = year == 0 || year > DateTime.Today.Year ? DateTime.Now.Year : year;
            int _month = month == 0 || (month > DateTime.Today.Month && year >= DateTime.Today.Year) ? DateTime.Now.Month : month;

            DateTime start = new DateTime(_year, _month, 1);
            DateTime end = new DateTime(_year, _month, start.AddMonths(1).AddDays(-1).Day);

            string sql = "Select z.ZU_LFDNR, z.ZU_BUCHUNGSDATUM, z.ZU_STOP, z.ZU_WEITER\r\n" +
                         "From ZEITUNTBR z\r\n" +
                        $"Where z.ZU_NUMMER = '{pe_nummer}' and z.ZU_BUCHUNGSDATUM >= '{start.Date}' and z.ZU_BUCHUNGSDATUM <= '{end.Date}'\r\n" +
                        $"Order by z.ZU_BUCHUNGSDATUM asc";

            return sql;
        }

        /// <summary>
        /// SQL Anweisung, zum abrufen der Unterbrechungszeiten für einen Tag
        /// </summary>
        /// <param name="pe_nummer">Personalnummer</param>
        /// <param name="zu_datum">Unterbrechungsdatum</param>
        /// <returns>SQL Anweisung</returns>
        public static string sqlSel_Interrupt(string pe_nummer, DateTime zu_datum)
        {
            string sql = "Select z.ZU_LFDNR, z.ZU_BUCHUNGSDATUM, z.ZU_STOP, z.ZU_WEITER\r\n" +
                         "From ZEITUNTBR z\r\n" +
                        $"Where z.ZU_NUMMER = '{pe_nummer}' and z.ZU_BUCHUNGSDATUM = '{zu_datum.Date}'";

            return sql;
        }
        #endregion //Select

        #region Insert
        /// <summary>
        /// SQL Anweisung, zum einfügen einer neuen Zeile in die tägliche Zeiterfassung
        /// </summary>
        /// <param name="pe_nummer">Personalnummer</param>
        /// <param name="pe_name">Personalname</param>
        /// <param name="pe_kbez">Personalkurzbezeichnung</param>
        /// <param name="start">Anfangszeit</param>
        /// <returns>SQL Anweisung</returns>
        public static string sqlIns_Come(string pe_nummer, string pe_name, string pe_kbez, DateTime start)
        {
            string sql = "INSERT INTO ZEITERFASSUNG (ZE_LFDNR, ZE_NUMMER, ZE_NAME, ZE_SCHICHT, ZE_KNZ, ZE_DATUM,\r\n" +
                                                    "ZE_START, ZE_ZF, ZE_AN, ZE_SCHICHTNR, ZE_ANNUTZER, NUTZERID, GEANDERT)\r\n\t" +
                             "VALUES (\r\n\t" +
                                 "(SELECT NEXT VALUE FOR ZELFDNR FROM RDB$DATABASE),\r\n\t\t" +
                                $"'{pe_nummer}',\r\n\t\t" +
                                $"'{pe_name}',\r\n\t\t" +
                                $"1,\r\n\t\t" +
                                $"'A',\r\n\t\t" +
                                $"'{start.Date}',\r\n\t\t" +
                                $"'{start}',\r\n\t\t" +
                                $"1,\r\n\t\t" +
                                $"'{start}',\r\n\t\t" +
                                $"1,\r\n\t\t" +
                                $"'{pe_kbez}',\r\n\t\t" +
                                $"'ZAT-{pe_kbez}',\r\n\t\t" +
                                 "CURRENT_TIMESTAMP(0)\r\n\t" +
                             ")";

            return sql;
        }

        /// <summary>
        /// SQL Anweisung, zum einfügen einer neuen Zeile in die tägliche Zeiterfassung
        /// </summary>
        /// <param name="pe_nummer">Personalnummer</param>
        /// <param name="pe_name">Personalname</param>
        /// <param name="pe_kbez">Personalkurzbezeichnung</param>
        /// <param name="start">Anfangszeit</param>
        /// <param name="end">Endezeit</param>
        /// <returns>SQL Anweisung</returns>
        public static string sqlIns_ComeLeave(string pe_nummer, string pe_name, string pe_kbez, double pause, DateTime start, DateTime end)
        {
            double time = Calc_TimeSpan(start, end);

            string sql = "INSERT INTO ZEITERFASSUNG (ZE_LFDNR, ZE_NUMMER, ZE_NAME, ZE_SCHICHT, ZE_KNZ, ZE_DATUM, ZE_START, ZE_ENDE, ZE_ZF, ZE_AN, ZE_AB,\r\n" +
                                                    "ZE_ZEIT, ZE_KORRZEIT, ZE_PAUSE, ZE_SCHICHTNR, ZE_ANNUTZER, ZE_ABNUTZER, NUTZERID, GEANDERT)\r\n\t" +
                             "VALUES (\r\n\t" +
                                 "(SELECT NEXT VALUE FOR ZELFDNR FROM RDB$DATABASE),\r\n\t\t" +
                                $"'{pe_nummer}',\r\n\t\t" +
                                $"'{pe_name}',\r\n\t\t" +
                                $"1,\r\n\t\t" +
                                $"'A',\r\n\t\t" +
                                $"'{start.Date}',\r\n\t\t" +
                                $"'{start}',\r\n\t\t" +
                                $"'{end}',\r\n\t\t" +
                                $"1,\r\n\t\t" +
                                $"'{start}',\r\n\t\t" +
                                $"'{end}',\r\n\t\t" +
                                $"{Format_DBL(time)},\r\n\t\t" +
                                $"{Format_DBL(time)},\r\n\t\t" +
                                $"{Format_DBL(pause)},\r\n\t\t" +
                                $"1,\r\n\t\t" +
                                $"'{pe_kbez}',\r\n\t\t" +
                                $"'{pe_kbez}',\r\n\t\t" +
                                $"'ZAT-{pe_kbez}',\r\n\t\t" +
                                 "CURRENT_TIMESTAMP(0)\r\n\t" +
                             ")";

            return sql;
        }

        /// <summary>
        /// SQL Anweisung, zum einfügen einer Unterbrechung in die Datenbank
        /// </summary>
        /// <param name="pe_nummer">Personalnummer</param>
        /// <param name="pe_kbez">Personalkurzbezeichnung</param>
        /// <param name="start">Unterbrechungszeit</param>
        /// <returns>SQL Anweisung</returns>
        public static string sqlIns_Interrupt(string pe_nummer, string pe_kbez, DateTime start)
        {
            string sql = "Insert Into ZEITUNTBR (\r\n\t" +
                            "ZU_NUMMER, ZU_BUCHUNGSDATUM,\r\n\t" +
                            "ZU_STOP, ZU_WEITER,\r\n\t" +
                            "ZU_ZF, ZU_UZEIT,\r\n\t" +
                            "ZU_STOPNUTZER, ZU_WEITERNUTZER,\r\n\t" +
                            "NUTZERID, ZU_LFDNR,\r\n\t" +
                            "GEANDERT)\r\n" +
                         "Values (\r\n\t" +
                           $"{pe_nummer}, '{start.Date}',\r\n\t" +
                           $"'{start}', null,\r\n\t" +
                            "1, null,\r\n\t" +
                           $"'{pe_kbez}', null,\r\n\t" +
                           $"'{Environment.MachineName}-{pe_kbez}', (SELECT NEXT VALUE FOR ZEULFDNR FROM RDB$DATABASE),\r\n\t" +
                           $"CURRENT_TIMESTAMP (0)\r\n" +
                         ")";

            return sql;
        }

        /// <summary>
        /// SQL Anweisung, zum einfügen einer Unterbrechung und Fortsetzung in die Datenbank
        /// </summary>
        /// <param name="pe_nummer">Personalnummer</param>
        /// <param name="pe_kbez">Personalkurzbezeichnung</param>
        /// <param name="start">Unterbrechungszeit</param>
        /// <param name="end">Fortsetzenzeit</param>
        /// <returns>SQL Anweisung</returns>
        public static string sqlIns_InterruptContinue(string pe_nummer, string pe_kbez, DateTime start, DateTime end)
        {
            double time = Calc_TimeSpan(start, end);

            string sql = "Insert Into ZEITUNTBR (\r\n\t" +
                            "ZU_NUMMER, ZU_BUCHUNGSDATUM,\r\n\t" +
                            "ZU_STOP, ZU_WEITER,\r\n\t" +
                            "ZU_ZF, ZU_UZEIT,\r\n\t" +
                            "ZU_STOPNUTZER, ZU_WEITERNUTZER,\r\n\t" +
                            "NUTZERID, ZU_LFDNR,\r\n\t" +
                            "GEANDERT)\r\n" +
                         "Values (\r\n\t" +
                           $"{pe_nummer}, '{start.Date}',\r\n\t" +
                           $"'{start}', {end},\r\n\t" +
                           $"1, {Format_DBL(time)},\r\n\t" +
                           $"'{pe_kbez}', {pe_kbez},\r\n\t" +
                           $"'{Environment.MachineName}-{pe_kbez}', (SELECT NEXT VALUE FOR ZEULFDNR FROM RDB$DATABASE),\r\n\t" +
                           $"CURRENT_TIMESTAMP (0)\r\n" +
                         ")";

            return sql;
        }
        #endregion //Insert

        #region Update
        /// <summary>
        /// SQL Anweisung, zum aktualisieren der täglichen Zeiterfassung
        /// </summary>
        /// <param name="pe_nummer">Personalnummer</param>
        /// <param name="pe_kbez">Personalkurzbezeichnung</param>
        /// <param name="start">Anfangszeit</param>
        /// <param name="ze_LfdNr">[Optional] Wenn die Laufende Nummer als Vergleichsbedingung vorhanden sein muss</param>
        /// <returns>SQL Anweisung</returns>
        public static string sqlUpd_Come(string pe_nummer, string pe_kbez, DateTime start, int ze_LfdNr = -1)
        {
            string LfdNr = ze_LfdNr > 0 ? $" and ZE_LFDNR = {ze_LfdNr}" : "";

            string sql = "MERGE INTO ZEITERFASSUNG AS Z\r\n\t" +
                           $"USING (select * from ZEITERFASSUNG where ZE_NUMMER = '{pe_nummer}' and ZE_DATUM = '{start.ToString("d")} 00:00:00'{LfdNr}) AS A\r\n\t\t" +
                                "ON A.ZE_NUMMER = Z.ZE_NUMMER AND A.ZE_DATUM = Z.ZE_DATUM\r\n\t\t" +
                                "WHEN MATCHED THEN\r\n\t\t\t" +
                                    "UPDATE SET\r\n\t\t\t\t" +
                                        "ZE_KNZ = 'A',\r\n\t\t\t\t" +
                                       //$"ZE_DATUM = '{start.ToString("d")}',\r\n\t\t\t\t" +
                                       $"ZE_START = '{start}',\r\n\t\t\t\t" +
                                       $"ZE_AN = '{start}',\r\n\t\t\t\t" +
                                        "ZE_BEREICH = null,\r\n\t\t\t\t" +
                                        "ZE_PAUSE = null,\r\n\t\t\t\t" +
                                        "ZE_SCHICHTNR = 1,\r\n\t\t\t\t" +
                                       $"ZE_ANNUTZER = '{pe_kbez}',\r\n\t\t\t\t" +
                                        "ZE_TEAM = null,\r\n\t\t\t\t" +
                                       $"NUTZERID = 'ZAT-{pe_kbez}',\r\n\t\t\t\t" +
                                        "GEANDERT = CURRENT_TIMESTAMP";

            return sql;
        }

        /// <summary>
        /// SQL Anweisung, zum aktualisieren der täglichen Zeiterfassung
        /// </summary>
        /// <param name="pe_nummer">Personalnummer</param>
        /// <param name="pe_kbez">Personalkurzbezeichnung</param>
        /// <param name="ze_lfdnr">Laufende Nummer</param>
        /// <param name="pause">Pausenzeit [h]</param>
        /// <param name="start">Anfangszeit</param>
        /// <param name="end">Endezeit</param>
        /// <param name="ze_datum">[Optional] muss das Buchungsdatum im Filter enthalten sein?</param>
        /// <returns>SQL Anweisung</returns>
        public static string sqlUpd_Leave(string pe_nummer, string pe_kbez, int ze_lfdnr, double pause, DateTime start, DateTime end, bool ze_datum = false)
        {
            double time = Calc_TimeSpan(start, end);
            string datum = ze_datum ? $" and ZE_DATUM = '{start.ToString("d")} 00:00:00'" : "";

            string sql = "MERGE INTO ZEITERFASSUNG AS Z\r\n\t" +
                           $"USING (select * from ZEITERFASSUNG where ZE_NUMMER = '{pe_nummer}' and ZE_LFDNR = {ze_lfdnr}{datum}) AS A\r\n\t\t" +
                                "ON A.ZE_NUMMER = Z.ZE_NUMMER AND A.ZE_LFDNR = Z.ZE_LFDNR\r\n\t\t" +
                                "WHEN MATCHED THEN\r\n\t\t\t" +
                                    "UPDATE SET\r\n\t\t\t\t" +
                                       $"ZE_ENDE = '{end}',\r\n\t\t\t\t" +
                                       $"ZE_AB = '{end}',\r\n\t\t\t\t" +
                                       $"ZE_ZEIT = {Format_DBL(time)},\r\n\t\t\t\t" +
                                       $"ZE_KORRZEIT = {Format_DBL(time)},\r\n\t\t\t\t" +
                                       $"ZE_PAUSE = {Format_DBL(pause)},\r\n\t\t\t\t" +
                                       $"ZE_ABNUTZER = '{pe_kbez}',\r\n\t\t\t\t" +
                                        "ZE_BEMERKUNG = '',\r\n\t\t\t\t" +
                                       $"NUTZERID = 'ZAT-{pe_kbez}',\r\n\t\t\t\t" +
                                        "GEANDERT = CURRENT_TIMESTAMP";

            return sql;
        }

        /// <summary>
        /// SQL Anweisung, zum aktualisieren der täglichen Zeiterfassung
        /// </summary>
        /// <param name="pe_nummer">Personalnummer</param>
        /// <param name="pe_kbez">Personalkurzbezeichnung</param>
        /// <param name="ze_lfdnr">Laufende Nummer</param>
        /// <param name="pause">Pausenzeit [h]</param>
        /// <param name="start">Anfangszeit</param>
        /// <param name="end">Endezeit</param>
        /// <param name="ze_datum">[Optional] muss das Buchungsdatum im Filter enthalten sein?</param>
        /// <returns>SQL Anweisung</returns>
        public static string sqlUpd_ComeLeave(string pe_nummer, string pe_kbez, int ze_lfdnr, double pause, DateTime start, DateTime end, bool ze_datum = false)
        {
            double time = Calc_TimeSpan(start, end);
            string datum = ze_datum ? $" and ZE_DATUM = '{start.ToString("d")} 00:00:00'" : "";

            string sql = "MERGE INTO ZEITERFASSUNG AS Z\r\n\t" +
                           $"USING (select * from ZEITERFASSUNG where ZE_NUMMER = '{pe_nummer}' and ZE_LFDNR = {ze_lfdnr}{datum}) AS A\r\n\t\t" +
                                "ON A.ZE_NUMMER = Z.ZE_NUMMER AND A.ZE_LFDNR = Z.ZE_LFDNR\r\n\t\t" +
                                "WHEN MATCHED THEN\r\n\t\t\t" +
                                    "UPDATE SET\r\n\t\t\t\t" +
                                       $"ZE_START = '{start}',\r\n\t\t\t\t" +
                                       $"ZE_AN = '{start}',\r\n\t\t\t\t" +
                                       $"ZE_ENDE = '{end}',\r\n\t\t\t\t" +
                                       $"ZE_AB = '{end}',\r\n\t\t\t\t" +
                                       $"ZE_ZEIT = {Format_DBL(time)},\r\n\t\t\t\t" +
                                       $"ZE_KORRZEIT = {Format_DBL(time)},\r\n\t\t\t\t" +
                                       $"ZE_PAUSE = {Format_DBL(pause)},\r\n\t\t\t\t" +
                                       $"ZE_ANNUTZER = '{pe_kbez}',\r\n\t\t\t\t" +
                                       $"ZE_ABNUTZER = '{pe_kbez}',\r\n\t\t\t\t" +
                                        "ZE_BEMERKUNG = '',\r\n\t\t\t\t" +
                                       $"NUTZERID = 'ZAT-{pe_kbez}',\r\n\t\t\t\t" +
                                        "GEANDERT = CURRENT_TIMESTAMP";

            return sql;
        }

        /// <summary>
        /// SQL Anweisung, zum ändern der täglichen Zeiterfassung
        /// </summary>
        /// <param name="pe_nummer"></param>
        /// <param name="pe_kbez"></param>
        /// <param name="ze_lfdnr"></param>
        /// <param name="ze_datum"></param>
        /// <param name="start"></param>
        /// <param name="end"></param>
        /// <param name="newStart"></param>
        /// <param name="newEnd"></param>
        /// <returns></returns>
        public static string sqlUpd_ComeLeave(string pe_nummer, string pe_kbez, string ze_lfdnr, DateTime ze_datum, DateTime start, DateTime end, DateTime newStart, DateTime newEnd)
        {
            DateTime _start = start == newStart ? start : newStart;
            DateTime _end = end == newEnd ? end : newEnd;

            string _sEnd = _end.ToString("T") == "00:00:00" ? "null" : $"'{_end}'";

            double time = _sEnd != "null" ? Calc_TimeSpan(_start, _end) : 0;

            string sql = "UPDATE ZEITERFASSUNG z " +
                            "SET " +
                                "z.ZE_START = '" + _start + "', " +
                                "z.ZE_AN = '" + _start + "', " +
                               $"z.ZE_ENDE = {_sEnd}, " +
                               $"z.ZE_AB = {_sEnd}, " +
                               $"z.ZE_ZEIT = {Format_DBL(time)}, " +
                               $"z.ZE_KORRZEIT = {Format_DBL(time)}, " +
                               $"z.NUTZERID = 'ZAT-{pe_kbez}', " +
                                "z.GEANDERT = CURRENT_TIMESTAMP " +
                            "WHERE " +
                               $"z.ZE_NUMMER = {pe_nummer} AND z.ZE_DATUM = '{ze_datum}' and z.ZE_LFDNR = {ze_lfdnr}";

            return sql;
        }

        /// <summary>
        /// SQL Anweisung, zum aktualisieren der Fortsetzen Zeit in der Datenbank
        /// </summary>
        /// <param name="pe_nummer">Personalnummer</param>
        /// <param name="pe_kbez">Personalkurzbezeichnung</param>
        /// <param name="zu_lfdnr">Laufende Nummer Zeitunterbrechung</param>
        /// <param name="start">Unterbrechenzeit</param>
        /// <param name="end">Fortsetzenzeit</param>
        /// <returns>SQL Anweisung</returns>
        private static string sqlUpd_Continue(string pe_nummer, string pe_kbez, int zu_lfdnr, DateTime start, DateTime end)
        {
            double time = Calc_TimeSpan(start, end);

            string sql = "UPDATE ZEITUNTBR a\r\n" +
                         "SET\r\n\t" +
                            $"a.ZU_WEITER = '{end}',\r\n\t" +
                            $"a.ZU_UZEIT = {Format_DBL(time)},\r\n\t" +
                            $"a.ZU_WEITERNUTZER = '{pe_kbez}',\r\n\t" +
                            $"a.GEANDERT = CURRENT_TIMESTAMP (0)\r\n" +
                         "WHERE\r\n\t" +
                            $"a.ZU_LFDNR = {zu_lfdnr} and a.ZU_NUMMER = {pe_nummer}";

            return sql;
        }
        #endregion //Update

        #region Delete
        #endregion //Delete
        #endregion //GFU

        #region PME
        #region Select
        /// <summary>
        /// SQL Abfrage String aller PME Daten
        /// </summary>
        /// <returns>SQL Abfrage</returns>
        public static string sqlSel_abrufPME()
        {
            string sql;

            sql = "SELECT CR.IDX_CHANTIER_RESSOURCE, C.IDX_CHANTIER, C.IDX_CLIENT,c.NOM_CHANTIER, c.[REMARQUE],"
                + "c.[TYPE_CHANTIER], d.[DATE_DEBUT], d.[DATE_FIN], c.[CHANTIER_FRACTIONNE], "
                + "c.[IDX_PROJET], c.[DATE_CREATION], c.[DATE_MODIFICATION], c.[IDX_SOUS_PROJET], "
                + "c.[POURCENTAGE_REALISATION], r.[EMAIL], r.[PRENOM_RESSOURCE], r.[NOM_RESSOURCE], "
                + "P.[PROJET], P.[_PROJNR], S.[SOUS_PROJET], s._KUNDE, s._PROJEKTNR, k.SOCIETE, k.VILLE, "
                + "x.LIBELLE as Kategorie, E.LIBELLE as Status,  a.SERVICE as Gruppe, AP.SERVICE as Hauptgruppe, "
                + "C._SOLLH, C._ISTH, U.EMAIL Projektleiter, r.LIBELLE_RESSOURCE, C.INDISPONIBILITE, P.INVALIDE, C._EXCHANGEID "
                + "FROM CHANTIER_RESSOURCE CR "
                + "inner join CHANTIER C on CR.IDX_CHANTIER = c.IDX_CHANTIER "
                + "inner join RESSOURCE R on CR.IDX_RESSOURCE = R.IDX_RESSOURCE "
                + "left join CHANTIER_DATE d on cr.IDX_CHANTIER = d.IDX_CHANTIER "
                + "left join PROJET P on C.IDX_PROJET = P.IDX_PROJET "
                + "left join SOUS_PROJET S on C.IDX_SOUS_PROJET = S.IDX_SOUS_PROJET "
                + "left join CLIENT K on P.IDX_CLIENT = K.IDX_CLIENT "
                + "left join ETAT E on c.IDX_ETAT = E.IDX_ETAT "
                + "left join SERVICE_ORDRE O on r.IDX_RESSOURCE = O.IDX_RESSOURCE "
                + "left join SERVICE A on o.IDX_SERVICE = A.IDX_SERVICE "
                + "left join SERVICE AP on A.IDX_SERVICE_PARENT = AP.IDX_SERVICE "
                + "left join (SELECT ROW_NUMBER() OVER(ORDER BY IDX_COULEUR ASC) AS Row#, LIBELLE FROM COULEUR) x on c.INDICE_COULEUR = x.Row# -1 "
                + "left join UTILISATEUR u on P.IDX_RESPONSABLE = U.IDX_UTILISATEUR "
                + "where R.INVALIDE = 0";

            return sql;
        }

        /// <summary>
        /// SQL Abfrage String aller PME Daten für einen bestimmten Nutzer
        /// </summary>
        /// <param name="user">Benutzerauswahl</param>
        /// <param name="group">Alle oder spezifische Gruppe</param>
        /// <returns>SQL Abfrage</returns>
        public static string sqlSel_abrufPME(string user, string group = "none")
        {
            string Vorname = "";
            string Nachname = "";

            if (user != null && user != string.Empty && user != "")
            {
                Vorname = user.Split(' ')[0];
                Nachname = user.Split(' ')[1];
            }

            string sql;

            if (group == "none")
                sql = "SELECT CR.IDX_CHANTIER_RESSOURCE, C.IDX_CHANTIER, C.IDX_CLIENT,c.NOM_CHANTIER, c.[REMARQUE],"
                    + "c.[TYPE_CHANTIER], d.[DATE_DEBUT], d.[DATE_FIN], c.[CHANTIER_FRACTIONNE], "
                    + "c.[IDX_PROJET], c.[DATE_CREATION], c.[DATE_MODIFICATION], c.[IDX_SOUS_PROJET], "
                    + "c.[POURCENTAGE_REALISATION], r.[EMAIL], r.[PRENOM_RESSOURCE], r.[NOM_RESSOURCE], "
                    + "P.[PROJET], P.[_PROJNR], S.[SOUS_PROJET], s._KUNDE, s._PROJEKTNR, k.SOCIETE, k.VILLE, "
                    + "x.LIBELLE as Kategorie, E.LIBELLE as Status,  a.SERVICE as Gruppe, AP.SERVICE as Hauptgruppe, "
                    + "C._SOLLH, C._ISTH, U.EMAIL Projektleiter, r.LIBELLE_RESSOURCE, C.INDISPONIBILITE, P.INVALIDE, C._EXCHANGEID "
                    + "FROM CHANTIER_RESSOURCE CR "
                    + "inner join CHANTIER C on CR.IDX_CHANTIER = c.IDX_CHANTIER "
                    + "inner join RESSOURCE R on CR.IDX_RESSOURCE = R.IDX_RESSOURCE "
                    + "left join CHANTIER_DATE d on cr.IDX_CHANTIER = d.IDX_CHANTIER "
                    + "left join PROJET P on C.IDX_PROJET = P.IDX_PROJET "
                    + "left join SOUS_PROJET S on C.IDX_SOUS_PROJET = S.IDX_SOUS_PROJET "
                    + "left join CLIENT K on P.IDX_CLIENT = K.IDX_CLIENT "
                    + "left join ETAT E on c.IDX_ETAT = E.IDX_ETAT "
                    + "left join SERVICE_ORDRE O on r.IDX_RESSOURCE = O.IDX_RESSOURCE "
                    + "left join SERVICE A on o.IDX_SERVICE = A.IDX_SERVICE "
                    + "left join SERVICE AP on A.IDX_SERVICE_PARENT = AP.IDX_SERVICE "
                    + "left join (SELECT ROW_NUMBER() OVER(ORDER BY IDX_COULEUR ASC) AS Row#, LIBELLE FROM COULEUR) x on c.INDICE_COULEUR = x.Row# -1 "
                    + "left join UTILISATEUR u on P.IDX_RESPONSABLE = U.IDX_UTILISATEUR "
                    + "where R.INVALIDE = 0 AND r.[PRENOM_RESSOURCE] = '" + Vorname + "' AND r.[NOM_RESSOURCE] = '" + Nachname + "'";
            else
                sql = "SELECT CR.IDX_CHANTIER_RESSOURCE, C.IDX_CHANTIER, C.IDX_CLIENT,c.NOM_CHANTIER, c.[REMARQUE],"
                    + "c.[TYPE_CHANTIER], d.[DATE_DEBUT], d.[DATE_FIN], c.[CHANTIER_FRACTIONNE], "
                    + "c.[IDX_PROJET], c.[DATE_CREATION], c.[DATE_MODIFICATION], c.[IDX_SOUS_PROJET], "
                    + "c.[POURCENTAGE_REALISATION], r.[EMAIL], r.[PRENOM_RESSOURCE], r.[NOM_RESSOURCE], "
                    + "P.[PROJET], P.[_PROJNR], S.[SOUS_PROJET], s._KUNDE, s._PROJEKTNR, k.SOCIETE, k.VILLE, "
                    + "x.LIBELLE as Kategorie, E.LIBELLE as Status,  a.SERVICE as Gruppe, AP.SERVICE as Hauptgruppe, "
                    + "C._SOLLH, C._ISTH, U.EMAIL Projektleiter, r.LIBELLE_RESSOURCE, C.INDISPONIBILITE, P.INVALIDE, C._EXCHANGEID "
                    + "FROM CHANTIER_RESSOURCE CR "
                    + "inner join CHANTIER C on CR.IDX_CHANTIER = c.IDX_CHANTIER "
                    + "inner join RESSOURCE R on CR.IDX_RESSOURCE = R.IDX_RESSOURCE "
                    + "left join CHANTIER_DATE d on cr.IDX_CHANTIER = d.IDX_CHANTIER "
                    + "left join PROJET P on C.IDX_PROJET = P.IDX_PROJET "
                    + "left join SOUS_PROJET S on C.IDX_SOUS_PROJET = S.IDX_SOUS_PROJET "
                    + "left join CLIENT K on P.IDX_CLIENT = K.IDX_CLIENT "
                    + "left join ETAT E on c.IDX_ETAT = E.IDX_ETAT "
                    + "left join SERVICE_ORDRE O on r.IDX_RESSOURCE = O.IDX_RESSOURCE "
                    + "left join SERVICE A on o.IDX_SERVICE = A.IDX_SERVICE "
                    + "left join SERVICE AP on A.IDX_SERVICE_PARENT = AP.IDX_SERVICE "
                    + "left join (SELECT ROW_NUMBER() OVER(ORDER BY IDX_COULEUR ASC) AS Row#, LIBELLE FROM COULEUR) x on c.INDICE_COULEUR = x.Row# -1 "
                    + "left join UTILISATEUR u on P.IDX_RESPONSABLE = U.IDX_UTILISATEUR "
                    + "where R.INVALIDE = 0 AND a.SERVICE = '" + group + "'";

            return sql;
        }

        /// <summary>
        /// SQL Abfrage zum lesen aller Namen im PME
        /// </summary>
        /// <param name="group">Alle oder spezifische Gruppe</param>
        /// <returns>SQL Abfrage</returns>
        public static string sqlSel_abrufPME_Namen(string group = "none")
        {
            string sql;

            if (group == "none")
                sql = "SELECT r.[PRENOM_RESSOURCE], r.[NOM_RESSOURCE] "
                    + "FROM CHANTIER_RESSOURCE CR "
                    + "inner join CHANTIER C on CR.IDX_CHANTIER = c.IDX_CHANTIER "
                    + "inner join RESSOURCE R on CR.IDX_RESSOURCE = R.IDX_RESSOURCE "
                    + "left join CHANTIER_DATE d on cr.IDX_CHANTIER = d.IDX_CHANTIER "
                    + "left join PROJET P on C.IDX_PROJET = P.IDX_PROJET "
                    + "left join SOUS_PROJET S on C.IDX_SOUS_PROJET = S.IDX_SOUS_PROJET "
                    + "left join CLIENT K on P.IDX_CLIENT = K.IDX_CLIENT "
                    + "left join ETAT E on c.IDX_ETAT = E.IDX_ETAT "
                    + "left join SERVICE_ORDRE O on r.IDX_RESSOURCE = O.IDX_RESSOURCE "
                    + "left join SERVICE A on o.IDX_SERVICE = A.IDX_SERVICE "
                    + "left join SERVICE AP on A.IDX_SERVICE_PARENT = AP.IDX_SERVICE "
                    + "left join (SELECT ROW_NUMBER() OVER(ORDER BY IDX_COULEUR ASC) AS Row#, LIBELLE FROM COULEUR) x on c.INDICE_COULEUR = x.Row# -1 "
                    + "left join UTILISATEUR u on P.IDX_RESPONSABLE = U.IDX_UTILISATEUR "
                    + "where R.INVALIDE = 0";
            else
                sql = "SELECT r.[PRENOM_RESSOURCE], r.[NOM_RESSOURCE] "
                    + "FROM CHANTIER_RESSOURCE CR "
                    + "inner join CHANTIER C on CR.IDX_CHANTIER = c.IDX_CHANTIER "
                    + "inner join RESSOURCE R on CR.IDX_RESSOURCE = R.IDX_RESSOURCE "
                    + "left join CHANTIER_DATE d on cr.IDX_CHANTIER = d.IDX_CHANTIER "
                    + "left join PROJET P on C.IDX_PROJET = P.IDX_PROJET "
                    + "left join SOUS_PROJET S on C.IDX_SOUS_PROJET = S.IDX_SOUS_PROJET "
                    + "left join CLIENT K on P.IDX_CLIENT = K.IDX_CLIENT "
                    + "left join ETAT E on c.IDX_ETAT = E.IDX_ETAT "
                    + "left join SERVICE_ORDRE O on r.IDX_RESSOURCE = O.IDX_RESSOURCE "
                    + "left join SERVICE A on o.IDX_SERVICE = A.IDX_SERVICE "
                    + "left join SERVICE AP on A.IDX_SERVICE_PARENT = AP.IDX_SERVICE "
                    + "left join (SELECT ROW_NUMBER() OVER(ORDER BY IDX_COULEUR ASC) AS Row#, LIBELLE FROM COULEUR) x on c.INDICE_COULEUR = x.Row# -1 "
                    + "left join UTILISATEUR u on P.IDX_RESPONSABLE = U.IDX_UTILISATEUR "
                    + "where R.INVALIDE = 0 AND a.SERVICE = '" + group + "'";
            return sql;
        }

        /// <summary>
        /// SQL Abfrage zum lesen aller Gruppen im PME
        /// </summary>
        /// <returns>SQL Abfrage</returns>
        public static string sqlSel_abrufPME_Gruppen()
        {
            string sql;

            sql = "SELECT a.SERVICE as Gruppe "
                + "FROM CHANTIER_RESSOURCE CR "
                + "inner join CHANTIER C on CR.IDX_CHANTIER = c.IDX_CHANTIER "
                + "inner join RESSOURCE R on CR.IDX_RESSOURCE = R.IDX_RESSOURCE "
                + "left join CHANTIER_DATE d on cr.IDX_CHANTIER = d.IDX_CHANTIER "
                + "left join PROJET P on C.IDX_PROJET = P.IDX_PROJET "
                + "left join SOUS_PROJET S on C.IDX_SOUS_PROJET = S.IDX_SOUS_PROJET "
                + "left join CLIENT K on P.IDX_CLIENT = K.IDX_CLIENT "
                + "left join ETAT E on c.IDX_ETAT = E.IDX_ETAT "
                + "left join SERVICE_ORDRE O on r.IDX_RESSOURCE = O.IDX_RESSOURCE "
                + "left join SERVICE A on o.IDX_SERVICE = A.IDX_SERVICE "
                + "left join SERVICE AP on A.IDX_SERVICE_PARENT = AP.IDX_SERVICE "
                + "left join (SELECT ROW_NUMBER() OVER(ORDER BY IDX_COULEUR ASC) AS Row#, LIBELLE FROM COULEUR) x on c.INDICE_COULEUR = x.Row# -1 "
                + "left join UTILISATEUR u on P.IDX_RESPONSABLE = U.IDX_UTILISATEUR "
                + "where R.INVALIDE = 0";

            return sql;
        }

        /// <summary>
        /// SQL Abfrage zum lesen aller Zeiten im PME
        /// </summary>
        /// <param name="user">Benutzer</param>
        /// <param name="filter">Filter</param>
        /// <param name="start">Datum Anfang</param>
        /// <param name="finish">Datum Ende</param>
        /// <returns>SQL Abfrage</returns>
        public static string sqlSel_abrufPME_Zeiten(string user, string filter, DateTime start, DateTime finish)
        {
            string Vorname = "";
            string Nachname = "";

            if (user != null && user != string.Empty && user != "")
            {
                Vorname = user.Split(' ')[0];
                Nachname = user.Split(' ')[1];
            }

            string sql;

            sql = "SELECT d.[DATE_DEBUT], d.[DATE_FIN] "
                + "FROM CHANTIER_RESSOURCE CR "
                + "inner join CHANTIER C on CR.IDX_CHANTIER = c.IDX_CHANTIER "
                + "inner join RESSOURCE R on CR.IDX_RESSOURCE = R.IDX_RESSOURCE "
                + "left join CHANTIER_DATE d on cr.IDX_CHANTIER = d.IDX_CHANTIER "
                + "left join PROJET P on C.IDX_PROJET = P.IDX_PROJET "
                + "left join SOUS_PROJET S on C.IDX_SOUS_PROJET = S.IDX_SOUS_PROJET "
                + "left join CLIENT K on P.IDX_CLIENT = K.IDX_CLIENT "
                + "left join ETAT E on c.IDX_ETAT = E.IDX_ETAT "
                + "left join SERVICE_ORDRE O on r.IDX_RESSOURCE = O.IDX_RESSOURCE "
                + "left join SERVICE A on o.IDX_SERVICE = A.IDX_SERVICE "
                + "left join SERVICE AP on A.IDX_SERVICE_PARENT = AP.IDX_SERVICE "
                + "left join (SELECT ROW_NUMBER() OVER(ORDER BY IDX_COULEUR ASC) AS Row#, LIBELLE FROM COULEUR) x on c.INDICE_COULEUR = x.Row# -1 "
                + "left join UTILISATEUR u on P.IDX_RESPONSABLE = U.IDX_UTILISATEUR "
                + "where R.INVALIDE = 0 AND r.[PRENOM_RESSOURCE] = '" + Vorname + "' AND r.[NOM_RESSOURCE] = '" + Nachname + "' "
                + "AND c.NOM_CHANTIER Like '%" + filter + "%' AND "
                + "d.[DATE_DEBUT] >= '" + start.ToString("dd.MM.yyyy") + "' AND d.[DATE_FIN] <= '" + finish.ToString("dd.MM.yyyy") + "'";

            return sql;
        }
        #endregion //Select

        #region Insert
        #endregion //Insert

        #region Update
        #endregion //Update

        #region Delete
        #endregion //Delete
        #endregion //PME

        #region Functions
        /// <summary>
        /// Berechnet die Zeitspanne zwischen zwei Zeiten in Stunden und rundet das Ergebnis auf zwei Stellen.
        /// </summary>
        /// <param name="start">Anfangszeit</param>
        /// <param name="end">Endezeit</param>
        /// <returns>Zeitspanne</returns>
        private static double Calc_TimeSpan(DateTime start, DateTime end)
        {
            return Math.Round(new TimeSpan(end.Ticks - start.Ticks).TotalMinutes / 60, 2);
        }

        /// <summary>
        /// Double Werte in ein SQL fähiges Format ändern.
        /// </summary>
        /// <param name="value">Wert als Double</param>
        /// <returns>Wert als String</returns>
        private static string Format_DBL(double value)
        {
            return value.ToString("#.00").Replace(',', '.');
        }
        #endregion //Functions
    }
}