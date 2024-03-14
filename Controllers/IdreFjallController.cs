using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;
using System.Web;
using System.Web.Mvc;
using WebAPI_Axess.Helpers;
using WebAPI_Axess.Models;
using WebAPI_Axess.ServiceReference1;


namespace WebAPI_Axess.Controllers
{
    public class IdreFjallController : Controller
    {
        string username = "QBIM";
        string password = "jEknak?9";
        string connectionString = "Data Source=qbim.database.windows.net;Initial Catalog=IdreFjall;Persist Security Info=True;User ID=Daniel;Password=QbimAdmin123%";

        public ActionResult Index()
        {
            ViewBag.Title = "Home Page";
            return View();
        }

        /// <summary>
        /// This action is getting a new sessionId by asking the SOAP API
        /// </summary>
        /// <returns>A Json object</returns>
        [HttpGet]
        public JsonResult Login()
        {
            string sessionID;
            AxDCI4CRMClient client = new AxDCI4CRMClient("BasicHttpsBinding_IAxDCI4CRM");

            try
            {
                var result = client.login(username, password);

                if (result.NERRORNO != 0)
                    throw new InvalidOperationException("Client.login return errror " + result.NERRORNO);

                sessionID = result.NSESSIONID.ToString();
            }
            catch (Exception ex)
            {
                return Json(new ResponseSessionMessage()
                {
                    ErrorMsg = ex.Message + "/" + ex.InnerException,
                    HasSucceeded = false,
                    SessionID = ""
                }, JsonRequestBehavior.AllowGet);
            }

            return Json(new ResponseSessionMessage()
            {
                ErrorMsg = "",
                HasSucceeded = true,
                SessionID = sessionID

            }, JsonRequestBehavior.AllowGet);
        }

        [HttpGet]
        public JsonResult TransferSales(string fromRow, string sessionId)
        {
            try
            {
                AxDCI4CRMClient client = new AxDCI4CRMClient("BasicHttpsBinding_IAxDCI4CRM");
                var result = client.getSalesList(decimal.Parse(sessionId), decimal.Parse(fromRow), 999, null, null);
                if (result.NERRORNO != 0)
                    throw new InvalidOperationException("Client.getSalesList return errror " + result.NERRORNO);

                client.Close();

                var response = InsertSales(result);

                return Json(response, JsonRequestBehavior.AllowGet);
            }
            catch (Exception ex)
            {
                return Json(new ResponseMessage()
                {
                    LastRow = 0,
                    NumberOfTransferedRows = 0,
                    HasSucceded = false,
                    ErrorMsg = ex.Message + "/" + ex.InnerException
                }, JsonRequestBehavior.AllowGet);
            }
        }

        public ResponseMessage InsertSales(DCI4CRMLOGLISTRESULT result)
        {
            string insertQuery = "INSERT INTO [dbo].[TABKassaTransAufbereit](nProjNr, nKassaNr, nSerienNr, nUniCodeNr, " +
                "nKassaTransAktArt, nEinzelJournalNr, nStornoJournalNr, nKundenKartenTypNr, nPersTypNr, nArtikelNr, nStk," +
                "fEinzelTarifTats, fEinzelTarifBlatt, dtAusgabeDat, dtAusgabeZeit, dtGiltAb, dtGiltBis, nKassierID, nPoolNr, nPoolPersTypNr," +
                "nKassierAbrechNr, nZeitStafNr, nZeitStafTabNr, nWochStafNr, nWochStafTabNr, nSaisStafNr, nSaisStafTabNr, nTarifBlattNr," +
                "nTarifBlattGueltNr, nLfdSaisonNr, nActProjNr, nActKassaNr, nActGesNr, nProjNrAlt, nKassaNrAlt, nSerienNrAlt, nUniCodeNrAlt, nRestPunkteAlt," +
                "bLeserTransIsModified, dtFirstLeserChanges, nLfdZaehler, nAnzGrpPersonen, nPersProjNr, nPersKassaNr, nPersPersNr, nFirmenProjNr," +
                "nFirmenKassaNr, nFirmenNr, nTransPersProjNr, nTransPersKassaNr, nTransPersPersNr, nKartenNr, dtInsertTimeStamp, nTransNr, nBlockNr," +
                "nRentalPersTypeNr, nRentalItemNr, nWareSaleItemNr, fAmount, nSaleQuantity, fWareTarif, nLogNumber)" +
                "VALUES(@nProjNr, @nKassaNr, @nSerienNr, @nUniCodeNr, " +
                "@nKassaTransAktArt, @nEinzelJournalNr, @nStornoJournalNr, @nKundenKartenTypNr, @nPersTypNr, @nArtikelNr, @nStk," +
                "@fEinzelTarifTats, @fEinzelTarifBlatt, @dtAusgabeDat, @dtAusgabeZeit, @dtGiltAb, @dtGiltBis, @nKassierID, @nPoolNr, @nPoolPersTypNr," +
                "@nKassierAbrechNr, @nZeitStafNr, @nZeitStafTabNr, @nWochStafNr, @nWochStafTabNr, @nSaisStafNr, @nSaisStafTabNr, @nTarifBlattNr," +
                "@nTarifBlattGueltNr, @nLfdSaisonNr, @nActProjNr, @nActKassaNr, @nActGesNr, @nProjNrAlt, @nKassaNrAlt, @nSerienNrAlt, @nUniCodeNrAlt, @nRestPunkteAlt," +
                "@bLeserTransIsModified, @dtFirstLeserChanges, @nLfdZaehler, @nAnzGrpPersonen, @nPersProjNr, @nPersKassaNr, @nPersPersNr, @nFirmenProjNr," +
                "@nFirmenKassaNr, @nFirmenNr, @nTransPersProjNr, @nTransPersKassaNr, @nTransPersPersNr, @nKartenNr, @dtInsertTimeStamp, @nTransNr," +
                "@nBlockNr, @nRentalPersTypeNr, @nRentalItemNr, @nWareSaleItemNr, @fAmount, @nSaleQuantity, @fWareTarif, @nLogNumber)";

            using (SqlConnection connection = new SqlConnection(connectionString))
            using (SqlCommand command = new SqlCommand(insertQuery, connection))
            {
                command.Parameters.Add("@nProjNr", SqlDbType.Int);
                command.Parameters.Add("@nKassaNr", SqlDbType.Int);
                command.Parameters.Add("@nSerienNr", SqlDbType.Int);
                command.Parameters.Add("@nUniCodeNr", SqlDbType.Int);
                command.Parameters.Add("@nKassaTransAktArt", SqlDbType.Int);
                command.Parameters.Add("@nEinzelJournalNr", SqlDbType.Int);
                command.Parameters.Add("@nStornoJournalNr", SqlDbType.Int);
                command.Parameters.Add("@nKundenKartenTypNr", SqlDbType.Int);

                command.Parameters.Add("@nPersTypNr", SqlDbType.Int);
                command.Parameters.Add("@nArtikelNr", SqlDbType.Int);
                command.Parameters.Add("@nStk", SqlDbType.Int);
                command.Parameters.Add("@fEinzelTarifTats", SqlDbType.Int);
                command.Parameters.Add("@fEinzelTarifBlatt", SqlDbType.Int);
                command.Parameters.Add("@dtAusgabeDat", SqlDbType.NVarChar);
                command.Parameters.Add("@dtAusgabeZeit", SqlDbType.NVarChar);
                command.Parameters.Add("@dtGiltAb", SqlDbType.NVarChar);

                command.Parameters.Add("@dtGiltBis", SqlDbType.NVarChar);
                command.Parameters.Add("@nKassierID", SqlDbType.Int);
                command.Parameters.Add("@nPoolNr", SqlDbType.Int);
                command.Parameters.Add("@nPoolPersTypNr", SqlDbType.Int);
                command.Parameters.Add("@nKassierAbrechNr", SqlDbType.Int);
                command.Parameters.Add("@nZeitStafNr", SqlDbType.Int);
                command.Parameters.Add("@nZeitStafTabNr", SqlDbType.Int);
                command.Parameters.Add("@nWochStafNr", SqlDbType.Int);

                command.Parameters.Add("@nWochStafTabNr", SqlDbType.Int);
                command.Parameters.Add("@nSaisStafNr", SqlDbType.Int);
                command.Parameters.Add("@nSaisStafTabNr", SqlDbType.Int);
                command.Parameters.Add("@nTarifBlattNr", SqlDbType.Int);
                command.Parameters.Add("@nTarifBlattGueltNr", SqlDbType.Int);
                command.Parameters.Add("@nLfdSaisonNr", SqlDbType.Int);
                command.Parameters.Add("@nActProjNr", SqlDbType.Int);
                command.Parameters.Add("@nActKassaNr", SqlDbType.Int);

                command.Parameters.Add("@nActGesNr", SqlDbType.Int);
                command.Parameters.Add("@nProjNrAlt", SqlDbType.Int);
                command.Parameters.Add("@nKassaNrAlt", SqlDbType.Int);
                command.Parameters.Add("@nSerienNrAlt", SqlDbType.Int);
                command.Parameters.Add("@nUniCodeNrAlt", SqlDbType.Int);
                command.Parameters.Add("@nRestPunkteAlt", SqlDbType.Int);
                command.Parameters.Add("@bLeserTransIsModified", SqlDbType.Int);
                command.Parameters.Add("@dtFirstLeserChanges", SqlDbType.NVarChar);

                command.Parameters.Add("@nLfdZaehler", SqlDbType.Int);
                command.Parameters.Add("@nAnzGrpPersonen", SqlDbType.Int);
                command.Parameters.Add("@nPersProjNr", SqlDbType.Int);
                command.Parameters.Add("@nPersKassaNr", SqlDbType.Int);
                command.Parameters.Add("@nPersPersNr", SqlDbType.Int);
                command.Parameters.Add("@nFirmenProjNr", SqlDbType.Int);
                command.Parameters.Add("@nFirmenKassaNr", SqlDbType.Int);
                command.Parameters.Add("@nFirmenNr", SqlDbType.Int);

                command.Parameters.Add("@nTransPersProjNr", SqlDbType.Int);
                command.Parameters.Add("@nTransPersKassaNr", SqlDbType.Int);
                command.Parameters.Add("@nTransPersPersNr", SqlDbType.Int);
                command.Parameters.Add("@nKartenNr", SqlDbType.VarChar);
                command.Parameters.Add("@dtInsertTimeStamp", SqlDbType.NVarChar);
                command.Parameters.Add("@nTransNr", SqlDbType.Int);
                command.Parameters.Add("@nBlockNr", SqlDbType.Int);
                command.Parameters.Add("@nRentalPersTypeNr", SqlDbType.Int);

                command.Parameters.Add("@nRentalItemNr", SqlDbType.Int);
                command.Parameters.Add("@nWareSaleItemNr", SqlDbType.Int);
                command.Parameters.Add("@fAmount", SqlDbType.Int);
                command.Parameters.Add("@nSaleQuantity", SqlDbType.Int);
                command.Parameters.Add("@fWareTarif", SqlDbType.Int);
                command.Parameters.Add("@nLogNumber", SqlDbType.Int);


                connection.Open();

                int lastRowNumber = 0;
                int numberOfTransferedRows = 0;
                foreach (var saleItem in result.ACTLOGLINE)
                {
                    try
                    {
                        command.Parameters["@nProjNr"].Value = CreateParameterValue(saleItem, "NPROJNR");
                        command.Parameters["@nKassaNr"].Value = CreateParameterValue(saleItem, "NKASSANR");
                        command.Parameters["@nSerienNr"].Value = CreateParameterValue(saleItem, "NSERIENNR");
                        command.Parameters["@nUniCodeNr"].Value = CreateParameterValue(saleItem, "NUNICODENR");
                        command.Parameters["@nKassaTransAktArt"].Value = CreateParameterValue(saleItem, "NKASSATRANSAKTART");
                        command.Parameters["@nEinzelJournalNr"].Value = CreateParameterValue(saleItem, "NEINZELJOURNALNR");
                        command.Parameters["@nStornoJournalNr"].Value = CreateParameterValue(saleItem, "NSTORNOJOURNALNR");
                        command.Parameters["@nKundenKartenTypNr"].Value = CreateParameterValue(saleItem, "NKUNDENKARTENTYPNR");

                        command.Parameters["@nPersTypNr"].Value = CreateParameterValue(saleItem, "NPERSTYPNR");
                        command.Parameters["@nArtikelNr"].Value = CreateParameterValue(saleItem, "NARTIKELNR");
                        command.Parameters["@nStk"].Value = CreateParameterValue(saleItem, "NSTK");

                        object fEinzelTarifTats = CreateParameterValue(saleItem, "FEINZELTARIFTATS");
                        command.Parameters["@fEinzelTarifTats"].Value = fEinzelTarifTats != DBNull.Value ? int.Parse(fEinzelTarifTats.ToString()) : fEinzelTarifTats;

                        object fEinzelTarifBlatt = CreateParameterValue(saleItem, "FEINZELTARIFBLATT");
                        command.Parameters["@fEinzelTarifBlatt"].Value = fEinzelTarifBlatt != DBNull.Value ? int.Parse(fEinzelTarifBlatt.ToString()) : fEinzelTarifBlatt;

                        command.Parameters["@dtAusgabeDat"].Value = CreateDateParameterValue(saleItem, "DTAUSGABEDAT");
                        command.Parameters["@dtAusgabeZeit"].Value = CreateDateParameterValue(saleItem, "DTAUSGABEZEIT");
                        command.Parameters["@dtGiltAb"].Value = CreateDateParameterValue(saleItem, "DTGILTAB");

                        command.Parameters["@dtGiltBis"].Value = CreateDateParameterValue(saleItem, "DTGILTBIS");
                        command.Parameters["@nKassierID"].Value = CreateParameterValue(saleItem, "NKASSIERID");
                        command.Parameters["@nPoolNr"].Value = CreateParameterValue(saleItem, "NPOOLNR");
                        command.Parameters["@nPoolPersTypNr"].Value = CreateParameterValue(saleItem, "NPOOLPERSTYPNR");
                        command.Parameters["@nKassierAbrechNr"].Value = CreateParameterValue(saleItem, "NKASSIERABRECHNR");
                        command.Parameters["@nZeitStafNr"].Value = CreateParameterValue(saleItem, "NZEITSTAFNR");
                        command.Parameters["@nZeitStafTabNr"].Value = CreateParameterValue(saleItem, "NZEITSTAFTABNR");
                        command.Parameters["@nWochStafNr"].Value = CreateParameterValue(saleItem, "NWOCHSTAFNR");

                        command.Parameters["@nWochStafTabNr"].Value = CreateParameterValue(saleItem, "NWOCHSTAFTABNR");
                        command.Parameters["@nSaisStafNr"].Value = CreateParameterValue(saleItem, "NSAISSTAFNR");
                        command.Parameters["@nSaisStafTabNr"].Value = CreateParameterValue(saleItem, "NSAISSTAFTABNR");
                        command.Parameters["@nTarifBlattNr"].Value = CreateParameterValue(saleItem, "NTARIFBLATTNR");
                        command.Parameters["@nTarifBlattGueltNr"].Value = CreateParameterValue(saleItem, "NTARIFBLATTGUELTNR");
                        command.Parameters["@nLfdSaisonNr"].Value = CreateParameterValue(saleItem, "NLFDSAISONNR");
                        command.Parameters["@nActProjNr"].Value = CreateParameterValue(saleItem, "NACTPROJNR");
                        command.Parameters["@nActKassaNr"].Value = CreateParameterValue(saleItem, "NACTKASSANR");

                        command.Parameters["@nActGesNr"].Value = CreateParameterValue(saleItem, "NACTGESNR");
                        command.Parameters["@nProjNrAlt"].Value = CreateParameterValue(saleItem, "NPROJNRALT");
                        command.Parameters["@nKassaNrAlt"].Value = CreateParameterValue(saleItem, "NKASSANRALT");
                        command.Parameters["@nSerienNrAlt"].Value = CreateParameterValue(saleItem, "NSERIENNRALT");
                        command.Parameters["@nUniCodeNrAlt"].Value = CreateParameterValue(saleItem, "NUNICODENRALT");
                        command.Parameters["@nRestPunkteAlt"].Value = CreateParameterValue(saleItem, "NRESTPUNKTEALT");
                        command.Parameters["@bLeserTransIsModified"].Value = CreateParameterValue(saleItem, "BLESERTRANSISMODIFIED");
                        command.Parameters["@dtFirstLeserChanges"].Value = CreateDateParameterValue(saleItem, "DTFIRSTLESERCHANGES");

                        command.Parameters["@nLfdZaehler"].Value = CreateParameterValue(saleItem, "NLFDZAEHLER");
                        command.Parameters["@nAnzGrpPersonen"].Value = CreateParameterValue(saleItem, "NANZGRPPERSONEN");
                        command.Parameters["@nPersProjNr"].Value = CreateParameterValue(saleItem, "NPERSPROJNR");
                        command.Parameters["@nPersKassaNr"].Value = CreateParameterValue(saleItem, "NPERSKASSANR");
                        command.Parameters["@nPersPersNr"].Value = CreateParameterValue(saleItem, "NPERSPERSNR");
                        command.Parameters["@nFirmenProjNr"].Value = CreateParameterValue(saleItem, "NFIRMENPROJNR");
                        command.Parameters["@nFirmenKassaNr"].Value = CreateParameterValue(saleItem, "NFIRMENKASSANR");
                        command.Parameters["@nFirmenNr"].Value = CreateParameterValue(saleItem, "NFIRMENNR");

                        command.Parameters["@nTransPersProjNr"].Value = CreateParameterValue(saleItem, "NTRANSPERSPROJNR");
                        command.Parameters["@nTransPersKassaNr"].Value = CreateParameterValue(saleItem, "NTRANSPERSKASSANR");
                        command.Parameters["@nTransPersPersNr"].Value = CreateParameterValue(saleItem, "NTRANSPERSPERSNR");
                        command.Parameters["@nKartenNr"].Value = CreateParameterValue(saleItem, "NKARTENNR");
                        command.Parameters["@dtInsertTimeStamp"].Value = CreateDateParameterValue(saleItem, "DTINSERTTIMESTAMP");
                        command.Parameters["@nTransNr"].Value = CreateParameterValue(saleItem, "NTRANSNR");
                        command.Parameters["@nBlockNr"].Value = CreateParameterValue(saleItem, "NBLOCKNR");
                        command.Parameters["@nRentalPersTypeNr"].Value = CreateParameterValue(saleItem, "NRENTALPERSTYPENR");

                        command.Parameters["@nRentalItemNr"].Value = CreateParameterValue(saleItem, "NRENTALITEMNR");
                        command.Parameters["@nWareSaleItemNr"].Value = CreateParameterValue(saleItem, "NWARESALEITEMNR");
                        
                        object fAmountValue = CreateParameterValue(saleItem, "FAMOUNT");
                        command.Parameters["@fAmount"].Value = fAmountValue != DBNull.Value ? int.Parse(fAmountValue.ToString()) : fAmountValue;

                        command.Parameters["@nSaleQuantity"].Value = CreateParameterValue(saleItem, "NSALEQUANTITY");

                        object fWareTarif = CreateParameterValue(saleItem, "FWARETARIF");
                        command.Parameters["@fWareTarif"].Value = fWareTarif != DBNull.Value ? int.Parse(fWareTarif.ToString()) : fWareTarif;

                        command.Parameters["@nLogNumber"].Value = int.Parse(saleItem.NLOGNR.Value.ToString());

                        command.ExecuteNonQuery();

                        numberOfTransferedRows++;

                        lastRowNumber = int.Parse(saleItem.NLOGNR.Value.ToString());
                    }
                    catch (Exception ex)
                    {
                        return new ResponseMessage()
                        {
                            LastRow = lastRowNumber,
                            NumberOfTransferedRows = numberOfTransferedRows,
                            HasSucceded = false,
                            ErrorMsg = ex.Message + "/" + ex.InnerException
                        };
                    };
                }

                return new ResponseMessage()
                {
                    LastRow = lastRowNumber,
                    NumberOfTransferedRows = numberOfTransferedRows,
                    HasSucceded = true,
                    ErrorMsg = ""
                };
            }
        }

        public object CreateParameterValue(DCI4CRMLOGLINE saleItem, string key)
        {
            var value = saleItem.ACTLOGVALUESNEW.Where(e => e.SZFIELDNAME == key).Select(x => x.SZFIELDVALUE).FirstOrDefault();
            if(value == null)
            {
                return DBNull.Value;
            }
            else
            {
                return value;
            }
        }

        public object CreateDateParameterValue(DCI4CRMLOGLINE saleItem, string key)
        {
            var value = saleItem.ACTLOGVALUESNEW.Where(e => e.SZFIELDNAME == key).Select(x => x.SZFIELDVALUE).FirstOrDefault();

            if (value == null)
            {
                return DBNull.Value;
            }
            else
            {
                return ConvertDate.ConvDecDateTime(value);
            }
        }

        [HttpGet]
        public JsonResult TransferUsageList(string fromRow, string sessionId )
        {
            try
            {
                AxDCI4CRMClient client = new AxDCI4CRMClient("BasicHttpsBinding_IAxDCI4CRM");
                var result = client.getUsageList(decimal.Parse(sessionId), decimal.Parse(fromRow), 999, null, null);
                
                if (result.NERRORNO != 0)
                    throw new InvalidOperationException("Client.getUsageList return errror " + result.NERRORNO);

                client.Close();

                var response = InsertUsageList(result);

                return Json(response, JsonRequestBehavior.AllowGet);
            }
            catch (Exception ex)
            {
                return Json(new ResponseMessage()
                {
                    LastRow = 0,
                    NumberOfTransferedRows = 0,
                    HasSucceded = false,
                    ErrorMsg = ex.Message + "/" + ex.InnerException
                }, JsonRequestBehavior.AllowGet);
            }
        }

        public ResponseMessage InsertUsageList(DCI4CRMLOGLISTRESULT result)
        {
            string insertQuery = "INSERT INTO [dbo].[TABLeserTrans](nCPUNr, nLfdCPUTransNr, nVerkProjNr, nKassaNr, " +
                "nSerienNr, dtVerwDat, nAkzProjNr, nPoolNr, nZutrNr, nLeserNr, nKartenGrundTypNr, nLeserPersGrpNr, nRestFahrt, dtStartZeitPunkt, " +
                "nDauerTage, nDauerStunden," +
                "bFreiFahrt, nRestPkteAlt, nRestPkteNeu, nTagesPkteAlt, nTagesPkteNeu, nRestTageAlt, nRestTageNeu, bInDepotTab," +
                "dtErstVerwZeitPunkt, nDatTraegTypNr, bZusatzAbbuch, nKompGesNr, nKompKundenKartenTypNr, nKompPersTypNr, nYear, nMonth, " +
                "nDay, bErstVerw, bErstVerwTag, bGastZutritt, bUniCodeNrSet, nKassaTransAKtArt, nUniCodeNrIntern, nUniCodeNrExtern," +
                "nLfdLeserTransNr, nKompPoolNr, nOPAusstellerNr, dtInsertTimeStamp, nSonderfallCode, szZusatzFelder, dtUnicodeNrSet, nOPTicketID," +
                "dtTicketGiltBis, dtTicketGiltAb, nPreis, nUserID, dtTicketAusgabeDat, nWaehrungsCode, nIssuerRegionID, nTargetRegionID," +
                "nOCCGesNr, nOCCWorkstationID, nHash, nCtrlModeNr, dtAblaufdatum, nKundenkartenTypnr, nPerstypNr, nRestzeitInMin," +
                "nLogNumber)" +


                "VALUES(@nCPUNr, @nLfdCPUTransNr, @nVerkProjNr, @nKassaNr, " +
                "@nSerienNr, @dtVerwDat, @nAkzProjNr, @nPoolNr, @nZutrNr, @nLeserNr, @nKartenGrundTypNr, @nLeserPersGrpNr, @nRestFahrt, @dtStartZeitPunkt, " +
                "@nDauerTage, @nDauerStunden, " +
                "@bFreiFahrt, @nRestPkteAlt, @nRestPkteNeu, @nTagesPkteAlt, @nTagesPkteNeu, @nRestTageAlt, @nRestTageNeu, @bInDepotTab," +
                "@dtErstVerwZeitPunkt, @nDatTraegTypNr, @bZusatzAbbuch, @nKompGesNr, @nKompKundenKartenTypNr, @nKompPersTypNr, @nYear, @nMonth, " +
                "@nDay, @bErstVerw, @bErstVerwTag, @bGastZutritt, @bUniCodeNrSet, @nKassaTransAKtArt, @nUniCodeNrIntern, @nUniCodeNrExtern," +
                "@nLfdLeserTransNr, @nKompPoolNr, @nOPAusstellerNr, @dtInsertTimeStamp, @nSonderfallCode, @szZusatzFelder, @dtUnicodeNrSet, @nOPTicketID," +
                "@dtTicketGiltBis, @dtTicketGiltAb, @nPreis, @nUserID, @dtTicketAusgabeDat, @nWaehrungsCode, @nIssuerRegionID, @nTargetRegionID, " +
                "@nOCCGesNr, @nOCCWorkstationID, @nHash, @nCtrlModeNr, @dtAblaufdatum, @nKundenkartenTypnr, @nPerstypNr, @nRestzeitInMin," +
                "@nLogNumber)";

            using (SqlConnection connection = new SqlConnection(connectionString))
            using (SqlCommand command = new SqlCommand(insertQuery, connection))
            {
                command.Parameters.Add("@nCPUNr", SqlDbType.BigInt);
                command.Parameters.Add("@nLfdCPUTransNr", SqlDbType.Int);
                command.Parameters.Add("@nVerkProjNr", SqlDbType.Int);
                command.Parameters.Add("@nKassaNr", SqlDbType.Int);
                command.Parameters.Add("@nSerienNr", SqlDbType.Int);
                command.Parameters.Add("@dtVerwDat", SqlDbType.VarChar);
                command.Parameters.Add("@nAkzProjNr", SqlDbType.Int);
                command.Parameters.Add("@nPoolNr", SqlDbType.Int);

                command.Parameters.Add("@nZutrNr", SqlDbType.Int);
                command.Parameters.Add("@nLeserNr", SqlDbType.Int);
                command.Parameters.Add("@nKartenGrundTypNr", SqlDbType.Int);
                command.Parameters.Add("@nLeserPersGrpNr", SqlDbType.Int);
                command.Parameters.Add("@nRestFahrt", SqlDbType.Int);
                command.Parameters.Add("@dtStartZeitPunkt", SqlDbType.VarChar);
                command.Parameters.Add("@nDauerTage", SqlDbType.Int);
                command.Parameters.Add("@nDauerStunden", SqlDbType.Int);

                command.Parameters.Add("@bFreiFahrt", SqlDbType.Int);
                command.Parameters.Add("@nRestPkteAlt", SqlDbType.Int);
                command.Parameters.Add("@nRestPkteNeu", SqlDbType.Int);
                command.Parameters.Add("@nTagesPkteAlt", SqlDbType.Int);
                command.Parameters.Add("@nTagesPkteNeu", SqlDbType.Int);
                command.Parameters.Add("@nRestTageAlt", SqlDbType.Int);
                command.Parameters.Add("@nRestTageNeu", SqlDbType.Int);
                command.Parameters.Add("@bInDepotTab", SqlDbType.Int);

                command.Parameters.Add("@dtErstVerwZeitPunkt", SqlDbType.VarChar);
                command.Parameters.Add("@nDatTraegTypNr", SqlDbType.Int);
                command.Parameters.Add("@bZusatzAbbuch", SqlDbType.Int);
                command.Parameters.Add("@nKompGesNr", SqlDbType.Int);
                command.Parameters.Add("@nKompKundenKartenTypNr", SqlDbType.Int);
                command.Parameters.Add("@nKompPersTypNr", SqlDbType.Int);
                command.Parameters.Add("@nYear", SqlDbType.Int);
                command.Parameters.Add("@nMonth", SqlDbType.Int);

                command.Parameters.Add("@nDay", SqlDbType.Int);
                command.Parameters.Add("@bErstVerw", SqlDbType.Int);
                command.Parameters.Add("@bErstVerwTag", SqlDbType.Int);
                command.Parameters.Add("@bGastZutritt", SqlDbType.Int);
                command.Parameters.Add("@bUniCodeNrSet", SqlDbType.Int);
                command.Parameters.Add("@nKassaTransAKtArt", SqlDbType.Int);
                command.Parameters.Add("@nUniCodeNrIntern", SqlDbType.Int);
                command.Parameters.Add("@nUniCodeNrExtern", SqlDbType.Int);

                command.Parameters.Add("@nLfdLeserTransNr", SqlDbType.Int);
                command.Parameters.Add("@nKompPoolNr", SqlDbType.Int);
                command.Parameters.Add("@nOPAusstellerNr", SqlDbType.Int);
                command.Parameters.Add("@dtInsertTimeStamp", SqlDbType.VarChar);
                command.Parameters.Add("@nSonderfallCode", SqlDbType.Int);
                command.Parameters.Add("@szZusatzFelder", SqlDbType.NVarChar);
                command.Parameters.Add("@dtUnicodeNrSet", SqlDbType.VarChar);
                command.Parameters.Add("@nOPTicketID", SqlDbType.Int);

                command.Parameters.Add("@dtTicketGiltBis", SqlDbType.VarChar);
                command.Parameters.Add("@dtTicketGiltAb", SqlDbType.VarChar);
                command.Parameters.Add("@nPreis", SqlDbType.Int);
                command.Parameters.Add("@nUserID", SqlDbType.Int);
                command.Parameters.Add("@dtTicketAusgabeDat", SqlDbType.VarChar);
                command.Parameters.Add("@nWaehrungsCode", SqlDbType.Int);
                command.Parameters.Add("@nIssuerRegionID", SqlDbType.Int);
                command.Parameters.Add("@nTargetRegionID", SqlDbType.Int);

                command.Parameters.Add("@nOCCGesNr", SqlDbType.Int);
                command.Parameters.Add("@nOCCWorkstationID", SqlDbType.Int);
                command.Parameters.Add("@nHash", SqlDbType.Int);
                command.Parameters.Add("@nCtrlModeNr", SqlDbType.Int);
                command.Parameters.Add("@dtAblaufdatum", SqlDbType.VarChar);
                command.Parameters.Add("@nKundenkartenTypnr", SqlDbType.Int);
                command.Parameters.Add("@nPerstypNr", SqlDbType.Int);
                command.Parameters.Add("@nRestzeitInMin", SqlDbType.Int);

                command.Parameters.Add("@nLogNumber", SqlDbType.Int);

                connection.Open();

                int lastRowNumber = 0;
                int numberOfTransferedRows = 0;
                foreach (var saleItem in result.ACTLOGLINE)
                {
                    try
                    {
                        command.Parameters["@nCPUNr"].Value = CreateParameterValue(saleItem, "NCPUNR");
                        command.Parameters["@nLfdCPUTransNr"].Value = CreateParameterValue(saleItem, "NLFDCPUTRANSNR");
                        command.Parameters["@nVerkProjNr"].Value = CreateParameterValue(saleItem, "NVERKPROJNR");
                        command.Parameters["@nKassaNr"].Value = CreateParameterValue(saleItem, "NKASSANR");
                        command.Parameters["@nSerienNr"].Value = CreateParameterValue(saleItem, "NSERIENNR");
                        command.Parameters["@dtVerwDat"].Value = CreateDateParameterValue(saleItem, "DTVERWDAT");
                        command.Parameters["@nAkzProjNr"].Value = CreateParameterValue(saleItem, "NAKZPROJNR");
                        command.Parameters["@nPoolNr"].Value = CreateParameterValue(saleItem, "NPOOLNR");

                        command.Parameters["@nZutrNr"].Value = CreateParameterValue(saleItem, "NZUTRNR");
                        command.Parameters["@nLeserNr"].Value = CreateParameterValue(saleItem, "NLESERNR");
                        command.Parameters["@nKartenGrundTypNr"].Value = CreateParameterValue(saleItem, "NKARTENGRUNDTYPNR");
                        command.Parameters["@nLeserPersGrpNr"].Value = CreateParameterValue(saleItem, "NLESERPERSGRPNR");
                        command.Parameters["@nRestFahrt"].Value = CreateParameterValue(saleItem, "NRESTFAHRT");
                        command.Parameters["@dtStartZeitPunkt"].Value = CreateDateParameterValue(saleItem, "DTSTARTZEITPUNKT");
                        command.Parameters["@nDauerTage"].Value = CreateParameterValue(saleItem, "NDAUERTAGE");
                        command.Parameters["@nDauerStunden"].Value = CreateParameterValue(saleItem, "NDAUERSTUNDEN");

                        command.Parameters["@bFreiFahrt"].Value = CreateParameterValue(saleItem, "BFREIFAHRT");
                        command.Parameters["@nRestPkteAlt"].Value = CreateParameterValue(saleItem, "NRESTPKTEALT");
                        command.Parameters["@nRestPkteNeu"].Value = CreateParameterValue(saleItem, "NRESTPKTENEU");
                        command.Parameters["@nTagesPkteAlt"].Value = CreateParameterValue(saleItem, "NTAGESPKTEALT");
                        command.Parameters["@nTagesPkteNeu"].Value = CreateParameterValue(saleItem, "NTAGESPKTENEU");
                        command.Parameters["@nRestTageAlt"].Value = CreateParameterValue(saleItem, "NRESTTAGEALT");
                        command.Parameters["@nRestTageNeu"].Value = CreateParameterValue(saleItem, "NRESTTAGENEU");
                        command.Parameters["@bInDepotTab"].Value = CreateParameterValue(saleItem, "BINDEPOTTAB");

                        command.Parameters["@dtErstVerwZeitPunkt"].Value = CreateDateParameterValue(saleItem, "DTERSTVERWZEITPUNKT");
                        command.Parameters["@nDatTraegTypNr"].Value = CreateParameterValue(saleItem, "NDATTRAEGTYPNR");
                        command.Parameters["@bZusatzAbbuch"].Value = CreateParameterValue(saleItem, "BZUSATZABBUCH");
                        command.Parameters["@nKompGesNr"].Value = CreateParameterValue(saleItem, "NKOMPGESNR");
                        command.Parameters["@nKompKundenKartenTypNr"].Value = CreateParameterValue(saleItem, "NKOMPKUNDENKARTENTYPNR");
                        command.Parameters["@nKompPersTypNr"].Value = CreateParameterValue(saleItem, "NKOMPPERSTYPNR");
                        command.Parameters["@nYear"].Value = CreateParameterValue(saleItem, "NYEAR");
                        command.Parameters["@nMonth"].Value = CreateParameterValue(saleItem, "NMONTH");

                        command.Parameters["@nDay"].Value = CreateParameterValue(saleItem, "NDAY");
                        command.Parameters["@bErstVerw"].Value = CreateParameterValue(saleItem, "BERSTVERW");
                        command.Parameters["@bErstVerwTag"].Value = CreateParameterValue(saleItem, "BERSTVERWTAG");
                        command.Parameters["@bGastZutritt"].Value = CreateParameterValue(saleItem, "BGASTZUTRITT");
                        command.Parameters["@bUniCodeNrSet"].Value = CreateParameterValue(saleItem, "BUNICODENRSET");
                        command.Parameters["@nKassaTransAKtArt"].Value = CreateParameterValue(saleItem, "NKASSATRANSAKTART");
                        command.Parameters["@nUniCodeNrIntern"].Value = CreateParameterValue(saleItem, "NUNICODENRINTERN");
                        command.Parameters["@nUniCodeNrExtern"].Value = CreateParameterValue(saleItem, "NUNICODENREXTERN");

                        command.Parameters["@nLfdLeserTransNr"].Value = CreateParameterValue(saleItem, "NLFDLESERTRANSNR");
                        command.Parameters["@nKompPoolNr"].Value = CreateParameterValue(saleItem, "NKOMPPOOLNR");
                        command.Parameters["@nOPAusstellerNr"].Value = CreateParameterValue(saleItem, "NOPAUSSTELLERNR");
                        command.Parameters["@dtInsertTimeStamp"].Value = CreateDateParameterValue(saleItem, "DTINSERTTIMESTAMP");
                        command.Parameters["@nSonderfallCode"].Value = CreateParameterValue(saleItem, "NSONDERFALLCODE");
                        command.Parameters["@szZusatzFelder"].Value = CreateParameterValue(saleItem, "SZZUSATZFELDER");
                        command.Parameters["@dtUnicodeNrSet"].Value = CreateDateParameterValue(saleItem, "DTUNICODENRSET");
                        command.Parameters["@nOPTicketID"].Value = CreateParameterValue(saleItem, "NOPTICKETID");

                        command.Parameters["@dtTicketGiltBis"].Value = CreateDateParameterValue(saleItem, "DTTICKETGILTBIS");
                        command.Parameters["@dtTicketGiltAb"].Value = CreateDateParameterValue(saleItem, "DTTICKETGILTAB");
                        command.Parameters["@nPreis"].Value = CreateParameterValue(saleItem, "NPREIS");
                        command.Parameters["@nUserID"].Value = CreateParameterValue(saleItem, "NUSERID");
                        command.Parameters["@dtTicketAusgabeDat"].Value = CreateDateParameterValue(saleItem, "DTTICKETAUSGABEDAT");
                        command.Parameters["@nWaehrungsCode"].Value = CreateParameterValue(saleItem, "NWAEHRUNGSCODE");
                        command.Parameters["@nIssuerRegionID"].Value = CreateParameterValue(saleItem, "NISSUERREGIONID");
                        command.Parameters["@nTargetRegionID"].Value = CreateParameterValue(saleItem, "NTARGETREGIONID");

                        command.Parameters["@nOCCGesNr"].Value = CreateParameterValue(saleItem, "NOCCGESNR");
                        command.Parameters["@nOCCWorkstationID"].Value = CreateParameterValue(saleItem, "NOCCWORKSTATIONID");
                        command.Parameters["@nHash"].Value = CreateParameterValue(saleItem, "NHASH");
                        command.Parameters["@nCtrlModeNr"].Value = CreateParameterValue(saleItem, "NCTRLMODENR");
                        command.Parameters["@dtAblaufdatum"].Value = CreateDateParameterValue(saleItem, "DTABLAUFDATUM");
                        command.Parameters["@nKundenkartenTypnr"].Value = CreateParameterValue(saleItem, "NKUNDENKARTENTYPNR");
                        command.Parameters["@nPerstypNr"].Value = CreateParameterValue(saleItem, "NPERSTYPNR");
                        command.Parameters["@nRestzeitInMin"].Value = CreateParameterValue(saleItem, "NRESTZEITINMIN");
                        command.Parameters["@nLogNumber"].Value = int.Parse(saleItem.NLOGNR.Value.ToString());

                        command.ExecuteNonQuery();

                        numberOfTransferedRows++;

                        lastRowNumber = int.Parse(saleItem.NLOGNR.Value.ToString());
                    }
                    catch (Exception ex)
                    {
                        return new ResponseMessage()
                        {
                            LastRow = lastRowNumber,
                            NumberOfTransferedRows = numberOfTransferedRows,
                            HasSucceded = false,
                            ErrorMsg = ex.Message + "/" + ex.InnerException
                        };
                    };
                }

                return new ResponseMessage()
                {
                    LastRow = lastRowNumber,
                    NumberOfTransferedRows = numberOfTransferedRows,
                    HasSucceded = true,
                    ErrorMsg = ""
                };
            }
        }

        public JsonResult TransferLifts()
        {
            var sessionId = LoginInternal(); // We need to fetch a new sessionId since this method only runs one time every night

            int countOfLifts = setEntitySnapshot(sessionId, "TABZutrKonf"); // We need to fetch countOfLifts before we can use getEntitySnaphot

            var result = getEntitySnapshot(sessionId, countOfLifts); // Fetch list of lifts

            var response = InsertLiftList(result);

            return Json(response, JsonRequestBehavior.AllowGet);
        }

        public JsonResult TransferTicketTypes()
        {
            var sessionId = LoginInternal(); // We need to fetch a new sessionId since this method only runs one time every night

            int countOfTypes = setEntitySnapshot(sessionId, "TABKundenKartenTypDef"); // We need to fetch countOfLifts before we can use getEntitySnaphot

            var result = getEntitySnapshot(sessionId, countOfTypes); // Fetch list of lifts

            var response = InsertTicketTypesList(result);

            return Json(response, JsonRequestBehavior.AllowGet);
        }

        public JsonResult TransferPersonTypes()
        {
            var sessionId = LoginInternal(); // We need to fetch a new sessionId since this method only runs one time every night

            int countOfTypes = setEntitySnapshot(sessionId, "TABPersTypDef"); // We need to fetch countOfLifts before we can use getEntitySnaphot

            var result = getEntitySnapshot(sessionId, countOfTypes); // Fetch list of lifts

            var response = InsertPersonTypes(result);

            return Json(response, JsonRequestBehavior.AllowGet);
        }

        public ResponseMessage InsertPersonTypes(DCI4CRMLOGLISTRESULT result)
        {
            var truncateQuery = "TRUNCATE TABLE [dbo].[persontype_definitions]";
            SqlConnection conn = new SqlConnection(connectionString);
            conn.Open();
            using (SqlCommand command = new SqlCommand(truncateQuery, conn)) // TRUNCATE TABLE BEFORE INSERT
            {
                command.ExecuteNonQuery();
            }
            conn.Close();

            string insertQuery = "INSERT INTO [dbo].[persontype_definitions]([NPERSTYPNR], [SZNAME] )" +
                "VALUES(@nPersTypNr, @szName)";

            using (SqlConnection connection = new SqlConnection(connectionString))
            using (SqlCommand command = new SqlCommand(insertQuery, connection))
            {
                command.Parameters.Add("@nPersTypNr", SqlDbType.Int);
                command.Parameters.Add("@szName", SqlDbType.VarChar);

                connection.Open();

                foreach (var saleItem in result.ACTLOGLINE)
                {
                    try
                    {
                        command.Parameters["@nPersTypNr"].Value = CreateParameterValue(saleItem, "NPERSTYPNR");
                        command.Parameters["@szName"].Value = CreateParameterValue(saleItem, "SZNAME");

                        command.ExecuteNonQuery();
                    }
                    catch
                    {
                        return new ResponseMessage()
                        {
                            HasSucceded = false
                        };
                    };
                }

                return new ResponseMessage()
                {
                    HasSucceded = true
                };
            }
        }

        public ResponseMessage InsertTicketTypesList(DCI4CRMLOGLISTRESULT result)
        {
            var truncateQuery = "TRUNCATE TABLE [dbo].[ticket_type_definitions]";
            SqlConnection conn = new SqlConnection(connectionString);
            conn.Open();
            using (SqlCommand command = new SqlCommand(truncateQuery, conn)) // TRUNCATE TABLE BEFORE INSERT
            {
                command.ExecuteNonQuery();
            }
            conn.Close();

            string insertQuery = "INSERT INTO [dbo].[ticket_type_definitions]([NKUNDENKARTENTYPNR], [SZNAME],  [NDAUERINTAGE])" +
                "VALUES(@NKUNDENKARTENTYPNR, @SZNAME, @NDAUERINTAGE)";

            using (SqlConnection connection = new SqlConnection(connectionString))
            using (SqlCommand command = new SqlCommand(insertQuery, connection))
            {
                command.Parameters.Add("@NKUNDENKARTENTYPNR", SqlDbType.Int);
                command.Parameters.Add("@SZNAME", SqlDbType.VarChar);
                command.Parameters.Add("@NDAUERINTAGE", SqlDbType.Int);

                connection.Open();

                foreach (var saleItem in result.ACTLOGLINE)
                {
                    try
                    {
                        command.Parameters["@NKUNDENKARTENTYPNR"].Value = CreateParameterValue(saleItem, "NKUNDENKARTENTYPNR");
                        command.Parameters["@SZNAME"].Value = CreateParameterValue(saleItem, "SZNAME");
                        command.Parameters["@NDAUERINTAGE"].Value = CreateParameterValue(saleItem, "NDAUERINTAGE");

                        command.ExecuteNonQuery();
                    }
                    catch
                    {
                        return new ResponseMessage()
                        {
                            HasSucceded = false
                        };
                    };
                }

                return new ResponseMessage()
                {
                    HasSucceded = true
                };
            }
        }

        public ResponseMessage InsertLiftList(DCI4CRMLOGLISTRESULT result)
        {
            var truncateQuery = "TRUNCATE TABLE [dbo].[lift_definitions]";
            SqlConnection conn = new SqlConnection(connectionString);
            conn.Open();
            using (SqlCommand command = new SqlCommand(truncateQuery, conn)) // TRUNCATE TABLE BEFORE INSERT
            {
                command.ExecuteNonQuery();
            }
            conn.Close();

            string insertQuery = "INSERT INTO [dbo].[lift_definitions]([NZUTRNR], [SZNAME] )" +
                "VALUES(@nZutrNr, @szName)";

            using (SqlConnection connection = new SqlConnection(connectionString))
            using (SqlCommand command = new SqlCommand(insertQuery, connection))
            {
                command.Parameters.Add("@nZutrNr", SqlDbType.Int);
                command.Parameters.Add("@szName", SqlDbType.VarChar);
       
                connection.Open();

                foreach (var saleItem in result.ACTLOGLINE)
                {
                    try
                    {
                        command.Parameters["@nZutrNr"].Value = CreateParameterValue(saleItem, "NZUTRNR");
                        command.Parameters["@szName"].Value = CreateParameterValue(saleItem, "SZNAME");

                        command.ExecuteNonQuery();
                    }
                    catch
                    {
                        return new ResponseMessage()
                        {
                            HasSucceded = false
                        };
                    };
                }

                return new ResponseMessage()
                {
                    HasSucceded = true
                };
            }
        }

        private DCI4CRMLOGLISTRESULT getEntitySnapshot(string sessionId, int countOfLifts)
        {
            AxDCI4CRMClient client = new AxDCI4CRMClient("BasicHttpsBinding_IAxDCI4CRM");
            var result = client.getSnapshotList(decimal.Parse(sessionId), 1, countOfLifts);

            return result;
        }

        private int setEntitySnapshot(string sessionId, string table)
        {
            AxDCI4CRMClient client = new AxDCI4CRMClient("BasicHttpsBinding_IAxDCI4CRM");
            var result = client.setEntitySnapshot(decimal.Parse(sessionId), table);

            return Convert.ToInt32(result.NCOUNT);
        }

        public string LoginInternal()
        {
            AxDCI4CRMClient client = new AxDCI4CRMClient("BasicHttpsBinding_IAxDCI4CRM");
            var result = client.login(username, password);

            client.Close();

            return result.NSESSIONID.ToString();
        }

    }
}
