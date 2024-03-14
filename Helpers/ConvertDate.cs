using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Web;


namespace WebAPI_Axess.Helpers
{
    /// <summary>
    /// This class will convert DateTime.
    /// </summary>
    public static class ConvertDate
    {
        /// <summary>
        /// This method will handle the convert of a DateTime values represented as a decimal value to
        /// format MM/DD/YYYY HH:mm:SS
        /// </summary>
        /// <param name="dateValue">This is a decimal value that represent a DateTime. 
        /// An example could be 40177.999988425926
        /// If this value is passed will the returned value be 2009-12-31 23:59:59</param>
        /// <returns>The DateTime is returned in format  MM/DD/YYYY HH:MM:SS.
        /// Note the Date is represented as american format</returns>
        public static string ConvDecDateTime(string dateValue)
        {
            string[] arr = dateValue.Split('.');

            //Get the number of days since 1899-12-30 into string format MM/DD/YYYY
            string amDate = DateTime.Parse("1899-12-30").AddDays(int.Parse(arr[0])).ToString();

            //Note if month or day is singular prefix with 0
            if (amDate.IndexOf('/') < 2)
                amDate = "0" + amDate;

            if (amDate.LastIndexOf('/') < 5)
                amDate = amDate.Insert(3,"0");

            //Get the decimal part which represent part of a day into sec
            //If this Web API is run locally you must use delimiter comma but if you run it in azure you must
            //use decimal dot
            double partOfdayInSec = double.Parse( "0." + arr[1]) * 24 *60 *60;
            
            //Convert the seconds into hh:mm:ss
             string time = TimeSpan.FromSeconds(partOfdayInSec).ToString("hh':'mm':'ss");
   
            //Will return a DateTime in format MM/DD/YYYY hh:mm:ss which is the american format
            return amDate.Substring(0, 10) + " " + time;
        }
    }
}