using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;

namespace WebAPI_Axess.Models
{
    public class ResponseSessionMessage
    {
        public bool HasSucceeded { get; set; }
        public string ErrorMsg { get; set; }
        public string SessionID { get; set; }
    }
}