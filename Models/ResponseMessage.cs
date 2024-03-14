using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;

namespace WebAPI_Axess.Models
{
    public class ResponseMessage
    {
        public int LastRow { get; set; }
        public int NumberOfTransferedRows { get; set; }
        public bool HasSucceded { get; set; }
        public string ErrorMsg { get; set; }
    }
}