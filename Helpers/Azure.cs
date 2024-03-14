using Microsoft.WindowsAzure.ServiceRuntime;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;

namespace WebAPI_Axess.Helpers
{
    public static class Azure
    {
        public static bool m_IsRunningAzure = GetIsRunningInAzure();

        private static bool GetIsRunningInAzure()
        {
            Guid guidId;
            if (RoleEnvironment.IsAvailable && Guid.TryParse(RoleEnvironment.DeploymentId, out guidId))
                return true;
            return false;
        }


        public static bool IsRunningInAzure()
        {
            return m_IsRunningAzure;
        }

        public static bool m_IsRunningAzureOrDevFabric = GetIsRunningInAzureOrDevFabric();

        private static bool GetIsRunningInAzureOrDevFabric()
        {
            return RoleEnvironment.IsAvailable;
        }

        public static bool IsRunningInAzureOrDevFabric()
        {
            return m_IsRunningAzureOrDevFabric;
        }
    }
}