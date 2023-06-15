using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data;

namespace busBillingConfiguration
{
    public class BillingConfigurationData
    {
        public static DataSet dsBillingConfiguration;
        public static DataSet dsBillingConfigurationCurrent;

        public BillingConfigurationData()
        {
            if (dsBillingConfiguration == null)
                dsBillingConfiguration = new DataSet();
            if (dsBillingConfigurationCurrent == null)
                dsBillingConfigurationCurrent = new DataSet();
        }

        public void SetCurrentConfiguration(DataSet ds)
        {
            dsBillingConfigurationCurrent = ds;
        }

        public void SetConfiguration(DataSet ds)
        {
            dsBillingConfiguration = ds;
        }
    }
}
