using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Drawing;
using System.Linq;
using System.Text;
using System.Windows.Forms;

namespace UnitTestBilling
{
    public partial class Form1 : Form
    {
        public Form1()
        {
            InitializeComponent();
        }

        private void button1_Click(object sender, EventArgs e)
        {
            BillingImplementation.clsBillingImplementation bill = new BillingImplementation.clsBillingImplementation();
            BillingImplementation.clsBillingImplementation.GetStaticData();
            //string strError = bill.BillingImplementation("PD0100", "PD0131605", 1, 4, 2016, "cmis", 65, 7);
        }
    }
}
