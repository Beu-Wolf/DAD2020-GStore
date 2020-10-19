using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using System.Windows.Forms;

namespace PuppetMaster
{
    static class Program
    {
        /// <summary>
        /// The main entry point for the application.
        /// </summary>
        [STAThread]
        static void Main()
        {
            Application.EnableVisualStyles();
            Application.SetCompatibleTextRenderingDefault(false);

            PuppetMaster pm = new PuppetMaster();
            PuppetMasterForm form = new PuppetMasterForm();
            pm.LinkForm(form);
            Application.Run(form);
        }
    }
}
