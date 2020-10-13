using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Drawing;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Forms;

namespace PuppetMaster
{
    public partial class PuppetMasterForm : Form
    {
        private PuppetMaster PuppetMaster;
        public PuppetMasterForm()
        {
            InitializeComponent();
        }

        public void LinkPuppetMaster(PuppetMaster pm)
        {
            this.PuppetMaster = pm;
        }
    }
}
