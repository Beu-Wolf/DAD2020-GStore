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
        private delegate void LogDelegate(string msg);
        private PuppetMaster PuppetMaster;
        public PuppetMasterForm()
        {
            InitializeComponent();
        }

        public void LinkPuppetMaster(PuppetMaster pm)
        {
            this.PuppetMaster = pm;
        }

        public void Log(string msg)
        {
            if (logBox.InvokeRequired)
            {
                LogDelegate ld = new LogDelegate(Log);
                logBox.Invoke(ld, new object[] { msg });
            }
            else
            {
                logBox.Text += msg + "\r\n";
            }
        }

        private void sendCommandToPuppetMaster()
        {
            this.PuppetMaster.ParseCommand(commandBox.Text);
            commandBox.Clear();
        }

        private void executeButton_Click(object sender, EventArgs e)
        {
            sendCommandToPuppetMaster();
        }

        private void commandBox_KeyUp(object sender, KeyEventArgs e)
        {
            if (e.KeyCode == Keys.Enter)
            {
                sendCommandToPuppetMaster();
            }
        }
    }
}
