using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Drawing;
using System.IO;
using System.Linq;
using System.Security;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Forms;

namespace PuppetMaster
{
    public partial class PuppetMasterForm : Form
    {
        private delegate void LogDelegate(string msg);
        private PuppetMaster PuppetMaster;
        private OpenFileDialog FileDialog = new OpenFileDialog();
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
                logBox.AppendText(msg + "\r\n");
            }
        }

        private void sendCommandToPuppetMaster()
        {
            this.PuppetMaster.ParseCommand(commandBox.Text);
            commandBox.Clear();
        }

        private void RunScriptButton_Click(object sender, EventArgs e)
        {
            if (this.FileDialog.ShowDialog() == DialogResult.OK)
            {
                try
                {
                    var streamReader = new StreamReader(this.FileDialog.FileName);
                    String line;
                    while ((line = streamReader.ReadLine()) != null)
                    {
                        this.PuppetMaster.ParseCommand(line);
                    }

                }
                catch (SecurityException ex)
                {
                    MessageBox.Show($"Security Error. Message: {ex.Message}\n\n" +
                        $"Details: {ex.StackTrace}");
                }
            }
        }

        private void commandBox_KeyDown(object sender, KeyEventArgs e)
        {
            if (e.KeyCode == Keys.Enter)
            {
                e.Handled = true;
                e.SuppressKeyPress = true;
                sendCommandToPuppetMaster();
            }
        }
    }
}
