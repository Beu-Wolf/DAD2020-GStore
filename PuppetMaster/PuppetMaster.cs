using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Threading;

namespace PuppetMaster
{
    public class PuppetMaster
    {
        private PuppetMasterForm Form;

        public PuppetMaster()
        {

        }

        public void ParseCommand(string command)
        {
            this.Form.Log(command);
        }

        public void LinkForm(PuppetMasterForm form)
        { 
            this.Form = form;
            this.Form.LinkPuppetMaster(this);
        }
    }
}
