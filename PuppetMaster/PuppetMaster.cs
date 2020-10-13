using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace PuppetMaster
{
    public class PuppetMaster
    {
        private PuppetMasterForm Form;

        public PuppetMaster()
        {

        }

        public void LinkForm(PuppetMasterForm form)
        { 
            this.Form = form;
            this.Form.LinkPuppetMaster(this);
        }
    }
}
