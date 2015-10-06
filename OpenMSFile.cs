using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace OpenMS.OpenMSFile
{
    public class OpenMSFile
    {
        private String file;

        public OpenMSFile() {}

        public OpenMSFile(string file)
        {
            this.file = file;
        }

        public String get_name()
        {
            return this.file;
        }
    }

    public class MzTabFile
    {
        private String file;

        public MzTabFile(string file)
        {
            this.file = file;
        }

        public String get_name()
        {
            return this.file;
        }
    }

    public class MzMLFile
    {
        private String file;

        public MzMLFile(string file)
        {
            this.file = file;
        }

        public String get_name()
        {
            return this.file;
        }
    }

    public class ConsensusXMLFile
    {
        private String file;

        public ConsensusXMLFile(string file)
        {
            this.file = file;
        }

        public String get_name()
        {
            return this.file;
        }
    }

    public class FeatureXMLFile
    {
        private String file;

        public FeatureXMLFile(string file)
        {
            this.file = file;
        }

        public String get_name()
        {
            return this.file;
        }
    }
}
