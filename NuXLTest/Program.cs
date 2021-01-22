using PD.OpenMS.AdapterNodes;
using System;
using System.Collections.Generic;

namespace NuXLTest
{
    class Program
    {
        static void Main(string[] args)
        {
            //OpenMSCommons.parseIdXML("D:\\RNPXL\\THERMO\\IdXMLParserTest\\1-AChernev_080219_dir_HeLa_cyt_U_perc.idXML");
            List<NuXLItem> ret = OpenMSCommons.parseIdXML("D:\\RNPXL\\THERMO\\IdXMLParserTest\\1-AChernev_080219_dir_HeLa_cyt_U_perc.idXML");
        }
    }
}
