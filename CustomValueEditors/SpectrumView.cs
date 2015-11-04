using System;
using System.Collections.Generic;
using System.Windows.Forms;
using System.Drawing;
using System.Text;
using ZedGraph;
using pwiz;
using pwiz.MSGraph;
using pwiz.Skyline.Controls.Graphs;

namespace Thermo.Discoverer.SampleNodes.CustomValueEditors
{
    /// <summary>
    /// Show a spectrum with some information in a very basic ad-hoc view using the M$ Chart control. 
    /// </summary>
    public partial class SpectrumView : Form
    {
        private List<Tuple<double, double>> m_peakList;
        private string m_overviewText;
        private MSGraphPane m_msGraphPane;

        public SpectrumView()
        {
            InitializeComponent();
            m_msGraphPane = new MSGraphPane();
        }

        /// <summary>
        /// Sets the peak list.
        /// </summary>
        /// <value>
        /// The peak list consists of m/z (X-axis) Intensity (Y-axis) pairs.
        /// </value>
        public List<Tuple<double, double>> PeakList
        {
            set
            {
                m_peakList = value;

                Graphics g = msGraphControl.CreateGraphics();
                msGraphControl.GraphPane = m_msGraphPane;
                m_msGraphPane.ReSize(g, new RectangleF(msGraphControl.Left, msGraphControl.Bounds.Top, msGraphControl.Width, msGraphControl.Height));
                msGraphControl.Refresh();

                var mzs = new List<double>();
                var ints = new List<double>();
                var annots = new List<string>();
                foreach (var peak in m_peakList)
                {
                    mzs.Add(peak.Item1);
                    ints.Add(peak.Item2);
                    annots.Add("123");
                }

                //m_msGraphPane.AddStick(m_overviewText, mzs.ToArray(), ints.ToArray(), Color.Blue);

                SpectrumGraphItem sgi = new SpectrumGraphItem(mzs, ints, annots);
                msGraphControl.AddGraphItem(m_msGraphPane, sgi);

                //sgi.CustomizeXAxis(m_msGraphPane.XAxis);
                //sgi.CustomizeYAxis(m_msGraphPane.YAxis);
                //sgi.CustomizeCurve(m_msGraphPane.CurveList[0]);

                m_msGraphPane.Draw(g);
                msGraphControl.AxisChange();
            }
        }

        /// <summary>
        /// Sets the overview text displayed in the ad-hoc view. 
        /// </summary>
        /// <value>
        /// The overview text.
        /// </value>
        public string OverviewText
        {
            set
            {
                m_overviewText = value;
            }
        }

        private void SpectrumView_Load(object sender, EventArgs e)
        {
            
        }

        private void msGraphControl_Load(object sender, EventArgs e)
        {
            
        }
    }
}
