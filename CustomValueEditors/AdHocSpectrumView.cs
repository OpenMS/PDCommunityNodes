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
    public partial class AdHocSpectrumView : Form
    {
        private List<Tuple<double, double>> m_peakList;
        private string m_overviewText;
        private MSGraphPane m_msGraphPane;

        public AdHocSpectrumView()
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

                Graphics g = zedGraphControl1.CreateGraphics();
                zedGraphControl1.GraphPane = m_msGraphPane;
                m_msGraphPane.ReSize(g, new RectangleF(zedGraphControl1.Left, zedGraphControl1.Bounds.Top, zedGraphControl1.Width, zedGraphControl1.Height));
                zedGraphControl1.Refresh();

                var mzs = new List<double>();
                var ints = new List<double>();
                var annots = new List<string>();
                foreach (var peak in m_peakList)
                {
                    mzs.Add(peak.Item1);
                    ints.Add(peak.Item2);
                    annots.Add("");
                }

                m_msGraphPane.AddStick(m_overviewText, mzs.ToArray(), ints.ToArray(), Color.Blue);

                SpectrumGraphItem sgi = new SpectrumGraphItem(mzs, ints, annots);
                sgi.CustomizeXAxis((Axis)m_msGraphPane.XAxis);
                sgi.CustomizeYAxis((Axis)m_msGraphPane.YAxis);
                //sgi.CustomizeCurve(m_msGraphPane.CurveList[0]);
                m_msGraphPane.Draw(g);

                zedGraphControl1.AxisChange();
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

        private void button1_Click(object sender, EventArgs e)
        {
            Close();
        }

        private void AdHocSpectrumView_Load(object sender, EventArgs e)
        {
            
        }

        private void zedGraphControl1_Load(object sender, EventArgs e)
        {
            
        }

        private void zedGraphControl1_Load_1(object sender, EventArgs e)
        {

        }
    }
}
