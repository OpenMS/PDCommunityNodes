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

        public AdHocSpectrumView()
        {
            InitializeComponent();
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

                //chart1.Series[0].Points.Clear();
                //foreach (var peak in m_peakList)
                //{
                //    chart1.Series[0].Points.AddXY(peak.Item1, peak.Item2);
                //}
                //chart1.Series[0].LegendText = "Mass Spectrum";
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
                richTextBox1.Text = m_overviewText;
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
    }
}
