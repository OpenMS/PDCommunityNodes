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
        private string m_annotations;
        private string m_title;
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
                var annotations = new List<string>();
                foreach (var peak in m_peakList)
                {
                    mzs.Add(peak.Item1);
                    ints.Add(peak.Item2);
                    annotations.Add("");
                }

                var annot_parts = m_annotations.Split('|');
                
                foreach (var annot_str in annot_parts)
                {
                    var parts = annot_str.Substring(1, annot_str.Length - 2).Split(',');
                    double mz = Convert.ToDouble(parts[0]);
                    string label = parts[2].Substring(1, parts[2].Length - 2); // remove double quotes

                    // find peak with closest m/z
                    // TODO: make nicer and more efficient
                    double min_delta = double.MaxValue;
                    int nearest_index = 0;
                    for (int i = 0; i < mzs.Count; ++i)
                    {
                        var delta = Math.Abs(mzs[i] - mz);
                        if (delta < min_delta)
                        {
                            min_delta = delta;
                            nearest_index = i;
                        }
                    }
                    annotations[nearest_index] = label;
                }
               
                RNPxlSpectrumGraphItem sgi = new RNPxlSpectrumGraphItem(m_title, mzs, ints, annotations);
                msGraphControl.AddGraphItem(m_msGraphPane, sgi);

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
        public string Title
        {
            set
            {
                m_title = value;
            }
        }

        /// <summary>
        /// Sets the annotations string 
        /// </summary>
        /// <value>
        /// The overview text.
        /// </value>
        public string Annotations
        {
            set
            {
                m_annotations = value;
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
