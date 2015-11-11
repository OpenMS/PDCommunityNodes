/*
 * Original author: Brendan MacLean <brendanx .at. u.washington.edu>,
 *                  MacCoss Lab, Department of Genome Sciences, UW
 *
 * Copyright 2009 University of Washington - Seattle, WA
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
using System;
using System.Collections.Generic;
using System.Drawing;
using System.Linq;
using System.Text;
using ZedGraph;
using pwiz.MSGraph;

namespace pwiz.Skyline.Controls.Graphs
{
    public class RNPxlSpectrumGraphItem : AbstractMSGraphItem
    {
        public override string Title
        {
            get
            {
                return m_title;
            }
        }
        protected bool IsMatch(double predictedMz)
        {
            return "is this method still needed?" != "probably not!";
        }

        //TODO: public
        public List<double> RNPxl_MZs;
        public List<double> RNPxl_Intensities;
        public List<string> RNPxl_Annotations;
        public double RNPxl_Tolerance = 0.3;

        private string m_title = "";

        private const string FONT_FACE = "Arial"; // Not L10N
        private static readonly Color COLOR_A = Color.YellowGreen;
        private static readonly Color COLOR_X = Color.Green;
        private static readonly Color COLOR_B = Color.BlueViolet;
        private static readonly Color COLOR_Y = Color.Blue;
        private static readonly Color COLOR_C = Color.Orange;
        private static readonly Color COLOR_Z = Color.OrangeRed;
        private static readonly Color COLOR_NONE = Color.Gray;
        private static readonly Color COLOR_OTHER = Color.Brown;
        //private static readonly Color COLOR_PRECURSOR = Color.DarkCyan;
        //public static readonly Color COLOR_SELECTED = Color.Red;

        public ICollection<int> ShowCharges { get; set; }
        public bool ShowRanks { get; set; }
        public bool ShowMz { get; set; }
        public bool ShowObservedMz { get; set; }
        public bool ShowDuplicates { get; set; }
        public float FontSize { get; set; }

        // ReSharper disable InconsistentNaming
        private FontSpec _fontSpecA;
        private FontSpec FONT_SPEC_A { get { return GetFontSpec(COLOR_A, ref _fontSpecA); } }
        private FontSpec _fontSpecX;
        private FontSpec FONT_SPEC_X { get { return GetFontSpec(COLOR_X, ref _fontSpecX); } }
        private FontSpec _fontSpecB;
        private FontSpec FONT_SPEC_B { get { return GetFontSpec(COLOR_B, ref _fontSpecB); } }
        private FontSpec _fontSpecY;
        private FontSpec FONT_SPEC_Y { get { return GetFontSpec(COLOR_Y, ref _fontSpecY); } }
        private FontSpec _fontSpecC;
        private FontSpec FONT_SPEC_C { get { return GetFontSpec(COLOR_C, ref _fontSpecC); } }
        private FontSpec _fontSpecZ;
        private FontSpec FONT_SPEC_Z { get { return GetFontSpec(COLOR_Z, ref _fontSpecZ); } }
        private FontSpec _fontSpecNone;
        private FontSpec FONT_SPEC_NONE { get { return GetFontSpec(COLOR_NONE, ref _fontSpecNone); } }
        private FontSpec _fontSpecNucl;
        private FontSpec FONT_SPEC_OTHER { get { return GetFontSpec(COLOR_OTHER, ref _fontSpecNucl); } }
        //private FontSpec FONT_SPEC_PRECURSOR { get { return GetFontSpec(COLOR_PRECURSOR, ref _fontSpecPrecursor); } }
        //private FontSpec _fontSpecPrecursor;
        //private FontSpec _fontSpecSelected;
        //private FontSpec FONT_SPEC_SELECTED { get { return GetFontSpec(COLOR_SELECTED, ref _fontSpecSelected); } }
        // ReSharper restore InconsistentNaming

        public RNPxlSpectrumGraphItem(string title, List<double> mzs, List<double> intensities, List<string> annotations)
        {
            m_title = title;
            RNPxl_MZs = mzs;
            RNPxl_Intensities = intensities;
            RNPxl_Annotations = annotations;
            
            // Default values
            FontSize = 10;
            LineWidth = 1;
        }

        private static FontSpec CreateFontSpec(Color color, float size)
        {
            return new FontSpec(FONT_FACE, size, color, false, false, false) { Border = { IsVisible = false } };
        }

        private FontSpec GetFontSpec(Color color, ref FontSpec fontSpec)
        {
            return fontSpec ?? (fontSpec = CreateFontSpec(color, FontSize));
        }

        public override void CustomizeCurve(CurveItem curveItem)
        {
            ((LineItem)curveItem).Line.Width = LineWidth;
        }

        public override IPointList Points
        {
            get
            {
                return new PointPairList(RNPxl_MZs.ToArray(), RNPxl_Intensities.ToArray());
            }
        }

        public override void AddPreCurveAnnotations(MSGraphPane graphPane, Graphics g, MSPointList pointList, GraphObjList annotations)
        {
            // Do nothing
        }
        
        public override void AddAnnotations(MSGraphPane graphPane, Graphics g, MSPointList pointList, GraphObjList annotations)
        {
            // ReSharper disable UseObjectOrCollectionInitializer
            for (int i = 0; i < RNPxl_MZs.Count; ++i)
            {
                var mz = RNPxl_MZs[i];
                var intensity = RNPxl_Intensities[i];
                var annotation = RNPxl_Annotations[i];

                Color color;
                string first_two_chars = annotation.Length >= 2 ? annotation.Substring(0, 2) : "--";
                switch (first_two_chars)
                {
                    default: color = COLOR_OTHER; break;
                    case "--": color = COLOR_NONE; break;
                    case "[a": color = COLOR_A; break;
                    case "[x": color = COLOR_X; break;
                    case "[b": color = COLOR_B; break;
                    case "[y": color = COLOR_Y; break;
                    case "[c": color = COLOR_C; break;
                    case "[z": color = COLOR_Z; break;
                    //case IonType.precursor: color = COLOR_PRECURSOR; break;
                }
                var stick = new LineObj(color, mz, intensity, mz, 0);
                stick.IsClippedToChartRect = true;
                stick.Location.CoordinateFrame = CoordType.AxisXYScale;
                stick.Line.Width = LineWidth + 1;
                annotations.Add(stick);
            }
            //ReSharper restore UseObjectOrCollectionInitializer
        }

        public override PointAnnotation AnnotatePoint(PointPair point)
        {
            // inefficient hack for now (TODO)
            string annotation = "";
            for (int i = 0; i < RNPxl_MZs.Count; ++i)
            {
                var mz = RNPxl_MZs[i];
                var it = RNPxl_Intensities[i];
                if (point.X == mz && point.Y == it)
                {
                    annotation = RNPxl_Annotations[i];
                    break;
                }
            }

            string first_two_chars = annotation.Length >= 2 ? annotation.Substring(0, 2) : "--";

            FontSpec fontSpec;
            switch (first_two_chars)
            {
                default: fontSpec = FONT_SPEC_OTHER; break;
                case "--": fontSpec = FONT_SPEC_NONE; break;
                case "[a": fontSpec = FONT_SPEC_A; break;
                case "[x": fontSpec = FONT_SPEC_X; break;
                case "[b": fontSpec = FONT_SPEC_B; break;
                case "[y": fontSpec = FONT_SPEC_Y; break;
                case "[c": fontSpec = FONT_SPEC_C; break;
                case "[z": fontSpec = FONT_SPEC_Z; break;
                //case IonType.precursor: fontSpec = FONT_SPEC_PRECURSOR; break;
            }
            string label = annotation + "\n" + GetDisplayMz(point.X);
            return new PointAnnotation(label, fontSpec);
        }

        private double GetDisplayMz(double mz)
        {
            //TODO: get search tolerance (from search node? from MSSpectrumInfo?) and re-enable this code

            //// Try to show enough decimal places to distinguish by tolerance
            //int places = 1;
            //while (places < 4 && ((int) (RNPxl_Tolerance * Math.Pow(10, places))) == 0) //TODO
            //    places++;

            int places = 3;
            return Math.Round(mz, places);
        }
    }

    public sealed class UnavailableMSGraphItem : NoDataMSGraphItem
    {
        public UnavailableMSGraphItem() : base("EMPTY GRAPH")
        {
        }
    }

    public class NoDataMSGraphItem : AbstractMSGraphItem
    {
        private readonly string _title;

        public NoDataMSGraphItem(string title)
        {
            _title = title;
        }

        public override string Title { get { return _title; } }

        public override PointAnnotation AnnotatePoint(PointPair point)
        {
            return null;
        }

        public override void AddAnnotations(MSGraphPane graphPane, Graphics g, MSPointList pointList, GraphObjList annotations)
        {
            // Do nothing
        }

        public override void AddPreCurveAnnotations(MSGraphPane graphPane, Graphics g, MSPointList pointList, GraphObjList annotations)
        {
            // Do nothing
        }

        public override IPointList Points
        {
            get
            {
                return new PointPairList(new double[0], new double[0]);
            }
        }
    }

    public abstract class AbstractMSGraphItem : IMSGraphItemExtended
    {
        public abstract string Title { get; }
        public abstract PointAnnotation AnnotatePoint(PointPair point);
        public abstract void AddAnnotations(MSGraphPane graphPane, Graphics g,
                                            MSPointList pointList, GraphObjList annotations);
        public abstract void AddPreCurveAnnotations(MSGraphPane graphPane, Graphics g,
                                            MSPointList pointList, GraphObjList annotations);
        public abstract IPointList Points { get; }

        public virtual Color Color
        {
            get { return Color.Gray; }
        }

        public float LineWidth { get; set; }

        public virtual void CustomizeCurve(CurveItem curveItem)
        {
            // Do nothing by default            
        }

        public MSGraphItemType GraphItemType
        {
            get { return MSGraphItemType.spectrum; }
        }

        public virtual MSGraphItemDrawMethod GraphItemDrawMethod
        {
            get { return MSGraphItemDrawMethod.stick; }
        }

        public void CustomizeYAxis(Axis axis)
        {
            CustomizeAxis(axis, "Intensity");
        }

        public void CustomizeXAxis(Axis axis)
        {
            CustomizeAxis(axis, "m/z");
        }

        private static void CustomizeAxis(Axis axis, string title)
        {
            axis.Title.FontSpec.Family = "Arial"; // Not L10N
            axis.Title.FontSpec.Size = 14;
            axis.Color = axis.Title.FontSpec.FontColor = Color.Black;
            axis.Title.FontSpec.Border.IsVisible = false;
            SetAxisText(axis, title);
        }

        /// <summary>
        /// Sets the title text of an axis, ensuring that it is italicized, if the text is "m/z".
        /// Someone actually reported a reviewer of a manuscript mentioning that the m/z axis
        /// title should be in italics.
        /// </summary>
        public static void SetAxisText(Axis axis, string title)
        {
            if (string.Equals(title, "m/z")) // Not L10N
                axis.Title.FontSpec.IsItalic = true;
            axis.Title.Text = title;
        }
    }
}