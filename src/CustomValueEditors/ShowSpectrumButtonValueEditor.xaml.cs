//-----------------------------------------------------------------------------
// Copyright (c) 2003-2015, Thermo Fisher Scientific
// All rights reserved
//-----------------------------------------------------------------------------

using System;
using System.Linq;
using System.Text;
using System.Windows;
using System.Windows.Controls;
using Infragistics.Windows.DataPresenter;
using Infragistics.Windows.Editors;
using Thermo.Discoverer.EntityDataFramework.Controls.HierarchicalEntityItemDataGrid.Models;
using Thermo.Discoverer.SampleNodes.CustomValueEditors;
using Thermo.Magellan.BL.Data;
using Thermo.Magellan.BL.ReportProcessing;
using Thermo.Magellan.EntityDataFramework;
using Thermo.Magellan.EntityDataFramework.ReportFile;
using Thermo.PD.EntityDataFramework;
using System.Collections.Generic;
using Thermo.Discoverer.EntityDataFramework.Controls.GenericGridControl.CustomValueEditors;

namespace PD.OpenMS.AdapterNodes
{
    /// <summary>
    /// Interaction logic ShowSpectrumButtonValueEditor.xaml. This class essentially handles the OnButtonPressed event, i.e, 
    /// it diaplays a spectrum using an ad-hoc spectrum view.
    /// </summary>
    [ApplicationExtension("WPFGridControlExtension", "39DF8074-C254-42E4-B5AC-ECDFC7E3EDDA", typeof(ICustomValueEditor))]
    public partial class SpectrumButtonValueEditor : ICustomValueEditor
    {
        // HACK: store all entity data services that are passed via PrepareEditorDataField(...) over time instead of just one.
        // 
        // This is currently the only known workaround to make this work with several RNPxl result tabs open at the same time.
        // An instance of this class is created automatically when needed and PrepareEditorDataField(...) is called when
        // a new result file is loaded by PD itself. We cannot access the code that does this.
        //
        // In the original implementation, the m_entityDataService member stored only the EntityDataService of the result file
        // that most recently triggered PrepareEditorDataField(...). In order to find out which of the entity data services
        // to use, we now store the GUID of the result file as a string in the button values and do a string comparison.
        //
        // When result files are closed, their EntityDataServices become invalid and must be removed from this list. Thus, we
        // first remove all invalidated EntityDataServices whenever the "Show Spectrum" button is pressed.
        private HashSet<IEntityDataService> m_entityDataServices = new HashSet<IEntityDataService>();

        /// <summary>
        /// Initializes a new instance of the <see cref="CheckImageValueEditor" /> class.
        /// </summary>
        public SpectrumButtonValueEditor()
        {
            InitializeComponent();
            IsToolTipEnabled = false;
        }


        /// <summary>
        /// This property is always true and is used as a kludge in the data trigger for storing the cell 
        /// value in the button's tag. 
        /// </summary>
        /// <value>
        ///   <c>true</c> if always; otherwise, <c>false</c>.
        /// </value>
        public bool Always { get { return true; } }

        /// <summary>
        /// Gets the button text. Set this to something appropriate if needed. 
        /// Note: It woud be possible to generate a dynamic text here (e.g., using the value property).
        /// </summary>
        /// <value>
        /// The button text.
        /// </value>
        public string ButtonText { get { return "Show Spectrum"; } }

        public bool EnterEditMode => throw new NotImplementedException();


        /// <summary>
        /// Prepares the <see cref="ValueEditor" /> to be used to display the data of a specific entity data property with a custom cell value control.
        /// </summary>
        /// <param name="dataField">The field used in the FieldLayout's <see cref="FieldLayout.Fields" /> collection to define the layout of a single field.</param>
        /// <param name="entityDataService">(optional) The <see cref="IEntityDataService" /> of the underlying <see cref="IEntityViewModel" /> which provides the grid data.</param>
        /// <param name="propertyColumn">(optional) The property column which represents a property of the entity data type which is display in this grid column</param>
        /// <remarks>
        /// Only the <b>Type</b> of the value editor to use is specified here, the <see cref="XamDataPresenter" /> creates then the value editor using this type information. Therefore it
        /// is not possible to provide custom data / information by using member variables, because the editor instance will be created on demand from the data presenter.
        /// The only way to provide additional data for the value editor of a column is to add this information somehow to the <see cref="FieldSettings.EditorStyle" /> and to set them to
        /// the value editor instance in an override of <see cref="ValueEditor.ApplyTemplate" /> method.
        /// </remarks>
        public void PrepareEditorDataField(Field dataField, IEntityDataService entityDataService = null, PropertyColumn propertyColumn = null)
        {
            PrepareEditorStyle<SpectrumButtonValueEditor>(dataField);
            if (!m_entityDataServices.Contains(entityDataService))
            {
                m_entityDataServices.Add(entityDataService);
            }
        }


        /// <summary>
        /// Determines whether this instance can edit the specified type.
        /// </summary>
        /// <param name="type">The type of the object to edit.</param>
        /// <returns>
        /// Deliberately always false.
        /// </returns>
        public override bool CanEditType(Type type)
        {
            return false;
        }

        /// <summary>
        /// Determines whether this instance can render the specified type.
        /// </summary>
        /// <param name="type">The type.</param>
        /// <returns></returns>
	    public override bool CanRenderType(Type type)
        {
            return true;
        }

        /// <summary>
        /// Called when button is pressed.
        /// Reads the corresponding MsnSpectrumInfo and MassSpectrum objects for a PSM and displays some examplary contents 
        /// using a simple ad-hoc spectrum view. 
        /// </summary>
        /// <param name="cellContents">The cell contents.</param>
        public void OnButtonPressed(object cellContents)
        {
            // Note: The cell content is a string that contains the ids of the PSM (workflow ID and peptide ID)
            // and in addition the string describing all fragment annotations concatenated using '§'.
            // This is a simple hack to enable reading the entire spectrum by its IDs and also handing over the
            // fragment annotation information without having to read the entire row again.
            // The cell contents is set by the node (see RNPxlConsensusNode.cs) especially for this.

            // Another hack: as of version 2.0.3, we also add a prefix specifying the result file name, so we know which
            // of the EntityDataServices to use. This is a workaround to make it work when several RNPxl result
            // tabs are open (see documentation of m_entityDataServices above)

            if (m_entityDataServices == null || !(cellContents is string))
            {
                ShowCouldNotShowSpectrumError("Unexpected data. Please report this bug to the OpenMS developers");
                return;
            }

            // clean up invalidated EntityDataServices (from result files that have been closed in the meantime)
            HashSet<IEntityDataService> remove_these = new HashSet<IEntityDataService>();
            foreach (var e in m_entityDataServices)
            {
                try
                {
                    // when this throws, we assume this EntityDataService is no longer available and remove it from our list
                    // (shouldn't throw an exception if EDS is still valid, even if there is no spectrum info for the specified indices)
                    var r = e.CreateEntityItemReader();
                    var crash_test = r.Read<MSnSpectrumInfo>((new[] { -1 as object, -1 as object }));
                }
                catch (Exception ex)
                {
                    remove_these.Add(e);
                }
            }
            foreach (var x in remove_these)
            {
                m_entityDataServices.Remove(x);
            }

            // split ID string
            var strings = ((string)cellContents).Split(new[] { '§' }, StringSplitOptions.None);

            // we want to be able to view results generated by older versions of RNPxl, so we also allow
            // IDs consisting of 3 parts (no result filename)
            if (strings.Count() != 3 && strings.Count() != 4)
            {
                ShowCouldNotShowSpectrumError("Unexpected number of IDs. Please report this bug to the OpenMS developers");
                return;
            }

            string report_guid = "";
            if (strings.Count() == 4)
            {
                if (!strings[3].StartsWith("REPORT_GUID="))
                {
                    ShowCouldNotShowSpectrumError("Unexpected ID string format. Report GUID is missing. Please report this bug to the OpenMS developers");
                    return;
                }
                report_guid = strings[3].Substring(12);
            }

            string annotations = strings[2];
            string[] idStrings = { strings[0], strings[1] };

            object[] ids;
            try
            {
                ids = idStrings.Select(id => Convert.ToInt32(id) as object).ToArray();
            }
            catch (Exception)
            {
                ShowCouldNotShowSpectrumError("Unable to decode id data. Please report this bug to the OpenMS developers");
                return;
            }

            // Now, choose which EntityDataService to use. If the result file was specified in the ID string,
            // we know which one to use. However, if the ID string is in the old format (generated by version
            // 2.0.2 or earlier), we have to guess.
            IEntityDataService eds = null;
            if (report_guid != "")
            {
                foreach (var e in m_entityDataServices)
                {
                    if (e.ReportFile.ReportGuid.ToString() == report_guid)
                    {
                        eds = e;
                        break;
                    }
                }
            }
            else
            {
                // ID string is in old format which we still support
                // ==> probe the different EntityDataServices and check whether they contain a spectrum with these IDs
                bool found_eds = false;
                foreach (var e in m_entityDataServices)
                {
                    var r = e.CreateEntityItemReader();
                    if (r.Read<MSnSpectrumInfo>(ids) != null)
                    {
                        if (found_eds)
                        {
                            ShowCouldNotShowSpectrumError("This is most likely due to a bug in the older version of the software with which this result file was generated. Please re-run the analysis using the currently installed version and try again, or close other cross-linking result files if you have multiple tabs open. If both doesn't help, please report this bug to the OpenMS developers");
                            return;
                        }
                        eds = e;
                        found_eds = true;
                    }
                }
            }

            if (eds == null)
            {
                ShowCouldNotShowSpectrumError("EntityDataService unavailable. Please report this bug to the OpenMS developers");
                return;
            }

            var reader = eds.CreateEntityItemReader();

            MSnSpectrumInfo spectrumInfo = null;
            bool no_spectrum_info = false;
            try
            {
                // Now read the corresponding spectrum using the EntityDataService.
                spectrumInfo = reader.Read<MSnSpectrumInfo>(ids);
            }
            catch (Exception)
            {
                no_spectrum_info = true;
            }
            if (no_spectrum_info || spectrumInfo == null)
            {
                ShowCouldNotShowSpectrumError("SpectrumInfo not found. Please make sure the 'Spectra to store' parameter in the 'MSF Files' node of your consensus workflow is set to 'All'. If it is not, please set it to 'All' and rerun the consensus workflow. If it is, you've found a bug. Please report it to the OpenMS developers");
                return;
            }

            // Actually, use the DiscoveryEntityDataService to read the whole spectrum. 
            // In PD this cast should always succeed, but we check anyway.
            var dds = eds; // as DiscovererEntityDataService;

            if (dds == null)
            {
                ShowCouldNotShowSpectrumError("Discoverer Entity Data Service not available");
                return;
            }

            Thermo.Magellan.MassSpec.MassSpectrum spectrum = null;
            bool no_spectrum = false;
            try
            {
                spectrum = dds.GetSpectrum(spectrumInfo);
            }
            catch (Exception)
            {
                no_spectrum = true;
            }
            if (no_spectrum || spectrum == null)
            {
                ShowCouldNotShowSpectrumError("Spectrum not found. Please make sure the 'Spectra to store' parameter in the 'MSF Files' node of your consensus workflow is set to 'All'. If it is not, please set it to 'All' and rerun the consensus workflow. If it is, you've found a bug. Please report it to the OpenMS developers");
                return;
            }

            string ot = "m/z " + String.Format("{0:0.0000}", spectrumInfo.MassOverCharge) + "  |  RT " + String.Format("{0:0.00}", spectrumInfo.RetentionTime) + "  |  Charge " + spectrumInfo.Charge;

            var view = new SpectrumView
            {
                Title = ot,
                Annotations = annotations,
                // Show centroids when available, otherwise profiles.
                PeakList = spectrum.HasPeakCentroids ? spectrum.PeakCentroids.Select(c => Tuple.Create(c.Position, c.Intensity)).ToList() : spectrum.ProfilePoints.ToList().Select(p => Tuple.Create(p.Position, p.Intensity)).ToList()
            };

            view.ShowDialog();
        }

        /// <summary>
        /// Shows an error message using a simple message box.
        /// </summary>
        /// <param name="additionalMessage">The MSG.</param>
	    private void ShowCouldNotShowSpectrumError(string additionalMessage = "")
        {
            MessageBox.Show(additionalMessage == "" ? "Could not show spectrum." : String.Format("Could not show spectrum: {0}.", additionalMessage), "Error");
        }

        /// <summary>
        /// Handles the OnClick event of the PART_LinkButton control.
        /// </summary>
        /// <param name="sender">The source of the event.</param>
        /// <param name="e">The <see cref="RoutedEventArgs"/> instance containing the event data.</param>
	    private void PART_LinkButton_OnClick(object sender, RoutedEventArgs e)
        {
            var button = sender as Button;
            if (button != null)
            {
                // Pass the Tag to the handler routine.
                OnButtonPressed(button.Tag);
            }
        }

      
        public void PrepareEditorDataField(Field dataField, CustomValueEditorOptions customValueEditorOptions = null)
        {
            PrepareEditorStyle<SpectrumButtonValueEditor>(dataField);
            m_entityDataServices.Add(customValueEditorOptions?.EntityDataService);
        }

    }
}
