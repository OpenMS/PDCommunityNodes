using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Thermo.Magellan.BL.Data;
using Thermo.Magellan.BL.Data.Constants;
using Thermo.Magellan.BL.Processing;
using Thermo.Magellan.BL.Processing.Interfaces;
using Thermo.Magellan.DataLayer.FileIO;
using Thermo.Magellan.EntityDataFramework;
using Thermo.Magellan.MassSpec;
using Thermo.Magellan.Utilities;
using Thermo.Magellan.Proteomics;
using Thermo.Magellan;
using Thermo.PD.EntityDataFramework;
using Thermo.Magellan.Core.Logging;
using Thermo.Magellan.Core.Exceptions;

namespace PD.OpenMS.AdapterNodes
{
    # region NodeSetup

    [ProcessingNode("c2ce420e-d723-469b-b8be-6b0d8332fa39",
        DisplayName = "NuXL Consensus",
        Description = "Post-processes the results of the NuXL search and connects them with the spectrum view",
        Category = ReportingNodeCategories.Miscellaneous,
        MainVersion = 1,
        MinorVersion = 49,
        WorkflowType = WorkflowTypeNames.Consensus)]

    [ConnectionPoint(
        "IncomingOpenMSNuXLs",
        ConnectionDirection = ConnectionDirection.Incoming,
        ConnectionMultiplicity = ConnectionMultiplicity.Single,
        ConnectionMode = ConnectionMode.AutomaticToAllPossibleParents,
        ConnectionRequirement = ConnectionRequirement.Optional,
        ConnectedParentNodeConstraint = ConnectedParentConstraint.OnlyToGeneratorsOfRequestedData)]

    [ConnectionPointDataContract(
        "IncomingOpenMSNuXLs",
        ProteomicsDataTypes.Psms)]

    [ProcessingNodeConstraints(UsageConstraint = UsageConstraint.OnlyOncePerWorkflow)]

    # endregion

    public class NuXLConsensusNode : ReportProcessingNode
    {
        /// <summary>
        /// Called when the parent node finished data processing.
        /// </summary>
        /// <param name="sender">The parent node.</param>
        /// <param name="eventArgs">The result event arguments.</param>
        public override void OnParentNodeFinished(IProcessingNode sender, ResultsArguments eventArgs)
        {
            // Add "Show spectrum" buttons to table

            // Create column
            var accessor = PropertyAccessorFactory.CreateDynamicPropertyAccessor<NuXLItem, string>(
                    new PropertyDescription
                    {
                        DisplayName = "Show Spectrum",
                    });

            // Set the value editor that displays a button (see ShowSpectrumButtonValueEditor.xaml)
            accessor.GridDisplayOptions.GridCellControlGuid = "39DF8074-C254-42E4-B5AC-ECDFC7E3EDDA";
            accessor.GridDisplayOptions.VisiblePosition = 9;
            EntityDataService.RegisterProperties(ProcessingNodeNumber, new[] { accessor });

            var xl_items = EntityDataService.CreateEntityItemReader().ReadAll<NuXLItem>().ToList();

            // store in RT-m/z-dictionary for associating NuXL table with PD spectra later
            // dictionary RT -> (dictionary m/z -> NuXLItem.Id)
            // (convert to string, round to 1 decimal)
            var rt_mz_to_nuxl_id = new Dictionary<string, Dictionary<string, NuXLItem>>();

            // Prepare a list that contains the button values
            var updates = new List<Tuple<object[], object[]>>();

            foreach (var r in xl_items)
            {
                string rt_str = String.Format("{0:0.0}", r.rt);
                string mz_str = String.Format("{0:0.0000}", r.orig_mz);
                Dictionary<string, NuXLItem> mz_dict;
                if (rt_mz_to_nuxl_id.ContainsKey(rt_str))
                {
                    mz_dict = rt_mz_to_nuxl_id[rt_str];
                }
                else
                {
                    mz_dict = new Dictionary<string, NuXLItem>();
                }
                mz_dict[mz_str] = r;
                rt_mz_to_nuxl_id[rt_str] = mz_dict;
            }

            // Also connect with MS/MS spectrum info table		
            EntityDataService.RegisterEntityConnection<NuXLItem, MSnSpectrumInfo>(ProcessingNodeNumber);		
            var nuxl_to_spectrum_connections = new List<Tuple<NuXLItem, MSnSpectrumInfo>>();

            var msn_spectrum_info_items = EntityDataService.CreateEntityItemReader().ReadAll<MSnSpectrumInfo>().ToList();
            foreach (var m in msn_spectrum_info_items)
            {
                string rt_str = String.Format("{0:0.0}", m.RetentionTime);
                string mz_str = String.Format("{0:0.0000}", m.MassOverCharge);
                Dictionary<string, NuXLItem> mz_dict = null;
                if (rt_mz_to_nuxl_id.ContainsKey(rt_str))
                {
                    mz_dict = rt_mz_to_nuxl_id[rt_str];
                    if (mz_dict.ContainsKey(mz_str))
                    {
                        NuXLItem r = mz_dict[mz_str];

                        // Add connection
                        nuxl_to_spectrum_connections.Add(Tuple.Create(r, m));

                        // Concatenate the spectrum ids and use them as the value that is stored in the button-cell. This value is not visible to the user but
                        // is used to re-read the spectrum when the button is pressed (see ShowSpectrumButtonValueEditor.xaml.cs).

                        // For simplicity, we also store the entire annotation string in the button value in order to avoid
                        // storing IDs for NuXLItems and re-reading them in ShowSpectrumButtonValueEditor.xaml.cs
                        //
                        // Additional HACK: also store GUID of result file (see ShowSpectrumButtonValueEditor.xaml.cs for an explanation)
                        var idString = string.Concat(m.WorkflowID, ";", m.SpectrumID, ";", r.fragment_annotation, ";REPORT_GUID=", EntityDataService.ReportFile.ReportGuid);

                        // use r.WorkflowID, r.Id to specify which NuXLItem to update
                        updates.Add(Tuple.Create(new[] { (object)r.WorkflowID, (object)r.Id }, new object[] { idString }));
                    }
                }
            }

            // Write back the data
            EntityDataService.UpdateItems(EntityDataService.GetEntity<NuXLItem>().Name, new[] { accessor.Name }, updates);

            // Register connections
            EntityDataService.ConnectItems(nuxl_to_spectrum_connections);
        }
    }
}
