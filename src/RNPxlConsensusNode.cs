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
using Thermo.Magellan.Exceptions;
using Thermo.Magellan.MassSpec;
using Thermo.Magellan.Utilities;
using Thermo.Magellan.Proteomics;
using Thermo.Magellan;
using Thermo.PD.EntityDataFramework;
using Thermo.Magellan.Semantics;

namespace PD.OpenMS.AdapterNodes
{
    # region NodeSetup

    [ProcessingNode("9A840689-B679-4D0B-8595-9448B1D3EB38",
        DisplayName = "RNPxl Consensus",
        Description = "Post-processes the results of the RNPxl search and connects them with the spectrum view",
        Category = ReportingNodeCategories.Miscellaneous,
        MainVersion = 1,
        MinorVersion = 49,
        WorkflowType = WorkflowTypeNames.Consensus)]

    [ConnectionPoint(
        "IncomingOpenMSRNPxls",
        ConnectionDirection = ConnectionDirection.Incoming,
        ConnectionMultiplicity = ConnectionMultiplicity.Single,
        ConnectionMode = ConnectionMode.AutomaticToAllPossibleParents,
        ConnectionRequirement = ConnectionRequirement.Optional,
        ConnectedParentNodeConstraint = ConnectedParentConstraint.OnlyToGeneratorsOfRequestedData)]

    [ConnectionPointDataContract(
        "IncomingOpenMSRNPxls",
        ProteomicsDataTypes.Psms)]

    [ProcessingNodeConstraints(UsageConstraint = UsageConstraint.OnlyOncePerWorkflow)]

    # endregion

    public class RNPxlConsensusNode : ReportProcessingNode
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
            var accessor = PropertyAccessorFactory.CreateDynamicPropertyAccessor<RNPxlItem, string>(
                    new PropertyDescription
                    {
                        DisplayName = "Show Spectrum",
                    });

            // Set the value editor that displays a button (see ShowSpectrumButtonValueEditor.xaml)
            accessor.GridDisplayOptions.GridCellControlGuid = "7875B499-672B-40D7-838E-91B65C7471E2";
            accessor.GridDisplayOptions.VisiblePosition = 9;
            EntityDataService.RegisterProperties(ProcessingNodeNumber, new[] { accessor });

            var rnpxl_items = EntityDataService.CreateEntityItemReader().ReadAll<RNPxlItem>().ToList();

            // store in RT-m/z-dictionary for associating RNPxl table with PD spectra later
            // dictionary RT -> (dictionary m/z -> RNPxlItem.Id)
            // (convert to string, round to 1 decimal)
            var rt_mz_to_rnpxl_id = new Dictionary<string, Dictionary<string, RNPxlItem>>();

            // Prepare a list that contains the button values
            var updates = new List<Tuple<object[], object[]>>();

            foreach (var r in rnpxl_items)
            {
                string rt_str = String.Format("{0:0.0}", r.rt);
                string mz_str = String.Format("{0:0.0000}", r.orig_mz);
                Dictionary<string, RNPxlItem> mz_dict = null;
                if (rt_mz_to_rnpxl_id.ContainsKey(rt_str))
                {
                    mz_dict = rt_mz_to_rnpxl_id[rt_str];
                }
                else
                {
                    mz_dict = new Dictionary<string, RNPxlItem>();
                }
                mz_dict[mz_str] = r;
                rt_mz_to_rnpxl_id[rt_str] = mz_dict;
            }

            var msn_spectrum_info_items = EntityDataService.CreateEntityItemReader().ReadAll<MSnSpectrumInfo>().ToList();
            foreach (var m in msn_spectrum_info_items)
            {
                string rt_str = String.Format("{0:0.0}", m.RetentionTime);
                string mz_str = String.Format("{0:0.0000}", m.MassOverCharge);
                Dictionary<string, RNPxlItem> mz_dict = null;
                if (rt_mz_to_rnpxl_id.ContainsKey(rt_str))
                {
                    mz_dict = rt_mz_to_rnpxl_id[rt_str];
                    if (mz_dict.ContainsKey(mz_str))
                    {
                        RNPxlItem r = mz_dict[mz_str];

                        // Concatenate the spectrum ids and use them as the value that is stored in the button-cell. This value is not visible to the user but
                        // is used to re-read the spectrum when the button is pressed (see ShowSpectrumButtonValueEditor.xaml.cs).

                        // For simplicity, we also store the entire annotation string in the button value in order to avoid
                        // storing IDs for RNPxlItems and re-reading them in ShowSpectrumButtonValueEditor.xaml.cs
                        var idString = string.Concat(m.WorkflowID, ";", m.SpectrumID, ";", r.fragment_annotation);

                        // use r.WorkflowID, r.Id to specify which RNPxlItem to update
                        updates.Add(Tuple.Create(new[] { (object)r.WorkflowID, (object)r.Id }, new object[] { idString }));
                    }
                }
            }

            // Write back the data
            EntityDataService.UpdateItems(EntityDataService.GetEntity<RNPxlItem>().Name, new[] { accessor.Name }, updates);
        }
    }
}
