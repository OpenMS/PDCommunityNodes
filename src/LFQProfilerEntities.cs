using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Thermo.Magellan.BL.Data.Constants;
using Thermo.Magellan.EntityDataFramework;
using Thermo.Magellan.Utilities;

namespace PD.OpenMS.AdapterNodes
{

    [EntityExport("0C17759F-09CE-43DB-9F90-DDAEF5616D20",
          EntityName = "ConsensusFeature",
          TableName = "ConsensusFeatures",
          DisplayName = "Quantified features",
          Description = "OpenMS quantified consensus features",
          Visibility = GridVisibility.Visible,
          VisiblePosition = 410)]
    [PredefinedEntityProperty(PredefinedEntityPropertyNames.Checkable)]
    public class ConsensusFeatureEntity : DynamicEntity
    {
        /// <summary>
        /// Gets or sets the workflow ID.
        /// </summary>		
        [EntityProperty(DataPurpose = EntityDataPurpose.WorkflowID)]
        [EntityId(1)]
        public int WorkflowID { get; set; }

        /// <summary>
        /// ID
        /// </summary>		
        [EntityProperty(DataPurpose = EntityDataPurpose.ID)]
        [EntityId(2)]
        public int Id { get; set; }
    }

    // ==========================================================================

    [EntityExport("FFA26121-A882-4BA5-B1EF-E5B2A43B8F91",
      EntityName = "DechargedPeptide",
      TableName = "DechargedPeptides",
      DisplayName = "Quantified peptides",
      Description = "OpenMS quantified peptides averaged over charge states",
      Visibility = GridVisibility.Visible,
      VisiblePosition = 410)]
    [PredefinedEntityProperty(PredefinedEntityPropertyNames.Checkable)]
    public class DechargedPeptideEntity : DynamicEntity
    {
        /// <summary>
        /// Gets or sets the workflow ID.
        /// </summary>		
        [EntityProperty(DataPurpose = EntityDataPurpose.WorkflowID)]
        [EntityId(1)]
        public int WorkflowID { get; set; }

        /// <summary>
        /// ID
        /// </summary>		
        [EntityProperty(DataPurpose = EntityDataPurpose.ID)]
        [EntityId(2)]
        public int Id { get; set; }

        [EntityProperty(DisplayName = "Sequence")]
        [GridDisplayOptions(VisiblePosition = 10)]
        public string sequence { get; set; }

        [EntityProperty(DisplayName = "Proteins")]
        [GridDisplayOptions(VisiblePosition = 20)]
        public string proteins { get; set; }

        [EntityProperty(DisplayName = "Descriptions")]
        [GridDisplayOptions(VisiblePosition = 30, ColumnWidth = 300)]
        public string descriptions { get; set; }

        [EntityProperty(DisplayName = "#Proteins")]
        [GridDisplayOptions(VisiblePosition = 40)]
        public int num_proteins { get; set; }

        //abundance columns are dynamically added (don't know how many)
    }

    // ==========================================================================

    [EntityExport("B6CB919F-885D-428C-B88C-6984FFFAFF86",
      EntityName = "QuantifiedProtein",
      TableName = "QuantifiedProteins",
      DisplayName = "Quantified proteins",
      Description = "OpenMS quantified proteins averaged over peptides",
      Visibility = GridVisibility.Visible,
      VisiblePosition = 410)]
    [PredefinedEntityProperty(PredefinedEntityPropertyNames.Checkable)]
    public class QuantifiedProteinEntity : DynamicEntity
    {
        /// <summary>
        /// Gets or sets the workflow ID.
        /// </summary>		
        [EntityProperty(DataPurpose = EntityDataPurpose.WorkflowID)]
        [EntityId(1)]
        public int WorkflowID { get; set; }

        /// <summary>
        /// ID
        /// </summary>		
        [EntityProperty(DataPurpose = EntityDataPurpose.ID)]
        [EntityId(2)]
        public int Id { get; set; }

        [EntityProperty(DisplayName = "Proteins")]
        [GridDisplayOptions(VisiblePosition = 10)]
        public string proteins { get; set; }

        [EntityProperty(DisplayName = "Descriptions")]
        [GridDisplayOptions(VisiblePosition = 20, ColumnWidth = 300)]
        public string descriptions { get; set; }

        [EntityProperty(DisplayName = "#Proteins")]
        [GridDisplayOptions(VisiblePosition = 30)]
        public int num_proteins { get; set; }

        [EntityProperty(DisplayName = "#Peptides")]
        [GridDisplayOptions(VisiblePosition = 40)]
        public int num_peptides { get; set; }

        //abundance columns are dynamically added (don't know how many)
    }
}

