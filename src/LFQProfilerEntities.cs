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
    public class ConsensusFeatureEntity : DynamicEntity, IEquatable<ConsensusFeatureEntity>
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

        [EntityProperty(DisplayName = "Sequence", Description = "Peptide Sequence")]
        [GridDisplayOptions(VisiblePosition = 10, ColumnWidth = 150)]
        public string Sequence { get; set; }

        [EntityProperty(DisplayName = "Accessions", Description = "Protein Accessions")]
        [GridDisplayOptions(VisiblePosition = 20, ColumnWidth = 150)]
        public string Accessions { get; set; }

        [EntityProperty(DisplayName = "Descriptions", Description = "Protein Descriptions")]
        [GridDisplayOptions(VisiblePosition = 30, ColumnWidth = 300)]
        public string Descriptions { get; set; }

        [EntityProperty(DisplayName = "Charge", Description = "Charge of the peptide")]
        [GridDisplayOptions(VisiblePosition = 40)]
        public int Charge { get; set; }

        [EntityProperty(DisplayName = "m/z", FormatString = "0.0000", Description = "m/z")]
        [GridDisplayOptions(VisiblePosition = 50, ColumnWidth = 90)]
        public double? MZ { get; set; }

        [EntityProperty(DisplayName = "RT", FormatString = "0.00", Description = "Retention Time")]
        [GridDisplayOptions(VisiblePosition = 60, ColumnWidth = 80)]
        public double? RT { get; set; }

        public override bool Equals(object obj)
        {
            if (obj == null || obj.GetType() != GetType())
            {
                return false;
            }
            return (Equals((ConsensusFeatureEntity)obj));
        }

        public bool Equals(ConsensusFeatureEntity rhs)
        {
            return rhs.WorkflowID == WorkflowID && rhs.Id == Id;
        }

        public override int GetHashCode()
        {
            return (int)((long)(WorkflowID + Id) * 2654435761) % 2^32;
        }
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
    public class DechargedPeptideEntity : DynamicEntity, IEquatable<DechargedPeptideEntity>
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

        public override bool Equals(object obj)
        {
            if (obj == null || obj.GetType() != GetType())
            {
                return false;
            }
            return (Equals((DechargedPeptideEntity)obj));
        }

        public bool Equals(DechargedPeptideEntity rhs)
        {
            return rhs.WorkflowID == WorkflowID && rhs.Id == Id;
        }

        public override int GetHashCode()
        {
            return (int)((long)(WorkflowID + Id) * 2654435761) % 2 ^ 32;
        }
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
    public class QuantifiedProteinEntity : DynamicEntity, IEquatable<QuantifiedProteinEntity>
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

        public override bool Equals(object obj)
        {
            if (obj == null || obj.GetType() != GetType())
            {
                return false;
            }
            return (Equals((QuantifiedProteinEntity)obj));
        }

        public bool Equals(QuantifiedProteinEntity rhs)
        {
            return rhs.WorkflowID == WorkflowID && rhs.Id == Id;
        }

        public override int GetHashCode()
        {
            return (int)((long)(WorkflowID + Id) * 2654435761) % 2 ^ 32;
        }
    }
}

