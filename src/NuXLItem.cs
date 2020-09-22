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
    [EntityExport("cd2d425e-3750-4a81-b6cc-a3847024a7cd",
          EntityName = "NuXLItem",
          TableName = "NuXLs",
          DisplayName = "NuXLs",
          Description = "NuXLs",
          Visibility = GridVisibility.Visible,
          VisiblePosition = 410)]
    [PredefinedEntityProperty(PredefinedEntityPropertyNames.Checkable)]
    public class NuXLItem : DynamicEntity
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

        [EntityProperty(DisplayName = "RT", FormatString = "0.0000")]
        [GridDisplayOptions(VisiblePosition = 10)]
        public double rt { get; set; }

        [EntityProperty(DisplayName = "Original m/z", FormatString = "0.0000")]
        [GridDisplayOptions(VisiblePosition = 10)]
        public double orig_mz { get; set; }

        [EntityProperty(DisplayName = "Proteins")]
        [GridDisplayOptions(VisiblePosition = 10, ColumnWidth = 150)]
        public string proteins { get; set; }

        [EntityProperty(DisplayName = "RNA")]
        [GridDisplayOptions(VisiblePosition = 10)]
        public string rna { get; set; }

        [EntityProperty(DisplayName = "Peptide")]
        [GridDisplayOptions(VisiblePosition = 10)]
        public string peptide { get; set; }

        [EntityProperty(DisplayName = "Charge")]
        [GridDisplayOptions(VisiblePosition = 10)]
        public int charge { get; set; }

        [EntityProperty(DisplayName = "Score", FormatString = "0.0000")]
        [GridDisplayOptions(VisiblePosition = 10)]
        public double score { get; set; }

        [EntityProperty(DisplayName = "Best loc score", FormatString = "0.0000")]
        [GridDisplayOptions(VisiblePosition = 10)]
        public double best_loc_score { get; set; }

        [EntityProperty(DisplayName = "All localization scores")]
        [GridDisplayOptions(VisiblePosition = 10)]
        public string loc_scores { get; set; }

        [EntityProperty(DisplayName = "Best localization(s)")]
        [GridDisplayOptions(VisiblePosition = 10)]
        public string best_localizations { get; set; }

        [EntityProperty(DisplayName = "Peptide weight")]
        [GridDisplayOptions(VisiblePosition = 10)]
        public double peptide_weight { get; set; }

        [EntityProperty(DisplayName = "RNA weight")]
        [GridDisplayOptions(VisiblePosition = 10)]
        public double rna_weight { get; set; }

        [EntityProperty(DisplayName = "Cross-link weight")]
        [GridDisplayOptions(VisiblePosition = 10)]
        public double xl_weight { get; set; }

        [EntityProperty(DisplayName = "A_136.06231")]
        [GridDisplayOptions(VisiblePosition = 10)]
        public double a_1 { get; set; }

        [EntityProperty(DisplayName = "A_330.06033")]
        [GridDisplayOptions(VisiblePosition = 10)]
        public double a_3 { get; set; }

        [EntityProperty(DisplayName = "C_112.05108")]
        [GridDisplayOptions(VisiblePosition = 10)]
        public double c_1 { get; set; }

        [EntityProperty(DisplayName = "C_306.0491")]
        [GridDisplayOptions(VisiblePosition = 10)]
        public double c_3 { get; set; }

        [EntityProperty(DisplayName = "G_152.05723")]
        [GridDisplayOptions(VisiblePosition = 10)]
        public double g_1 { get; set; }

        [EntityProperty(DisplayName = "G_346.05525")]
        [GridDisplayOptions(VisiblePosition = 10)]
        public double g_3 { get; set; }

        [EntityProperty(DisplayName = "U_113.03509")]
        [GridDisplayOptions(VisiblePosition = 10)]
        public double u_1 { get; set; }

        [EntityProperty(DisplayName = "U_307.03311")]
        [GridDisplayOptions(VisiblePosition = 10)]
        public double u_3 { get; set; }

        [EntityProperty(DisplayName = "\x0394m/z [Da]")]
        [GridDisplayOptions(VisiblePosition = 10)]
        public double abs_prec_error_da { get; set; }

        [EntityProperty(DisplayName = "\x0394M [ppm]")]
        [GridDisplayOptions(VisiblePosition = 10)]
        public double rel_prec_error_ppm { get; set; }

        [EntityProperty(DisplayName = "M+H", FormatString = "0.00000")]
        [GridDisplayOptions(VisiblePosition = 10)]
        public double m_h { get; set; }

        [EntityProperty(DisplayName = "M+2H", FormatString = "0.00000")]
        [GridDisplayOptions(VisiblePosition = 10)]
        public double m_2h { get; set; }

        [EntityProperty(DisplayName = "M+3H", FormatString = "0.00000")]
        [GridDisplayOptions(VisiblePosition = 10)]
        public double m_3h { get; set; }

        [EntityProperty(DisplayName = "M+4H", FormatString = "0.00000")]
        [GridDisplayOptions(VisiblePosition = 10)]
        public double m_4h { get; set; }

        [EntityProperty(DisplayName = "Fragment annotation")]
        [GridDisplayOptions(VisiblePosition = 10)]
        public string fragment_annotation { get; set; }
    }
}

