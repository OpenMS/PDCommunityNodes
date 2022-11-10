using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Xml;
using System.Xml.Linq;
using System.Text;
using System.Web.UI;
using Thermo.Magellan.BL.Data;
using Thermo.Magellan.BL.Data.ProcessingNodeScores;
using Thermo.Magellan.BL.Processing;
using Thermo.Magellan.BL.Processing.Interfaces;
using Thermo.Magellan.DataLayer.FileIO;
using Thermo.Magellan.EntityDataFramework;
using Thermo.Magellan.Exceptions;
using Thermo.Magellan.MassSpec;
using Thermo.Magellan.Utilities;
using Thermo.Magellan.Proteomics;
using Thermo.Magellan.StudyManagement;
using Thermo.Magellan.StudyManagement.DataObjects;

namespace PD.OpenMS.AdapterNodes
{
    # region NodeSetup

    [ProcessingNode("5FBAC8EA-D69A-4401-AAAD-DD86092754A0",
        Category = ProcessingNodeCategories.SequenceDatabaseSearch,
        DisplayName = "RNPxl",
        MainVersion = 1,
        MinorVersion = 44,
        Description = "Analyze Protein-RNA cross-linking data")]

    [ConnectionPoint("IncomingSpectra",
        ConnectionDirection = ConnectionDirection.Incoming,
        ConnectionMultiplicity = ConnectionMultiplicity.Single,
        ConnectionMode = ConnectionMode.Manual,
        ConnectionRequirement = ConnectionRequirement.RequiredAtDesignTime,
        ConnectionDisplayName = ProcessingNodeCategories.SpectrumAndFeatureRetrieval,
        ConnectionDataHandlingType = ConnectionDataHandlingType.InMemory)]

    [ConnectionPointDataContract(
        "IncomingSpectra",
        MassSpecDataTypes.MSnSpectra)]

    [ProcessingNodeConstraints(UsageConstraint = UsageConstraint.OnlyOncePerWorkflow)]

    # endregion

    public class RNPxlProcessingNode : PeptideAndProteinIdentificationNode, IResultsSink<MassSpectrumCollection>
    {
        #region Parameters

        [BooleanParameter(
            Category = "1. General",
            DisplayName = "Preprocess using ID filtering",
            Description = "If set to true, spectra containing regular (non-cross-linked) peptides are filtered out before the actual search for cross-links.",
            DefaultValue = "true",
            Position = 10)]
        public BooleanParameter param_general_run_id_filtering;

        [BooleanParameter(
            Category = "1. General",
            DisplayName = "Preprocess using XIC filtering",
            Description = "Only relevant when a control file is available. Remove XICs at precursor positions that are also found in the control file from the cross-link file.",
            DefaultValue = "true",
            Position = 20)]
        public BooleanParameter param_general_run_xic_filtering;

        [BooleanParameter(
            Category = "1. General",
            DisplayName = "XIC filtering uses RT alignment",
            Description = "Only relevant when a control file is available and 'Preprocess using XIC filtering' is set to 'true'. Specifies whether control file should be aligned to cross-link file.",
            DefaultValue = "true",
            Position = 30)]
        public BooleanParameter param_general_run_map_alignment;

        [FastaFileParameter(
            Category = "1. General",
            DisplayName = "Protein database",
            Description = "The sequence database to be used for both peptide ID filtering and searching for peptide-RNA crosslinks",
            IntendedPurpose = ParameterPurpose.SequenceDatabase,
            ValueRequired = true,
            IsMultiSelect = true,
            Position = 40)]
        public FastaFileParameter param_general_fasta_dbs;

        [IntegerParameter(
            Category = "1. General",
            DisplayName = "CPU cores",
            Description = "Maximum number of CPU cores allowed to be used by the algorithms",
            DefaultValue = "1",
            MinimumValue = "1",
            Position = 50)]
        public IntegerParameter param_general_num_threads;

        [IntegerParameter(
            Category = "2. Cross-links",
            DisplayName = "Length",
            Description = "Maximum length of oligonucleotides. 0 = disable search for RNA variants.",
            DefaultValue = "1",
            MinimumValue = "0",
            Position = 60)]
        public IntegerParameter param_cross_linking_length;

        [StringParameter(
            Category = "2. Cross-links",
            DisplayName = "Sequence",
            Description = "Sequence to restrict the generation of oligonucleotide chains. (disabled for empty sequence)",
            DefaultValue = "",
            Position = 70)]
        public StringParameter param_cross_linking_sequence;

        [StringParameter(
            Category = "2. Cross-links",
            DisplayName = "Target nucleotides",
            Description = "Format: target nucleotide=empirical formula of nucleoside monophosphate, e.g., A=C10H14N5O7P, ..., U=C10H14N5O7P, X=C9H13N2O8PS  where X represents e.g. tU or e.g. Y=C10H14N5O7PS where Y represents tG (default: '[A=C10H14N5O7P C=C9H14N3O8P G=C10H14N5O8P U=C9H13N2O9P]')",
            DefaultValue = "[A=C10H14N5O7P C=C9H14N3O8P G=C10H14N5O8P U=C9H13N2O9P]",
            IsAdvanced = true,
            Position = 80)]
        public StringParameter param_cross_linking_target_nucleotides;

        [StringParameter(
            Category = "2. Cross-links",
            DisplayName = "Mapping",
            Description = "Format: source->target e.g. A->A, ..., U->U, U->X (default: '[A->A C->C G->G U->U]')",
            DefaultValue = "[A->A C->C G->G U->U]",
            IsAdvanced = true,
            Position = 90)]
        public StringParameter param_cross_linking_mapping;

        [StringParameter(
            Category = "2. Cross-links",
            DisplayName = "Restrictions",
            Description = "Format: target nucleotide=min_count: e.g U=1 if at least one U must be in the generated sequence. (default: '[A=0 C=0 U=1 G=0]')",
            DefaultValue = "[A=0 C=0 U=1 G=0]",
            IsAdvanced = true,
            Position = 100)]
        public StringParameter param_cross_linking_restrictions;

        [StringParameter(
            Category = "2. Cross-links",
            DisplayName = "Fragment adducts",
            Description = "Format: [formula] or [precursor adduct]->[fragment adduct formula];[name]: e.g 'C9H10N2O5;U-H3PO4' or 'U-H2O->C9H11N2O8P1;U-H2O' (default: '[C9H10N2O5;U-H3PO4 C4H4N2O2;U' C4H2N2O1;U'-H2O C3O;C3O C9H13N2O9P1;U C9H11N2O8P1;U-H2O C9H12N2O6;U-HPO3]')",
            DefaultValue = "[C9H10N2O5;U-H3PO4 C4H4N2O2;U' C4H2N2O1;U'-H2O C3O;C3O C9H13N2O9P1;U C9H11N2O8P1;U-H2O C9H12N2O6;U-HPO3]",
            IsAdvanced = true,
            Position = 101)]
        public StringParameter param_cross_linking_fragment_adducts;

        [StringParameter(
            Category = "2. Cross-links",
            DisplayName = "Modifications",
            Description = "Format: empirical formula e.g -H2O, ..., H2O+PO3 (default: '[ -H2O -H2O-HPO3 -HPO3 -H2O+HPO3 +HPO3]')",
            DefaultValue = "[ -H2O -H2O-HPO3 -HPO3 -H2O+HPO3 +HPO3]",
            IsAdvanced = true,
            Position = 110)]
        public StringParameter param_cross_linking_modifications;

        [BooleanParameter(
            Category = "2. Cross-links",
            DisplayName = "Localization",
            Description = "Use this flag to perform crosslink localization by partial loss scoring as post-analysis.",
            DefaultValue = "true",
            Position = 120)]
        public BooleanParameter param_cross_linking_localization;

        [BooleanParameter(
            Category = "2. Cross-links",
            DisplayName = "Cysteine adduct",
            Description = "Use this flag if the +152 adduct is expected.",
            DefaultValue = "false",
            Position = 130)]
        public BooleanParameter param_cross_linking_cysteine_adduct;

        [BooleanParameter(
            Category = "2. Cross-links",
            DisplayName = "Filter fractional mass",
            Description = "Use this flag to filter non-crosslinks by fractional mass.",
            DefaultValue = "false",
            Position = 140)]
        public BooleanParameter param_cross_linking_filter_fractional_mass;

        [BooleanParameter(
            Category = "2. Cross-links",
            DisplayName = "Carbon-labeled fragments",
            Description = "Generate fragment shifts assuming full labeling of carbon (e.g. completely labeled U13).",
            DefaultValue = "false",
            Position = 150)]
        public BooleanParameter param_cross_linking_carbon_labeled_fragments;

        [MassToleranceParameter(
            Category = "3. Peptide identification",
            DisplayName = "Precursor mass tolerance",
            Description = "This parameter specifies the precursor mass tolerance for peptide identification",
            DefaultValue = "10 ppm",
            Position = 160,
            IntendedPurpose = ParameterPurpose.MassTolerance)]
        public MassToleranceParameter param_rnpxlsearch_precursor_mass_tolerance;

        [MassToleranceParameter(
            Category = "3. Peptide identification",
            DisplayName = "Fragment mass tolerance",
            Description = "This parameter specifies the fragment mass tolerance for peptide identification",
            DefaultValue = "10 ppm",
            Position = 170,
            IntendedPurpose = ParameterPurpose.MassTolerance)]
        public MassToleranceParameter param_rnpxlsearch_fragment_mass_tolerance;

        [IntegerParameter(
            Category = "3. Peptide identification",
            DisplayName = "Charge low",
            Description = "Lowest charge state to search for.",
            DefaultValue = "2",
            MinimumValue = "1",
            Position = 180)]
        public IntegerParameter param_rnpxlsearch_charge_low;

        [IntegerParameter(
            Category = "3. Peptide identification",
            DisplayName = "Charge high",
            Description = "Highest charge state to search for.",
            DefaultValue = "5",
            MinimumValue = "1",
            Position = 190)]
        public IntegerParameter param_rnpxlsearch_charge_high;

        [StringSelectionParameter(
            Category = "3. Peptide identification",
            DisplayName = "Enzyme",
            Description = "The enzyme used for cleaving the proteins",
            DefaultValue = "Trypsin",
            SelectionValues = new string[] { "Trypsin", "Asp-N", "CNBr", "Formic_acid", "Chymotrypsin", "Lys-C", "Asp-N_ambic", "Arg-C", "V8-DE", "glutamyl endopeptidase", "leukocyte elastase", "no cleavage", "PepsinA", "Lys-C/P", "2-iodobenzoate", "prolineendopeptidase", "V8-E", "TrypChymo", "unspecific cleavage", "Trypsin/P" },
            Position = 200)]
        public SimpleSelectionParameter<string> param_rnpxlsearch_enzyme;

        [IntegerParameter(
            Category = "3. Peptide identification",
            DisplayName = "Missed cleavages",
            Description = "Maximum allowed number of missed cleavages.",
            DefaultValue = "1",
            MinimumValue = "0",
            Position = 210)]
        public IntegerParameter param_rnpxlsearch_missed_cleavages;

        [ModificationParameter(
            Category = "3. Peptide identification",
            DisplayName = "Static N-terminal modification",
            Description = "Select any known or suspected modification. Static modifications are applied universally, to every instance of the specified residue(s) or terminus. ",
            ModificationType = ModificationType.Static,
            PositionType = AminoAcidModificationPositionType.Any_N_Terminus,
            IntendedPurpose = ParameterPurpose.StaticTerminalModification,
            Position = 220)]
        public ModificationParameter param_rnpxlsearch_static_n_terminal_mod;

        [ModificationParameter(
            Category = "3. Peptide identification",
            DisplayName = "Static C-terminal modification",
            Description = "Select any known or suspected modification. Static modifications are applied universally, to every instance of the specified residue(s) or terminus. ",
            ModificationType = ModificationType.Static,
            PositionType = AminoAcidModificationPositionType.Any_C_Terminus,
            IntendedPurpose = ParameterPurpose.StaticTerminalModification,
            Position = 230)]
        public ModificationParameter param_rnpxlsearch_static_c_terminal_mod;

        [ModificationParameter(
            Category = "3. Peptide identification",
            DisplayName = "1. Static modification",
            Description = "Select any known or suspected modification. Static modifications are applied universally, to every instance of the specified residue(s) or terminus. ",
            ModificationType = ModificationType.Static,
            IsMultiSelect = true,
            IntendedPurpose = ParameterPurpose.StaticModification,
            Position = 240)]
        [ParameterGroup(GroupName = "Static Modifications", IsDominant = true, IsValueUnique = true)]
        [ParameterGroup(GroupName = "Modifications", IsDominant = true, IsControlledValueUnique = true)]
        public ModificationParameter param_rnpxlsearch_static_mod_1;

        [ModificationParameter(
            Category = "3. Peptide identification",
            DisplayName = "2. Static modification",
            Description = "Select any known or suspected modification. Static modifications are applied universally, to every instance of the specified residue(s) or terminus. ",
            ModificationType = ModificationType.Static,
            IsMultiSelect = true,
            IntendedPurpose = ParameterPurpose.StaticModification,
            Position = 250)]
        [ParameterGroup(GroupName = "Static Modifications", IsDominant = true, IsValueUnique = true)]
        [ParameterGroup(GroupName = "Modifications", IsDominant = true, IsControlledValueUnique = true)]
        public ModificationParameter param_rnpxlsearch_static_mod_2;

        [ModificationParameter(
            Category = "3. Peptide identification",
            DisplayName = "3. Static modification",
            Description = "Select any known or suspected modification. Static modifications are applied universally, to every instance of the specified residue(s) or terminus. ",
            ModificationType = ModificationType.Static,
            IsMultiSelect = true,
            IntendedPurpose = ParameterPurpose.StaticModification,
            Position = 260)]
        [ParameterGroup(GroupName = "Static Modifications", IsDominant = true, IsValueUnique = true)]
        [ParameterGroup(GroupName = "Modifications", IsDominant = true, IsControlledValueUnique = true)]
        public ModificationParameter param_rnpxlsearch_static_mod_3;

        [ModificationParameter(
            Category = "3. Peptide identification",
            DisplayName = "4. Static modification",
            Description = "Select any known or suspected modification. Static modifications are applied universally, to every instance of the specified residue(s) or terminus. ",
            ModificationType = ModificationType.Static,
            IsMultiSelect = true,
            IntendedPurpose = ParameterPurpose.StaticModification,
            Position = 270)]
        [ParameterGroup(GroupName = "Static Modifications", IsDominant = true, IsValueUnique = true)]
        [ParameterGroup(GroupName = "Modifications", IsDominant = true, IsControlledValueUnique = true)]
        public ModificationParameter param_rnpxlsearch_static_mod_4;

        [ModificationParameter(
            Category = "3. Peptide identification",
            DisplayName = "5. Static modification",
            Description = "Select any known or suspected modification. Static modifications are applied universally, to every instance of the specified residue(s) or terminus. ",
            ModificationType = ModificationType.Static,
            IsMultiSelect = true,
            IntendedPurpose = ParameterPurpose.StaticModification,
            Position = 280)]
        [ParameterGroup(GroupName = "Static Modifications", IsDominant = true, IsValueUnique = true)]
        [ParameterGroup(GroupName = "Modifications", IsDominant = true, IsControlledValueUnique = true)]
        public ModificationParameter param_rnpxlsearch_static_mod_5;

        [ModificationParameter(
            Category = "3. Peptide identification",
            DisplayName = "6. Static modification",
            Description = "Select any known or suspected modification. Static modifications are applied universally, to every instance of the specified residue(s) or terminus. ",
            ModificationType = ModificationType.Static,
            IsMultiSelect = true,
            IntendedPurpose = ParameterPurpose.StaticModification,
            Position = 290)]
        [ParameterGroup(GroupName = "Static Modifications", IsDominant = true, IsValueUnique = true)]
        [ParameterGroup(GroupName = "Modifications", IsDominant = true, IsControlledValueUnique = true)]
        public ModificationParameter param_rnpxlsearch_static_mod_6;

        [IntegerParameter(
            Category = "3. Peptide identification",
            DisplayName = "Max. number of dynamic modifications",
            Description = "Maximum number of dynamic modifications per peptide. Includes all terminal and internal modifications.",
            DefaultValue = "2",
            MinimumValue = "0",
            Position = 300)]
        public IntegerParameter param_rnpxlsearch_num_dynamic_mods;

        [ModificationParameter(
            Category = "3. Peptide identification",
            DisplayName = "1. Dynamic N-terminal modification",
            Description = "Select any known or suspected modification.",
            ModificationType = ModificationType.Dynamic,
            PositionType = AminoAcidModificationPositionType.Any_N_Terminus,
            IntendedPurpose = ParameterPurpose.DynamicTerminalModification,
            Position = 400)]
        public ModificationParameter param_rnpxlsearch_dynamic_n_terminal_mod_1;

        [ModificationParameter(
            Category = "3. Peptide identification",
            DisplayName = "2. Dynamic N-terminal modification",
            Description = "Select any known or suspected modification.",
            ModificationType = ModificationType.Dynamic,
            PositionType = AminoAcidModificationPositionType.Any_N_Terminus,
            IntendedPurpose = ParameterPurpose.DynamicTerminalModification,
            Position = 410)]
        public ModificationParameter param_rnpxlsearch_dynamic_n_terminal_mod_2;

        [ModificationParameter(
            Category = "3. Peptide identification",
            DisplayName = "3. Dynamic N-terminal modification",
            Description = "Select any known or suspected modification.",
            ModificationType = ModificationType.Dynamic,
            PositionType = AminoAcidModificationPositionType.Any_N_Terminus,
            IntendedPurpose = ParameterPurpose.DynamicTerminalModification,
            Position = 420)]
        public ModificationParameter param_rnpxlsearch_dynamic_n_terminal_mod_3;

        [ModificationParameter(
            Category = "3. Peptide identification",
            DisplayName = "1. Dynamic C-terminal modification",
            Description = "Select any known or suspected modification.",
            ModificationType = ModificationType.Dynamic,
            PositionType = AminoAcidModificationPositionType.Any_C_Terminus,
            IntendedPurpose = ParameterPurpose.DynamicTerminalModification,
            Position = 430)]
        public ModificationParameter param_rnpxlsearch_dynamic_c_terminal_mod_1;

        [ModificationParameter(
            Category = "3. Peptide identification",
            DisplayName = "2. Dynamic C-terminal modification",
            Description = "Select any known or suspected modification.",
            ModificationType = ModificationType.Dynamic,
            PositionType = AminoAcidModificationPositionType.Any_C_Terminus,
            IntendedPurpose = ParameterPurpose.DynamicTerminalModification,
            Position = 440)]
        public ModificationParameter param_rnpxlsearch_dynamic_c_terminal_mod_2;

        [ModificationParameter(
            Category = "3. Peptide identification",
            DisplayName = "3. Dynamic C-terminal modification",
            Description = "Select any known or suspected modification.",
            ModificationType = ModificationType.Dynamic,
            PositionType = AminoAcidModificationPositionType.Any_C_Terminus,
            IntendedPurpose = ParameterPurpose.DynamicTerminalModification,
            Position = 450)]
        public ModificationParameter param_rnpxlsearch_dynamic_c_terminal_mod_3;

        [ModificationParameter(
            Category = "3. Peptide identification",
            DisplayName = "1. Dynamic modification",
            Description = "Select any known or suspected modification.",
            ModificationType = ModificationType.Dynamic,
            IsMultiSelect = true,
            IntendedPurpose = ParameterPurpose.DynamicModification,
            Position = 460)]
        [ParameterGroup(GroupName = "Dynamic Modifications", IsDominant = true, IsValueUnique = true)]
        [ParameterGroup(GroupName = "Modifications", IsDominant = true, IsControlledValueUnique = true)]
        public ModificationParameter param_rnpxlsearch_dynamic_mod_1;

        [ModificationParameter(
            Category = "3. Peptide identification",
            DisplayName = "2. Dynamic modification",
            Description = "Select any known or suspected modification.",
            ModificationType = ModificationType.Dynamic,
            IsMultiSelect = true,
            IntendedPurpose = ParameterPurpose.DynamicModification,
            Position = 470)]
        [ParameterGroup(GroupName = "Dynamic Modifications", IsDominant = true, IsValueUnique = true)]
        [ParameterGroup(GroupName = "Modifications", IsDominant = true, IsControlledValueUnique = true)]
        public ModificationParameter param_rnpxlsearch_dynamic_mod_2;

        [ModificationParameter(
            Category = "3. Peptide identification",
            DisplayName = "3. Dynamic modification",
            Description = "Select any known or suspected modification.",
            ModificationType = ModificationType.Dynamic,
            IsMultiSelect = true,
            IntendedPurpose = ParameterPurpose.DynamicModification,
            Position = 480)]
        [ParameterGroup(GroupName = "Dynamic Modifications", IsDominant = true, IsValueUnique = true)]
        [ParameterGroup(GroupName = "Modifications", IsDominant = true, IsControlledValueUnique = true)]
        public ModificationParameter param_rnpxlsearch_dynamic_mod_3;

        [ModificationParameter(
            Category = "3. Peptide identification",
            DisplayName = "4. Dynamic modification",
            Description = "Select any known or suspected modification.",
            ModificationType = ModificationType.Dynamic,
            IsMultiSelect = true,
            IntendedPurpose = ParameterPurpose.DynamicModification,
            Position = 490)]
        [ParameterGroup(GroupName = "Dynamic Modifications", IsDominant = true, IsValueUnique = true)]
        [ParameterGroup(GroupName = "Modifications", IsDominant = true, IsControlledValueUnique = true)]
        public ModificationParameter param_rnpxlsearch_dynamic_mod_4;

        [ModificationParameter(
            Category = "3. Peptide identification",
            DisplayName = "5. Dynamic modification",
            Description = "Select any known or suspected modification.",
            ModificationType = ModificationType.Dynamic,
            IsMultiSelect = true,
            IntendedPurpose = ParameterPurpose.DynamicModification,
            Position = 500)]
        [ParameterGroup(GroupName = "Dynamic Modifications", IsDominant = true, IsValueUnique = true)]
        [ParameterGroup(GroupName = "Modifications", IsDominant = true, IsControlledValueUnique = true)]
        public ModificationParameter param_rnpxlsearch_dynamic_mod_5;

        [ModificationParameter(
            Category = "3. Peptide identification",
            DisplayName = "6. Dynamic modification",
            Description = "Select any known or suspected modification.",
            ModificationType = ModificationType.Dynamic,
            IsMultiSelect = true,
            IntendedPurpose = ParameterPurpose.DynamicModification,
            Position = 510)]
        [ParameterGroup(GroupName = "Dynamic Modifications", IsDominant = true, IsValueUnique = true)]
        [ParameterGroup(GroupName = "Modifications", IsDominant = true, IsControlledValueUnique = true)]
        public ModificationParameter param_rnpxlsearch_dynamic_mod_6;

        [DoubleParameter(
            Category = "4. ID filtering",
            DisplayName = "q-value threshold",
            Description = "The q-value threshold for PSMs to be kept.",
            DefaultValue = "0.01",
            Position = 520)]
        public DoubleParameter param_id_filtering_q_value_threshold;

        [BooleanParameter(
            Category = "4. ID filtering",
            DisplayName = "Use same settings as in main peptide identification",
            Description = "If set to false, the advanced settings from this category will be used instead for in the ID filtering step ('Show Advanced Parameters' above)",
            DefaultValue = "true",
            Position = 530)]
        public BooleanParameter param_id_filtering_use_same_settings;

        [MassToleranceParameter(
            Category = "4. ID filtering",
            DisplayName = "Precursor mass tolerance",
            Description = "This parameter specifies the precursor mass tolerance for peptide identification",
            DefaultValue = "10 ppm",
            Position = 540,
            IsAdvanced = true,
            IntendedPurpose = ParameterPurpose.MassTolerance)]
        public MassToleranceParameter param_id_filtering_precursor_mass_tolerance;

        [MassToleranceParameter(
            Category = "4. ID filtering",
            DisplayName = "Fragment mass tolerance",
            Description = "This parameter specifies the fragment mass tolerance for peptide identification",
            DefaultValue = "10 ppm",
            Position = 550,
            IsAdvanced = true,
            IntendedPurpose = ParameterPurpose.MassTolerance)]
        public MassToleranceParameter param_id_filtering_fragment_mass_tolerance;

        [IntegerParameter(
            Category = "4. ID filtering",
            DisplayName = "Charge low",
            Description = "Lowest charge state to search for.",
            DefaultValue = "2",
            MinimumValue = "1",
            IsAdvanced = true,
            Position = 560)]
        public IntegerParameter param_id_filtering_charge_low;

        [IntegerParameter(
            Category = "4. ID filtering",
            DisplayName = "Charge high",
            Description = "Highest charge state to search for.",
            DefaultValue = "5",
            MinimumValue = "1",
            IsAdvanced = true,
            Position = 570)]
        public IntegerParameter param_id_filtering_charge_high;

        [StringSelectionParameter(
            Category = "4. ID filtering",
            DisplayName = "Enzyme",
            Description = "The enzyme used for cleaving the proteins",
            DefaultValue = "Trypsin",
            IsAdvanced = true,
            SelectionValues = new string[] { "Trypsin", "Asp-N", "CNBr", "Formic_acid", "Chymotrypsin", "Lys-C", "Asp-N_ambic", "Arg-C", "V8-DE", "glutamyl endopeptidase", "leukocyte elastase", "no cleavage", "PepsinA", "Lys-C/P", "2-iodobenzoate", "prolineendopeptidase", "V8-E", "TrypChymo", "unspecific cleavage", "Trypsin/P" },
            Position = 580)]
        public SimpleSelectionParameter<string> param_id_filtering_enzyme;

        [IntegerParameter(
            Category = "4. ID filtering",
            DisplayName = "Missed cleavages",
            Description = "Maximum allowed number of missed cleavages.",
            DefaultValue = "1",
            MinimumValue = "0",
            IsAdvanced = true,
            Position = 590)]
        public IntegerParameter param_id_filtering_missed_cleavages;

        [ModificationParameter(
            Category = "4. ID filtering",
            DisplayName = "Static N-terminal modification",
            Description = "Select any known or suspected modification. Static modifications are applied universally, to every instance of the specified residue(s) or terminus. ",
            ModificationType = ModificationType.Static,
            PositionType = AminoAcidModificationPositionType.Any_N_Terminus,
            IntendedPurpose = ParameterPurpose.StaticTerminalModification,
            IsAdvanced = true,
            Position = 600)]
        public ModificationParameter param_id_filtering_static_n_terminal_mod;

        [ModificationParameter(
            Category = "4. ID filtering",
            DisplayName = "Static C-terminal modification",
            Description = "Select any known or suspected modification. Static modifications are applied universally, to every instance of the specified residue(s) or terminus. ",
            ModificationType = ModificationType.Static,
            PositionType = AminoAcidModificationPositionType.Any_C_Terminus,
            IntendedPurpose = ParameterPurpose.StaticTerminalModification,
            IsAdvanced = true,
            Position = 610)]
        public ModificationParameter param_id_filtering_static_c_terminal_mod;

        [ModificationParameter(
            Category = "4. ID filtering",
            DisplayName = "1. Static modification",
            Description = "Select any known or suspected modification. Static modifications are applied universally, to every instance of the specified residue(s) or terminus. ",
            ModificationType = ModificationType.Static,
            IsMultiSelect = true,
            IntendedPurpose = ParameterPurpose.StaticModification,
            IsAdvanced = true,
            Position = 620)]
        [ParameterGroup(GroupName = "Static Modifications for ID Filtering", IsDominant = true, IsValueUnique = true)]
        [ParameterGroup(GroupName = "Modifications for ID Filtering", IsDominant = true, IsControlledValueUnique = true)]
        public ModificationParameter param_id_filtering_static_mod_1;

        [ModificationParameter(
            Category = "4. ID filtering",
            DisplayName = "2. Static modification",
            Description = "Select any known or suspected modification. Static modifications are applied universally, to every instance of the specified residue(s) or terminus. ",
            ModificationType = ModificationType.Static,
            IsMultiSelect = true,
            IntendedPurpose = ParameterPurpose.StaticModification,
            IsAdvanced = true,
            Position = 630)]
        [ParameterGroup(GroupName = "Static Modifications for ID Filtering", IsDominant = true, IsValueUnique = true)]
        [ParameterGroup(GroupName = "Modifications for ID Filtering", IsDominant = true, IsControlledValueUnique = true)]
        public ModificationParameter param_id_filtering_static_mod_2;

        [ModificationParameter(
            Category = "4. ID filtering",
            DisplayName = "3. Static modification",
            Description = "Select any known or suspected modification. Static modifications are applied universally, to every instance of the specified residue(s) or terminus. ",
            ModificationType = ModificationType.Static,
            IsMultiSelect = true,
            IntendedPurpose = ParameterPurpose.StaticModification,
            IsAdvanced = true,
            Position = 640)]
        [ParameterGroup(GroupName = "Static Modifications for ID Filtering", IsDominant = true, IsValueUnique = true)]
        [ParameterGroup(GroupName = "Modifications for ID Filtering", IsDominant = true, IsControlledValueUnique = true)]
        public ModificationParameter param_id_filtering_static_mod_3;

        [ModificationParameter(
            Category = "4. ID filtering",
            DisplayName = "4. Static modification",
            Description = "Select any known or suspected modification. Static modifications are applied universally, to every instance of the specified residue(s) or terminus. ",
            ModificationType = ModificationType.Static,
            IsMultiSelect = true,
            IntendedPurpose = ParameterPurpose.StaticModification,
            IsAdvanced = true,
            Position = 650)]
        [ParameterGroup(GroupName = "Static Modifications for ID Filtering", IsDominant = true, IsValueUnique = true)]
        [ParameterGroup(GroupName = "Modifications for ID Filtering", IsDominant = true, IsControlledValueUnique = true)]
        public ModificationParameter param_id_filtering_static_mod_4;

        [ModificationParameter(
            Category = "4. ID filtering",
            DisplayName = "5. Static modification",
            Description = "Select any known or suspected modification. Static modifications are applied universally, to every instance of the specified residue(s) or terminus. ",
            ModificationType = ModificationType.Static,
            IsMultiSelect = true,
            IntendedPurpose = ParameterPurpose.StaticModification,
            IsAdvanced = true,
            Position = 660)]
        [ParameterGroup(GroupName = "Static Modifications for ID Filtering", IsDominant = true, IsValueUnique = true)]
        [ParameterGroup(GroupName = "Modifications for ID Filtering", IsDominant = true, IsControlledValueUnique = true)]
        public ModificationParameter param_id_filtering_static_mod_5;

        [ModificationParameter(
            Category = "4. ID filtering",
            DisplayName = "6. Static modification",
            Description = "Select any known or suspected modification. Static modifications are applied universally, to every instance of the specified residue(s) or terminus. ",
            ModificationType = ModificationType.Static,
            IsMultiSelect = true,
            IntendedPurpose = ParameterPurpose.StaticModification,
            IsAdvanced = true,
            Position = 670)]
        [ParameterGroup(GroupName = "Static Modifications for ID Filtering", IsDominant = true, IsValueUnique = true)]
        [ParameterGroup(GroupName = "Modifications for ID Filtering", IsDominant = true, IsControlledValueUnique = true)]
        public ModificationParameter param_id_filtering_static_mod_6;

        [IntegerParameter(
            Category = "4. ID filtering",
            DisplayName = "Max. number of dynamic modifications",
            Description = "Maximum number of dynamic modifications per peptide. Includes all terminal and internal modifications.",
            DefaultValue = "2",
            MinimumValue = "0",
            IsAdvanced = true,
            Position = 680)]
        public IntegerParameter param_id_filtering_num_dynamic_mods;

        [ModificationParameter(
            Category = "4. ID filtering",
            DisplayName = "1. Dynamic N-terminal modification",
            Description = "Select any known or suspected modification.",
            ModificationType = ModificationType.Dynamic,
            PositionType = AminoAcidModificationPositionType.Any_N_Terminus,
            IntendedPurpose = ParameterPurpose.DynamicTerminalModification,
            IsAdvanced = true,
            Position = 690)]
        public ModificationParameter param_id_filtering_dynamic_n_terminal_mod_1;

        [ModificationParameter(
            Category = "4. ID filtering",
            DisplayName = "2. Dynamic N-terminal modification",
            Description = "Select any known or suspected modification.",
            ModificationType = ModificationType.Dynamic,
            PositionType = AminoAcidModificationPositionType.Any_N_Terminus,
            IntendedPurpose = ParameterPurpose.DynamicTerminalModification,
            IsAdvanced = true,
            Position = 700)]
        public ModificationParameter param_id_filtering_dynamic_n_terminal_mod_2;

        [ModificationParameter(
            Category = "4. ID filtering",
            DisplayName = "3. Dynamic N-terminal modification",
            Description = "Select any known or suspected modification.",
            ModificationType = ModificationType.Dynamic,
            PositionType = AminoAcidModificationPositionType.Any_N_Terminus,
            IntendedPurpose = ParameterPurpose.DynamicTerminalModification,
            IsAdvanced = true,
            Position = 710)]
        public ModificationParameter param_id_filtering_dynamic_n_terminal_mod_3;

        [ModificationParameter(
            Category = "4. ID filtering",
            DisplayName = "1. Dynamic C-terminal modification",
            Description = "Select any known or suspected modification.",
            ModificationType = ModificationType.Dynamic,
            PositionType = AminoAcidModificationPositionType.Any_C_Terminus,
            IntendedPurpose = ParameterPurpose.DynamicTerminalModification,
            IsAdvanced = true,
            Position = 720)]
        public ModificationParameter param_id_filtering_dynamic_c_terminal_mod_1;

        [ModificationParameter(
            Category = "4. ID filtering",
            DisplayName = "2. Dynamic C-terminal modification",
            Description = "Select any known or suspected modification.",
            ModificationType = ModificationType.Dynamic,
            PositionType = AminoAcidModificationPositionType.Any_C_Terminus,
            IntendedPurpose = ParameterPurpose.DynamicTerminalModification,
            IsAdvanced = true,
            Position = 730)]
        public ModificationParameter param_id_filtering_dynamic_c_terminal_mod_2;

        [ModificationParameter(
            Category = "4. ID filtering",
            DisplayName = "3. Dynamic C-terminal modification",
            Description = "Select any known or suspected modification.",
            ModificationType = ModificationType.Dynamic,
            PositionType = AminoAcidModificationPositionType.Any_C_Terminus,
            IntendedPurpose = ParameterPurpose.DynamicTerminalModification,
            IsAdvanced = true,
            Position = 740)]
        public ModificationParameter param_id_filtering_dynamic_c_terminal_mod_3;

        [ModificationParameter(
            Category = "4. ID filtering",
            DisplayName = "1. Dynamic modification",
            Description = "Select any known or suspected modification.",
            ModificationType = ModificationType.Dynamic,
            IsMultiSelect = true,
            IntendedPurpose = ParameterPurpose.DynamicModification,
            IsAdvanced = true,
            Position = 750)]
        [ParameterGroup(GroupName = "Dynamic Modifications for ID Filtering", IsDominant = true, IsValueUnique = true)]
        [ParameterGroup(GroupName = "Modifications for ID Filtering", IsDominant = true, IsControlledValueUnique = true)]
        public ModificationParameter param_id_filtering_dynamic_mod_1;

        [ModificationParameter(
            Category = "4. ID filtering",
            DisplayName = "2. Dynamic modification",
            Description = "Select any known or suspected modification.",
            ModificationType = ModificationType.Dynamic,
            IsMultiSelect = true,
            IntendedPurpose = ParameterPurpose.DynamicModification,
            IsAdvanced = true,
            Position = 760)]
        [ParameterGroup(GroupName = "Dynamic Modifications for ID Filtering", IsDominant = true, IsValueUnique = true)]
        [ParameterGroup(GroupName = "Modifications for ID Filtering", IsDominant = true, IsControlledValueUnique = true)]
        public ModificationParameter param_id_filtering_dynamic_mod_2;

        [ModificationParameter(
            Category = "4. ID filtering",
            DisplayName = "3. Dynamic modification",
            Description = "Select any known or suspected modification.",
            ModificationType = ModificationType.Dynamic,
            IsMultiSelect = true,
            IntendedPurpose = ParameterPurpose.DynamicModification,
            IsAdvanced = true,
            Position = 770)]
        [ParameterGroup(GroupName = "Dynamic Modifications for ID Filtering", IsDominant = true, IsValueUnique = true)]
        [ParameterGroup(GroupName = "Modifications for ID Filtering", IsDominant = true, IsControlledValueUnique = true)]
        public ModificationParameter param_id_filtering_dynamic_mod_3;

        [ModificationParameter(
            Category = "4. ID filtering",
            DisplayName = "4. Dynamic modification",
            Description = "Select any known or suspected modification.",
            ModificationType = ModificationType.Dynamic,
            IsMultiSelect = true,
            IntendedPurpose = ParameterPurpose.DynamicModification,
            IsAdvanced = true,
            Position = 780)]
        [ParameterGroup(GroupName = "Dynamic Modifications for ID Filtering", IsDominant = true, IsValueUnique = true)]
        [ParameterGroup(GroupName = "Modifications for ID Filtering", IsDominant = true, IsControlledValueUnique = true)]
        public ModificationParameter param_id_filtering_dynamic_mod_4;

        [ModificationParameter(
            Category = "4. ID filtering",
            DisplayName = "5. Dynamic modification",
            Description = "Select any known or suspected modification.",
            ModificationType = ModificationType.Dynamic,
            IsMultiSelect = true,
            IntendedPurpose = ParameterPurpose.DynamicModification,
            IsAdvanced = true,
            Position = 790)]
        [ParameterGroup(GroupName = "Dynamic Modifications for ID Filtering", IsDominant = true, IsValueUnique = true)]
        [ParameterGroup(GroupName = "Modifications for ID Filtering", IsDominant = true, IsControlledValueUnique = true)]
        public ModificationParameter param_id_filtering_dynamic_mod_5;

        [ModificationParameter(
            Category = "4. ID filtering",
            DisplayName = "6. Dynamic modification",
            Description = "Select any known or suspected modification.",
            ModificationType = ModificationType.Dynamic,
            IsMultiSelect = true,
            IntendedPurpose = ParameterPurpose.DynamicModification,
            IsAdvanced = true,
            Position = 800)]
        [ParameterGroup(GroupName = "Dynamic Modifications for ID Filtering", IsDominant = true, IsValueUnique = true)]
        [ParameterGroup(GroupName = "Modifications for ID Filtering", IsDominant = true, IsControlledValueUnique = true)]
        public ModificationParameter param_id_filtering_dynamic_mod_6;

        [IntegerParameter(
            Category = "5. XIC filtering",
            DisplayName = "Fold change",
            Description = "Minimum fold change required for an eluting peptide in the UV sample to be considered a putative cross-link. Extracted ion chromatograms (XICs) of eluting analytes are compared between control and treatment, If the UV signal is not significantly stronger than the control (according to a minimum fold change threshold), the analyte is considered a co-eluting non-cross-linked species or contaminant and its tandem spectra are removed from the analysis.",
            DefaultValue = "2",
            MinimumValue = "0",
            Position = 810)]
        public IntegerParameter param_xic_filtering_fold_change;

        [DoubleParameter(
            Category = "5. XIC filtering",
            DisplayName = "Max. RT difference [min]",
            Description = "This parameter specifies the maximum allowed retention time difference between corresponding XICs.",
            DefaultValue = "0.33",
            Position = 820)]
        public DoubleParameter param_xic_filtering_rt_threshold;

        [MassToleranceParameter(
            Category = "5. XIC filtering",
            DisplayName = "Max. m/z difference",
            Subset = "ppm",
            Description = "This parameter specifies the maximum allowed m/z difference between corresponding XICs",
            DefaultValue = "10 ppm",
            IntendedPurpose = ParameterPurpose.MassTolerance,
            Position = 830)]
        public MassToleranceParameter param_xic_filtering_mz_threshold;

        [DoubleParameter(
            Category = "6. RT alignment",
            DisplayName = "Max. RT difference [min]",
            Description = "This parameter specifies the maximum allowed retention time difference between corresponding peaks.",
            DefaultValue = "0.33",
            Position = 840)]
        public DoubleParameter param_alignment_rt_threshold;

        [MassToleranceParameter(
            Category = "6. RT alignment",
            DisplayName = "Max. m/z difference",
            Subset = "ppm",
            Description = "This parameter specifies the maximum allowed m/z difference between corresponding peaks.",
            DefaultValue = "10 ppm",
            IntendedPurpose = ParameterPurpose.MassTolerance,
            Position = 850)]
        public MassToleranceParameter param_alignment_mz_threshold;


        // this is needed in order to prevent a WorkflowJobBuilder error upon execution
        // since we inherit from PeptideAndProteinIdentificationNode, which in turn
        // seems to be necessary in order to be able to persist spectra into the consensus step
        [Score(ProteomicsDataTypes.Psms, isMainScore: true, category: ScoreCategoryType.HigherIsBetter,
            DisplayName = "DummyScore",
            Description = "Dummy score to prevent a WorkflowJobBuilder error",
            FormatString = "F2",
            Guid = "843C15D2-1648-44FF-A0BD-D1ACE753E96D")]
        public Score MyScore1;

        #endregion

        private int m_current_step;
        private int m_num_steps;
        private int m_num_files;
        private readonly SpectrumDescriptorCollection m_spectrum_descriptors = new SpectrumDescriptorCollection();
        private List<WorkflowInputFile> m_workflow_input_files;
        private NodeDelegates m_node_delegates;

        #region Top-level program flow

        /// <summary>
        /// Initializes the progress.
        /// </summary>
        /// <returns></returns>
        public override ProgressInitializationHint InitializeProgress()
        {
            return new ProgressInitializationHint(4 * ProcessingServices.CurrentWorkflow.GetWorkflow().GetWorkflowInputFiles().ToList().Count, ProgressDependenceType.Independent);
        }

        /// <summary>
        /// Portion of mass spectra received.
        /// 
        /// </summary>
        public new void OnResultsSent(IProcessingNode sender, MassSpectrumCollection spectra)
        {
            //persist spectra to make them available in the consensus step
            var spectra_to_store = new MassSpectrumCollection();
            foreach (var s in spectra)
            {
                if (s.ScanEvent.MSOrder == MSOrderType.MS2)
                {
                    spectra_to_store.Add(s);
                }
            }
            try
            {
                ProcessingServices.SpectrumProcessingService.PersistSpectra(sender, this, spectra_to_store, true, true);
            }
            catch (MagellanException e)
            {
                SendAndLogErrorMessage("Cannot store spectra: {0}", e.Message);
                throw;
            }

            //store spectra in cache for mzML export
            ArgumentHelper.AssertNotNull(spectra, "spectra");
            m_spectrum_descriptors.AddRange(ProcessingServices.SpectrumProcessingService.StoreSpectraInCache(this, spectra));
        }

        /// <summary>
        /// Called when the parent node finished data processing.
        /// </summary>
        /// <param name="sender">The parent node.</param>
        /// <param name="eventArgs">The result event arguments.</param>
        public override void OnParentNodeFinished(IProcessingNode sender, ResultsArguments eventArgs)
        {
            // Node delegates
            m_node_delegates = new NodeDelegates()
            {
                errorLog = new NodeDelegates.NodeLoggerErrorDelegate(NodeLogger.ErrorFormat),
                warnLog = new NodeDelegates.NodeLoggerWarningDelegate(NodeLogger.WarnFormat),
                logTmpMessage = new NodeDelegates.SendAndLogTemporaryMessageDelegate(SendAndLogTemporaryMessage),
                logMessage = new NodeDelegates.SendAndLogMessageDelegate(SendAndLogMessage),
                errorLogMessage = new NodeDelegates.SendAndLogErrorMessageDelegate(SendAndLogErrorMessage),
                writeLogMessage = new NodeDelegates.WriteLogMessageDelegate(WriteLogMessage)
            };

            // determine number of inputfiles which have to be converted
            m_workflow_input_files = EntityDataService.CreateEntityItemReader().ReadAll<WorkflowInputFile>().ToList();
            m_num_files = m_workflow_input_files.Count;

            if (m_num_files != 1 && m_num_files != 2)
            {
                string err = "You need to specify exactly 1 or 2 input file(s) (cross-linked or cross-linked + control)";
                SendAndLogErrorMessage(err);
                throw new MagellanProcessingException(err);
            }

            // prepare (super) approximate progress bars
            int num_export_substeps = m_num_files;
            int num_idfilter_substeps = 6 * Convert.ToInt32(param_general_run_id_filtering.Value);
            int num_xicfilter_substeps = 1 * Convert.ToInt32(param_general_run_xic_filtering.Value);
            int num_mapalignment_substeps = 5 * Convert.ToInt32(param_general_run_xic_filtering.Value && param_general_run_map_alignment.Value);
            int num_rnpxl_substeps = 1;
            m_num_steps = num_export_substeps + num_idfilter_substeps + num_xicfilter_substeps + num_mapalignment_substeps + num_rnpxl_substeps + 1;
            m_current_step = 0;

            // input files and mzML-exported files
            var raw_files = new List<string>(m_num_files);
            var exported_files = new List<string>(m_num_files);

            // Group spectra by file id
            foreach (var spectrumDescriptorsGroupedByFileId in m_spectrum_descriptors.GroupBy(g => g.Header.FileID))
            {
                int file_id = spectrumDescriptorsGroupedByFileId.Key;

                // Flatten the spectrum tree to a collection of spectrum descriptors. 
                var spectrum_descriptors = spectrumDescriptorsGroupedByFileId.ToList();

                // Export spectra to temporary mzML file
                var file_to_export = m_workflow_input_files.Where(w => w.FileID == file_id).ToList().First().PhysicalFileName;
                var spectrum_export_file_name = Path.Combine(NodeScratchDirectory, Path.GetFileNameWithoutExtension(file_to_export)) + "_" + Guid.NewGuid().ToString().Replace('-', '_') + ".mzML";

                raw_files.Add(file_to_export);
                exported_files.Add(spectrum_export_file_name);

                ExportSpectraToMzMl(spectrum_descriptors, spectrum_export_file_name);

                m_current_step += 1;
                ReportTotalProgress((double)m_current_step / m_num_steps);
            }

            // ======================== Run OpenMS pipeline ==============================

            var timer = Stopwatch.StartNew();
            SendAndLogMessage("Starting OpenMS pipeline ...");

            if (m_num_files == 1)
            {
                RunWorkflowOnSingleFile(exported_files[0]);
            }
            else // m_num_files == 2
            {
                // determine UV and control from PD SampleType info (Sample vs. Control)
                string uv_file = null;
                string control_file = null;
                var samples = ProcessingServices.CurrentWorkflow.GetWorkflow().AnalysisDefinition.StudyDefinition.Samples;
                for (int i = 0; i < 2; ++i)
                {
                    // HACK: for now, use file name and size to check for correspondence
                    //       between workflow input files and sample filesets
                    // TODO: find out how to do this by ID or in any other nicer way
                    var wf_input_file = m_workflow_input_files[i];
                    var file_name = wf_input_file.FileName;
                    var file_size = wf_input_file.FileSize;

                    var matching_sample = samples.Single(s => s.FileSet.Files[0].FileName == file_name && s.FileSet.Files[0].FileSize.FileSizeValue == file_size);
                    if (matching_sample == null)
                    {
                        continue;
                    }

                    if (matching_sample.SampleType == SampleType.Sample)
                    {
                        uv_file = exported_files[i];
                    }
                    else if (matching_sample.SampleType == SampleType.Control)
                    {
                        control_file = exported_files[i];
                    }
                }
                if (uv_file == null || control_file == null)
                {
                    // error - we don't have exactly 1 sample and 1 control
                    string err = "When specifying two input files, one needs to be sample and the other one control.";
                    SendAndLogErrorMessage(err);
                    throw new MagellanProcessingException(err);
                }

                // we're good to go
                RunWorkflowOnTwoFiles(uv_file, control_file);
            }

            SendAndLogMessage("OpenMS pipeline processing took {0}.", StringHelper.GetDisplayString(timer.Elapsed));
            FireProcessingFinishedEvent(new ResultsArguments());
            ReportTotalProgress(1.0);
        }

        #endregion

        #region mzML export

        /// <summary>
        /// Exports the correspoding spectra to a new created mzML.
        /// </summary>
        /// <param name="spectrumDescriptorsGroupByFileId">The spectrum descriptors grouped by file identifier.</param>
        /// <returns>The file name of the new created mzML file, containing the exported spectra.</returns>
        /// <exception cref="Thermo.Magellan.Exceptions.MagellanProcessingException"></exception>
        private void ExportSpectraToMzMl(IEnumerable<ISpectrumDescriptor> spectrumDescriptorsGroupByFileId, string spectrum_export_file_name)
        {
            var timer = Stopwatch.StartNew();

            // Get the unique spectrum identifier from each spectrum descriptor
            var spectrum_ids = spectrumDescriptorsGroupByFileId
                .OrderBy(o => o.Header.RetentionTimeCenter)
                .Select(s => s.Header.SpectrumID)
                .ToList();

            SendAndLogTemporaryMessage("Start export of {0} spectra ...", spectrum_ids.Count);

            var exporter = new mzML
            {
                SoftwareName = "Proteome Discoverer",
                SoftwareVersion = new Version(FileVersionInfo.GetVersionInfo(Assembly.GetEntryAssembly().Location).FileVersion)
            };

            bool export_file_is_open = exporter.Open(spectrum_export_file_name, FileMode.CreateNew, FileAccess.ReadWrite, FileShare.Read);

            if (!export_file_is_open)
            {
                throw new MagellanProcessingException(String.Format("Cannot create or open mzML file: {0}", spectrum_export_file_name));
            }

            // Retrieve spectra in bunches from the spectrum cache and export themto the new created mzML file.			
            var spectra = new MassSpectrumCollection(1000);

            foreach (var spectrum in ProcessingServices.SpectrumProcessingService.ReadSpectraFromCache(spectrum_ids))
            {
                spectra.Add(spectrum);

                if (spectra.Count == 1000)
                {
                    exporter.ExportMassSpectra(spectra);
                    spectra.Clear();
                }
            }

            exporter.ExportMassSpectra(spectra);

            exporter.Close();

            SendAndLogMessage("Exporting {0} spectra took {1}.", spectrum_ids.Count, StringHelper.GetDisplayString(timer.Elapsed));
        }

        #endregion


        /// <summary>
        /// Run the workflow on a single (cross-linked) mzML file
        /// </summary>
        private void RunWorkflowOnSingleFile(string input_file)
        {
            ArgumentHelper.AssertStringNotNullOrWhitespace(input_file, "input_file");

            // ID filtering
            string id_filtered_uv_file = RunIDFilterPipeline(input_file);

            // RNPxlSearch
            string csv_file = RunRNPxlSearch(id_filtered_uv_file);
            ParseCSVResults(csv_file);
        }


        /// <summary>
        /// Run the workflow on two mzML files (one cross-linked, one control)
        /// </summary>
        private void RunWorkflowOnTwoFiles(string uv_input_file, string control_input_file)
        {
            ArgumentHelper.AssertStringNotNullOrWhitespace(uv_input_file, "uv_input_file");
            ArgumentHelper.AssertStringNotNullOrWhitespace(control_input_file, "control_input_file");

            // map alignment
            Tuple<string, string> aligned_files = RunMapAlignment(uv_input_file, control_input_file);
            string aligned_uv_file = aligned_files.Item1;
            string aligned_control_file = aligned_files.Item2;

            // ID filtering
            string id_filtered_uv_file = RunIDFilterPipeline(aligned_uv_file);

            // XIC filtering
            string xic_filtered_uv_file = RunXICFilter(id_filtered_uv_file, aligned_control_file);

            // RNPxlSearch
            string csv_file = RunRNPxlSearch(xic_filtered_uv_file);
            ParseCSVResults(csv_file);
        }


        /// <summary>
        /// Run ID + filtering pipeline on a single UV file (perform ID; remove peptides that aren't r-linked)
        /// </summary>
        /// <returns>the result file name</returns>
        private string RunIDFilterPipeline(string uv_mzml_filename)
        {
            if (!param_general_run_id_filtering.Value)
            {
                return uv_mzml_filename;
            }

            string openms_dir = Path.Combine(ServerConfiguration.ToolsDirectory, "OpenMS-2.0/");
            string exec_path = "";
            string ini_path = "";
            string result_filename = Path.Combine(NodeScratchDirectory, "UV_ID_filtered.mzML");

            // ==============================================================================================
            // ================= STEP 1: Peptide ID (without cross links) using RNPxlSearch =================
            // ==============================================================================================

            if (param_id_filtering_use_same_settings.Value)
            {
                // use same parameters as for main RNPxlSearch and ignore parameters from "ID filtering" category
                param_id_filtering_precursor_mass_tolerance = param_rnpxlsearch_precursor_mass_tolerance;
                param_id_filtering_fragment_mass_tolerance = param_rnpxlsearch_fragment_mass_tolerance;
                param_id_filtering_charge_low = param_rnpxlsearch_charge_low;
                param_id_filtering_charge_high = param_rnpxlsearch_charge_high;
                param_id_filtering_enzyme = param_rnpxlsearch_enzyme;
                param_id_filtering_missed_cleavages = param_rnpxlsearch_missed_cleavages;
                param_id_filtering_static_n_terminal_mod = param_rnpxlsearch_static_n_terminal_mod;
                param_id_filtering_static_c_terminal_mod = param_rnpxlsearch_static_c_terminal_mod;
                param_id_filtering_static_mod_1 = param_rnpxlsearch_static_mod_1;
                param_id_filtering_static_mod_2 = param_rnpxlsearch_static_mod_2;
                param_id_filtering_static_mod_3 = param_rnpxlsearch_static_mod_3;
                param_id_filtering_static_mod_4 = param_rnpxlsearch_static_mod_4;
                param_id_filtering_static_mod_5 = param_rnpxlsearch_static_mod_5;
                param_id_filtering_static_mod_6 = param_rnpxlsearch_static_mod_6;
                param_id_filtering_num_dynamic_mods = param_rnpxlsearch_num_dynamic_mods;
                param_id_filtering_dynamic_n_terminal_mod_1 = param_rnpxlsearch_dynamic_n_terminal_mod_1;
                param_id_filtering_dynamic_n_terminal_mod_2 = param_rnpxlsearch_dynamic_n_terminal_mod_2;
                param_id_filtering_dynamic_n_terminal_mod_3 = param_rnpxlsearch_dynamic_n_terminal_mod_3;
                param_id_filtering_dynamic_c_terminal_mod_1 = param_rnpxlsearch_dynamic_c_terminal_mod_1;
                param_id_filtering_dynamic_c_terminal_mod_2 = param_rnpxlsearch_dynamic_c_terminal_mod_2;
                param_id_filtering_dynamic_c_terminal_mod_3 = param_rnpxlsearch_dynamic_c_terminal_mod_3;
                param_id_filtering_dynamic_mod_1 = param_rnpxlsearch_dynamic_mod_1;
                param_id_filtering_dynamic_mod_2 = param_rnpxlsearch_dynamic_mod_2;
                param_id_filtering_dynamic_mod_3 = param_rnpxlsearch_dynamic_mod_3;
                param_id_filtering_dynamic_mod_4 = param_rnpxlsearch_dynamic_mod_4;
                param_id_filtering_dynamic_mod_5 = param_rnpxlsearch_dynamic_mod_5;
                param_id_filtering_dynamic_mod_6 = param_rnpxlsearch_dynamic_mod_6;
            }

            exec_path = Path.Combine(openms_dir, @"bin/RNPxlSearch.exe");

            // result filenames
            string rnpxlsearch_result_tsv_filename = Path.Combine(NodeScratchDirectory, "search_results_id_filtering.tsv");
            string rnpxlsearch_result_idxml_filename = Path.Combine(NodeScratchDirectory, "search_results_id_filtering.idXML");

            // FASTA DB

            // concatenate all selected fasta files to a single file for OpenMS
            var fasta_path = Path.Combine(NodeScratchDirectory, "rnpxl_target_decoy_db.fasta");
            var tmp_fasta_file = Path.Combine(NodeScratchDirectory, @"tmp.fasta");
            var fasta_file_values = param_general_fasta_dbs.Values;
            foreach (var v in fasta_file_values)
            {
                var fn = v.FullPhysicalFileName;
                if (!File.Exists(fn))
                {
                    SendAndLogErrorMessage("Cannot access FASTA file because the file cannot be found!");
                    throw new FileNotFoundException(String.Format("The FASTA file {0} cannot be found!", fn), fn);
                }
                if (File.Exists(tmp_fasta_file))
                {
                    File.Delete(tmp_fasta_file);
                }
                ProcessingServices.FastaFileService.CreateOriginalFastaFile(v, tmp_fasta_file, true);
                File.AppendAllText(fasta_path, File.ReadAllText(tmp_fasta_file));
            }

            //PeptideIndexer fails when the database contains multiple sequences with the same accession
            OpenMSCommons.RemoveDuplicatesInFastaFile(fasta_path);

            addDecoys(fasta_path);

            // INI file
            ini_path = Path.Combine(NodeScratchDirectory, "RNPxlSearch_IDFiltering.ini");
            OpenMSCommons.CreateDefaultINI(exec_path, ini_path, NodeScratchDirectory, m_node_delegates);
            Dictionary<string, string> rnpxlsearch_parameters = new Dictionary<string, string> {
                            {"in", uv_mzml_filename},
                            {"database", fasta_path},
                            {"out_tsv", rnpxlsearch_result_tsv_filename},
                            {"out", rnpxlsearch_result_idxml_filename},
                            {"enzyme", param_id_filtering_enzyme.Value},
                            {"missed_cleavages", param_id_filtering_missed_cleavages.ToString()},
                            {"variable_max_per_peptide", param_id_filtering_num_dynamic_mods.ToString()},
                            {"threads", "1"} // for now: force to 1 because of OpenMS bug
                            //{"threads", param_general_num_threads.ToString()}
            };
            OpenMSCommons.WriteParamsToINI(ini_path, rnpxlsearch_parameters);

            OpenMSCommons.WriteNestedParamToINI(ini_path, new Triplet("precursor", "min_charge", param_id_filtering_charge_low));
            OpenMSCommons.WriteNestedParamToINI(ini_path, new Triplet("precursor", "max_charge", param_id_filtering_charge_high));
            OpenMSCommons.WriteNestedParamToINI(ini_path, new Triplet("precursor", "mass_tolerance", param_id_filtering_precursor_mass_tolerance.Value.Tolerance.ToString()));
            OpenMSCommons.WriteNestedParamToINI(ini_path, new Triplet("precursor", "mass_tolerance_unit", param_id_filtering_precursor_mass_tolerance.UnitToString()));
            OpenMSCommons.WriteNestedParamToINI(ini_path, new Triplet("fragment", "mass_tolerance", param_id_filtering_fragment_mass_tolerance.Value.Tolerance.ToString()));
            OpenMSCommons.WriteNestedParamToINI(ini_path, new Triplet("fragment", "mass_tolerance_unit", param_id_filtering_fragment_mass_tolerance.UnitToString()));

            // disable searching for oligonucleotides => normal peptide identification
            OpenMSCommons.WriteNestedParamToINI(ini_path, new Triplet("RNPxl", "length", "0"));

            var static_mods = new List<string>();
            var variable_mods = new List<string>();
            static_mods.AddRange(convertParamToModStringArray(param_id_filtering_static_c_terminal_mod, "C-TERM"));
            static_mods.AddRange(convertParamToModStringArray(param_id_filtering_static_n_terminal_mod, "N-TERM"));
            static_mods.AddRange(convertParamToModStringArray(param_id_filtering_static_mod_1, "RESIDUE"));
            static_mods.AddRange(convertParamToModStringArray(param_id_filtering_static_mod_2, "RESIDUE"));
            static_mods.AddRange(convertParamToModStringArray(param_id_filtering_static_mod_3, "RESIDUE"));
            static_mods.AddRange(convertParamToModStringArray(param_id_filtering_static_mod_4, "RESIDUE"));
            static_mods.AddRange(convertParamToModStringArray(param_id_filtering_static_mod_5, "RESIDUE"));
            static_mods.AddRange(convertParamToModStringArray(param_id_filtering_static_mod_6, "RESIDUE"));
            variable_mods.AddRange(convertParamToModStringArray(param_id_filtering_dynamic_c_terminal_mod_1, "C-TERM"));
            variable_mods.AddRange(convertParamToModStringArray(param_id_filtering_dynamic_c_terminal_mod_2, "C-TERM"));
            variable_mods.AddRange(convertParamToModStringArray(param_id_filtering_dynamic_c_terminal_mod_3, "C-TERM"));
            variable_mods.AddRange(convertParamToModStringArray(param_id_filtering_dynamic_n_terminal_mod_1, "N-TERM"));
            variable_mods.AddRange(convertParamToModStringArray(param_id_filtering_dynamic_n_terminal_mod_2, "N-TERM"));
            variable_mods.AddRange(convertParamToModStringArray(param_id_filtering_dynamic_n_terminal_mod_3, "N-TERM"));
            variable_mods.AddRange(convertParamToModStringArray(param_id_filtering_dynamic_mod_1, "RESIDUE"));
            variable_mods.AddRange(convertParamToModStringArray(param_id_filtering_dynamic_mod_2, "RESIDUE"));
            variable_mods.AddRange(convertParamToModStringArray(param_id_filtering_dynamic_mod_3, "RESIDUE"));
            variable_mods.AddRange(convertParamToModStringArray(param_id_filtering_dynamic_mod_4, "RESIDUE"));
            variable_mods.AddRange(convertParamToModStringArray(param_id_filtering_dynamic_mod_5, "RESIDUE"));
            variable_mods.AddRange(convertParamToModStringArray(param_id_filtering_dynamic_mod_6, "RESIDUE"));

            OpenMSCommons.WriteItemListToINI(variable_mods.ToArray(), ini_path, "variable");
            OpenMSCommons.WriteItemListToINI(static_mods.ToArray(), ini_path, "fixed");

            SendAndLogMessage("Preprocessing -- ID filtering -- Peptide identification");
            OpenMSCommons.RunTOPPTool(exec_path, ini_path, NodeScratchDirectory, m_node_delegates);

            m_current_step += 1;
            ReportTotalProgress((double)m_current_step / m_num_steps);

            // ==============================================================================================
            // =================================== STEP 2: Peptide Indexer ==================================
            // ==============================================================================================

            exec_path = Path.Combine(openms_dir, @"bin/PeptideIndexer.exe");
            ini_path = Path.Combine(NodeScratchDirectory, @"PeptideIndexer.ini");
            string peptide_indexer_output_file = Path.Combine(NodeScratchDirectory, "peptide_indexer_output.idXML");

            OpenMSCommons.CreateDefaultINI(exec_path, ini_path, NodeScratchDirectory, m_node_delegates);
            Dictionary<string, string> pi_parameters = new Dictionary<string, string> {
                        {"in", rnpxlsearch_result_idxml_filename},
                        {"fasta", fasta_path},
                        {"out", peptide_indexer_output_file},
                        {"prefix", "true"},
                        {"decoy_string", "REV_"},
                        {"missing_decoy_action", "warn"},
                        {"allow_unmatched", "true"},
                        {"threads", param_general_num_threads.ToString()}
            };
            OpenMSCommons.WriteParamsToINI(ini_path, pi_parameters);

            SendAndLogMessage("Preprocessing -- ID filtering -- PeptideIndexer");
            OpenMSCommons.RunTOPPTool(exec_path, ini_path, NodeScratchDirectory, m_node_delegates);

            m_current_step += 1;
            ReportTotalProgress((double)m_current_step / m_num_steps);

            // ==============================================================================================
            // ================================= STEP 3: FalseDiscoveryRate =================================
            // ==============================================================================================

            exec_path = Path.Combine(openms_dir, @"bin/FalseDiscoveryRate.exe");
            ini_path = Path.Combine(NodeScratchDirectory, @"FalseDiscoveryRate.ini");
            string fdr_output_file = Path.Combine(NodeScratchDirectory, "fdr_output.idXML");

            OpenMSCommons.CreateDefaultINI(exec_path, ini_path, NodeScratchDirectory, m_node_delegates);
            Dictionary<string, string> fdr_parameters = new Dictionary<string, string> {
                        {"in", peptide_indexer_output_file},
                        {"out", fdr_output_file},
                        {"threads", param_general_num_threads.ToString()}
            };
            OpenMSCommons.WriteParamsToINI(ini_path, fdr_parameters);

            SendAndLogMessage("Preprocessing -- ID filtering -- FalseDiscoveryRate");
            OpenMSCommons.RunTOPPTool(exec_path, ini_path, NodeScratchDirectory, m_node_delegates);

            m_current_step += 1;
            ReportTotalProgress((double)m_current_step / m_num_steps);

            // ==============================================================================================
            // ===================================== STEP 4: IDFilter =======================================
            // ==============================================================================================

            exec_path = Path.Combine(openms_dir, @"bin/IDFilter.exe");
            ini_path = Path.Combine(NodeScratchDirectory, @"IDFilter.ini");
            string idfilter_output_file = Path.Combine(NodeScratchDirectory, "idfilter_output.idXML");

            OpenMSCommons.CreateDefaultINI(exec_path, ini_path, NodeScratchDirectory, m_node_delegates);
            Dictionary<string, string> idfilter_parameters = new Dictionary<string, string> {
                        {"in", fdr_output_file},
                        {"out", idfilter_output_file},
                        {"threads", param_general_num_threads.ToString()}
            };
            OpenMSCommons.WriteParamsToINI(ini_path, idfilter_parameters);
            OpenMSCommons.WriteNestedParamToINI(ini_path, new Triplet("score", "pep", param_id_filtering_q_value_threshold));

            SendAndLogMessage("Preprocessing -- ID filtering -- IDFilter");
            OpenMSCommons.RunTOPPTool(exec_path, ini_path, NodeScratchDirectory, m_node_delegates);

            m_current_step += 1;
            ReportTotalProgress((double)m_current_step / m_num_steps);

            // ==============================================================================================
            // ==================================== STEP 5: FileFilter ======================================
            // ==============================================================================================

            exec_path = Path.Combine(openms_dir, @"bin/FileFilter.exe");
            ini_path = Path.Combine(NodeScratchDirectory, @"FileFilter.ini");
            string filefilter_output_file = Path.Combine(NodeScratchDirectory, "filefilter_output.idXML");

            OpenMSCommons.CreateDefaultINI(exec_path, ini_path, NodeScratchDirectory, m_node_delegates);
            Dictionary<string, string> filefilter_parameters = new Dictionary<string, string> {
                        {"in", uv_mzml_filename},
                        {"out", result_filename},
                        {"threads", param_general_num_threads.ToString()}
            };
            OpenMSCommons.WriteParamsToINI(ini_path, filefilter_parameters);
            OpenMSCommons.WriteNestedParamToINI(ini_path, new Triplet("id", "blacklist", idfilter_output_file));
            OpenMSCommons.WriteNestedParamToINI(ini_path, new Triplet("id", "mz", 0.01));

            SendAndLogMessage("Preprocessing -- ID filtering -- FileFilter");
            OpenMSCommons.RunTOPPTool(exec_path, ini_path, NodeScratchDirectory, m_node_delegates);

            m_current_step += 1;
            ReportTotalProgress((double)m_current_step / m_num_steps);

            return result_filename;
        }

        /// <summary>
        /// Run XICFilter pipeline on 2 mzML files UV and control; produces a single UV mzML output file
        /// </summary>
        /// <returns>the result file name</returns>
        private string RunXICFilter(string uv_mzml_filename, string control_mzml_filename)
        {
            if (!param_general_run_xic_filtering.Value)
            {
                return uv_mzml_filename;
            }

            string result_filename = Path.Combine(NodeScratchDirectory, "UV_XIC_filtered.mzML");

            string openms_dir = Path.Combine(ServerConfiguration.ToolsDirectory, "OpenMS-2.0/");
            var exec_path = Path.Combine(openms_dir, @"bin/RNPxlXICFilter.exe");
            var ini_path = Path.Combine(NodeScratchDirectory, @"RNPxlXICFilter.ini");

            OpenMSCommons.CreateDefaultINI(exec_path, ini_path, NodeScratchDirectory, m_node_delegates);
            Dictionary<string, string> fdr_parameters = new Dictionary<string, string>
            {
                {"treatment", uv_mzml_filename},
                {"control", control_mzml_filename},
                {"out", result_filename},
                {"fold_change", param_xic_filtering_fold_change.ToString()},
                {"rt_tol", (param_xic_filtering_rt_threshold.Value * 60.0).ToString()},
                {"mz_tol", param_xic_filtering_mz_threshold.Value.Tolerance.ToString()},
                {"threads", param_general_num_threads.ToString()}
            };
            OpenMSCommons.WriteParamsToINI(ini_path, fdr_parameters);

            SendAndLogMessage("Preprocessing -- XIC filtering -- RNPxlXICFiltering");
            OpenMSCommons.RunTOPPTool(exec_path, ini_path, NodeScratchDirectory, m_node_delegates);

            m_current_step += 1;
            ReportTotalProgress((double)m_current_step / m_num_steps);

            return result_filename;
        }

        /// <summary>
        /// Run map alignment on 2 mzML files UV and control
        /// </summary>
        /// <returns>both result file names</returns>
        private Tuple<string, string> RunMapAlignment(string uv_mzml_filename, string control_mzml_filename)
        {
            if (!param_general_run_map_alignment.Value || !param_general_run_xic_filtering.Value)
            {
                return Tuple.Create(uv_mzml_filename, control_mzml_filename);
            }

            string openms_dir = Path.Combine(ServerConfiguration.ToolsDirectory, "OpenMS-2.0/");
            string exec_path = "";
            string ini_path = "";
            string aligned_uv_filename = Path.Combine(NodeScratchDirectory, "UV_aligned.mzML");
            string aligned_control_filename = Path.Combine(NodeScratchDirectory, "Control_aligned.mzML");
            var result = Tuple.Create(aligned_uv_filename, aligned_control_filename);

            // ==============================================================================================
            // =============================== STEP 1: FeatuerFinderCentroided ==============================
            // ==============================================================================================

            exec_path = Path.Combine(openms_dir, @"bin/FeatureFinderCentroided.exe");
            ini_path = Path.Combine(NodeScratchDirectory, @"FeatureFinderCentroided.ini");

            foreach (var f in new List<string>() { uv_mzml_filename, control_mzml_filename })
            {
                OpenMSCommons.CreateDefaultINI(exec_path, ini_path, NodeScratchDirectory, m_node_delegates);
                Dictionary<string, string> ffc_parameters = new Dictionary<string, string> {
                            {"in", f},
                            {"out", f.Replace(".mzML", ".featureXML")},
                            {"charge_low", "2"},
                            {"charge_high", "5"},
                            {"max_missing", "1"},
                            {"min_spectra", "6"},
                            {"slope_bound", "0.1"},
                            {"threads", param_general_num_threads.ToString()}
                };

                OpenMSCommons.WriteParamsToINI(ini_path, ffc_parameters);

                OpenMSCommons.WriteNestedParamToINI(ini_path, new Triplet("feature", "min_score", "0.7"));
                OpenMSCommons.WriteNestedParamToINI(ini_path, new Triplet("mass_trace", "mz_tolerance", "0.01"));
                OpenMSCommons.WriteNestedParamToINI(ini_path, new Triplet("isotopic_pattern", "mz_tolerance", "0.01"));

                SendAndLogMessage("Preprocessing -- Map alignment -- FeatureFinderCentroided");
                OpenMSCommons.RunTOPPTool(exec_path, ini_path, NodeScratchDirectory, m_node_delegates);

                m_current_step += 1;
                ReportTotalProgress((double)m_current_step / m_num_steps);
            }

            // ==============================================================================================
            // ============================== STEP 2: MapAlignerPoseClustering ==============================
            // ==============================================================================================

            exec_path = Path.Combine(openms_dir, @"bin/MapAlignerPoseClustering.exe");

            var in_files = new List<string>()
            {
                uv_mzml_filename.Replace(".mzML", ".featureXML"),
                control_mzml_filename.Replace(".mzML", ".featureXML")
            }.ToArray();
            var out_files = new List<string>()
            {
                uv_mzml_filename.Replace(".mzML", ".trafoXML"),
                control_mzml_filename.Replace(".mzML", ".trafoXML")
            }.ToArray();

            Dictionary<string, string> map_parameters = new Dictionary<string, string>
            {
                {"max_num_peaks_considered", "-1"},
                {"ignore_charge", "false"},
                {"threads", param_general_num_threads.ToString()}
            };
            ini_path = Path.Combine(NodeScratchDirectory, @"MapAlignerPoseClustering.ini");
            OpenMSCommons.CreateDefaultINI(exec_path, ini_path, NodeScratchDirectory, m_node_delegates);
            OpenMSCommons.WriteParamsToINI(ini_path, map_parameters);
            OpenMSCommons.WriteItemListToINI(in_files, ini_path, "in");
            OpenMSCommons.WriteItemListToINI(out_files, ini_path, "trafo_out");
            OpenMSCommons.WriteNestedParamToINI(ini_path, new Triplet("reference", "index", "1")); //use UV file as reference! (transform only control, so RTs stay constant for UV)
            OpenMSCommons.WriteThresholdsToINI(param_alignment_mz_threshold, param_alignment_rt_threshold, ini_path);

            SendAndLogMessage("Preprocessing -- Map alignment -- MapAlignerPoseClustering");
            OpenMSCommons.RunTOPPTool(exec_path, ini_path, NodeScratchDirectory, m_node_delegates);

            m_current_step += 1;
            ReportTotalProgress((double)m_current_step / m_num_steps);

            // ==============================================================================================
            // ================================= STEP 3: MapRTTransformer ===================================
            // ==============================================================================================

            exec_path = Path.Combine(openms_dir, @"bin/MapRTTransformer.exe");

            var mzml_in_files = new List<string>()
            {
                uv_mzml_filename,
                control_mzml_filename
            };
            var trafo_in_files = new List<string>()
            {
                uv_mzml_filename.Replace(".mzML", ".trafoXML"),
                control_mzml_filename.Replace(".mzML", ".trafoXML")
            };
            var result_list = new List<string>()
            {
                aligned_uv_filename,
                aligned_control_filename
            };

            ini_path = Path.Combine(NodeScratchDirectory, @"MapRTTransformer.ini");
            OpenMSCommons.CreateDefaultINI(exec_path, ini_path, NodeScratchDirectory, m_node_delegates);

            for (int i = 0; i < 2; ++i)
            {
                Dictionary<string, string> transformer_parameters = new Dictionary<string, string>
                {
                    {"in", mzml_in_files[i]},
                    {"trafo_in", trafo_in_files[i]},
                    {"out", result_list[i]},
                    {"threads", param_general_num_threads.ToString()}
                };
                OpenMSCommons.WriteParamsToINI(ini_path, transformer_parameters);

                SendAndLogMessage("Preprocessing -- Map alignment -- MapRTTransformer");
                OpenMSCommons.RunTOPPTool(exec_path, ini_path, NodeScratchDirectory, m_node_delegates);

                m_current_step += 1;
                ReportTotalProgress((double)m_current_step / m_num_steps);
            }

            return result;
        }

        /// <summary>
        /// Run RNPxl tool on (preprocessed) UV mzML file
        /// </summary>
        /// <returns>the csv result file name</returns>
        private string RunRNPxlSearch(string uv_mzml_filename)
        {
            var openms_dir = Path.Combine(ServerConfiguration.ToolsDirectory, "OpenMS-2.0/");
            var exec_path = Path.Combine(openms_dir, @"bin/RNPxlSearch.exe");

            // result filenames
            string result_tsv_filename = Path.Combine(NodeScratchDirectory, "rnpxl_search_results.tsv");
            string idxml_filename = Path.Combine(NodeScratchDirectory, "rnpxl_search_results.idXML");

            // FASTA DB

            // concatenate all selected fasta files to a single file for OpenMS
            var fasta_path = Path.Combine(NodeScratchDirectory, "rnpxl_db.fasta");
            var tmp_fasta_file = Path.Combine(NodeScratchDirectory, @"tmp.fasta");
            var fasta_file_values = param_general_fasta_dbs.Values;
            foreach (var v in fasta_file_values)
            {
                var fn = v.FullPhysicalFileName;
                if (!File.Exists(fn))
                {
                    SendAndLogErrorMessage("Cannot access FASTA file because the file cannot be found!");
                    throw new FileNotFoundException(String.Format("The FASTA file {0} cannot be found!", fn), fn);
                }
                if (File.Exists(tmp_fasta_file))
                {
                    File.Delete(tmp_fasta_file);
                }
                ProcessingServices.FastaFileService.CreateOriginalFastaFile(v, tmp_fasta_file, true);
                File.AppendAllText(fasta_path, File.ReadAllText(tmp_fasta_file));
            }

            //PeptideIndexer fails when the database contains multiple sequences with the same accession
            OpenMSCommons.RemoveDuplicatesInFastaFile(fasta_path);
            
            //addDecoys(fasta_path); // here, we don't need the decoys anymore

            // INI file
            string rnpxlsearch_ini_file = Path.Combine(NodeScratchDirectory, "RNPxlSearch.ini");
            OpenMSCommons.CreateDefaultINI(exec_path, rnpxlsearch_ini_file, NodeScratchDirectory, m_node_delegates);
            Dictionary<string, string> rnpxlsearch_parameters = new Dictionary<string, string> {
                            {"in", uv_mzml_filename},
                            {"database", fasta_path},
                            {"out_tsv", result_tsv_filename},
                            {"out", idxml_filename},
                            {"enzyme", param_rnpxlsearch_enzyme.Value},
                            {"missed_cleavages", param_rnpxlsearch_missed_cleavages.ToString()},
                            {"variable_max_per_peptide", param_rnpxlsearch_num_dynamic_mods.ToString()},
                            {"threads", "1"} // for now: force to 1 because of OpenMS bug
                            //{"threads", param_general_num_threads.ToString()}
            };
            OpenMSCommons.WriteParamsToINI(rnpxlsearch_ini_file, rnpxlsearch_parameters);

            OpenMSCommons.WriteNestedParamToINI(rnpxlsearch_ini_file, new Triplet("RNPxl", "length", param_cross_linking_length));
            OpenMSCommons.WriteNestedParamToINI(rnpxlsearch_ini_file, new Triplet("RNPxl", "sequence", param_cross_linking_sequence));
            OpenMSCommons.WriteNestedParamToINI(rnpxlsearch_ini_file, new Triplet("RNPxl", "localization", param_cross_linking_localization.ToString().ToLower()));
            OpenMSCommons.WriteNestedParamToINI(rnpxlsearch_ini_file, new Triplet("RNPxl", "CysteineAdduct", param_cross_linking_cysteine_adduct.ToString().ToLower()));
            OpenMSCommons.WriteNestedParamToINI(rnpxlsearch_ini_file, new Triplet("RNPxl", "filter_fractional_mass", param_cross_linking_filter_fractional_mass.ToString().ToLower()));
            OpenMSCommons.WriteNestedParamToINI(rnpxlsearch_ini_file, new Triplet("RNPxl", "carbon_labeled_fragments", param_cross_linking_carbon_labeled_fragments.ToString().ToLower()));
            OpenMSCommons.WriteNestedParamToINI(rnpxlsearch_ini_file, new Triplet("precursor", "min_charge", param_rnpxlsearch_charge_low));
            OpenMSCommons.WriteNestedParamToINI(rnpxlsearch_ini_file, new Triplet("precursor", "max_charge", param_rnpxlsearch_charge_high));
            OpenMSCommons.WriteNestedParamToINI(rnpxlsearch_ini_file, new Triplet("precursor", "mass_tolerance", param_rnpxlsearch_precursor_mass_tolerance.Value.Tolerance.ToString()));
            OpenMSCommons.WriteNestedParamToINI(rnpxlsearch_ini_file, new Triplet("precursor", "mass_tolerance_unit", param_rnpxlsearch_precursor_mass_tolerance.UnitToString()));
            OpenMSCommons.WriteNestedParamToINI(rnpxlsearch_ini_file, new Triplet("fragment", "mass_tolerance", param_rnpxlsearch_fragment_mass_tolerance.Value.Tolerance.ToString()));
            OpenMSCommons.WriteNestedParamToINI(rnpxlsearch_ini_file, new Triplet("fragment", "mass_tolerance_unit", param_rnpxlsearch_fragment_mass_tolerance.UnitToString()));

            var static_mods = new List<string>();
            var variable_mods = new List<string>();
            static_mods.AddRange(convertParamToModStringArray(param_rnpxlsearch_static_c_terminal_mod, "C-TERM"));
            static_mods.AddRange(convertParamToModStringArray(param_rnpxlsearch_static_n_terminal_mod, "N-TERM"));
            static_mods.AddRange(convertParamToModStringArray(param_rnpxlsearch_static_mod_1, "RESIDUE"));
            static_mods.AddRange(convertParamToModStringArray(param_rnpxlsearch_static_mod_2, "RESIDUE"));
            static_mods.AddRange(convertParamToModStringArray(param_rnpxlsearch_static_mod_3, "RESIDUE"));
            static_mods.AddRange(convertParamToModStringArray(param_rnpxlsearch_static_mod_4, "RESIDUE"));
            static_mods.AddRange(convertParamToModStringArray(param_rnpxlsearch_static_mod_5, "RESIDUE"));
            static_mods.AddRange(convertParamToModStringArray(param_rnpxlsearch_static_mod_6, "RESIDUE"));
            variable_mods.AddRange(convertParamToModStringArray(param_rnpxlsearch_dynamic_c_terminal_mod_1, "C-TERM"));
            variable_mods.AddRange(convertParamToModStringArray(param_rnpxlsearch_dynamic_c_terminal_mod_2, "C-TERM"));
            variable_mods.AddRange(convertParamToModStringArray(param_rnpxlsearch_dynamic_c_terminal_mod_3, "C-TERM"));
            variable_mods.AddRange(convertParamToModStringArray(param_rnpxlsearch_dynamic_n_terminal_mod_1, "N-TERM"));
            variable_mods.AddRange(convertParamToModStringArray(param_rnpxlsearch_dynamic_n_terminal_mod_2, "N-TERM"));
            variable_mods.AddRange(convertParamToModStringArray(param_rnpxlsearch_dynamic_n_terminal_mod_3, "N-TERM"));
            variable_mods.AddRange(convertParamToModStringArray(param_rnpxlsearch_dynamic_mod_1, "RESIDUE"));
            variable_mods.AddRange(convertParamToModStringArray(param_rnpxlsearch_dynamic_mod_2, "RESIDUE"));
            variable_mods.AddRange(convertParamToModStringArray(param_rnpxlsearch_dynamic_mod_3, "RESIDUE"));
            variable_mods.AddRange(convertParamToModStringArray(param_rnpxlsearch_dynamic_mod_4, "RESIDUE"));
            variable_mods.AddRange(convertParamToModStringArray(param_rnpxlsearch_dynamic_mod_5, "RESIDUE"));
            variable_mods.AddRange(convertParamToModStringArray(param_rnpxlsearch_dynamic_mod_6, "RESIDUE"));

            OpenMSCommons.WriteItemListToINI(variable_mods.ToArray(), rnpxlsearch_ini_file, "variable");
            OpenMSCommons.WriteItemListToINI(static_mods.ToArray(), rnpxlsearch_ini_file, "fixed");

            var openms_param_names = new List<string>()
            {
                "mapping",
                "modifications",
                "restrictions",
                "target_nucleotides",
                "fragment_adducts"
            };
            var pd_parameters = new List<StringParameter>()
            {
                param_cross_linking_mapping,
                param_cross_linking_modifications,
                param_cross_linking_restrictions,
                param_cross_linking_target_nucleotides,
                param_cross_linking_fragment_adducts
            };

            try
            {
                for (int i = 0; i < openms_param_names.Count; ++i)
                {
                    var oms_p = openms_param_names[i];
                    var pd_p = pd_parameters[i];
                    var without_brackets = pd_p.Value.Substring(1, pd_p.Value.Length - 2);
                    var parts = without_brackets.Split(' ').ToArray();
                    OpenMSCommons.WriteItemListToINI(parts, rnpxlsearch_ini_file, oms_p, true);
                }
            }
            catch (Exception)
            {
                string err = "Error while parsing string list parameter. Valid format for string lists: '[a b c ...]'";
                SendAndLogErrorMessage(err);
                throw;
            }

            SendAndLogMessage("Starting main RNPxl search for file [{0}]", uv_mzml_filename);
            OpenMSCommons.RunTOPPTool(exec_path, rnpxlsearch_ini_file, NodeScratchDirectory, m_node_delegates);

            m_current_step += 1;
            ReportTotalProgress((double)m_current_step / m_num_steps);

            return result_tsv_filename;
        }

        /// <summary>
        /// Add decoys to a given FASTA file
        /// </summary>
        private void addDecoys(string fasta_file)
        {
            string openms_dir = Path.Combine(ServerConfiguration.ToolsDirectory, "OpenMS-2.0/");
            string exec_path = Path.Combine(openms_dir, @"bin/DecoyDatabase.exe");
            string ini_path = Path.Combine(NodeScratchDirectory, @"DecoyDatabase.ini");
            OpenMSCommons.CreateDefaultINI(exec_path, ini_path, NodeScratchDirectory, m_node_delegates);
            Dictionary<string, string> dd_parameters = new Dictionary<string, string> {
                        {"out", fasta_file},
                        {"append", "true"},
                        {"decoy_string_position", "prefix"},
                        {"decoy_string", "REV_"},
                        {"threads", param_general_num_threads.ToString()}
                };
            OpenMSCommons.WriteParamsToINI(ini_path, dd_parameters);
            string[] in_list = new string[1];
            in_list[0] = fasta_file;
            OpenMSCommons.WriteItemListToINI(in_list, ini_path, "in");

            //SendAndLogMessage("Preprocessing / DecoyDatabase");
            OpenMSCommons.RunTOPPTool(exec_path, ini_path, NodeScratchDirectory, m_node_delegates);

            m_current_step += 1;
            ReportTotalProgress((double)m_current_step / m_num_steps);
        }

        /// <summary>
        /// Parse results in csv_filename and add to EntityDataService
        /// </summary>
        private void ParseCSVResults(string csv_filename)
        {
            if (EntityDataService.ContainsEntity<RNPxlItem>() == false)
            {
                EntityDataService.RegisterEntity<RNPxlItem>(ProcessingNodeNumber);
            }

            var rnpxl_items = new List<RNPxlItem>();

            StreamReader reader = File.OpenText(csv_filename);
            string line;
            line = reader.ReadLine(); //ignore header

            while ((line = reader.ReadLine()) != null)
            {
                string[] items = line.Split(new char[] { '\t' }, StringSplitOptions.None);

                var x = new RNPxlItem();

                x.WorkflowID = WorkflowID;
                x.Id = EntityDataService.NextId<RNPxlItem>();

                double dbl_val;
                Int32 int_val;

                x.rt = Double.TryParse(items[0], out dbl_val) ? (dbl_val / 60.0) : 0.0;
                x.orig_mz = Double.TryParse(items[1], out dbl_val) ? dbl_val : 0.0;
                x.proteins = items[2];
                x.rna = items[3];
                x.peptide = items[4];
                x.charge = Int32.TryParse(items[5], out int_val) ? int_val : 0;
                x.score = Double.TryParse(items[6], out dbl_val) ? dbl_val : 0.0;
                x.best_loc_score = Double.TryParse(items[7], out dbl_val) ? (dbl_val > 1e-20 ? dbl_val * 100.0 : 0.0) : 0.0;
                x.loc_scores = items[8];
                x.best_localizations = items[9];
                x.peptide_weight = Double.TryParse(items[10], out dbl_val) ? dbl_val : 0.0;
                x.rna_weight = Double.TryParse(items[11], out dbl_val) ? dbl_val : 0.0;
                x.xl_weight = Double.TryParse(items[12], out dbl_val) ? dbl_val : 0.0;
                x.a_1 = Double.TryParse(items[13], out dbl_val) ? dbl_val : 0.0;
                x.a_3 = Double.TryParse(items[14], out dbl_val) ? dbl_val : 0.0;
                x.c_1 = Double.TryParse(items[15], out dbl_val) ? dbl_val : 0.0;
                x.c_3 = Double.TryParse(items[16], out dbl_val) ? dbl_val : 0.0;
                x.g_1 = Double.TryParse(items[17], out dbl_val) ? dbl_val : 0.0;
                x.g_3 = Double.TryParse(items[18], out dbl_val) ? dbl_val : 0.0;
                x.u_1 = Double.TryParse(items[19], out dbl_val) ? dbl_val : 0.0;
                x.u_3 = Double.TryParse(items[20], out dbl_val) ? dbl_val : 0.0;
                x.abs_prec_error_da = Double.TryParse(items[21], out dbl_val) ? dbl_val : 0.0;
                x.rel_prec_error_ppm = Double.TryParse(items[22], out dbl_val) ? dbl_val : 0.0;
                x.m_h = Double.TryParse(items[23], out dbl_val) ? dbl_val : 0.0;
                x.m_2h = Double.TryParse(items[24], out dbl_val) ? dbl_val : 0.0;
                x.m_3h = Double.TryParse(items[25], out dbl_val) ? dbl_val : 0.0;
                x.m_4h = Double.TryParse(items[26], out dbl_val) ? dbl_val : 0.0;
                x.fragment_annotation = items[27];                

                if (x.peptide == "" && x.rna == "")
                {
                    continue;
                }

                rnpxl_items.Add(x);
            }

            EntityDataService.InsertItems(rnpxl_items);
        }

        /// <summary>
        /// For a ModificationParameter object (from the UI parameter settings), return a string list representing the modifications in OpenMS-format.
        /// </summary>
        private List<string> convertParamToModStringArray(ModificationParameter mod_param, string type)
        {
            var result = new List<string>();

            if (mod_param.ToString() == "None")
            {
                return result;
            }

            if (type == "RESIDUE")
            {
                // multiple residues can be selected => return list
                var s = mod_param.ValueToString();
                // looks like "Phospho / +79.966 Da (S, T, Y)"
                var m = s.Split(' ')[0];
                var residue_str = s.Split(new string[] { " Da (" }, StringSplitOptions.None)[1];
                residue_str = residue_str.Substring(0, residue_str.Length - 1);
                var residues = residue_str.Split(new string[] { ", " }, StringSplitOptions.None);

                foreach (var r in residues)
                {
                    result.Add(m + " (" + r + ")");
                }
            }
            else if (type == "C-TERM")
            {
                var s = mod_param.ValueToString();
                var m = s.Split(' ')[0];
                result.Add(m + " (C-term)");
            }
            else if (type == "N-TERM")
            {
                var s = mod_param.ValueToString();
                var m = s.Split(' ')[0];
                result.Add(m + " (N-term)");
            }
            else
            {
                string err = "Cannot parse unknown modification type (Known are: RESIDUE, C-TERM, N-TERM)";
                SendAndLogErrorMessage(err);
                throw new MagellanException(err);
            }

            return result;
        }

        protected override void OnAllSpectraSentForSearch(IProcessingNode sender, ResultsArguments eventArgs)
        {
            //throw new NotImplementedException();
        }

        protected override void OnSpectraSentForSearch(IProcessingNode sender, MassSpectrumCollection spectra)
        {
            //throw new NotImplementedException();
        }
    }
}

