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
using Thermo.Magellan.BL.Processing.Interfaces;
using Thermo.Magellan.BL.Processing;
using Thermo.Magellan.DataLayer.FileIO;
using Thermo.Magellan.EntityDataFramework;
using Thermo.Magellan.MassSpec;
using Thermo.Magellan.Utilities;
using Thermo.Magellan.Proteomics;
using Thermo.Magellan.StudyManagement;
using Thermo.Magellan.StudyManagement.DataObjects;
using Thermo.Magellan.Core.Logging;
using Thermo.Magellan.Core.Exceptions;
using Thermo.Magellan.PeptideIdentificationNodes;
using Thermo.Magellan.BL.Data.Constants;
using Thermo.PD.EntityDataFramework;

// manual deployment:
// - build project
// - copy PD.OpenMS.AdapterNodes.dll to System/Release subfolder in PD folder e.g., from D:\RNPXL\THERMO\RNPxl\src\bin\x64\Debug to C:\Program Files\Thermo\Proteome Discoverer 2.5\System\Release
// - run .\Thermo.Magellan.Server.exe -install
// - run Thermo.Discoverer.exe –startServer –showServerWindow

// Log files can be found in: C:\ProgramData\Thermo\Proteome Discoverer 2.5\Logs

namespace PD.OpenMS.AdapterNodes
{
    # region NodeSetup

    [ProcessingNode("5418b2e2-1e65-4af1-8253-0839e4f91a1a",
        Category = ProcessingNodeCategories.SequenceDatabaseSearch,
        DisplayName = "NuXL",
        MainVersion = 1,
        MinorVersion = 44,
        Description = "Analyze Protein-Nucleic Acid cross-linking data")]

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

    public class NuXLProcessingNode : PeptideAndProteinIdentificationNode, IResultsSink<MassSpectrumCollection>
    {
        private const string NuXLToolDirectory = "NuXL/";
        private const string NuXLExecutablePath = @"bin/OpenNuXL.exe";
        private const string NuXLIniFileName = "NuXL.ini";
        private const string XICFilterExecutable = @"bin/RNPxlXICFilter.exe";
        private const string MapRTTransformerExecutable = @"bin/MapRTTransformer.exe";
        private const string FeatureFinderExecutable = @"bin/FeatureFinderCentroided.exe";
        private const string MapAlignerExecutable = @"bin/MapAlignerPoseClustering.exe";
        private const string FeatureFinderIniFileName = @"FeatureFinderCentroided.ini";
        private const string MapAlignerIniFileName = @"MapAlignerPoseClustering.ini";
        private const string MapRTTransformerIniFileName = @"MapRTTransformer.ini";
        private const string XICFilterIniFilename = @"RNPxlXICFilter.ini";
        private const string PercolatorExecutable = @"bin/percolator.exe";        

        #region Parameters

        [BooleanParameter(
            Category = "1. General",
            DisplayName = "Preprocess using ID filtering",
            Description = "If set to true, spectra containing regular (non-cross-linked) peptides (max. 1% FDR) are skipped in the search for cross-links.",
            DefaultValue = "true",
            IsAdvanced = true,
            Position = 10)]
        public BooleanParameter param_general_id_filter;

        [BooleanParameter(
            Category = "1. General",
            DisplayName = "Autotune search settings",
            Description = "If set to true, search tolerances are estimated from a search for non-cross-linked peptides.",
            DefaultValue = "true",
            IsAdvanced = true,
            Position = 11)]
        public BooleanParameter param_general_autotune;

        [BooleanParameter(
            Category = "1. General",
            DisplayName = "Discard PSMs with high precursor mass errors",
            Description = "If set to true, PSMs are filtered if the precursor mass error is larger than 5 std. devs.",
            IsAdvanced = true,
            DefaultValue = "false",
            Position = 12)]
        public BooleanParameter param_general_discard_bad_pcmass;

        [BooleanParameter(
            Category = "1. General",
            DisplayName = "Filter bad partial loss scores",
            Description = "If set to true, Cross-link PSMs are filtered out early if they show no or nearly no shifted peaks.",
            DefaultValue = "false",
            IsAdvanced = true,
            Position = 13)]
        public BooleanParameter param_general_filter_bad_partial_loss_scores;

        [BooleanParameter(
            Category = "1. General",
            DisplayName = "Preprocess using XIC filtering",
            Description = "Only relevant when a control file is available. Remove XICs at precursor positions that are also found in the control file from the cross-link file.",
            DefaultValue = "false",
            IsAdvanced = true,
            Position = 20)]
        public BooleanParameter param_general_run_xic_filtering;

        [BooleanParameter(
            Category = "1. General",
            DisplayName = "XIC filtering uses RT alignment",
            Description = "Only relevant when a control file is available and 'Preprocess using XIC filtering' is set to 'true'. Specifies whether control file should be aligned to cross-link file.",
            IsAdvanced = true,
            DefaultValue = "false",
            Position = 30)]
        public BooleanParameter param_general_run_map_alignment;

        [FastaFileParameter(
            Category = "1. General",
            DisplayName = "Protein database",
            Description = "The sequence database to be used for both peptide ID filtering and searching for peptide-NA crosslinks",
            IntendedPurpose = ParameterPurpose.SequenceDatabase,
            ValueRequired = true,
            IsMultiSelect = true,
            Position = 40)]
        public FastaFileParameter param_general_fasta_dbs;

        [IntegerParameter(
            Category = "1. General",
            DisplayName = "Isotope misassignments",
            Description = "Corrects for mono-isotopic peak misassignments. (E.g.: 1 = prec. may be misassigned to first isotopic peak) ",
            DefaultValue = "0",
            MinimumValue = "0",
            MaximumValue = "1",
            IsAdvanced = true,
            Position = 50)]
        public IntegerParameter param_nuxl_isotopes;

        [IntegerParameter(
            Category = "1. General",
            DisplayName = "CPU cores",
            Description = "Maximum number of CPU cores allowed to be used by the algorithms",
            DefaultValue = "4",
            MinimumValue = "1",
            Position = 55)]
        public IntegerParameter param_general_num_threads;

        [IntegerParameter(
            Category = "2. Cross-links",
            DisplayName = "Length",
            Description = "Maximum length of oligonucleotides. 0 = disable search for NA variants.",
            DefaultValue = "1",
            MinimumValue = "0",
            MaximumValue = "4",
            Position = 60)]
        public IntegerParameter param_cross_linking_length;
        
        [StringSelectionParameter(
            Category = "2. Cross-links",
            DisplayName = "Presets",
            Description = "One of supported cross-linking protocols ('none' = use user provided precursor & fragment adducts and cross-linked NAs).",
            DefaultValue = "none",
            SelectionValues = new string[] {
                "none", "RNA-UV (U)", "RNA-UV (UCGA)", "RNA-UV (4SU)", "DNA-UV", "RNA-DEB", "DNA-DEB", "RNA-NM", "DNA-NM"},
            Position = 65)]
        public SimpleSelectionParameter<string> param_cross_linking_presets;

        [StringParameter(
            Category = "2. Cross-links",
            DisplayName = "Sequence",
            Description = "Sequence to restrict the generation of oligonucleotide chains. (disabled for empty sequence)",
            DefaultValue = "",
            IsAdvanced = true,
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
            Category = "2. Cross-link identification",
            DisplayName = "Can cross-link",
            Description = "Format: 'U' if only U forms cross-links. 'CATG' if C, A, G, and T form cross - links. (default: 'U')",
            DefaultValue = "U",
            IsAdvanced = true,
            Position = 100)]
        public StringParameter param_cross_linking_can_xls;

        [StringParameter(
            Category = "2. Cross-link identification",
            DisplayName = "Fragment adducts",
            Description = "Format: [target nucleotide]:[formula] or [precursor adduct]->[fragment adduct formula];[name]: e.g., 'U:C9H10N2O5;U-H3PO4' or 'U:U-H2O->C9H11N2O8P1;U-H2O', (default: '[U:C9H10N2O5;U-H3PO4 U:C4H4N2O2;U' U:C4H2N2O1;U'-H2O U:C3O;C3O U:C9H13N2O9P1;U U:C9H11N2O8P1;U-H2O U:C9H12N2O6;U-HPO3]')",
            DefaultValue = "[U:C9H10N2O5;U-H3PO4 U:C4H4N2O2;u U:C4H2N2O1;u-H2O U:C3O;C3O U:C9H13N2O9P1;U U:C9H11N2O8P1;U-H2O U:C9H12N2O6;U-HPO3]",
            IsAdvanced = true,
            Position = 101)]
        public StringParameter param_cross_linking_fragment_adducts;

        [StringParameter(
            Category = "2. Cross-link identification",
            DisplayName = "Modifications",
            Description = "Format: empirical formula e.g -H2O, ..., H2O+PO3 (default: '[U:  U:-H2O U:-H2O-HPO3 U:-HPO3]')",
            DefaultValue = "[U: U:-H2O U:-H2O-HPO3 U:-HPO3]",
            IsAdvanced = true,
            Position = 110)]
        public StringParameter param_cross_linking_modifications;

        /*        
        [StringParameter(
            Category = "2. Cross-links",
            DisplayName = "Nucleotide groups",
            Description = "Restrict which nucleotides can cooccur in a precursor adduct to be able to search both RNA and DNA (Format e.g.: AU CG).",
            DefaultValue = "",
            IsAdvanced = true,
            Position = 110)]
        public StringParameter param_cross_linking_modifications;
         */

        [BooleanParameter(
            Category = "2. Cross-link identification",
            DisplayName = "Cysteine adduct",
            Description = "Use this flag if the +152 adduct is expected.",
            DefaultValue = "false",
            IsAdvanced = true,
            Position = 130)]
        public BooleanParameter param_cross_linking_cysteine_adduct;

/*
        [BooleanParameter(
            Category = "2. Cross-links",
            DisplayName = "Filter fractional mass",
            Description = "Use this flag to filter non-crosslinks by fractional mass.",
            DefaultValue = "false",
            IsAdvanced = true,
            Position = 140)]
        public BooleanParameter param_cross_linking_filter_fractional_mass;

        [BooleanParameter(
            Category = "2. Cross-links",
            DisplayName = "Carbon-labeled fragments",
            Description = "Generate fragment shifts assuming full labeling of carbon (e.g. completely labeled U13).",
            DefaultValue = "false",
            IsAdvanced = true,
            Position = 150)]
        public BooleanParameter param_cross_linking_carbon_labeled_fragments;
*/
        [MassToleranceParameter(
            Category = "3. Peptide identification",
            DisplayName = "Precursor mass tolerance",
            Description = "This parameter specifies the precursor mass tolerance for peptide identification",
            DefaultValue = "6 ppm",
            Position = 160,
            IntendedPurpose = ParameterPurpose.MassTolerance)]
        public MassToleranceParameter param_nuxl_precursor_mass_tolerance;

        [MassToleranceParameter(
            Category = "3. Peptide identification",
            DisplayName = "Fragment mass tolerance",
            Description = "This parameter specifies the fragment mass tolerance for peptide identification",
            DefaultValue = "20 ppm",
            Position = 170,
            IntendedPurpose = ParameterPurpose.MassTolerance)]
        public MassToleranceParameter param_nuxl_fragment_mass_tolerance;

        [IntegerParameter(
            Category = "3. Peptide identification",
            DisplayName = "Charge low",
            Description = "Lowest charge state to search for.",
            DefaultValue = "2",
            MinimumValue = "1",
            Position = 180)]
        public IntegerParameter param_nuxl_charge_low;

        [IntegerParameter(
            Category = "3. Peptide identification",
            DisplayName = "Charge high",
            Description = "Highest charge state to search for.",
            DefaultValue = "5",
            MinimumValue = "1",
            Position = 190)]
        public IntegerParameter param_nuxl_charge_high;

        [IntegerParameter(
            Category = "3. Peptide identification",
            DisplayName = "Peptide length min",
            Description = "Minimum length of peptides.",
            DefaultValue = "6",
            MinimumValue = "5",
            Position = 195)]
        public IntegerParameter param_nuxl_peptide_length_min;

        [IntegerParameter(
            Category = "3. Peptide identification",
            DisplayName = "Peptide length max",
            Description = "Maximum length of peptides.",
            DefaultValue = "40",
            MinimumValue = "6",
            Position = 196)]
        public IntegerParameter param_nuxl_peptide_length_max;

        [StringSelectionParameter(
            Category = "3. Peptide identification",
            DisplayName = "Enzyme",
            Description = "The enzyme used for cleaving the proteins",
            DefaultValue = "Trypsin/P",
            SelectionValues = new string[] {
                "Trypsin", "Lys-C/P", "PepsinA", "no cleavage", "unspecific cleavage", 
                "Glu-C+P", "PepsinA + P", "Formic_acid", "CNBr", "Chymotrypsin",
                "Chymotrypsin/P", "Lys-C", "Lys-N", "TrypChymo", "Trypsin/P", "Arg-C/P",
                "Asp-N", "V8-DE", "V8-E", "leukocyte elastase", "proline endopeptidase",
                "glutamyl endopeptidase", "Alpha-lytic protease", "2-iodobenzoate", "iodosobenzoate",
                "staphylococcal protease/D", "proline-endopeptidase/HKR", "cyanogen-bromide",
                "Clostripain/P", "elastase-trypsin-chymotrypsin", "Arg-C", "Asp-N/B", "Asp-N_ambic"},
            Position = 200)]
        public SimpleSelectionParameter<string> param_nuxl_enzyme;

        [StringSelectionParameter(
            Category = "3. Peptide identification",
            DisplayName = "Scoring",
            Description = "The scoring method used (no fragment adducts=total loss of all NAs assumed in prescoring, include fragment adducts=all partial loss ions are used in prescoring.)",
            DefaultValue = "include fragment adducts",
            SelectionValues = new string[] {"no fragment adducts", "include fragment adducts" },
                Position = 205)]
        public SimpleSelectionParameter<string> param_nuxl_scoring;

        [IntegerParameter(
            Category = "3. Peptide identification",
            DisplayName = "Missed cleavages",
            Description = "Maximum allowed number of missed cleavages.",
            DefaultValue = "1",
            MinimumValue = "0",
            Position = 210)]
        public IntegerParameter param_nuxl_missed_cleavages;

        [ModificationParameter(
            Category = "3. Peptide identification",
            DisplayName = "Static N-terminal modification",
            Description = "Select any known or suspected modification. Static modifications are applied universally, to every instance of the specified residue(s) or terminus. ",
            ModificationType = ModificationType.Static,
            PositionType = AminoAcidModificationPositionType.Any_N_Terminus,
            IntendedPurpose = ParameterPurpose.StaticTerminalModification,
            Position = 220)]
        public ModificationParameter param_nuxl_static_n_terminal_mod;

        [ModificationParameter(
            Category = "3. Peptide identification",
            DisplayName = "Static C-terminal modification",
            Description = "Select any known or suspected modification. Static modifications are applied universally, to every instance of the specified residue(s) or terminus. ",
            ModificationType = ModificationType.Static,
            PositionType = AminoAcidModificationPositionType.Any_C_Terminus,
            IntendedPurpose = ParameterPurpose.StaticTerminalModification,
            Position = 230)]
        public ModificationParameter param_nuxl_static_c_terminal_mod;

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
        public ModificationParameter param_nuxl_static_mod_1;

        [ModificationParameter(
            Category = "3. Peptide identification",
            DisplayName = "2. Static modification",
            Description = "Select any known or suspected modification. Static modifications are applied universally, to every instance of the specified residue(s) or terminus. ",
            ModificationType = ModificationType.Static,
            IsMultiSelect = true,
            IsAdvanced = true,
            IntendedPurpose = ParameterPurpose.StaticModification,
            Position = 250)]
        [ParameterGroup(GroupName = "Static Modifications", IsDominant = true, IsValueUnique = true)]
        [ParameterGroup(GroupName = "Modifications", IsDominant = true, IsControlledValueUnique = true)]
        public ModificationParameter param_nuxl_static_mod_2;

        [ModificationParameter(
            Category = "3. Peptide identification",
            DisplayName = "3. Static modification",
            Description = "Select any known or suspected modification. Static modifications are applied universally, to every instance of the specified residue(s) or terminus. ",
            ModificationType = ModificationType.Static,
            IsMultiSelect = true,
            IsAdvanced = true,
            IntendedPurpose = ParameterPurpose.StaticModification,
            Position = 260)]
        [ParameterGroup(GroupName = "Static Modifications", IsDominant = true, IsValueUnique = true)]
        [ParameterGroup(GroupName = "Modifications", IsDominant = true, IsControlledValueUnique = true)]
        public ModificationParameter param_nuxl_static_mod_3;

        [ModificationParameter(
            Category = "3. Peptide identification",
            DisplayName = "4. Static modification",
            Description = "Select any known or suspected modification. Static modifications are applied universally, to every instance of the specified residue(s) or terminus. ",
            ModificationType = ModificationType.Static,
            IsMultiSelect = true,
            IsAdvanced = true,
            IntendedPurpose = ParameterPurpose.StaticModification,
            Position = 270)]
        [ParameterGroup(GroupName = "Static Modifications", IsDominant = true, IsValueUnique = true)]
        [ParameterGroup(GroupName = "Modifications", IsDominant = true, IsControlledValueUnique = true)]
        public ModificationParameter param_nuxl_static_mod_4;

        [ModificationParameter(
            Category = "3. Peptide identification",
            DisplayName = "5. Static modification",
            Description = "Select any known or suspected modification. Static modifications are applied universally, to every instance of the specified residue(s) or terminus. ",
            ModificationType = ModificationType.Static,
            IsMultiSelect = true,
            IsAdvanced = true,
            IntendedPurpose = ParameterPurpose.StaticModification,
            Position = 280)]
        [ParameterGroup(GroupName = "Static Modifications", IsDominant = true, IsValueUnique = true)]
        [ParameterGroup(GroupName = "Modifications", IsDominant = true, IsControlledValueUnique = true)]
        public ModificationParameter param_nuxl_static_mod_5;

        [ModificationParameter(
            Category = "3. Peptide identification",
            DisplayName = "6. Static modification",
            Description = "Select any known or suspected modification. Static modifications are applied universally, to every instance of the specified residue(s) or terminus. ",
            ModificationType = ModificationType.Static,
            IsMultiSelect = true,
            IsAdvanced = true,
            IntendedPurpose = ParameterPurpose.StaticModification,
            Position = 290)]
        [ParameterGroup(GroupName = "Static Modifications", IsDominant = true, IsValueUnique = true)]
        [ParameterGroup(GroupName = "Modifications", IsDominant = true, IsControlledValueUnique = true)]
        public ModificationParameter param_nuxl_static_mod_6;

        [IntegerParameter(
            Category = "3. Peptide identification",
            DisplayName = "Max. number of dynamic modifications",
            Description = "Maximum number of dynamic modifications per peptide. Includes all terminal and internal modifications.",
            DefaultValue = "2",
            MinimumValue = "0",
            Position = 300)]
        public IntegerParameter param_nuxl_num_dynamic_mods;

        [ModificationParameter(
            Category = "3. Peptide identification",
            DisplayName = "1. Dynamic N-terminal modification",
            Description = "Select any known or suspected modification.",
            ModificationType = ModificationType.Dynamic,
            PositionType = AminoAcidModificationPositionType.Any_N_Terminus,
            IntendedPurpose = ParameterPurpose.DynamicTerminalModification,
            Position = 400)]
        public ModificationParameter param_nuxl_dynamic_n_terminal_mod_1;

        [ModificationParameter(
            Category = "3. Peptide identification",
            DisplayName = "2. Dynamic N-terminal modification",
            Description = "Select any known or suspected modification.",
            ModificationType = ModificationType.Dynamic,
            IsAdvanced = true,
            PositionType = AminoAcidModificationPositionType.Any_N_Terminus,
            IntendedPurpose = ParameterPurpose.DynamicTerminalModification,
            Position = 410)]
        public ModificationParameter param_nuxl_dynamic_n_terminal_mod_2;

        [ModificationParameter(
            Category = "3. Peptide identification",
            DisplayName = "3. Dynamic N-terminal modification",
            IsAdvanced = true,
            Description = "Select any known or suspected modification.",
            ModificationType = ModificationType.Dynamic,
            PositionType = AminoAcidModificationPositionType.Any_N_Terminus,
            IntendedPurpose = ParameterPurpose.DynamicTerminalModification,
            Position = 420)]
        public ModificationParameter param_nuxl_dynamic_n_terminal_mod_3;

        [ModificationParameter(
            Category = "3. Peptide identification",
            DisplayName = "1. Dynamic C-terminal modification",
            Description = "Select any known or suspected modification.",
            ModificationType = ModificationType.Dynamic,
            PositionType = AminoAcidModificationPositionType.Any_C_Terminus,
            IntendedPurpose = ParameterPurpose.DynamicTerminalModification,
            Position = 430)]
        public ModificationParameter param_nuxl_dynamic_c_terminal_mod_1;

        [ModificationParameter(
            Category = "3. Peptide identification",
            DisplayName = "2. Dynamic C-terminal modification",
            IsAdvanced = true,
            Description = "Select any known or suspected modification.",
            ModificationType = ModificationType.Dynamic,
            PositionType = AminoAcidModificationPositionType.Any_C_Terminus,
            IntendedPurpose = ParameterPurpose.DynamicTerminalModification,
            Position = 440)]
        public ModificationParameter param_nuxl_dynamic_c_terminal_mod_2;

        [ModificationParameter(
            Category = "3. Peptide identification",
            DisplayName = "3. Dynamic C-terminal modification",
            IsAdvanced = true,
            Description = "Select any known or suspected modification.",
            ModificationType = ModificationType.Dynamic,
            PositionType = AminoAcidModificationPositionType.Any_C_Terminus,
            IntendedPurpose = ParameterPurpose.DynamicTerminalModification,
            Position = 450)]
        public ModificationParameter param_nuxl_dynamic_c_terminal_mod_3;

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
        public ModificationParameter param_nuxl_dynamic_mod_1;

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
        public ModificationParameter param_nuxl_dynamic_mod_2;

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
        public ModificationParameter param_nuxl_dynamic_mod_3;

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
        public ModificationParameter param_nuxl_dynamic_mod_4;

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
        public ModificationParameter param_nuxl_dynamic_mod_5;

        [ModificationParameter(
            Category = "3. Peptide identification",
            DisplayName = "6. Dynamic modification",
            Description = "Select any known or suspected modification.",
            ModificationType = ModificationType.Dynamic,
            IsMultiSelect = true,
            IsAdvanced = true,
            IntendedPurpose = ParameterPurpose.DynamicModification,
            Position = 510)]
        [ParameterGroup(GroupName = "Dynamic Modifications", IsDominant = true, IsValueUnique = true)]
        [ParameterGroup(GroupName = "Modifications", IsDominant = true, IsControlledValueUnique = true)]
        public ModificationParameter param_nuxl_dynamic_mod_6;

        [IntegerParameter(
            Category = "5. XIC filtering",
            DisplayName = "Fold change",
            Description = "Minimum fold change required for an eluting peptide in the UV sample to be considered a putative cross-link. Extracted ion chromatograms (XICs) of eluting analytes are compared between control and treatment, If the UV signal is not significantly stronger than the control (according to a minimum fold change threshold), the analyte is considered a co-eluting non-cross-linked species or contaminant and its tandem spectra are removed from the analysis.",
            DefaultValue = "2",
            MinimumValue = "0",
            IsAdvanced = true,
            Position = 810)]
        public IntegerParameter param_xic_filtering_fold_change;

        [DoubleParameter(
            Category = "5. XIC filtering",
            DisplayName = "max. RT difference [min]",
            Description = "Maximum allowed retention time difference between corresponding XICs.",
            DefaultValue = "0.33",
            IsAdvanced = true,
            Position = 820)]
        public DoubleParameter param_xic_filtering_rt_threshold;

        [MassToleranceParameter(
            Category = "5. XIC filtering",
            DisplayName = "Max. m/z difference",
            Subset = "ppm",
            Description = "Maximum allowed m/z difference between corresponding XICs",
            DefaultValue = "10 ppm",
            IsAdvanced = true,
            IntendedPurpose = ParameterPurpose.MassTolerance,
            Position = 830)]
        public MassToleranceParameter param_xic_filtering_mz_threshold;



        // this is needed in order to prevent a WorkflowJobBuilder error upon execution
        // since we inherit from PeptideAndProteinIdentificationNode, which in turn
        // seems to be necessary in order to be able to persist spectra into the consensus step
        [Score(ProteomicsDataTypes.Psms, isMainScore: true, category: ScoreCategoryType.HigherIsBetter,
            DisplayName = "DummyScore",
            Description = "Dummy score to prevent a WorkflowJobBuilder error",
            FormatString = "F2",
            Guid = "0d802bb3-6057-4f30-bc00-4576f4ca73c2")]
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
                ProcessingServices.SpectrumPersistenceService.PersistMSnSpectra(sender, spectra_to_store);
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
            int num_xicfilter_substeps = 1 * Convert.ToInt32(param_general_run_xic_filtering.Value);
            int num_mapalignment_substeps = 5 * Convert.ToInt32(param_general_run_xic_filtering.Value && param_general_run_map_alignment.Value);
            int num_nuxl_substeps = 1;
            m_num_steps = num_export_substeps + num_xicfilter_substeps + num_mapalignment_substeps + num_nuxl_substeps + 1;
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

            bool export_file_is_open = exporter.Open(spectrum_export_file_name);

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
                    exporter.SendMassSpectra(spectra, WorkflowID);
                    spectra.Clear();
                }
            }

            exporter.SendMassSpectra(spectra, WorkflowID);

            exporter.Dispose();

            SendAndLogMessage("Exporting {0} spectra took {1}.", spectrum_ids.Count, StringHelper.GetDisplayString(timer.Elapsed));
        }

        #endregion


        /// <summary>
        /// Run the workflow on a single (cross-linked) mzML file
        /// </summary>
        private void RunWorkflowOnSingleFile(string input_file)
        {
            ArgumentHelper.AssertStringNotNullOrWhitespace(input_file, "input_file");

            // NuXL Search
            string idXML_file = RunNuXL(input_file);
            //ParseCSVResults(csv_file);

            string xl_id_perc = idXML_file.Replace(".idXML", "") + "_perc_1.0000_XLs.idXML";
            string id_perc = idXML_file.Replace(".idXML", "") + "_1.0000_XLs.idXML";
            if (File.Exists(idXML_file.Replace(".idXML", "") + "_perc_1.0000_XLs.idXML"))
            { 
                ParseIdXMLResults(xl_id_perc);
            }
            else
            {
                ParseIdXMLResults(id_perc);
            }
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

            // XIC filtering
            string xic_filtered_uv_file = RunXICFilter(aligned_uv_file, aligned_control_file);

            // NuXL Search
            string idXML_file = RunNuXL(xic_filtered_uv_file);
            //ParseCSVResults(csv_file);
            string xl_id_perc = idXML_file.Replace(".idXML", "") + "_perc_1.0000_XLs.idXML";
            string id_perc = idXML_file.Replace(".idXML", "") + "_1.0000_XLs.idXML";
            if (File.Exists(idXML_file.Replace(".idXML", "") + "_perc_1.0000_XLs.idXML"))
            {
                ParseIdXMLResults(xl_id_perc);
            }
            else
            {
                ParseIdXMLResults(id_perc);
            }
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

            string openms_dir = Path.Combine(ServerConfiguration.ToolsDirectory, NuXLToolDirectory);
            var exec_path = Path.Combine(openms_dir, XICFilterExecutable);
            var ini_path = Path.Combine(NodeScratchDirectory, XICFilterIniFilename);
            var openms_log_path = Path.Combine(NodeScratchDirectory, "xic.log");

            OpenMSCommons.CreateDefaultINI(exec_path, ini_path, NodeScratchDirectory, m_node_delegates);
            Dictionary<string, string> fdr_parameters = new Dictionary<string, string>
            {
                {"treatment", uv_mzml_filename},
                {"control", control_mzml_filename},
                {"out", result_filename},
                {"fold_change", param_xic_filtering_fold_change.ToString()},
                {"rt_tol", (param_xic_filtering_rt_threshold.Value * 60.0).ToString()},
                {"mz_tol", param_xic_filtering_mz_threshold.Value.Tolerance.ToString()},
                {"threads", param_general_num_threads.ToString()},
                {"log",  openms_log_path.ToString()}
            };
            OpenMSCommons.WriteParamsToINI(ini_path, fdr_parameters);

            SendAndLogMessage("Preprocessing -- XIC filtering");
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

            string openms_dir = Path.Combine(ServerConfiguration.ToolsDirectory, NuXLToolDirectory);

            string aligned_uv_filename = Path.Combine(NodeScratchDirectory, "UV_aligned.mzML");
            string aligned_control_filename = Path.Combine(NodeScratchDirectory, "Control_aligned.mzML");
            var result = Tuple.Create(aligned_uv_filename, aligned_control_filename);

            // ==============================================================================================
            // =============================== STEP 1: FeatuerFinderCentroided ==============================
            // ==============================================================================================

            string exec_path = Path.Combine(openms_dir, FeatureFinderExecutable);
            string ini_path = Path.Combine(NodeScratchDirectory, FeatureFinderIniFileName);

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

            exec_path = Path.Combine(openms_dir, MapAlignerExecutable);

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
            ini_path = Path.Combine(NodeScratchDirectory, MapAlignerIniFileName);
            OpenMSCommons.CreateDefaultINI(exec_path, ini_path, NodeScratchDirectory, m_node_delegates);
            OpenMSCommons.WriteParamsToINI(ini_path, map_parameters);
            OpenMSCommons.WriteItemListToINI(in_files, ini_path, "in");
            OpenMSCommons.WriteItemListToINI(out_files, ini_path, "trafo_out");
            OpenMSCommons.WriteNestedParamToINI(ini_path, new Triplet("reference", "index", "1")); //use UV file as reference! (transform only control, so RTs stay constant for UV)
            OpenMSCommons.WriteThresholdsToINI(param_xic_filtering_mz_threshold, param_xic_filtering_rt_threshold, ini_path);

            SendAndLogMessage("Preprocessing -- Map alignment -- MapAlignerPoseClustering");
            OpenMSCommons.RunTOPPTool(exec_path, ini_path, NodeScratchDirectory, m_node_delegates);

            m_current_step += 1;
            ReportTotalProgress((double)m_current_step / m_num_steps);

            // ==============================================================================================
            // ================================= STEP 3: MapRTTransformer ===================================
            // ==============================================================================================

            exec_path = Path.Combine(openms_dir, MapRTTransformerExecutable);

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

            ini_path = Path.Combine(NodeScratchDirectory, MapRTTransformerIniFileName);
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
        /// Run NuXL tool on (preprocessed) UV mzML file
        /// </summary>
        /// <returns>the csv result file name</returns>
        private string RunNuXL(string uv_mzml_filename)
        {
            var openms_dir = Path.Combine(ServerConfiguration.ToolsDirectory, NuXLToolDirectory);
            var exec_path = Path.Combine(openms_dir, NuXLExecutablePath);

            // result filenames
            string result_tsv_filename = Path.Combine(NodeScratchDirectory, "rnpxl_search_results.tsv");
            string idxml_filename = Path.Combine(NodeScratchDirectory, "rnpxl_search_results.idXML");

            // FASTA DB

            // concatenate all selected fasta files to a single file for OpenMS
            var fasta_path = Path.Combine(NodeScratchDirectory, "nuxl_db.fasta");
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

            // INI file
            string nuxl_ini_file = Path.Combine(NodeScratchDirectory, NuXLIniFileName);
            string openms_log_path = Path.Combine(NodeScratchDirectory, "nuxl.log");
            OpenMSCommons.CreateDefaultINI(exec_path, nuxl_ini_file, NodeScratchDirectory, m_node_delegates);
            Dictionary<string, string> nuxl_parameters = new Dictionary<string, string> {
                            {"in", uv_mzml_filename},
                            {"database", fasta_path},
                            {"out_tsv", result_tsv_filename},
                            {"out", idxml_filename},
                            {"percolator_executable", Path.Combine(openms_dir, PercolatorExecutable)},
                            {"threads", param_general_num_threads.ToString()},
                            {"log",  openms_log_path.ToString()}
            };
            OpenMSCommons.WriteParamsToINI(nuxl_ini_file, nuxl_parameters);

            OpenMSCommons.WriteNestedParamToINI(nuxl_ini_file, new Triplet("precursor", "mass_tolerance", param_nuxl_precursor_mass_tolerance.Value.Tolerance.ToString()));
            OpenMSCommons.WriteNestedParamToINI(nuxl_ini_file, new Triplet("precursor", "mass_tolerance_unit", param_nuxl_precursor_mass_tolerance.UnitToString()));
            OpenMSCommons.WriteNestedParamToINI(nuxl_ini_file, new Triplet("precursor", "min_charge", param_nuxl_charge_low));
            OpenMSCommons.WriteNestedParamToINI(nuxl_ini_file, new Triplet("precursor", "max_charge", param_nuxl_charge_high));
            OpenMSCommons.WriteNestedParamToINI(nuxl_ini_file, new Triplet("precursor", "isotopes", param_nuxl_isotopes));

            OpenMSCommons.WriteNestedParamToINI(nuxl_ini_file, new Triplet("fragment", "mass_tolerance", param_nuxl_fragment_mass_tolerance.Value.Tolerance.ToString()));
            OpenMSCommons.WriteNestedParamToINI(nuxl_ini_file, new Triplet("fragment", "mass_tolerance_unit", param_nuxl_fragment_mass_tolerance.UnitToString()));

            var static_mods = new List<string>();
            var variable_mods = new List<string>();
            static_mods.AddRange(convertParamToModStringArray(param_nuxl_static_c_terminal_mod, "C-TERM"));
            static_mods.AddRange(convertParamToModStringArray(param_nuxl_static_n_terminal_mod, "N-TERM"));
            static_mods.AddRange(convertParamToModStringArray(param_nuxl_static_mod_1, "RESIDUE"));
            static_mods.AddRange(convertParamToModStringArray(param_nuxl_static_mod_2, "RESIDUE"));
            static_mods.AddRange(convertParamToModStringArray(param_nuxl_static_mod_3, "RESIDUE"));
            static_mods.AddRange(convertParamToModStringArray(param_nuxl_static_mod_4, "RESIDUE"));
            static_mods.AddRange(convertParamToModStringArray(param_nuxl_static_mod_5, "RESIDUE"));
            static_mods.AddRange(convertParamToModStringArray(param_nuxl_static_mod_6, "RESIDUE"));
            variable_mods.AddRange(convertParamToModStringArray(param_nuxl_dynamic_c_terminal_mod_1, "C-TERM"));
            variable_mods.AddRange(convertParamToModStringArray(param_nuxl_dynamic_c_terminal_mod_2, "C-TERM"));
            variable_mods.AddRange(convertParamToModStringArray(param_nuxl_dynamic_c_terminal_mod_3, "C-TERM"));
            variable_mods.AddRange(convertParamToModStringArray(param_nuxl_dynamic_n_terminal_mod_1, "N-TERM"));
            variable_mods.AddRange(convertParamToModStringArray(param_nuxl_dynamic_n_terminal_mod_2, "N-TERM"));
            variable_mods.AddRange(convertParamToModStringArray(param_nuxl_dynamic_n_terminal_mod_3, "N-TERM"));
            variable_mods.AddRange(convertParamToModStringArray(param_nuxl_dynamic_mod_1, "RESIDUE"));
            variable_mods.AddRange(convertParamToModStringArray(param_nuxl_dynamic_mod_2, "RESIDUE"));
            variable_mods.AddRange(convertParamToModStringArray(param_nuxl_dynamic_mod_3, "RESIDUE"));
            variable_mods.AddRange(convertParamToModStringArray(param_nuxl_dynamic_mod_4, "RESIDUE"));
            variable_mods.AddRange(convertParamToModStringArray(param_nuxl_dynamic_mod_5, "RESIDUE"));
            variable_mods.AddRange(convertParamToModStringArray(param_nuxl_dynamic_mod_6, "RESIDUE"));
            OpenMSCommons.WriteItemListToINI(variable_mods.ToArray(), nuxl_ini_file, "variable", true);
            OpenMSCommons.WriteItemListToINI(static_mods.ToArray(), nuxl_ini_file, "fixed", true);
            OpenMSCommons.WriteNestedParamToINI(nuxl_ini_file, new Triplet("modifications", "variable_max_per_peptide", param_nuxl_num_dynamic_mods.ToString()));

            OpenMSCommons.WriteNestedParamToINI(nuxl_ini_file, new Triplet("peptide", "min_size", param_nuxl_peptide_length_min.ToString()));
            OpenMSCommons.WriteNestedParamToINI(nuxl_ini_file, new Triplet("peptide", "max_size", param_nuxl_peptide_length_max.ToString()));
            OpenMSCommons.WriteNestedParamToINI(nuxl_ini_file, new Triplet("peptide", "missed_cleavages", param_nuxl_missed_cleavages.ToString()));
            OpenMSCommons.WriteNestedParamToINI(nuxl_ini_file, new Triplet("peptide", "enzyme", param_nuxl_enzyme.Value));

            OpenMSCommons.WriteNestedParamToINI(nuxl_ini_file, new Triplet("RNPxl", "presets", param_cross_linking_presets));
            OpenMSCommons.WriteNestedParamToINI(nuxl_ini_file, new Triplet("RNPxl", "length", param_cross_linking_length));
            OpenMSCommons.WriteNestedParamToINI(nuxl_ini_file, new Triplet("RNPxl", "sequence", param_cross_linking_sequence));
            OpenMSCommons.WriteNestedParamToINI(nuxl_ini_file, new Triplet("RNPxl", "CysteineAdduct", param_cross_linking_cysteine_adduct.ToString().ToLower()));

            OpenMSCommons.WriteNestedParamToINI(nuxl_ini_file, new Triplet("RNPxl", "decoys", "true"));

            if (param_nuxl_scoring == "include fragment adducts")
            {
                OpenMSCommons.WriteNestedParamToINI(nuxl_ini_file, new Triplet("RNPxl", "scoring", "slow"));
            }
            else
            {
                OpenMSCommons.WriteNestedParamToINI(nuxl_ini_file, new Triplet("RNPxl", "scoring", "fast"));
            }

            OpenMSCommons.WriteNestedParamToINI(nuxl_ini_file, new Triplet("RNPxl", "can_cross_link", param_cross_linking_can_xls));

            OpenMSCommons.WriteNestedParamToINI(nuxl_ini_file, new Triplet("report", "top_hits", "1")); // TODO: change?
            OpenMSCommons.WriteNestedParamToINI(nuxl_ini_file, new Triplet("report", "peptideFDR", 0.01)); // TODO: change?

            List<string> xl_FDR_options = new List<string>();
            xl_FDR_options.Add("0.01");
            xl_FDR_options.Add("0.1");
            xl_FDR_options.Add("1.0");
            OpenMSCommons.WriteItemListToINI(xl_FDR_options.ToArray(), nuxl_ini_file, "report", "xlFDR", true);

            List<string> filter_options = new List<string>();
            if (param_general_autotune.Value) { filter_options.Add("autotune"); }
            if (param_general_id_filter.Value) { filter_options.Add("idfilter"); }
            if (param_general_discard_bad_pcmass.Value) { filter_options.Add("filter_pc_mass_error"); }
            if (param_general_filter_bad_partial_loss_scores.Value) { filter_options.Add("filter_bad_partial_loss_scores"); }
            OpenMSCommons.WriteItemListToINI(filter_options.ToArray(), nuxl_ini_file, "filter", true);

            var openms_param_names = new List<string>()
            {
                "mapping",
                "modifications",
                "target_nucleotides",
                "fragment_adducts"
            };
            var pd_parameters = new List<StringParameter>()
            {
                param_cross_linking_mapping,
                param_cross_linking_modifications,
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
                    parts = parts.Where(x => !string.IsNullOrEmpty(x)).ToArray(); // remove empty string TODO due to multiple spaces
                    OpenMSCommons.WriteItemListToINI(parts, nuxl_ini_file, oms_p, true);
                }
            }
            catch (Exception)
            {
                string err = "Error while parsing string list parameter. Valid format for string lists: '[a b c ...]'";
                SendAndLogErrorMessage(err);
                throw;
            }

            SendAndLogMessage("Starting main NuXL search for file [{0}]", uv_mzml_filename);
            OpenMSCommons.RunTOPPTool(exec_path, nuxl_ini_file, NodeScratchDirectory, m_node_delegates);

            m_current_step += 1;
            ReportTotalProgress((double)m_current_step / m_num_steps);

            return idxml_filename;
        }


        private void ParseIdXMLResults(string idXML_file)
        {
            if (EntityDataService.ContainsEntity<NuXLItem>() == false)
            {
                EntityDataService.RegisterEntity<NuXLItem>(ProcessingNodeNumber);
            }

            var nuxl_items = OpenMSCommons.parseIdXML(idXML_file);

            foreach (var x in nuxl_items)
            {
                x.WorkflowID = WorkflowID;
                x.Id = EntityDataService.NextId<NuXLItem>();
            }

            EntityDataService.InsertItems(nuxl_items);

            // establish connection between results and spectra
            connectNuXLItemWithSpectra();

            // add CV column
            AddCompVoltageToCsm();
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

        protected override void OnSpectraSentForSearch(IProcessingNode sender, MassSpectrumCollection spectra)
        {
            //throw new NotImplementedException();
        }

        protected override void OnAllSpectraSentForSearch()
        {
            //throw new NotImplementedException();
        }

        private void connectNuXLItemWithSpectra()
        {
            // TODO: add nativeID column to csv

            // TODO: map between nativeID from NuXLItem (from csv) and from spectra


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
                // TODO: use m.SpectrumID() to match between 
                string rt_str = String.Format("{0:0.0}", m.RetentionTime);
                string mz_str = String.Format("{0:0.0000}", m.MassOverCharge);
                if (rt_mz_to_nuxl_id.ContainsKey(rt_str))
                {
                    Dictionary<string, NuXLItem> mz_dict = rt_mz_to_nuxl_id[rt_str];
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
                        var idString = string.Concat(m.WorkflowID, "§", m.SpectrumID, "§", r.fragment_annotation, "§REPORT_GUID=", EntityDataService.ReportFile.ReportGuid);

                        // use r.WorkflowID, r.Id to specify which NuXLItem to update
                        updates.Add(Tuple.Create(new[] { (object)r.WorkflowID, (object)r.Id }, new object[] { idString }));
                    }
                }
            }

            // Register connections
            EntityDataService.ConnectItems(nuxl_to_spectrum_connections);
        }

        private void AddCompVoltageToCsm()
        {
            // check if a comp voltage entry is existing
            var compensationVoltageProperty = EntityDataService
                .GetProperties<MSnSpectrumInfo>(dataPurpose: EntityDataPurpose.CompensationVoltage).FirstOrDefault();

            if (compensationVoltageProperty != null)
            {
                //create column
                var compVoltageProperty = PropertyAccessorFactory.CreateDynamicPropertyAccessor<NuXLItem, double?>(
                    new PropertyDescription
                    {
                        DisplayName = "Comp. Voltage",
                        Description = "The compensation voltage used for identification",
                        FormatString = "0.00",
                        DataPurpose = EntityDataPurpose.CompensationVoltage
                    });

                compVoltageProperty.GridDisplayOptions.TextHAlign = GridCellHAlign.Right;
                compVoltageProperty.GridDisplayOptions.VisiblePosition = 505;
                compVoltageProperty.GridDisplayOptions.DataVisibility = GridVisibility.Visible;
                compVoltageProperty.PlottingOptions = new PlottingOptions
                {
                    PlotType = PlotType.NumericAndOrdinal | PlotType.Venn
                };

                EntityDataService.RegisterProperty(ProcessingNodeNumber, compVoltageProperty);

                // fill column with data
                var valueCache = new EntityPropertyValues<NuXLItem>(EntityDataService, compVoltageProperty);

                var rangeGenerator =
                    new EntityReaderRangeFactory(EntityDataService.GetEntityItemCount<NuXLItem>(), 10000);

                while (rangeGenerator.GetNextRange(out Tuple<int, int> range))
                {
                    foreach (var csmsWithSpectra in EntityDataService.CreateEntityItemReader().ReadAllFlatWithConnectionProperties<NuXLItem, MSnSpectrumInfo>(
                        range: range))
                    {
                        // check the connection property if it is an diagnostic spectrum
                        var csm = csmsWithSpectra.Item1;

                        foreach (var spectrum in csmsWithSpectra.Item2)
                        {
                            //var spectrum = spectrumItem.EntityItem;
                            // get the CV
                            var cv = spectrum.EntityItem.AdditionalValues.GetValue(compensationVoltageProperty.Name);

                            try
                            {
                                var compensationVoltage = Convert.ToDouble(cv);
                                valueCache.Add(csm, compensationVoltage);  // save the info
                            }
                            catch
                            {
                            }
                        }
                    }

                    EntityDataService.UpdateItems(valueCache);
                    valueCache.Clear();
                }
            }
        }

    }
}

