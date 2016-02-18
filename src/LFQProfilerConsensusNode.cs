using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Xml;
using System.Web.UI;
using System.Xml.Linq;
using System.Text;

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

    [ProcessingNode("1DB9D65C-EFDD-4136-8038-A555A39459FD",
        DisplayName = "LFQProfiler",
        Description = "Quantifies peptides and proteins based on results from the LFQProfiler FF processing node.",
        Category = ReportingNodeCategories.Quantification,
        MainVersion = 1,
        MinorVersion = 49,
        WorkflowType = WorkflowTypeNames.Consensus)]

    [ConnectionPoint(
        "IncomingOpenMSFeatures",
        ConnectionDirection = ConnectionDirection.Incoming,
        ConnectionMultiplicity = ConnectionMultiplicity.Single,
        ConnectionMode = ConnectionMode.AutomaticToAllPossibleParents,
        ConnectionRequirement = ConnectionRequirement.Optional,
        ConnectedParentNodeConstraint = ConnectedParentConstraint.OnlyToGeneratorsOfRequestedData)]

    [ConnectionPointDataContract(
        "IncomingOpenMSFeatures",
        ProteomicsDataTypes.Psms)]

    [ConnectionPoint(
        "Outgoing",
        ConnectionDirection = ConnectionDirection.Outgoing,
        ConnectionMultiplicity = ConnectionMultiplicity.Multiple,
        ConnectionMode = ConnectionMode.Manual,
        ConnectionRequirement = ConnectionRequirement.Optional,
        ConnectionDisplayName = ReportingNodeCategories.PeptideValidation)]

    [ConnectionPointDataContract(
        "Outgoing",
        ProteomicsDataTypes.Psms,
        DataTypeAttributes = new[] { "Quantified" })]

    [ProcessingNodeConstraints(UsageConstraint = UsageConstraint.OnlyOncePerWorkflow)]

    # endregion

    public class LFQProfilerConsensusNode
        : ReportProcessingNode
    {
        # region Parameters

        [BooleanParameter(Category = "1. Feature linking",
            DisplayName = "Perform RT alignment",
            Description = "This parameter specifies whether map alignment (RT transformation) should be performed before linking.",
            DefaultValue = "true",
            Position = 10)]
        public BooleanParameter param_perform_map_alignment;

        [IntegerParameter(Category = "1. Feature linking",
            DisplayName = "Peptide min #runs",
            Description = "If map alignment is perormed: minimum number of runs a peptide must occur in to be used for the alignment. Unless you have very few runs or identifications, increase this value to focus on more informative peptides.",
            DefaultValue = "2",
            MinimumValue = "2",
            Position = 20)]
        public IntegerParameter param_min_run_occur;

        [DoubleParameter(
            Category = "1. Feature linking",
            DisplayName = "Max. RT difference [min]",
            Description = "This parameter specifies the maximum allowed retention time difference for features to be linked together.",
            DefaultValue = "1.0",
            MinimumValue = "0",
            Position = 30)]
        public DoubleParameter param_rt_threshold;

        [MassToleranceParameter(
            Category = "1. Feature linking",
            DisplayName = "Max. m/z difference",
            Subset = "ppm",
            Description = "This parameter specifies the maximum allowed m/z difference for features to be linked together.",
            DefaultValue = "10 ppm",
            IntendedPurpose = ParameterPurpose.MassTolerance,
            Position = 40)]
        public MassToleranceParameter param_mz_threshold;

        [DoubleParameter(
            Category = "2. ID mapping",
            DisplayName = "Max. RT difference [min]",
            Description = "This parameter specifies the maximum allowed retention time difference for IDs to be mapped onto features.",
            DefaultValue = "0.33",
            MinimumValue = "0",
            Position = 50)]
        public DoubleParameter param_id_mapping_rt_threshold;

        [MassToleranceParameter(
            Category = "2. ID mapping",
            DisplayName = "Max. m/z difference",
            Subset = "ppm",
            Description = "This parameter specifies the maximum allowed m/z difference for IDs to be mapped onto features.",
            DefaultValue = "10 ppm",
            IntendedPurpose = ParameterPurpose.MassTolerance,
            Position = 60)]
        public MassToleranceParameter param_id_mapping_mz_threshold;

        [DoubleParameter(
            Category = "2. ID mapping",
            DisplayName = "q-Value threshold",
            Description = "PSMs with a q-Value larger than this threshold will not be used for ID mapping.",
            DefaultValue = "0.01",
            MinimumValue = "0",
            MaximumValue = "1",
            Position = 70)]
        public DoubleParameter param_q_value_threshold;

        [FastaFileParameter(Category = "2. ID mapping",
            DisplayName = "Protein database",
            Description = "The sequence database to be used for (re-)indexing protein hits",
            IntendedPurpose = ParameterPurpose.SequenceDatabase,
            ValueRequired = true,
            Position = 80)]
        public FastaFileParameter param_fasta_db;

        // not needed because specificity = none

        //[StringSelectionParameter(Category = "2. ID mapping",
        //    DisplayName = "Enzyme",
        //    Description = "The enzyme used for cleaving the proteins",
        //    DefaultValue = "Trypsin",
        //    SelectionValues = new string[] { "Trypsin", "Asp-N", "CNBr", "Formic_acid", "Chymotrypsin", "Lys-C", "Asp-N_ambic", "Arg-C", "V8-DE", "glutamyl endopeptidase", "leukocyte elastase", "no cleavage", "PepsinA", "Lys-C/P", "2-iodobenzoate", "prolineendopeptidase", "V8-E", "TrypChymo", "unspecific cleavage", "Trypsin/P" },
        //    Position = 90)]
        //public SimpleSelectionParameter<string> param_enzyme;

        [StringSelectionParameter(Category = "2. ID mapping",
            DisplayName = "m/z reference",
            Description = "Source of m/z values for peptide identifications. If 'precursor', the precursor-m/z as determined by the instrument is used. If 'peptide', masses are computed from the sequences of peptide hits.",
            DefaultValue = "peptide",
            SelectionValues = new string[] { "precursor", "peptide" },
            Position = 100)]
        public SimpleSelectionParameter<string> param_mz_reference;

        //[StringSelectionParameter(Category = "2. ID mapping",
        //    DisplayName = "ID conflict resolution",
        //    Description = "Resolve conflicts with different peptide identifications matching to the same consensus feature before protein quantification? 'best_score' always chooses the hit with the best score; 'upvote_identical' also takes the number of identical hits into account and multiplies the scores of identical sequences before choosing.",
        //    DefaultValue = "none",
        //    SelectionValues = new string[] { "none", "best_score", "upvote_identical" },
        //    Position = 110,
        //    IsAdvanced = true)]
        //public SimpleSelectionParameter<string> param_id_conflict_resolution;

        [StringSelectionParameter(Category = "3. Intensity normalization",
            DisplayName = "Method",
            Description = "Normalization method for intensity normalization on feature level",
            DefaultValue = "median",
            SelectionValues = new string[] { "median", "quantile", "none" },
            Position = 120)]
        public SimpleSelectionParameter<string> param_normalization_method;

        [StringParameter(Category = "3. Intensity normalization",
            DisplayName = "Accession filter",
            Description = "For median normalization: compute normalization coefficients based only on features with a protein accession matching this regular expression (e.g., your housekeeping proteins). If empty, all features (including unidentified ones) pass this filter. If set to \".\", all identified features are used. No effect if quantile normalization is used.",
            DefaultValue = "",
            Position = 130)]
        public StringParameter param_normalization_acc_filter;

        [StringParameter(Category = "3. Intensity normalization",
            DisplayName = "Description filter",
            Description = "For median normalization: compute normalization coefficients based only on features with a protein description matching this regular expression (e.g., your housekeeping proteins). If empty, all features (including unidentified ones) pass this filter. If set to \".\", all identified features are used. No effect if quantile normalization is used.",
            DefaultValue = "",
            Position = 140)]
        public StringParameter param_normalization_desc_filter;

        [DoubleParameter(
            Category = "4. Protein quantification",
            DisplayName = "Protein-level FDR",
            Description = "Protein-level false discovery rate threshold. Peptides corresponding to proteins with a q-value larger than this threshold are filtered out before running the actual protein quantification step.",
            DefaultValue = "0.05",
            MinimumValue = "0",
            MaximumValue = "1",
            Position = 141)]
        public DoubleParameter param_protein_q_value_threshold;

        [StringSelectionParameter(Category = "4. Protein quantification",
            DisplayName = "Use peptides",
            Description = "Specify which peptides should be used for quantification: only unique peptides, unique + indistinguishable proteins, or unique + indistinguishable + other shared peptides (using a greedy resolution which is similar to selecting the razor peptides)",
            DefaultValue = "indistinguishable",
            SelectionValues = new string[] { "unique", "indistinguishable", "greedy" },
            Position = 150)]
        public SimpleSelectionParameter<string> param_protein_quant_mode;

        [DoubleParameter(
            Category = "4. Protein quantification",
            DisplayName = "Fido pre-filtering PEP threshold",
            Description = "Filter out PSMs with posterior error probability (PEP) exceeding this threshold before running protein inference using FidoAdapter. Thresholds < 1 can significantly reduce running time but the pre-filtering has an impact on the result of protein-level FDR filtering: the smaller this threshold, the more optimistic the protein-level FDR q-values become, i.e., the more likely that the protein-level FDR is underestimated.",
            DefaultValue = "0.3",
            MinimumValue = "0",
            MaximumValue = "1",
            IsAdvanced = true,
            Position = 160)]
        public DoubleParameter param_fido_prefiltering_threshold;

        [IntegerParameter(Category = "4. Protein quantification",
            DisplayName = "Top",
            Description = "Calculate protein abundance from this number of peptides (most abundant first; '0' for all)",
            DefaultValue = "0",
            MinimumValue = "0",
            Position = 170)]
        public IntegerParameter param_top;

        [StringSelectionParameter(Category = "4. Protein quantification",
            DisplayName = "Averaging",
            Description = "Averaging method used to compute protein abundances from peptide abundances",
            DefaultValue = "sum",
            SelectionValues = new string[] { "mean", "weighted_mean", "median", "sum" },
            Position = 180)]
        public SimpleSelectionParameter<string> param_averaging;

        [BooleanParameter(Category = "4. Protein quantification",
            DisplayName = "Include all",
            Description = "Include results for proteins with fewer peptides than indicated by 'top' (no effect if 'top' is 0 or 1)",
            DefaultValue = "false",
            Position = 190)]
        public BooleanParameter param_include_all;

        [BooleanParameter(Category = "4. Protein quantification",
            DisplayName = "Filter charge",
            Description = "Distinguish between charge states of a peptide. For peptides, abundances will be reported separately for each charge; for proteins, abundances will be computed based only on the most prevalent charge of each peptide. Otherwise, abundances are summed over all charge states.",
            DefaultValue = "false",
            Position = 200)]
        public BooleanParameter param_filter_charge;

        [BooleanParameter(Category = "4. Protein quantification",
            DisplayName = "Fix peptides",
            Description = "Use the same peptides for protein quantification across all samples. With 'top 0', all peptides that occur in every sample are considered. Otherwise ('top N'), the N peptides that occur in the most samples (independently of each other) are selected, breaking ties by total abundance (there is no guarantee that the best co-ocurring peptides are chosen!).",
            DefaultValue = "false",
            Position = 210)]
        public BooleanParameter param_fix_peptides;

        [IntegerParameter(Category = "5. General",
        DisplayName = "CPU Cores",
        Description = "How many CPU cores should at most be used by the algorithms.",
        DefaultValue = "1",
        MinimumValue = "1",
        Position = 220)]
        public IntegerParameter param_num_threads;

        # endregion

        private string m_openms_dir;
        private string m_openms_fasta_file;
        private List<string> m_raw_files;
        private NodeDelegates m_node_delegates;
        private int m_current_step;
        private int m_num_steps;
        private int m_num_files;

        /// <summary>
        /// Called when the parent node finished data processing.
        /// </summary>
        /// <param name="sender">The parent node.</param>
        /// <param name="eventArgs">The result event arguments.</param>
        public override void OnParentNodeFinished(IProcessingNode sender, ResultsArguments eventArgs)
        {
            // Set up approximate progress bar
            m_num_steps = 10;
            m_current_step = 0;

            // OpenMS binary directory
            m_openms_dir = Path.Combine(ServerConfiguration.ToolsDirectory, "OpenMS-2.0/");

            // FASTA file (exported from PD, decoys will be added)
            m_openms_fasta_file = "";

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

            // Extract input filenames from MSF file
            List<string> featurexml_files_orig;
            ReadInputFilenames(out featurexml_files_orig);
            var idxml_files = new List<string>(from f in m_raw_files select Path.Combine(NodeScratchDirectory, Path.GetFileNameWithoutExtension(f) + ".idXML"));
            var featurexml_files_idmapped = new List<string>();
            
            for (int i = 0; i < m_raw_files.Count; ++i)
            {
                var raw_file = m_raw_files[i];
                var idxml_file = idxml_files[i];

                // Export filtered PSMs to idXML
                ExportPSMsToIdXML(idxml_file, false, raw_file);

                // Run PeptideIndexer in order to get peptide <-> protein associations
                var indexed_idxml = Path.Combine(NodeScratchDirectory, Path.GetFileNameWithoutExtension(idxml_file) + "_indexed.idXML");
                RunPeptideIndexer(idxml_file, indexed_idxml);

                // Map IDs to features
                var idmapped_featurexml = Path.Combine(NodeScratchDirectory, Path.GetFileNameWithoutExtension(idxml_file) + "_idmapped.featureXML");
                featurexml_files_idmapped.Add(idmapped_featurexml);
                RunIDMapper(featurexml_files_orig[i], indexed_idxml, idmapped_featurexml);
            }

            // Run alignment and linking
            string consensus_xml_file = AlignAndLink(featurexml_files_idmapped);

            //// Resolve conflicting IDs within consensus features before protein quantification (if advanced parameter is not set to 'none')
            //var conflictresolved_idmapped_consensusxml = RunIDConflictResolver(consensus_xml_file);

            // Normalize intensities
            var normalized_consensus_xml_file = RunConsensusMapNormalizer(consensus_xml_file);

            // Dictionary {feature ID -> consensusXML element} with original RTs
            Dictionary<string, XmlElement> consensus_dict;

            // ConsensusXML file with original RTs
            string final_consensus_xml_file_orig_rt;

            // Create consensusXML with original RTs (from featurexml_files_orig), keep mapping in consensus_dict
            BuildConsensusXMLWithOrigRTs(normalized_consensus_xml_file, featurexml_files_orig, out consensus_dict, out final_consensus_xml_file_orig_rt);

            // Set up consensus feature table ("Quantified features") and fill it
            var feature_table_column_names = SetupConsensusFeaturesTable();
            PopulateConsensusFeaturesTable(consensus_dict, final_consensus_xml_file_orig_rt, feature_table_column_names);

            // Export all PSMs for Fido
            var combined_idxml_filename = Path.Combine(NodeScratchDirectory, "all_psms.idXML");
            ExportPSMsToIdXML(combined_idxml_filename, true);

            // Run PeptideIndexer in order to get peptide <-> protein associations
            var indexed_idxml_file = Path.Combine(NodeScratchDirectory, "all_psms_indexed.idXML");
            RunPeptideIndexer(combined_idxml_filename, indexed_idxml_file);

            var pq_input_idxml = indexed_idxml_file;
            if (param_protein_quant_mode.Value != "unique")
            {
                // Run FidoAdapter
                var fido_output_file = RunFidoAdapter(indexed_idxml_file);

                // Filter FidoAdapter output by protein-level FDR
                pq_input_idxml = FilterFidoOutputByProteinLevelFDR(fido_output_file);
            }

            // Run ProteinQuantifier
            RunProteinQuantifier(final_consensus_xml_file_orig_rt, pq_input_idxml);

            // Finished!
            FireProcessingFinishedEvent(new SingleResultsArguments(new[] { ProteomicsDataTypes.Psms }, this));
        }

        /// <summary>
        /// Filter FidoAdapter output by protein-level FDR
        /// </summary>
        private string FilterFidoOutputByProteinLevelFDR(string input_file)
        {
            if (param_protein_q_value_threshold.Value == 1.0)
            {
                return input_file;
            }

            // ================================ FalseDiscoveryRate =================================

            string exec_path = Path.Combine(m_openms_dir, @"bin/FalseDiscoveryRate.exe");
            string ini_path = Path.Combine(NodeScratchDirectory, @"FalseDiscoveryRate.ini");
            string fdr_output_file = Path.Combine(NodeScratchDirectory, "fido_results_fdr_output.idXML");

            OpenMSCommons.CreateDefaultINI(exec_path, ini_path, NodeScratchDirectory, m_node_delegates);
            Dictionary<string, string> fdr_parameters = new Dictionary<string, string> {
                        {"in", input_file},
                        {"out", fdr_output_file},
                        {"proteins_only", "true"},
                        {"threads", param_num_threads.ToString()}
            };
            OpenMSCommons.WriteParamsToINI(ini_path, fdr_parameters);

            SendAndLogMessage("Starting FalseDiscoveryRate");
            OpenMSCommons.RunTOPPTool(exec_path, ini_path, NodeScratchDirectory, m_node_delegates);

            //m_current_step += 1;
            //ReportTotalProgress((double)m_current_step / m_num_steps);

            // ===================================== IDFilter =======================================

            exec_path = Path.Combine(m_openms_dir, @"bin/IDFilter.exe");
            ini_path = Path.Combine(NodeScratchDirectory, @"IDFilter.ini");
            string idfilter_output_file = Path.Combine(NodeScratchDirectory, "fido_results_idfilter_output.idXML");

            OpenMSCommons.CreateDefaultINI(exec_path, ini_path, NodeScratchDirectory, m_node_delegates);
            Dictionary<string, string> idfilter_parameters = new Dictionary<string, string> {
                        {"in", fdr_output_file},
                        {"out", idfilter_output_file},
                        {"delete_unreferenced_peptide_hits", "true"},
                        {"threads", param_num_threads.ToString()}
            };
            OpenMSCommons.WriteParamsToINI(ini_path, idfilter_parameters);
            OpenMSCommons.WriteNestedParamToINI(ini_path, new Triplet("score", "prot", param_protein_q_value_threshold));

            SendAndLogMessage("Starting IDFilter");
            OpenMSCommons.RunTOPPTool(exec_path, ini_path, NodeScratchDirectory, m_node_delegates);

            //m_current_step += 1;
            //ReportTotalProgress((double)m_current_step / m_num_steps);

            return idfilter_output_file;
        }


        ///// <summary>
        ///// Run IDConflictResolver on the input_file (consensusXML) and write results to output_file (consensusXML)
        ///// </summary>
        //private string RunIDConflictResolver(string input_file)
        //{
        //    if (param_id_conflict_resolution.Value == "none")
        //    {
        //        return input_file;
        //    }

        //    string output_file = Path.Combine(NodeScratchDirectory, "idmapped_resolved.consensusXML");
        //    var exec_path = Path.Combine(m_openms_dir, @"bin/IDConflictResolver.exe");
        //    var ini_path = Path.Combine(NodeScratchDirectory, @"IDConflictResolver.ini");

        //    Dictionary<string, string> ini_params = new Dictionary<string, string> {
        //                    {"in", input_file},
        //                    {"out", output_file},
        //                    {"method", param_id_conflict_resolution.Value},
        //                    {"threads", param_num_threads.ToString()}};
        //    OpenMSCommons.CreateDefaultINI(exec_path, ini_path, NodeScratchDirectory, m_node_delegates);
        //    OpenMSCommons.WriteParamsToINI(ini_path, ini_params);

        //    SendAndLogMessage("Starting IDConflictResolver");
        //    OpenMSCommons.RunTOPPTool(exec_path, ini_path, NodeScratchDirectory, m_node_delegates);
            
        //    // TODO: fix progress bars!
        //    //m_current_step += 1;
        //    //ReportTotalProgress((double)m_current_step / m_num_steps);

        //    return output_file;
        //}

        /// <summary>
        /// Run ProteinQuantifier on a given consensusXML file, using Fido results as protein inference input. Parse results into PD tables.
        /// </summary>
        private void RunProteinQuantifier(string consensusxml_file, string fido_idxml_file)
        {
            var exec_path = Path.Combine(m_openms_dir, @"bin/ProteinQuantifier.exe");
            var ini_path = Path.Combine(NodeScratchDirectory, @"ProteinQuantifier.ini");
            var pep_output_file = Path.Combine(NodeScratchDirectory, "pq_peptides.csv");
            var prot_output_file = Path.Combine(NodeScratchDirectory, "pq_proteins.csv");

            Dictionary<string, string> ini_params = new Dictionary<string, string> {
                            {"in", consensusxml_file},
                            {"out", prot_output_file},
                            {"peptide_out", pep_output_file},
                            {"top", param_top.ToString()},
                            {"average", param_averaging.Value},
                            {"include_all", param_include_all.ToString().ToLower()},
                            {"filter_charge", param_filter_charge.ToString().ToLower()},
                            {"fix_peptides", param_fix_peptides.ToString().ToLower()},
                            {"threads", param_num_threads.ToString()}};
            if (param_protein_quant_mode.Value != "unique")
            {
                ini_params["protein_groups"] = fido_idxml_file;
            }
            OpenMSCommons.CreateDefaultINI(exec_path, ini_path, NodeScratchDirectory, m_node_delegates);
            OpenMSCommons.WriteParamsToINI(ini_path, ini_params);

            SendAndLogMessage("Starting ProteinQuantifier");
            OpenMSCommons.RunTOPPTool(exec_path, ini_path, NodeScratchDirectory, m_node_delegates);
            m_current_step += 1;
            ReportTotalProgress((double)m_current_step / m_num_steps);

            ParsePQPeptides(pep_output_file);
            ParsePQProteins(prot_output_file);
        }

        /// <summary>
        /// Run protein inference using FidoAdapter on given idXML file
        /// </summary>
        private string RunFidoAdapter(string idxml_file)
        {
            var exec_path = Path.Combine(m_openms_dir, @"bin/FidoAdapter.exe");
            var ini_path = Path.Combine(NodeScratchDirectory, @"FidoAdapter.ini");
            var output_file = Path.Combine(NodeScratchDirectory, "fido_results.idXML");


            Dictionary<string, string> ini_params = new Dictionary<string, string> {
                        {"in", idxml_file},
                        {"out", output_file},
                        {"greedy_group_resolution", param_protein_quant_mode.Value == "greedy" ? "true" : "false"},
                        {"threads", param_num_threads.ToString()}};
            OpenMSCommons.CreateDefaultINI(exec_path, ini_path, NodeScratchDirectory, m_node_delegates);
            OpenMSCommons.WriteParamsToINI(ini_path, ini_params);

            SendAndLogMessage("Starting FidoAdapter");
            OpenMSCommons.RunTOPPTool(exec_path, ini_path, NodeScratchDirectory, m_node_delegates);
            m_current_step += 1;
            ReportTotalProgress((double)m_current_step / m_num_steps);
            
            return output_file;
        }

        /// <summary>
        /// Fill "Quantified features" table using final consensusXML file and a dictionary to get the original RTs
        /// </summary>
        private void PopulateConsensusFeaturesTable(Dictionary<string, XmlElement> consensus_dict, string cn_output_file, List<string> feature_table_column_names)
        {
            EntityDataService.RegisterEntityConnection<TargetPeptideSpectrumMatch, ConsensusFeatureEntity>(ProcessingNodeNumber);
            var psms_with_quantification = new List<Tuple<TargetPeptideSpectrumMatch, ConsensusFeatureEntity>>();

            var doc = new XmlDocument();
            doc.Load(cn_output_file);
            XmlNodeList consensus_elements = doc.GetElementsByTagName("consensusElement");

            var new_consensus_items = new List<ConsensusFeatureEntity>();
            Int32 idCounter = 1;

            var eds_reader = EntityDataService.CreateEntityItemReader();

            var scoreProperties = EntityDataService
                    .GetProperties<TargetPeptideSpectrumMatch>(ScoreSemanticTerms.Score)
                    .Where(item => item.ValueType == typeof(double)
                            || item.ValueType == typeof(double?))
                    .ToArray();
            PropertyAccessor<TargetPeptideSpectrumMatch> pep_score = null;
            foreach (var sp in scoreProperties)
            {
                var score_name = sp.Name;

                if (score_name.Contains("PEP"))
                {
                    pep_score = sp;
                }
            }

            foreach (XmlElement ce in consensus_elements)
            {
                var cons_id = ce.Attributes["id"].Value;
                double quality = Convert.ToDouble(ce.Attributes["quality"].Value);
                int charge = Convert.ToInt32(ce.Attributes["charge"].Value);

                XmlNode centroid_node = ce.SelectSingleNode("centroid");
                double mz = Convert.ToDouble(centroid_node.Attributes["mz"].Value);
                // TODO: Centroid RT is from aligned RTs (if alignment was performed)
                // Maybe recompute value from individual subfeatures? Or also replace centroid RTs when writing orig_rt.consensusXML...
                double rt = Convert.ToDouble(centroid_node.Attributes["rt"].Value) / 60.0; //changed to minute!

                var grouped_elements = ce.SelectSingleNode("groupedElementList");

                ConsensusFeatureEntity new_consensus_item = new ConsensusFeatureEntity()
                {
                    WorkflowID = WorkflowID,
                    Id = idCounter++
                };

                new_consensus_item.SetValue("Sequence", "");
                new_consensus_item.SetValue("Accessions", "");
                new_consensus_item.SetValue("Charge", charge);
                new_consensus_item.SetValue("mz", mz);
                new_consensus_item.SetValue("RT", rt);

                var peptide_ids = ce.GetElementsByTagName("PeptideIdentification");

                Double best_pep_score = 1.1;
                foreach (XmlNode peptide_id in peptide_ids)
                {
                    var peptide_hit = peptide_id.SelectSingleNode("PeptideHit");
                    Int32 pd_peptide_id = 0;
                    Int32 workflow_id = 0;
                    var up_1 = peptide_hit.SelectNodes("userParam");  // keep for backward compatibility
                    var up_2 = peptide_hit.SelectNodes("UserParam");
                    List<XmlNode> user_params = new List<XmlNode>();
                    user_params.AddRange(up_1.Cast<XmlNode>());
                    user_params.AddRange(up_2.Cast<XmlNode>());

                    bool decoy = false;

                    foreach (XmlNode up in user_params)
                    {
                        if (up.Attributes["name"].Value == "pd_peptide_id")
                        {
                            var val = up.Attributes["value"].Value;

                            //ignore decoy peptides
                            if (val.Length >= 6 && val.Substring(0, 6) == "decoy_")
                            {
                                decoy = true;
                                val = val.Substring(6);
                            }
                            else
                            {
                                var parts = val.Split(new[] { ';' });
                                if (parts.Length < 2)
                                {
                                    SendAndLogErrorMessage("UserParam pd_peptide_id has wrong format.");
                                }
                                workflow_id = Convert.ToInt32(parts[0]);
                                pd_peptide_id = Convert.ToInt32(parts[1]);
                            }
                            break;
                        }
                    }
                    if (decoy)
                    {
                        continue;
                    }

                    var psm = eds_reader.Read<TargetPeptideSpectrumMatch>(new object[] { workflow_id, pd_peptide_id });

                    psms_with_quantification.Add(Tuple.Create(psm, new_consensus_item));

                    var current_pep_score = Convert.ToDouble(pep_score.GetValue(psm));
                    if (current_pep_score < best_pep_score)
                    {
                        new_consensus_item.SetValue("Sequence", peptide_hit.Attributes["sequence"].Value);
                        new_consensus_item.SetValue("Accessions", psm.ParentProteinAccessions);
                        new_consensus_item.SetValue("Descriptions", psm.ParentProteinDescriptions);
                        best_pep_score = current_pep_score;
                    }
                }

                foreach (XmlNode element in grouped_elements.SelectNodes("element"))
                {
                    var feat_id = element.Attributes["id"].Value;
                    var feat_intensity = Convert.ToDouble(element.Attributes["it"].Value);
                    var feat_mz = Convert.ToDouble(element.Attributes["mz"].Value);
                    var feat_transf_rt = Convert.ToDouble(element.Attributes["rt"].Value) / 60.0;
                    var feat_orig_rt = Convert.ToDouble(consensus_dict[feat_id].Attributes["rt"].Value) / 60.0;
                    var feat_map = Convert.ToInt32(element.Attributes["map"].Value);
                    new_consensus_item.SetValue(feature_table_column_names[feat_map], feat_intensity);
                }
                new_consensus_items.Add(new_consensus_item);

            }
            EntityDataService.InsertItems(new_consensus_items);
            EntityDataService.ConnectItems(psms_with_quantification);
        }

        /// <summary>
        /// Store an additional consensusXML file containing original RTs.
        /// </summary>
        private void BuildConsensusXMLWithOrigRTs(string consensus_xml_file, List<string> featurexml_files_orig, out Dictionary<string, XmlElement> consensus_dict, out string consensus_xml_file_orig_rt)
        {
            // Read consensusXML
            XmlDocument consensus_doc = new XmlDocument();
            consensus_doc.Load(consensus_xml_file);
            XmlNodeList consensus_list = consensus_doc.GetElementsByTagName("element");

            // Create dictionary of elements, in which we overwrite the original RT. 
            // Note: this mutates XmlDocument consensus_doc which we then save into new file
            consensus_dict = new Dictionary<string, XmlElement>(consensus_list.Count);
            foreach (XmlElement element in consensus_list)
            {
                consensus_dict[element.Attributes["id"].Value] = element;
            }

            // The consensus contains feature IDs from all featureXMLs, thus we have to look into all featureXML files
            for (int file_id = 0; file_id < m_num_files; file_id++)
            {
                XmlDocument orig_feat_xml = new XmlDocument();
                orig_feat_xml.Load(featurexml_files_orig[file_id]);
                XmlNodeList orig_featurelist = orig_feat_xml.GetElementsByTagName("feature");
                foreach (XmlElement feature in orig_featurelist)
                {
                    var id = feature.Attributes["id"].Value.Substring(2);
                    var rt = feature.SelectNodes("position")[0].InnerText;
                    consensus_dict[id].SetAttribute("rt", rt);
                }
            }

            // Save consensusXML file with original RTs
            consensus_xml_file_orig_rt = Path.Combine(NodeScratchDirectory, "Consensus_orig_RT.consensusXML");
            consensus_doc.Save(consensus_xml_file_orig_rt);
        }

        /// <summary>
        /// Normalize intensities using ConsensusMapNormalizer
        /// </summary>
        private string RunConsensusMapNormalizer(string consensusxml_file)
        {
            if (param_normalization_method.Value == "none")
            {
                return consensusxml_file;
            }

            var exec_path = Path.Combine(m_openms_dir, @"bin/ConsensusMapNormalizer.exe");
            var ini_path = Path.Combine(NodeScratchDirectory, @"ConsensusMapNormalizer.ini");
            var output_file = Path.Combine(NodeScratchDirectory, "normalized.consensusXML");

            Dictionary<string, string> cn_params = new Dictionary<string, string> {
                            {"in", consensusxml_file},
                            {"out", output_file},
                            {"algorithm_type", param_normalization_method.Value},
                            {"accession_filter", param_normalization_acc_filter.Value},
                            {"description_filter", param_normalization_desc_filter.Value},
                            {"threads", param_num_threads.ToString()}};
            OpenMSCommons.CreateDefaultINI(exec_path, ini_path, NodeScratchDirectory, m_node_delegates);
            OpenMSCommons.WriteParamsToINI(ini_path, cn_params);

            SendAndLogMessage("Starting ConsensusMapNormalizer");
            OpenMSCommons.RunTOPPTool(exec_path, ini_path, NodeScratchDirectory, m_node_delegates);
            m_current_step += 1;
            ReportTotalProgress((double)m_current_step / m_num_steps);
            return output_file;
        }

        /// <summary>
        /// Set up the "Quantified Features" table (with dynamic number of abundance columns)
        /// </summary>
        private List<string> SetupConsensusFeaturesTable()
        {
            EntityDataService.RegisterEntity<ConsensusFeatureEntity>(ProcessingNodeNumber);

            // Add columns to (yet empty) consensus item table
            // First: sequence, charge, average mz & rt (only once per row)
            var seq_column = PropertyAccessorFactory.CreateDynamicPropertyAccessor<ConsensusFeatureEntity, string>(
                new PropertyDescription()
                {
                    DisplayName = "Sequence",
                    Description = "Peptide Sequence"
                }
            );
            seq_column.GridDisplayOptions.ColumnWidth = 150;
            EntityDataService.RegisterProperties(ProcessingNodeNumber, seq_column);

            var acc_column = PropertyAccessorFactory.CreateDynamicPropertyAccessor<ConsensusFeatureEntity, string>(
                new PropertyDescription()
                {
                    DisplayName = "Accessions",
                    Description = "Protein Accessions"
                }
            );
            acc_column.GridDisplayOptions.ColumnWidth = 150;
            EntityDataService.RegisterProperties(ProcessingNodeNumber, acc_column);

            var desc_column = PropertyAccessorFactory.CreateDynamicPropertyAccessor<ConsensusFeatureEntity, string>(
                new PropertyDescription()
                {
                    DisplayName = "Descriptions",
                    Description = "Protein Descriptions"
                }
            );
            desc_column.GridDisplayOptions.ColumnWidth = 300;
            EntityDataService.RegisterProperties(ProcessingNodeNumber, desc_column);

            var charge_column = PropertyAccessorFactory.CreateDynamicPropertyAccessor<ConsensusFeatureEntity, int>(
               new PropertyDescription()
               {
                   DisplayName = "Charge",
                   Description = "Charge of the peptide"
               }
            );
            EntityDataService.RegisterProperties(ProcessingNodeNumber, charge_column);

            var mz_column = PropertyAccessorFactory.CreateDynamicPropertyAccessor<ConsensusFeatureEntity, double?>(
               new PropertyDescription()
               {
                   DisplayName = "m/z",
                   FormatString = "0.0000",
                   Description = "m/z"
               }
            );
            mz_column.GridDisplayOptions.ColumnWidth = 90;
            EntityDataService.RegisterProperties(ProcessingNodeNumber, mz_column);

            var rt_column = PropertyAccessorFactory.CreateDynamicPropertyAccessor<ConsensusFeatureEntity, double?>(
               new PropertyDescription()
               {
                   DisplayName = "RT",
                   FormatString = "0.00",
                   Description = "Retention Time"
               }
            );
            rt_column.GridDisplayOptions.ColumnWidth = 80;
            EntityDataService.RegisterProperties(ProcessingNodeNumber, rt_column);

            //second: add one intensity column for each sample
            var column_names = new List<String>(m_num_files);
            for (int i = 0; i < m_num_files; i++)
            {
                string raw_file_name = Path.GetFileName(m_raw_files[i]);

                var new_intensity_column = PropertyAccessorFactory.CreateDynamicPropertyAccessor<ConsensusFeatureEntity, double?>(
                    new PropertyDescription()
                    {
                        DisplayName = "Abundance " + (i + 1) + " (" + raw_file_name + ")",
                        Description = "Abundance " + (i + 1) + " (" + raw_file_name + ")",
                        FormatString = "0.0000e-00"
                    }
                );
                new_intensity_column.GridDisplayOptions.ColumnWidth = 90;
                EntityDataService.RegisterProperties(ProcessingNodeNumber, new_intensity_column);
                column_names.Add(new_intensity_column.Name);
            }
            return column_names;
        }

        /// <summary>
        /// Run map alignment and feature linking on a set of featureXML files. Return file name of the resulting consensusXML file.
        /// </summary>
        private string AlignAndLink(List<string> featurexml_files_orig)
        {
            //input and ouput files
            string output_file = "";
            string[] in_files = new string[m_num_files];
            string[] out_files = new string[m_num_files];
            string ini_path = "";

            //if only one file, convert featureXML (unaligned) to consensus, no alignment or linking will occur
            if (m_num_files == 1)
            {
                in_files[0] = featurexml_files_orig[0];
                out_files[0] = Path.Combine(NodeScratchDirectory,
                    Path.GetFileNameWithoutExtension(in_files[0])) +
                    ".consensusXML";
                output_file = out_files[0];

                var exec_path = Path.Combine(m_openms_dir, @"bin/FileConverter.exe");
                Dictionary<string, string> convert_parameters = new Dictionary<string, string> {
                            {"in", in_files[0]},
                            {"in_type", "featureXML"},
                            {"out", out_files[0]},
                            {"out_type", "consensusXML"},
                            {"threads", param_num_threads.ToString()}};
                ini_path = Path.Combine(NodeScratchDirectory, @"FileConverterDefault.ini");
                OpenMSCommons.CreateDefaultINI(exec_path, ini_path, NodeScratchDirectory, m_node_delegates);
                OpenMSCommons.WriteParamsToINI(ini_path, convert_parameters);
                OpenMSCommons.RunTOPPTool(exec_path, ini_path, NodeScratchDirectory, m_node_delegates);
                m_current_step += 1;
                ReportTotalProgress((double)m_current_step / m_num_steps);
            }
            else if (m_num_files > 1)
            {
                string exec_path;
                var aligned_featurexmls = new List<string>();
                if (param_perform_map_alignment.Value)
                {
                    exec_path = Path.Combine(m_openms_dir, @"bin/MapAlignerIdentification.exe");
                    for (int i = 0; i < m_num_files; i++)
                    {
                        in_files[i] = featurexml_files_orig[i];
                        out_files[i] = Path.Combine(NodeScratchDirectory,
                                                    Path.GetFileNameWithoutExtension(in_files[i])) + ".aligned.featureXML";
                        aligned_featurexmls.Add(out_files[i]);
                    }

                    Dictionary<string, string> map_parameters = new Dictionary<string, string> {
                        {"min_run_occur", param_min_run_occur.Value.ToString()},
                        {"threads", param_num_threads.ToString()}
                    };
                    ini_path = Path.Combine(NodeScratchDirectory, @"MapAlignerIdentification.ini");
                    OpenMSCommons.CreateDefaultINI(exec_path, ini_path, NodeScratchDirectory, m_node_delegates);
                    OpenMSCommons.WriteParamsToINI(ini_path, map_parameters);
                    OpenMSCommons.WriteItemListToINI(in_files, ini_path, "in");
                    OpenMSCommons.WriteItemListToINI(out_files, ini_path, "out");

                    SendAndLogMessage("Starting MapAlignerIdentification");
                    OpenMSCommons.RunTOPPTool(exec_path, ini_path, NodeScratchDirectory, m_node_delegates);
                    m_current_step += 1;
                    ReportTotalProgress((double)m_current_step / m_num_steps);
                }

                // Feature linking

                // out_files might be original featureXML, might be aligned.featureXML
                for (int i = 0; i < m_num_files; i++)
                {
                    if (param_perform_map_alignment.Value)
                    {
                        in_files[i] = aligned_featurexmls[i];
                    }
                    else
                    {
                        in_files[i] = featurexml_files_orig[i];
                    }
                }
                out_files[0] = Path.Combine(NodeScratchDirectory, "featureXML_consensus.consensusXML");
                output_file = out_files[0];

                exec_path = Path.Combine(m_openms_dir, @"bin/FeatureLinkerUnlabeledQT.exe");
                Dictionary<string, string> fl_unlabeled_parameters = new Dictionary<string, string> {
                        {"ignore_charge", "false"},
                        {"out", out_files[0]},
                        {"threads", param_num_threads.ToString()}
                };
                ini_path = Path.Combine(NodeScratchDirectory, @"FeatureLinkerUnlabeledQTDefault.ini");
                OpenMSCommons.CreateDefaultINI(exec_path, ini_path, NodeScratchDirectory, m_node_delegates);
                OpenMSCommons.WriteParamsToINI(ini_path, fl_unlabeled_parameters);
                OpenMSCommons.WriteItemListToINI(in_files, ini_path, "in");
                OpenMSCommons.WriteThresholdsToINI(param_mz_threshold, param_rt_threshold, ini_path);

                SendAndLogMessage("FeatureLinkerUnlabeledQT");
                OpenMSCommons.RunTOPPTool(exec_path, ini_path, NodeScratchDirectory, m_node_delegates);
                m_current_step += 1;
                ReportTotalProgress((double)m_current_step / m_num_steps);
            }
            return output_file;
        }

        /// <summary>
        /// Read file names of featureXML files and RAW files from MSF file / entity data service (were stored by the ProcessingNode)
        /// </summary>
        private void ReadInputFilenames(out List<string> orig_features)
        {
            //Read in featureXmls contained in the study result folder, read only those associated with the project MSF
            var all_custom_data_raw_files = EntityDataService.CreateEntityItemReader().ReadAll<ProcessingNodeCustomData>().Where(c => c.DataPurpose == "RawFiles").ToDictionary(c => c.WorkflowID, c => c);
            var all_custom_data_featurexml_files = EntityDataService.CreateEntityItemReader().ReadAll<ProcessingNodeCustomData>().Where(c => c.DataPurpose == "FeatureXmlFiles").ToDictionary(c => c.WorkflowID, c => c);
            //var all_custom_data_mzml_files = EntityDataService.CreateEntityItemReader().ReadAll<ProcessingNodeCustomData>().Where(c => c.DataPurpose == "MzMLFiles").ToDictionary(c => c.WorkflowID, c => c);

            var separator = new string[] { "____S_E_P_A_R_A_T_O_R____" };

            m_num_files = 0;
            foreach (var item in all_custom_data_featurexml_files)
            {
                m_num_files += ((string)item.Value.CustomValue).Split(separator, StringSplitOptions.RemoveEmptyEntries).Count();
            }

            m_raw_files = new List<string>();
            foreach (var item in all_custom_data_raw_files)
            {
                var raw_files_str = (string)item.Value.CustomValue;
                var tmp = new List<string>(raw_files_str.Split(separator, StringSplitOptions.RemoveEmptyEntries));

                foreach (var file_name in tmp)
                {
                    m_raw_files.Add(file_name);
                }
            }

            orig_features = new List<string>(m_num_files);
            foreach (var item in all_custom_data_featurexml_files)
            {
                var featurexml_files = (string)item.Value.CustomValue;
                var featurexml_files_list = new List<string>(featurexml_files.Split(separator, StringSplitOptions.RemoveEmptyEntries));

                foreach (var file_name in featurexml_files_list)
                {
                    orig_features.Add(file_name);
                }
            }
        }

        /// <summary>
        /// Export PSMs from PD search results to idXML
        /// </summary>
        public void ExportPSMsToIdXML(string idxml_filename, bool fido, string original_raw_file = "")
        {
            XmlDocument doc = new XmlDocument();
            XmlNode docNode = doc.CreateXmlDeclaration("1.0", "UTF-8", null);
            doc.AppendChild(docNode);

            // ------------------------------------------------------

            var idxml_node = doc.CreateElement("IdXML");

            var version_attr = doc.CreateAttribute("version");
            version_attr.Value = "1.2";
            idxml_node.Attributes.Append(version_attr);

            var schema_attr = doc.CreateAttribute("xsi:noNamespaceSchemaLocation");
            schema_attr.Value = "http://open-ms.sourceforge.net/schemas/IdXML_1_2.xsd";
            idxml_node.Attributes.Append(schema_attr);

            var xmlns_attr = doc.CreateAttribute("xmlns:xsi");
            xmlns_attr.Value = "http://www.w3.org/2001/XMLSchema-instance";
            idxml_node.Attributes.Append(xmlns_attr);

            doc.AppendChild(idxml_node);

            // ------------------------------------------------------

            var search_params_node = doc.CreateElement("SearchParameters");

            var id_attr = doc.CreateAttribute("id");
            id_attr.Value = "SP_0";
            search_params_node.Attributes.Append(id_attr);

            var db_attr = doc.CreateAttribute("db");
            db_attr.Value = "fnord.fasta";
            search_params_node.Attributes.Append(db_attr);

            var dbv_attr = doc.CreateAttribute("db_version");
            dbv_attr.Value = "";
            search_params_node.Attributes.Append(dbv_attr);

            var tax_attr = doc.CreateAttribute("taxonomy");
            tax_attr.Value = "0";
            search_params_node.Attributes.Append(tax_attr);

            var mass_type_attr = doc.CreateAttribute("mass_type");
            mass_type_attr.Value = "monoisotopic";
            search_params_node.Attributes.Append(mass_type_attr);

            var charges_attr = doc.CreateAttribute("charges");
            charges_attr.Value = "";
            search_params_node.Attributes.Append(charges_attr);

            var enzyme_attr = doc.CreateAttribute("enzyme");
            enzyme_attr.Value = "unknown_enzyme";
            search_params_node.Attributes.Append(enzyme_attr);

            var mc_attr = doc.CreateAttribute("missed_cleavages");
            mc_attr.Value = "0";
            search_params_node.Attributes.Append(mc_attr);

            var prec_tol_attr = doc.CreateAttribute("precursor_peak_tolerance");
            prec_tol_attr.Value = "42.0";
            search_params_node.Attributes.Append(prec_tol_attr);

            var peak_tol_attr = doc.CreateAttribute("peak_mass_tolerance");
            peak_tol_attr.Value = "42.0";
            search_params_node.Attributes.Append(peak_tol_attr);

            idxml_node.AppendChild(search_params_node);

            // ------------------------------------------------------

            var id_run_node = doc.CreateElement("IdentificationRun");

            var search_engine_attr = doc.CreateAttribute("search_engine");
            search_engine_attr.Value = "PD";
            id_run_node.Attributes.Append(search_engine_attr);

            var sev_attr = doc.CreateAttribute("search_engine_version");
            sev_attr.Value = "2.0";
            id_run_node.Attributes.Append(sev_attr);

            var date_attr = doc.CreateAttribute("date");
            date_attr.Value = "2011-11-11T11:11:11";
            id_run_node.Attributes.Append(date_attr);

            var sp_ref_attr = doc.CreateAttribute("search_parameters_ref");
            sp_ref_attr.Value = "SP_0";
            id_run_node.Attributes.Append(sp_ref_attr);

            idxml_node.AppendChild(id_run_node);

            IdXMLExportHelper<TargetPeptideSpectrumMatch>(doc, id_run_node, fido, original_raw_file);

            if (fido)
            {
                IdXMLExportHelper<DecoyPeptideSpectrumMatch>(doc, id_run_node, fido, original_raw_file);
            }

            doc.Save(idxml_filename);
        }

        /// <summary>
        /// Helper method for ExportPSMsToIdXML(...). Does the actual export of search engine results.
        /// </summary>
        void IdXMLExportHelper<PSMType>(XmlDocument doc, XmlElement id_run_node, bool fido, string original_raw_file = "")
            where PSMType : PeptideSpectrumMatch
        {
            bool decoy = typeof(PSMType) == typeof(DecoyPeptideSpectrumMatch);

            IEnumerable<Tuple<PSMType, IList<MSnSpectrumInfo>>> psms = null;
            if (original_raw_file != "")
            {
                // write only IDs originating from original_raw_file
                psms = EntityDataService.CreateEntityItemReader().ReadAllFlat<PSMType, MSnSpectrumInfo>().Where(x => x.Item1.SpectrumFileName == Path.GetFileName(original_raw_file));
            }
            else
            {
                // write all IDs
                psms = EntityDataService.CreateEntityItemReader().ReadAllFlat<PSMType, MSnSpectrumInfo>();
            }

            var scoreProperties = EntityDataService
                    .GetProperties<PSMType>(ScoreSemanticTerms.Score)
                    .Where(item => item.ValueType == typeof(double)
                            || item.ValueType == typeof(double?))
                    .ToArray();

            PropertyAccessor<PSMType> pep_score = null;
            PropertyAccessor<PSMType> qvalue_score = null;
            foreach (var sp in scoreProperties)
            {
                var score_name = sp.Name;

                if (score_name.Contains("PEP"))
                {
                    pep_score = sp;
                }
                else if (score_name.Contains("qValue"))
                {
                    qvalue_score = sp;
                }
            }

            foreach (var psm in psms)
            {
                //retrieve scores for PSM
                double pep_score_val = pep_score == null ? 1.0 : Convert.ToDouble(pep_score.GetValue(psm.Item1));
                double qvalue_score_val = qvalue_score == null ? 1.0 : Convert.ToDouble(qvalue_score.GetValue(psm.Item1));

                if (fido && pep_score_val > param_fido_prefiltering_threshold.Value)
                {
                    continue;
                }

                if (!fido && qvalue_score_val > param_q_value_threshold.Value)
                {
                    continue;
                }

                //write entry to idXML file
                var pep_id_node = doc.CreateElement("PeptideIdentification");
                id_run_node.AppendChild(pep_id_node);

                if (!fido)
                {
                    // use normal q-value scores
                    var st_attr = doc.CreateAttribute("score_type");
                    st_attr.Value = "Percolator q-Value";
                    pep_id_node.Attributes.Append(st_attr);

                    var hsb_attr = doc.CreateAttribute("higher_score_better");
                    hsb_attr.Value = "false";
                    pep_id_node.Attributes.Append(hsb_attr);
                }
                else
                {
                    // this file will be a Fido input => use posterior probability
                    var st_attr = doc.CreateAttribute("score_type");
                    st_attr.Value = "Posterior Probability_score";
                    pep_id_node.Attributes.Append(st_attr);

                    var hsb_attr = doc.CreateAttribute("higher_score_better");
                    hsb_attr.Value = "true";
                    pep_id_node.Attributes.Append(hsb_attr);
                }

                var mz_attr = doc.CreateAttribute("MZ");
                mz_attr.Value = psm.Item2[0].MassOverCharge.ToString();
                pep_id_node.Attributes.Append(mz_attr);

                var rt_attr = doc.CreateAttribute("RT");
                rt_attr.Value = (psm.Item2[0].RetentionTime * 60.0).ToString();
                pep_id_node.Attributes.Append(rt_attr);

                // ------------------------------------------------------

                var pep_hit_node = doc.CreateElement("PeptideHit");
                pep_id_node.AppendChild(pep_hit_node);

                var score_attr = doc.CreateAttribute("score");
                // use q-values for IDMapping, otherwise (for Fido input) use posterior probability
                score_attr.Value = fido ? (1.0 - pep_score_val).ToString() : qvalue_score_val.ToString();
                pep_hit_node.Attributes.Append(score_attr);

                var seq_attr = doc.CreateAttribute("sequence");
                seq_attr.Value = OpenMSCommons.ModSequence(psm.Item1.Sequence, psm.Item1.Modifications);
                pep_hit_node.Attributes.Append(seq_attr);

                var charge_attr = doc.CreateAttribute("charge");
                charge_attr.Value = psm.Item1.Charge.ToString();
                pep_hit_node.Attributes.Append(charge_attr);

                XmlElement user_param = null;
                XmlAttribute type_attr = null;
                XmlAttribute name_attr = null;
                XmlAttribute value_attr = null;

                // PD peptide ID for mapping back later
                user_param = doc.CreateElement("UserParam");
                pep_hit_node.AppendChild(user_param);

                type_attr = doc.CreateAttribute("type");
                type_attr.Value = "string";
                user_param.Attributes.Append(type_attr);

                name_attr = doc.CreateAttribute("name");
                name_attr.Value = "pd_peptide_id";
                user_param.Attributes.Append(name_attr);

                value_attr = doc.CreateAttribute("value");
                value_attr.Value = decoy ? "decoy_" : "";
                value_attr.Value += string.Join(";", from x in psm.Item1.GetIDs() select x.ToString());
                user_param.Attributes.Append(value_attr);
            }
        }

        /// <summary>
        /// Run IDMapper on a given featureXML and idXML file.
        /// </summary>
        void RunIDMapper(string featurexml_file, string idxml_file, string result_featurexml_file)
        {
            var exec_path = Path.Combine(m_openms_dir, @"bin/IDMapper.exe");
            var ini_path = Path.Combine(NodeScratchDirectory, @"IDMapper.ini");
            OpenMSCommons.CreateDefaultINI(exec_path, ini_path, NodeScratchDirectory, m_node_delegates);
            Dictionary<string, string> idmapper_parameters = new Dictionary<string, string> {
                        {"mz_tolerance", param_id_mapping_mz_threshold.Value.Tolerance.ToString()},
                        {"rt_tolerance", (param_id_mapping_rt_threshold.Value * 60.0).ToString()},
                        {"in", featurexml_file},
                        {"id", idxml_file},
                        {"out", result_featurexml_file},
                        {"threads", param_num_threads.ToString()}
            };
            if (param_mz_reference.Value == "precursor")
            {
                idmapper_parameters["mz_reference"] = "precursor";
                idmapper_parameters["use_centroid_mz"] = "false";
            }
            else // "peptide"
            {
                idmapper_parameters["mz_reference"] = "peptide";
                idmapper_parameters["use_centroid_mz"] = "true";
            }
            OpenMSCommons.WriteParamsToINI(ini_path, idmapper_parameters);

            SendAndLogMessage("Starting IDMapper");
            OpenMSCommons.RunTOPPTool(exec_path, ini_path, NodeScratchDirectory, m_node_delegates);
            m_current_step += 1;
            ReportTotalProgress((double)m_current_step / m_num_steps);
        }


        /// <summary>
        /// Run PeptideIndexer in order to get target/decoy information. Export original FASTA from PD and (re-)generate decoys first.
        /// </summary>
        void RunPeptideIndexer(string input_file, string output_file)
        {
            string exec_path = "";
            string ini_path = "";

            if (m_openms_fasta_file == "" || !File.Exists(m_openms_fasta_file))
            {
                // check whether FASTA file exists
                string fasta_filename = param_fasta_db.Value.FullPhysicalFileName;
                if (!File.Exists(fasta_filename))
                {
                    SendAndLogErrorMessage("Cannot access FASTA file because the file cannot be found!");
                    throw new FileNotFoundException(String.Format("The FASTA file {0} cannot be found!", param_fasta_db.Value.VirtualFileName), fasta_filename);
                }

                m_openms_fasta_file = Path.Combine(NodeScratchDirectory, @"peptide_indexer.fasta");
                ProcessingServices.FastaFileService.CreateOriginalFastaFile(param_fasta_db.Value, m_openms_fasta_file, true);

                exec_path = Path.Combine(m_openms_dir, @"bin/DecoyDatabase.exe");
                ini_path = Path.Combine(NodeScratchDirectory, @"DecoyDatabase.ini");
                OpenMSCommons.CreateDefaultINI(exec_path, ini_path, NodeScratchDirectory, m_node_delegates);
                Dictionary<string, string> dd_parameters = new Dictionary<string, string> {
                        {"out", m_openms_fasta_file},
                        {"append", "true"},
                        {"decoy_string_position", "prefix"},
                        {"decoy_string", "REV_"},
                        {"threads", param_num_threads.ToString()}
                };
                OpenMSCommons.WriteParamsToINI(ini_path, dd_parameters);
                string[] in_list = new string[1];
                in_list[0] = m_openms_fasta_file;
                OpenMSCommons.WriteItemListToINI(in_list, ini_path, "in");

                SendAndLogMessage("Starting DecoyDatabase");
                OpenMSCommons.RunTOPPTool(exec_path, ini_path, NodeScratchDirectory, m_node_delegates);
                m_current_step += 1;
                ReportTotalProgress((double)m_current_step / m_num_steps);
            }

            //run PeptideIndexer
            exec_path = Path.Combine(m_openms_dir, @"bin/PeptideIndexer.exe");
            ini_path = Path.Combine(NodeScratchDirectory, @"PeptideIndexer.ini");
            OpenMSCommons.CreateDefaultINI(exec_path, ini_path, NodeScratchDirectory, m_node_delegates);
            Dictionary<string, string> pi_parameters = new Dictionary<string, string> {
                        {"in", input_file},
                        {"fasta", m_openms_fasta_file},
                        {"out", output_file},
                        {"prefix", "true"},
                        {"decoy_string", "REV_"},
                        {"missing_decoy_action", "warn"},
                        {"allow_unmatched", "true"},
                        {"write_protein_description", "true"},
                        {"threads", param_num_threads.ToString()}
            };
            OpenMSCommons.WriteParamsToINI(ini_path, pi_parameters);
            //OpenMSCommons.WriteNestedParamToINI(ini_path, new Triplet("enzyme", "name", param_enzyme.Value));
            OpenMSCommons.WriteNestedParamToINI(ini_path, new Triplet("enzyme", "specificity", "none"));

            SendAndLogMessage("Starting PeptideIndexer");
            OpenMSCommons.RunTOPPTool(exec_path, ini_path, NodeScratchDirectory, m_node_delegates);
            m_current_step += 1;
            ReportTotalProgress((double)m_current_step / m_num_steps);
        }


        /// <summary>
        /// Parse peptide quantification results of ProteinQuantifier, fill "Quantified peptides" table
        /// </summary>
        void ParsePQPeptides(string pq_pep_file)
        {
            EntityDataService.RegisterEntity<DechargedPeptideEntity>(ProcessingNodeNumber);

            StreamReader reader = File.OpenText(pq_pep_file);
            string line;

            // ignore #comment lines
            while ((line = reader.ReadLine()) != null && line.Substring(0, 1) == "#") ;

            // store header (should be ||| "peptide"       "protein"       "n_proteins"    "charge"        "abundance_1", ..., "abundance_n" |||)
            string header = line;

            // add dynamic columns
            int n_abundances = header.Split('\t').Length - 4;
            var column_names = new List<String>(n_abundances);
            for (int i = 0; i < n_abundances; ++i)
            {
                string raw_file_name = Path.GetFileName(m_raw_files[i]);
                var new_abundance_column = PropertyAccessorFactory.CreateDynamicPropertyAccessor<DechargedPeptideEntity, double?>(
                    new PropertyDescription()
                    {
                        DisplayName = "Abundance " + (i + 1) + " (" + raw_file_name + ")",
                        Description = "Abundance " + (i + 1) + " (" + raw_file_name + ")",
                        FormatString = "0.0000e-00"
                    }
                );
                new_abundance_column.GridDisplayOptions.ColumnWidth = 80;
                EntityDataService.RegisterProperties(ProcessingNodeNumber, new_abundance_column);
                column_names.Add(new_abundance_column.Name);
            }

            // parse peptides
            var new_peptide_items = new List<DechargedPeptideEntity>();
            int idCounter = 1;
            Dictionary<string, string> fasta_acc_to_descr = BuildFastaAccToDescDict();
            while ((line = reader.ReadLine()) != null)
            {
                string[] items = line.Split('\t');
                string pep_seq = items[0].Substring(1, items[0].Length - 2);
                string protein_accs = items[1].Substring(1, items[1].Length - 2);
                var protein_acc_list = protein_accs.Split('/').ToList();
                string protein_descs = string.Join(" /// ", from p in protein_acc_list where fasta_acc_to_descr.ContainsKey(p) select fasta_acc_to_descr[p]);
                Int32 n_proteins = Convert.ToInt32(items[2]);
                List<double?> abundances = new List<double?>();
                for (int i = 4; i < items.Length; ++i)
                {
                    double? a = null;
                    if (items[i] != "" && items[i] != "0")
                    {
                        a = Convert.ToDouble(items[i]);
                    }
                    abundances.Add(a);
                }

                DechargedPeptideEntity new_peptide_item = new DechargedPeptideEntity()
                {
                    WorkflowID = WorkflowID,
                    Id = idCounter++,
                    sequence = pep_seq,
                    proteins = protein_accs,
                    descriptions = protein_descs,
                    num_proteins = n_proteins
                };

                for (int i = 0; i < abundances.Count; ++i)
                {
                    new_peptide_item.SetValue(column_names[i], abundances[i]);
                }
                new_peptide_items.Add(new_peptide_item);
            }
            EntityDataService.InsertItems(new_peptide_items);
        }

        /// <summary>
        /// Parse protein quantification results of ProteinQuantifier, fill "Quantified proteins" table
        /// </summary>
        void ParsePQProteins(string pq_prot_file)
        {
            EntityDataService.RegisterEntity<QuantifiedProteinEntity>(ProcessingNodeNumber);

            StreamReader reader = File.OpenText(pq_prot_file);
            string line;

            // ignore #comment lines
            while ((line = reader.ReadLine()) != null && line.Substring(0, 1) == "#") ;

            // store header (should be ||| "protein"       "n_proteins"    "protein_score" "n_peptides"    "abundance_1", ..., "abundance_n" |||)
            string header = line;

            // add dynamic columns
            int n_abundances = header.Split('\t').Length - 4;
            var column_names = new List<String>(n_abundances);
            for (int i = 0; i < n_abundances; ++i)
            {
                string raw_file_name = Path.GetFileName(m_raw_files[i]);
                var new_abundance_column = PropertyAccessorFactory.CreateDynamicPropertyAccessor<QuantifiedProteinEntity, double?>(
                    new PropertyDescription()
                    {
                        DisplayName = "Abundance " + (i + 1) + " (" + raw_file_name + ")",
                        Description = "Abundance " + (i + 1) + " (" + raw_file_name + ")",
                        FormatString = "0.0000e-00"
                    }
                );
                new_abundance_column.GridDisplayOptions.ColumnWidth = 80;
                EntityDataService.RegisterProperties(ProcessingNodeNumber, new_abundance_column);
                column_names.Add(new_abundance_column.Name);
            }

            // parse proteins
            var new_protein_items = new List<QuantifiedProteinEntity>();
            int idCounter = 1;
            Dictionary<string, string> fasta_acc_to_descr = BuildFastaAccToDescDict();
            while ((line = reader.ReadLine()) != null)
            {
                string[] items = line.Split('\t');
                string protein_accs = items[0].Substring(1, items[0].Length - 2);
                var protein_acc_list = protein_accs.Split('/').ToList();
                string protein_descs = string.Join(" /// ", from p in protein_acc_list where fasta_acc_to_descr.ContainsKey(p) select fasta_acc_to_descr[p]);
                Int32 n_proteins = Convert.ToInt32(items[1]);
                Int32 n_peptides = Convert.ToInt32(items[3]);
                List<double?> abundances = new List<double?>();
                for (int i = 4; i < items.Length; ++i)
                {
                    double? a = null;
                    if (items[i] != "" && items[i] != "0")
                    {
                        a = Convert.ToDouble(items[i]);
                    }
                    abundances.Add(a);
                }

                QuantifiedProteinEntity new_protein_item = new QuantifiedProteinEntity()
                {
                    WorkflowID = WorkflowID,
                    Id = idCounter++,
                    proteins = protein_accs,
                    descriptions = protein_descs,
                    num_proteins = n_proteins,
                    num_peptides = n_peptides
                };

                for (int i = 0; i < abundances.Count; ++i)
                {
                    new_protein_item.SetValue(column_names[i], abundances[i]);
                }
                new_protein_items.Add(new_protein_item);
            }
            EntityDataService.InsertItems(new_protein_items);
        }

        /// <summary>
        /// Return a dictionary {protein accession -> protein description} for a given FASTA file
        /// </summary>
        Dictionary<string, string> BuildFastaAccToDescDict()
        {
            var result = new Dictionary<string, string>();
            try
            {
                StreamReader reader = File.OpenText(m_openms_fasta_file);
                string line;
                while ((line = reader.ReadLine()) != null)
                {
                    line = line.Trim();
                    if (line.Length == 0 || line[0] != '>')
                    {
                        continue;
                    }
                    line = line.Substring(1).Trim();
                    var items = line.Split(' ').ToList();
                    if (items.Count < 2)
                    {
                        continue;
                    }
                    string acc = items[0];
                    string desc = string.Join(" ", items.Skip(1));
                    result[acc] = desc;
                }
                reader.Close();
            }
            catch (Exception)
            {
                SendAndLogErrorMessage("Could not parse FASTA file '{0}'", m_openms_fasta_file);
            }
            return result;
        }
    }
}
