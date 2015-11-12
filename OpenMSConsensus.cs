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

    public class OpenMSConsensus
        : ReportProcessingNode
    {
        # region Parameters

        [BooleanParameter(Category = "1. Feature linking",
            DisplayName = "Perform map alignment",
            Description = "This parameter specifies whether map alignment (RT transformation) should be performed before linking.",
            DefaultValue = "true",
            Position = 1)]
        public BooleanParameter param_perform_map_alignment;

        /// <summary>
        /// This parameter specifies the maximum allowed retention time difference for features to be linked together.
        /// </summary>
        [DoubleParameter(
            Category = "1. Feature linking",
            DisplayName = "Max. RT difference [min]",
            Description = "This parameter specifies the maximum allowed retention time difference for features to be linked together.",
            DefaultValue = "1.0",
            Position = 2)]
        public DoubleParameter param_rt_threshold;

        /// <summary>
        /// This parameter specifies the maximum allowed m/z difference for features to be linked together.
        /// </summary>
        [MassToleranceParameter(
            Category = "1. Feature linking",
            DisplayName = "Max. m/z difference",
            Subset = "ppm",
            Description = "This parameter specifies the maximum allowed m/z difference for features to be linked together.",
            DefaultValue = "10 ppm",
            IntendedPurpose = ParameterPurpose.MassTolerance,
            Position = 3)]
        public MassToleranceParameter param_mz_threshold;

        [DoubleParameter(
            Category = "2. ID mapping",
            DisplayName = "Max. RT difference [min]",
            Description = "This parameter specifies the maximum allowed retention time difference for features to be linked together.",
            DefaultValue = "0.33",
            Position = 4)]
        public DoubleParameter param_id_mapping_rt_threshold;

        [MassToleranceParameter(
            Category = "2. ID mapping",
            DisplayName = "Max. m/z difference",
            Subset = "ppm",
            Description = "This parameter specifies the maximum allowed m/z difference for features to be linked together.",
            DefaultValue = "10 ppm",
            IntendedPurpose = ParameterPurpose.MassTolerance,
            Position = 5)]
        public MassToleranceParameter param_id_mapping_mz_threshold;

        [DoubleParameter(
            Category = "2. ID mapping",
            DisplayName = "q-Value threshold",
            Description = "PSMs with a q-Value larger than this threshold will not be used for ID mapping and subsequent protein inference.",
            DefaultValue = "0.05",
            Position = 6)]
        public DoubleParameter param_q_value_threshold;

        [FastaFileParameter(Category = "2. ID mapping",
            DisplayName = "Protein database",
            Description = "The sequence database to be used for (re-)indexing protein hits",
            IntendedPurpose = ParameterPurpose.SequenceDatabase,
            ValueRequired = true,
            Position = 7)]
        public FastaFileParameter param_fasta_db;

        [StringSelectionParameter(Category = "2. ID mapping",
            DisplayName = "Enzyme",
            Description = "The enzyme used for cleaving the proteins",
            DefaultValue = "Trypsin",
            SelectionValues = new string[] { "Trypsin", "Asp-N", "CNBr", "Formic_acid", "Chymotrypsin", "Lys-C", "Asp-N_ambic", "Arg-C", "V8-DE", "glutamyl endopeptidase", "leukocyte elastase", "no cleavage", "PepsinA", "Lys-C/P", "2-iodobenzoate", "prolineendopeptidase", "V8-E", "TrypChymo", "unspecific cleavage", "Trypsin/P" },
            Position = 8)]
        public SimpleSelectionParameter<string> param_enzyme;

        [StringSelectionParameter(Category = "3. Protein quantification",
            DisplayName = "Use peptides",
            Description = "Specify which peptides should be used for quantification: only unique peptides, unique + indistinguishable proteins, or unique + indistinguishable + other shared peptides (using a greedy resolution which is similar to selecting the razor peptides)",
            DefaultValue = "greedy",
            SelectionValues = new string[] { "unique", "indistinguishable", "greedy" },
            Position = 9)]
        public SimpleSelectionParameter<string> param_protein_quant_mode;

        [IntegerParameter(Category = "3. Protein quantification",
            DisplayName = "top",
            Description = "Calculate protein abundance from this number of peptides (most abundant first; '0' for all)",
            DefaultValue = "0",
            MinimumValue = "0",
            Position = 10)]
        public IntegerParameter param_top;

        [StringSelectionParameter(Category = "3. Protein quantification",
            DisplayName = "Averaging",
            Description = "Averaging method used to compute protein abundances from peptide abundances",
            DefaultValue = "mean",
            SelectionValues = new string[] { "mean", "weighted_mean", "median", "sum" },
            Position = 11)]
        public SimpleSelectionParameter<string> param_averaging;

        [BooleanParameter(Category = "3. Protein quantification",
            DisplayName = "Include all",
            Description = "Include results for proteins with fewer peptides than indicated by 'top' (no effect if 'top' is 0 or 1)",
            DefaultValue = "false",
            Position = 12)]
        public BooleanParameter param_include_all;

        [BooleanParameter(Category = "3. Protein quantification",
            DisplayName = "Filter charge",
            Description = "Distinguish between charge states of a peptide. For peptides, abundances will be reported separately for each charge; for proteins, abundances will be computed based only on the most prevalent charge of each peptide. Otherwise, abundances are summed over all charge states.",
            DefaultValue = "true",
            Position = 13)]
        public BooleanParameter param_filter_charge;

        [BooleanParameter(Category = "3. Protein quantification",
            DisplayName = "Fix peptides",
            Description = "Use the same peptides for protein quantification across all samples. With 'top 0', all peptides that occur in every sample are considered. Otherwise ('top N'), the N peptides that occur in the most samples (independently of each other) are selected, breaking ties by total abundance (there is no guarantee that the best co-ocurring peptides are chosen!).",
            DefaultValue = "true",
            Position = 14)]
        public BooleanParameter param_fix_peptides;

        [IntegerParameter(Category = "4. General",
        DisplayName = "CPU Cores",
        Description = "How many CPU cores should at most be used by the algorithms.",
        DefaultValue = "1",
        MinimumValue = "1",
        Position = 15)]
        public IntegerParameter param_num_threads;

        # endregion

        private string m_openms_dir;
        private int m_current_step;
        private int m_num_steps;
        private int m_num_files;
        private readonly SpectrumDescriptorCollection m_spectrum_descriptors = new SpectrumDescriptorCollection();
        private List<WorkflowInputFile> m_workflow_input_files;
        private string m_consensusxml;
        private string m_consensusxml_orig_rt;

        public override void OnParentNodeFinished(IProcessingNode sender, ResultsArguments eventArgs)
        {
            // OpenMS binary directory
            m_openms_dir = Path.Combine(ServerConfiguration.ToolsDirectory, "OpenMS-2.0/");

            // Original featureXML files will be used for reading original RTs later
            List<string> featurexml_files_orig;

            // RAW file names will be used for display in their respective sample columns
            List<string> raw_files;

            // Run alignment and linking
            AlignAndLink(out featurexml_files_orig, out raw_files);

            // Create dictionary {feature ID -> consensusXML element} with original RTs
            Dictionary<string, XmlElement> consensus_dict = BuildConsensusXMLWithOrigRTs(featurexml_files_orig);

            // Normalize intensities
            var cn_output_file = RunConsensusMapNormalizer(m_consensusxml_orig_rt);

            // Export filtered PSMs to idXML
            var filtered_idxml = Path.Combine(NodeScratchDirectory, "filtered_psms.idXML");
            ExportPSMsToIdXML(filtered_idxml, true);

            // Run PeptideIndexer in order to get peptide <-> protein associations
            var filtered_indexed_idxml = Path.Combine(NodeScratchDirectory, "filtered_psms_peptides_indexed.idXML");
            RunPeptideIndexer(filtered_idxml, filtered_indexed_idxml);

            // Map IDs to intensity-normalized consensusXML file
            var idmapped_consensusxml = Path.Combine(NodeScratchDirectory, "idmapped.consensusXML");
            RunIDMapper(cn_output_file, filtered_idxml, idmapped_consensusxml);

            // Set up consensus feature table ("Quantified features") and fill it
            var feature_table_column_names = SetupConsensusFeaturesTable(raw_files);
            PopulateConsensusFeaturesTable(consensus_dict, idmapped_consensusxml, feature_table_column_names);

            // Export all PSMs (unfiltered!) for Fido
            var idxml_filename_for_fido = Path.Combine(NodeScratchDirectory, "all_psms.idXML");
            ExportPSMsToIdXML(idxml_filename_for_fido, false);

            // Run PeptideIndexer in order to get peptide <-> protein associations
            var indexed_idxml_filename_for_fido = Path.Combine(NodeScratchDirectory, "all_psms_peptides_indexed.idXML");
            RunPeptideIndexer(idxml_filename_for_fido, indexed_idxml_filename_for_fido);

            // Run FidoAdapter
            var fido_output_file = RunFidoAdapter(indexed_idxml_filename_for_fido);

            // Run ProteinQuantifier
            RunProteinQuantifier(idmapped_consensusxml, fido_output_file);

            // Finished!
            FireProcessingFinishedEvent(new SingleResultsArguments(new[] { ProteomicsDataTypes.Psms }, this));
        }

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
                            {"protein_groups", param_protein_quant_mode.Value != "unique" ? fido_idxml_file : ""},
                            {"threads", param_num_threads.ToString()}};
            OpenMSCommons.CreateDefaultINI(exec_path, ini_path, NodeScratchDirectory, new NodeLoggerErrorDelegate(NodeLogger.ErrorFormat), new NodeLoggerWarningDelegate(NodeLogger.WarnFormat));
            OpenMSCommons.WriteParamsToINI(ini_path, ini_params);

            SendAndLogMessage("Starting ProteinQuantifier");
            OpenMSCommons.RunTOPPTool(exec_path, ini_path, NodeScratchDirectory, new SendAndLogMessageDelegate(SendAndLogMessage), new SendAndLogTemporaryMessageDelegate(SendAndLogTemporaryMessage), new WriteLogMessageDelegate(WriteLogMessage), new NodeLoggerWarningDelegate(NodeLogger.WarnFormat), new NodeLoggerErrorDelegate(NodeLogger.ErrorFormat));

            m_current_step += m_num_files;
            ReportTotalProgress((double)m_current_step / m_num_steps);

            ParsePQPeptides(pep_output_file);
            ParsePQProteins(prot_output_file);
        }

        private string RunFidoAdapter(string idxml_file)
        {
            var exec_path = Path.Combine(m_openms_dir, @"bin/FidoAdapter.exe");
            var ini_path = Path.Combine(NodeScratchDirectory, @"FidoAdapter.ini");
            var output_file = Path.Combine(NodeScratchDirectory, "fido_results.idXML");

            if (param_protein_quant_mode.Value != "unique")
            {
                Dictionary<string, string> ini_params = new Dictionary<string, string> {
                            {"in", idxml_file},
                            {"out", output_file},
                            {"greedy_group_resolution", param_protein_quant_mode.Value == "greedy" ? "true" : "false"},
                            {"threads", param_num_threads.ToString()}};
                OpenMSCommons.CreateDefaultINI(exec_path, ini_path, NodeScratchDirectory, new NodeLoggerErrorDelegate(NodeLogger.ErrorFormat), new NodeLoggerWarningDelegate(NodeLogger.WarnFormat));
                OpenMSCommons.WriteParamsToINI(ini_path, ini_params);

                SendAndLogMessage("Starting FidoAdapter");
                OpenMSCommons.RunTOPPTool(exec_path, ini_path, NodeScratchDirectory, new SendAndLogMessageDelegate(SendAndLogMessage), new SendAndLogTemporaryMessageDelegate(SendAndLogTemporaryMessage), new WriteLogMessageDelegate(WriteLogMessage), new NodeLoggerWarningDelegate(NodeLogger.WarnFormat), new NodeLoggerErrorDelegate(NodeLogger.ErrorFormat));
            }
            return output_file;
        }

        private void PopulateConsensusFeaturesTable(Dictionary<string, XmlElement> consensus_dict, string cn_output_file, List<string> feature_table_column_names)
        {
            // Connect PSM table with consensus features table (TODO: improve comments)
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
                //TODO: centroid RT is from aligned RTs (if alignment was performed)! recompute value from individual subfeatures? or also replace centroid RTs when writing orig_rt.consensusXML...
                double rt = Convert.ToDouble(centroid_node.Attributes["rt"].Value) / 60.0; //changed to minute!

                var grouped_elements = ce.SelectSingleNode("groupedElementList");

                //tableIdToFeatureId.Add(idCounter, Convert.ToUInt64(cons_id.Substring(2)));
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
                    var user_params = peptide_hit.SelectNodes("userParam");

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
                                //TODO: exception handling (index out of bounds!)
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
                        //new_consensus_item.SetValue("Sequence", psm.ModifiedSequence);
                        new_consensus_item.SetValue("Sequence", peptide_hit.Attributes["sequence"].Value);
                        new_consensus_item.SetValue("Accessions", psm.ParentProteinAccessions);
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

        private Dictionary<string, XmlElement> BuildConsensusXMLWithOrigRTs(List<string> featurexml_files_orig)
        {
            // Read consensusXML
            XmlDocument consensus_doc = new XmlDocument();
            consensus_doc.Load(m_consensusxml);
            XmlNodeList consensus_list = consensus_doc.GetElementsByTagName("element");

            // Create dictionary of elements, in which we overwrite the original RT. 
            // Note: this mutates XmlDocument consensus_doc which we then save into new file
            Dictionary<string, XmlElement> consensus_dict = new Dictionary<string, XmlElement>(consensus_list.Count);
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
            m_consensusxml_orig_rt = Path.Combine(NodeScratchDirectory, "Consensus_orig_RT.consensusXML");
            consensus_doc.Save(m_consensusxml_orig_rt);
            return consensus_dict;
        }

        private string RunConsensusMapNormalizer(string consensusxml_file)
        {
            var exec_path = Path.Combine(m_openms_dir, @"bin/ConsensusMapNormalizer.exe");
            var ini_path = Path.Combine(NodeScratchDirectory, @"ConsensusMapNormalizer.ini");
            var output_file = Path.Combine(NodeScratchDirectory, "normalized.consensusXML");

            Dictionary<string, string> cn_params = new Dictionary<string, string> {
                            {"in", consensusxml_file},
                            {"out", output_file},
                            {"algorithm_type", "median"},
                            {"threads", param_num_threads.ToString()}};
            OpenMSCommons.CreateDefaultINI(exec_path, ini_path, NodeScratchDirectory, new NodeLoggerErrorDelegate(NodeLogger.ErrorFormat), new NodeLoggerWarningDelegate(NodeLogger.WarnFormat));
            OpenMSCommons.WriteParamsToINI(ini_path, cn_params);

            SendAndLogMessage("Starting ConsensusMapNormalizer");
            OpenMSCommons.RunTOPPTool(exec_path, ini_path, NodeScratchDirectory, new SendAndLogMessageDelegate(SendAndLogMessage), new SendAndLogTemporaryMessageDelegate(SendAndLogTemporaryMessage), new WriteLogMessageDelegate(WriteLogMessage), new NodeLoggerWarningDelegate(NodeLogger.WarnFormat), new NodeLoggerErrorDelegate(NodeLogger.ErrorFormat));
            return output_file;
        }

        private List<string> SetupConsensusFeaturesTable(List<string> raw_files)
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
                string raw_file_name = Path.GetFileName(raw_files[i]);

                var new_intensity_column = PropertyAccessorFactory.CreateDynamicPropertyAccessor<ConsensusFeatureEntity, double?>(
                    new PropertyDescription()
                    {
                        DisplayName = "Abundance " + (i + 1),
                        FormatString = "0.000e-00",
                        Description = raw_file_name
                    }
                );
                new_intensity_column.GridDisplayOptions.ColumnWidth = 80;
                EntityDataService.RegisterProperties(ProcessingNodeNumber, new_intensity_column);
                column_names.Add(new_intensity_column.Name);
            }
            return column_names;
        }

        private void AlignAndLink(out List<string> featurexml_files_orig, out List<string> raw_files)
        {
            List<string> aligned_featurexmls;

            ReadInputFiles(out featurexml_files_orig, out aligned_featurexmls, out raw_files);

            //list of input and output files of specific OpenMS tools
            string[] in_files = new string[m_num_files];
            string[] out_files = new string[m_num_files];
            string ini_path = ""; //path to configuration files with parameters for the OpenMS Tool

            //if only one file, convert featureXML (unaligned) to consensus, no alignment or linking will occur
            if (m_num_files == 1)
            {
                in_files[0] = featurexml_files_orig[0];
                out_files[0] = Path.Combine(NodeScratchDirectory,
                    Path.GetFileNameWithoutExtension(in_files[0])) +
                    ".consensusXML";
                m_consensusxml = out_files[0];

                var exec_path = Path.Combine(m_openms_dir, @"bin/FileConverter.exe");
                Dictionary<string, string> convert_parameters = new Dictionary<string, string> {
                            {"in", in_files[0]}, //as only one file, outvar was assigned the result from FFC
                            {"in_type", "featureXML"},
                            {"out", out_files[0]},
                            {"out_type", "consensusXML"},
                            {"threads", param_num_threads.ToString()}};
                ini_path = Path.Combine(NodeScratchDirectory, @"FileConverterDefault.ini");
                OpenMSCommons.CreateDefaultINI(exec_path, ini_path, NodeScratchDirectory, new NodeLoggerErrorDelegate(NodeLogger.ErrorFormat), new NodeLoggerWarningDelegate(NodeLogger.WarnFormat));
                OpenMSCommons.WriteParamsToINI(ini_path, convert_parameters);
                OpenMSCommons.RunTOPPTool(exec_path, ini_path, NodeScratchDirectory, new SendAndLogMessageDelegate(SendAndLogMessage), new SendAndLogTemporaryMessageDelegate(SendAndLogTemporaryMessage), new WriteLogMessageDelegate(WriteLogMessage), new NodeLoggerWarningDelegate(NodeLogger.WarnFormat), new NodeLoggerErrorDelegate(NodeLogger.ErrorFormat));
                //not really worth own progress
            }
            else if (m_num_files > 1)
            {
                string exec_path;
                if (param_perform_map_alignment.Value)
                {
                    exec_path = Path.Combine(m_openms_dir, @"bin/MapAlignerPoseClustering.exe");
                    for (int i = 0; i < m_num_files; i++)
                    {
                        in_files[i] = featurexml_files_orig[i]; // current in_files will be featureXML
                        out_files[i] = Path.Combine(NodeScratchDirectory,
                                                    Path.GetFileNameWithoutExtension(in_files[i])) + ".aligned.featureXML";
                        aligned_featurexmls.Add(out_files[i]);
                    }

                    Dictionary<string, string> map_parameters = new Dictionary<string, string> {
                        {"max_num_peaks_considered", "10000"},
                        {"ignore_charge", "false"},
                        {"threads", param_num_threads.ToString()}
                    };
                    ini_path = Path.Combine(NodeScratchDirectory, @"MapAlignerPoseClusteringDefault.ini");
                    OpenMSCommons.CreateDefaultINI(exec_path, ini_path, NodeScratchDirectory, new NodeLoggerErrorDelegate(NodeLogger.ErrorFormat), new NodeLoggerWarningDelegate(NodeLogger.WarnFormat));
                    OpenMSCommons.WriteParamsToINI(ini_path, map_parameters);
                    OpenMSCommons.WriteItemListToINI(in_files, ini_path, "in");
                    OpenMSCommons.WriteItemListToINI(out_files, ini_path, "out");
                    OpenMSCommons.WriteThresholdsToINI(param_mz_threshold, param_rt_threshold, ini_path);

                    SendAndLogMessage("Starting MapAlignerPoseClustering");
                    OpenMSCommons.RunTOPPTool(exec_path, ini_path, NodeScratchDirectory, new SendAndLogMessageDelegate(SendAndLogMessage), new SendAndLogTemporaryMessageDelegate(SendAndLogTemporaryMessage), new WriteLogMessageDelegate(WriteLogMessage), new NodeLoggerWarningDelegate(NodeLogger.WarnFormat), new NodeLoggerErrorDelegate(NodeLogger.ErrorFormat));
                    m_current_step += m_num_files;
                    ReportTotalProgress((double)m_current_step / m_num_steps);
                }

                //FeatureLinkerUnlabeledQT

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
                //save as consensus.consensusXML, filenames are stored inside, file should normally be accessed from inside CD
                out_files[0] = Path.Combine(NodeScratchDirectory, "featureXML_consensus.consensusXML");
                m_consensusxml = out_files[0];

                exec_path = Path.Combine(m_openms_dir, @"bin/FeatureLinkerUnlabeledQT.exe");
                Dictionary<string, string> fl_unlabeled_parameters = new Dictionary<string, string> {
                        {"ignore_charge", "false"},
                        {"out", out_files[0]},
                        {"threads", param_num_threads.ToString()}
                };
                ini_path = Path.Combine(NodeScratchDirectory, @"FeatureLinkerUnlabeledQTDefault.ini");
                OpenMSCommons.CreateDefaultINI(exec_path, ini_path, NodeScratchDirectory, new NodeLoggerErrorDelegate(NodeLogger.ErrorFormat), new NodeLoggerWarningDelegate(NodeLogger.WarnFormat));
                OpenMSCommons.WriteParamsToINI(ini_path, fl_unlabeled_parameters);
                OpenMSCommons.WriteItemListToINI(in_files, ini_path, "in");
                OpenMSCommons.WriteThresholdsToINI(param_mz_threshold, param_rt_threshold, ini_path);

                SendAndLogMessage("FeatureLinkerUnlabeledQT");
                OpenMSCommons.RunTOPPTool(exec_path, ini_path, NodeScratchDirectory, new SendAndLogMessageDelegate(SendAndLogMessage), new SendAndLogTemporaryMessageDelegate(SendAndLogTemporaryMessage), new WriteLogMessageDelegate(WriteLogMessage), new NodeLoggerWarningDelegate(NodeLogger.WarnFormat), new NodeLoggerErrorDelegate(NodeLogger.ErrorFormat));
                m_current_step += m_num_files;
                ReportTotalProgress((double)m_current_step / m_num_steps);
            }
        }

        private void ReadInputFiles(out List<string> orig_features, out List<string> aligned_features, out List<string> raw_files_list)
        {
            //Read in featureXmls contained in the study result folder, read only those associated with the project msf
            var all_custom_data_raw_files = EntityDataService.CreateEntityItemReader().ReadAll<ProcessingNodeCustomData>().Where(c => c.DataPurpose == "RawFiles").ToDictionary(c => c.WorkflowID, c => c);
            var all_custom_data_mzml_files = EntityDataService.CreateEntityItemReader().ReadAll<ProcessingNodeCustomData>().Where(c => c.DataPurpose == "MzMLFiles").ToDictionary(c => c.WorkflowID, c => c);
            var all_custom_data_featurexml_files = EntityDataService.CreateEntityItemReader().ReadAll<ProcessingNodeCustomData>().Where(c => c.DataPurpose == "FeatureXmlFiles").ToDictionary(c => c.WorkflowID, c => c);

            m_num_files = 0;
            foreach (var item in all_custom_data_featurexml_files)
            {
                //TODO: ugly
                m_num_files += ((string)item.Value.CustomValue).Split(',').Count();
            }

            orig_features = new List<string>(m_num_files);
            aligned_features = new List<string>(m_num_files);



            raw_files_list = new List<string>();
            foreach (var item in all_custom_data_raw_files)
            {
                var raw_files = (string)item.Value.CustomValue;
                raw_files_list = new List<string>(raw_files.Split(','));
                //TODO: do we need them? not used ATM.
            }

            foreach (var item in all_custom_data_mzml_files)
            {
                var mzml_files = (string)item.Value.CustomValue;
                var mzml_files_list = new List<string>(mzml_files.Split(','));
                //TODO: do we need them? not used ATM.
            }

            foreach (var item in all_custom_data_featurexml_files)
            {
                var featurexml_files = (string)item.Value.CustomValue;
                var featurexml_files_list = new List<string>(featurexml_files.Split(','));

                //TODO: understand implications of different worfklow IDs...
                foreach (var file_name in featurexml_files_list)
                {
                    orig_features.Add(file_name);
                }
            }
        }

        public void ExportPSMsToIdXML(string idxml_filename, bool filter = false)
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

            IdXMLExportHelper<TargetPeptideSpectrumMatch>(doc, id_run_node, filter);

            if (!filter)
            {
                var decoy_psms = EntityDataService.CreateEntityItemReader().ReadAllFlat<DecoyPeptideSpectrumMatch, MSnSpectrumInfo>();
                IdXMLExportHelper<DecoyPeptideSpectrumMatch>(doc, id_run_node, false);
            }

            doc.Save(idxml_filename);
        }

        void IdXMLExportHelper<PSMType>(XmlDocument doc, XmlElement id_run_node, bool filter = false)
            where PSMType : PeptideSpectrumMatch
        {
            bool decoy = typeof(PSMType) == typeof(DecoyPeptideSpectrumMatch);

            var psms = EntityDataService.CreateEntityItemReader().ReadAllFlat<PSMType, MSnSpectrumInfo>();

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

                if (filter && qvalue_score_val > param_q_value_threshold.Value)
                {
                    continue;
                }

                //write entry to idXML file
                var pep_id_node = doc.CreateElement("PeptideIdentification");
                id_run_node.AppendChild(pep_id_node);

                var st_attr = doc.CreateAttribute("score_type");
                st_attr.Value = "Percolator q-Value";
                pep_id_node.Attributes.Append(st_attr);

                var hsb_attr = doc.CreateAttribute("higher_score_better");
                hsb_attr.Value = "false";
                pep_id_node.Attributes.Append(hsb_attr);

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
                score_attr.Value = qvalue_score_val.ToString();
                pep_hit_node.Attributes.Append(score_attr);

                var seq_attr = doc.CreateAttribute("sequence");
                seq_attr.Value = OpenMSCommons.ModSequence(psm.Item1.Sequence, psm.Item1.Modifications);
                pep_hit_node.Attributes.Append(seq_attr);

                var charge_attr = doc.CreateAttribute("charge");
                charge_attr.Value = psm.Item1.Charge.ToString();
                pep_hit_node.Attributes.Append(charge_attr);

                // posterior probability score as UserParam for Fido
                var user_param = doc.CreateElement("UserParam");
                pep_hit_node.AppendChild(user_param);

                var type_attr = doc.CreateAttribute("type");
                type_attr.Value = "float";
                user_param.Attributes.Append(type_attr);

                var name_attr = doc.CreateAttribute("name");
                name_attr.Value = "Posterior Probability_score";
                user_param.Attributes.Append(name_attr);

                var value_attr = doc.CreateAttribute("value");
                value_attr.Value = (1.0 - pep_score_val).ToString();
                user_param.Attributes.Append(value_attr);

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

        void RunIDMapper(string consensusxml_file, string idxml_file, string result_consensusxml_file)
        {
            var exec_path = Path.Combine(m_openms_dir, @"bin/IDMapper.exe");
            var ini_path = Path.Combine(NodeScratchDirectory, @"IDMapper.ini");
            OpenMSCommons.CreateDefaultINI(exec_path, ini_path, NodeScratchDirectory, new NodeLoggerErrorDelegate(NodeLogger.ErrorFormat), new NodeLoggerWarningDelegate(NodeLogger.WarnFormat));
            Dictionary<string, string> idmapper_parameters = new Dictionary<string, string> {
                        {"mz_tolerance", param_id_mapping_mz_threshold.Value.Tolerance.ToString()},
                        {"rt_tolerance", (param_id_mapping_rt_threshold.Value * 60.0).ToString()},
                        {"in", consensusxml_file},
                        {"id", idxml_file},
                        {"out", result_consensusxml_file},
                        {"threads", param_num_threads.ToString()}
            };
            OpenMSCommons.WriteParamsToINI(ini_path, idmapper_parameters);
            OpenMSCommons.WriteNestedParamToINI(ini_path, new Triplet("consensus", "use_subelements", "true"));

            SendAndLogMessage("IDMapper");
            OpenMSCommons.RunTOPPTool(exec_path, ini_path, NodeScratchDirectory, new SendAndLogMessageDelegate(SendAndLogMessage), new SendAndLogTemporaryMessageDelegate(SendAndLogTemporaryMessage), new WriteLogMessageDelegate(WriteLogMessage), new NodeLoggerWarningDelegate(NodeLogger.WarnFormat), new NodeLoggerErrorDelegate(NodeLogger.ErrorFormat));
        }

        void RunPeptideIndexer(string input_file, string output_file)
        {
            string exec_path = "";
            string ini_path = "";

            string fasta_filename = param_fasta_db.Value.FullPhysicalFileName;

            // check whether selected FASTA file exists
            if (File.Exists(fasta_filename) == false)
            {
                SendAndLogErrorMessage("Cannot access FASTA file because the file cannot be found!");
                throw new FileNotFoundException(String.Format("The FASTA file {0} cannot be found!", param_fasta_db.Value.VirtualFileName), fasta_filename);
            }

            string pi_fasta_fn = Path.Combine(NodeScratchDirectory, @"peptide_indexer.fasta");
            ProcessingServices.FastaFileService.CreateOriginalFastaFile(param_fasta_db.Value, pi_fasta_fn, true); //TODO true / false?

            string decoy_filename = Path.Combine(
                Path.GetDirectoryName(fasta_filename),
                Path.GetFileNameWithoutExtension(fasta_filename) + "_reversed" + Path.GetExtension(fasta_filename));
            if (File.Exists(decoy_filename))
            {
                //decoy filename exists => we'll also add decoy sequences to our DB
                exec_path = Path.Combine(m_openms_dir, @"bin/DecoyDatabase.exe");
                ini_path = Path.Combine(NodeScratchDirectory, @"DecoyDatabase.ini");
                OpenMSCommons.CreateDefaultINI(exec_path, ini_path, NodeScratchDirectory, new NodeLoggerErrorDelegate(NodeLogger.ErrorFormat), new NodeLoggerWarningDelegate(NodeLogger.WarnFormat));
                Dictionary<string, string> dd_parameters = new Dictionary<string, string> {
                        {"out", pi_fasta_fn},
                        {"append", "true"},
                        {"decoy_string_position", "prefix"},
                        {"decoy_string", "REV_"},
                        {"threads", param_num_threads.ToString()}
                };
                OpenMSCommons.WriteParamsToINI(ini_path, dd_parameters);
                string[] in_list = new string[1];
                in_list[0] = pi_fasta_fn;
                OpenMSCommons.WriteItemListToINI(in_list, ini_path, "in");

                SendAndLogMessage("DecoyDatabase");
                OpenMSCommons.RunTOPPTool(exec_path, ini_path, NodeScratchDirectory, new SendAndLogMessageDelegate(SendAndLogMessage), new SendAndLogTemporaryMessageDelegate(SendAndLogTemporaryMessage), new WriteLogMessageDelegate(WriteLogMessage), new NodeLoggerWarningDelegate(NodeLogger.WarnFormat), new NodeLoggerErrorDelegate(NodeLogger.ErrorFormat));
            }

            //run PeptideIndexer
            exec_path = Path.Combine(m_openms_dir, @"bin/PeptideIndexer.exe");
            ini_path = Path.Combine(NodeScratchDirectory, @"PeptideIndexer.ini");
            OpenMSCommons.CreateDefaultINI(exec_path, ini_path, NodeScratchDirectory, new NodeLoggerErrorDelegate(NodeLogger.ErrorFormat), new NodeLoggerWarningDelegate(NodeLogger.WarnFormat));
            Dictionary<string, string> pi_parameters = new Dictionary<string, string> {
                        {"in", input_file},
                        {"fasta", pi_fasta_fn},
                        {"out", output_file},
                        {"prefix", "true"},
                        {"decoy_string", "REV_"},
                        {"missing_decoy_action", "warn"},
                        {"allow_unmatched", "true"},
                        {"threads", param_num_threads.ToString()}
            };
            OpenMSCommons.WriteParamsToINI(ini_path, pi_parameters);
            OpenMSCommons.WriteNestedParamToINI(ini_path, new Triplet("enzyme", "name", param_enzyme.Value));
            OpenMSCommons.WriteNestedParamToINI(ini_path, new Triplet("enzyme", "specificity", "none"));

            SendAndLogMessage("PeptideIndexer");
            OpenMSCommons.RunTOPPTool(exec_path, ini_path, NodeScratchDirectory, new SendAndLogMessageDelegate(SendAndLogMessage), new SendAndLogTemporaryMessageDelegate(SendAndLogTemporaryMessage), new WriteLogMessageDelegate(WriteLogMessage), new NodeLoggerWarningDelegate(NodeLogger.WarnFormat), new NodeLoggerErrorDelegate(NodeLogger.ErrorFormat));
        }

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
                var new_abundance_column = PropertyAccessorFactory.CreateDynamicPropertyAccessor<DechargedPeptideEntity, double?>(
                    new PropertyDescription()
                    {
                        DisplayName = "Abundance " + (i + 1),
                        FormatString = "0.000e-00"
                    }
                );
                new_abundance_column.GridDisplayOptions.ColumnWidth = 80;
                EntityDataService.RegisterProperties(ProcessingNodeNumber, new_abundance_column);
                column_names.Add(new_abundance_column.Name);
            }

            // parse peptides
            var new_peptide_items = new List<DechargedPeptideEntity>();
            int idCounter = 1;
            while ((line = reader.ReadLine()) != null)
            {
                string[] items = line.Split('\t');
                string pep_seq = items[0].Substring(1, items[0].Length - 2);
                string protein_accs = items[1].Substring(1, items[1].Length - 2);
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
                var new_abundance_column = PropertyAccessorFactory.CreateDynamicPropertyAccessor<QuantifiedProteinEntity, double?>(
                    new PropertyDescription()
                    {
                        DisplayName = "Abundance " + (i + 1),
                        FormatString = "0.000e-00"
                    }
                );
                new_abundance_column.GridDisplayOptions.ColumnWidth = 80;
                EntityDataService.RegisterProperties(ProcessingNodeNumber, new_abundance_column);
                column_names.Add(new_abundance_column.Name);
            }

            // parse peptides
            var new_protein_items = new List<QuantifiedProteinEntity>();
            int idCounter = 1;
            while ((line = reader.ReadLine()) != null)
            {
                string[] items = line.Split('\t');
                string protein_accs = items[0].Substring(1, items[0].Length - 2);
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
    }
}
