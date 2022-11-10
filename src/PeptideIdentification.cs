using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Thermo.Magellan.BL.Data;
using Thermo.Magellan.EntityDataFramework.Constants;
using Thermo.Magellan.BL.Processing.Interfaces;
using Thermo.Magellan.EntityDataFramework;
using Thermo.Magellan.MassSpec;
using Thermo.Magellan.Proteomics;
using Thermo.Magellan.Proteomics.FastaFileHandling;
using Thermo.PD.EntityDataFramework;
using Thermo.Magellan.PeptideIdentificationNodes;
using Thermo.Magellan.Processing.Parameters;
using Thermo.Magellan.Processing.Parameters.Attributes;
using Thermo.Magellan.Processing.Workflows;
using Thermo.Magellan.Processing.Workflows.Enums;
using Thermo.Magellan.Processing.Workflows.Legal;
using Thermo.Magellan.Processing.Workflows.ProcessingNodeScores;
using Thermo.Proteomics.Services.Interfaces.Data;
using Thermo.Proteomics.Services.Interfaces;



namespace PD.OpenMS.AdapterNodes
{

    [ProcessingNode("55511F33-59BE-41C7-9AD2-CEFC6EBB8FC8",
       Category = ProcessingNodeCategories.SampleNodes,
       DisplayName = "Peptide Identification",
       MainVersion = 0,
       MinorVersion = 11,
       Description = "Perform a Peptide Identifier search.",
       Visible = true)]
    [PublisherInformation(Publisher = "Thermo Scientific")]
    [ProcessingNodeAppearance(
        ImageSmallSource = "IMG_CubeBlue_16x16.png",
        ImageLargeSource = "IMG_CubeBlue_32x32.png")]
    [LegalInformation(LegalNotice = @"PeptideIdentifier® is used under license from ....
Protected by U.S. patents: 5, .....")]
    [ConnectionPoint(
        "IncomingSpectra",
        ConnectionDirection = ConnectionDirection.Incoming,
        ConnectionMultiplicity = ConnectionMultiplicity.Single,
        ConnectionMode = ConnectionMode.Manual,
        ConnectionRequirement = ConnectionRequirement.RequiredAtDesignTime,
        ConnectionDisplayName = ProcessingNodeCategories.SpectrumAndFeatureRetrieval,
        ConnectionDataHandlingType = ConnectionDataHandlingType.InMemory)]
    [ConnectionPointDataContract(
        "IncomingSpectra",
        MassSpecDataTypes.MSnSpectra)]
    /*<DiscovererDoc>
	* <Title>Create outgoing connection point for 'Identifications'</Title>
	* <Description>Definition of a connection point, that allows other nodes expecting incoming 'Identifications' to connect.</Description>
	* <Tags>
	* <Tag>node connection point</Tag>
	* <Tag>outgoing connection</Tag>
	* </Tags>
	*  </DiscovererDoc>
	*/
    [ConnectionPoint(
        "OutgoingIdentifications",
        ConnectionDirection = ConnectionDirection.Outgoing,
        ConnectionMultiplicity = ConnectionMultiplicity.Single,
        ConnectionMode = ConnectionMode.Manual,
        ConnectionRequirement = ConnectionRequirement.RequiredAtDesignTime,
        ConnectionDisplayName = ProcessingNodeCategories.PsmValidation)]
    [ConnectionPointDataContract(
        "OutgoingIdentifications",
        ProteomicsDataTypes.Psms,
        DataTypeAttributes = new[] { ProteomicsDataTypeAttributes.ScoredWithNativeScore })]
    [ConnectionPointDataContract(
        "OutgoingIdentifications",
        ProteomicsDataTypes.Proteins)]
    public class PeptideIdentification
        : PeptideAndProteinIdentificationNode
    {
        #region Private Fields

        public static readonly string ProgressInfoTag = "PeptideIdentifierSearch";
        private PropertyAccessor<TargetProtein> m_proteinColumn = null;

        #endregion

        #region General Search Parameters
        [FastaFileParameter(Category = "1. Input Data",
        DisplayName = "Protein Database",
        Description = "The sequence database to be searched.",
        IntendedPurpose = ParameterPurpose.SequenceDatabase,
        ValueRequired = true,
        Position = 1)]

        public FastaFileParameter FastaDatabase;

        [EnzymeParameter(Category = "1. Input Data",
            DisplayName = "Enzyme Name",
            Description = "The reagent used for protein digestion.",
            IntendedPurpose = ParameterPurpose.CleavageReagent,
            ValueRequired = true,
            DefaultValue = "Trypsin (Full)",
            Position = 2)]
        public EnzymeParameter Enzyme;

        [IntegerParameter(Category = "1. Input Data",
            DisplayName = "Maximum Peptides Considered",
            Description = "The maximum number of peptides searched/scored per spectrum.",
            DefaultValue = "20", MinimumValue = "1", MaximumValue = "1000",
            Position = 3, IsAdvanced = true)]
        public IntegerParameter MaxPeptidesConsidered;


        #endregion

        #region Static Modifications
        /// <DiscovererDoc>/// <Title>usage of parameter groups</Title>/// <Description>Example for the usage of parameter groups. Here groups are used to exclude already selected modifications from the selection list. This avoids the duplicated usage of a modification.</Description>/// <Tags>/// <Tag>parameter groups</Tag>/// </Tags>/// </DiscovererDoc>

        [ModificationParameter(Category = "6. Static Modifications",
            DisplayName = "1. Static Modification",
            Description = "Select any known or suspected modification. Static modifications are applied universally, to every instance of the specified residue(s) or terminus. ",
            ModificationType = ModificationType.Static,
            IsMultiSelect = true,
            IntendedPurpose = ParameterPurpose.StaticModification,
            Position = 5)]
        [ParameterGroup(GroupName = "Static Modifications", IsDominant = true, IsValueUnique = true)]
        [ParameterGroup(GroupName = "Modifications", IsDominant = true, IsControlledValueUnique = true)]
        public ModificationParameter StatMod1;

        [ModificationParameter(Category = "6. Static Modifications",
            DisplayName = "1. Static Terminal Modification",
            Description = "Select any known or suspected modification. Static modifications are applied universally, to every instance of the specified residue(s) or terminus. ",
            ModificationType = ModificationType.Static,
            PositionType = AminoAcidModificationPositionType.Any_N_Terminus,
            IntendedPurpose = ParameterPurpose.StaticTerminalModification,
            Position = 6)]
        public ModificationParameter TerminalStaticModification1;

        #endregion

        #region Global Parameters
        /// <DiscovererDoc>/// <Title>configuration parameter, definition</Title>/// <Description>Example for a configuration parameter, that is only visible in the Administration -> Configuration section. Configuration parameters are recommended for /// settings which need to be set by the user but are usually unchanged for a longer period of time. E.g. the URL of an external server, the package size to send data.</Description>/// <Tags>/// <Tag>configuration parameter</Tag>/// <Tag>parameter</Tag>/// </Tags>/// </DiscovererDoc>
        [DoubleParameter(Category = "1. Probability Score Confidence Thresholds",
            DisplayName = "Strict Confidence Probability Score",
            DefaultValue = "8",
            IntendedPurpose = ParameterPurpose.DefaultFDRCalculatorParameter,
            PurposeDetails = PeptideValidationPurpose.DefaultStrictScoreThreshold,
            IsHidden = true,
            IsConfig = true)]
        public DoubleParameter MyScoreHighConfidenceThreshold;

        [DoubleParameter(Category = "1. Probability Score Confidence Thresholds",
            DisplayName = "Relaxed Confidence Probability Score",
            DefaultValue = "5",
            IntendedPurpose = ParameterPurpose.DefaultFDRCalculatorParameter,
            PurposeDetails = PeptideValidationPurpose.DefaultRelaxedScoreThreshold,
            IsHidden = true,
            IsConfig = true)]
        public DoubleParameter MyScoreMiddleConfidenceThreshold;

        #endregion

        #region Scores

        /// <DiscovererDoc>/// <Title>definition of protein score</Title>/// <Description>Definition of protein score by a GUID of the score calculator. This score is calculated and used in the 'Protein Scorer' node of the consensus workflow, after /// the PSMs and peptides are validated and filtered.</Description>/// <Tags>/// <Tag>score</Tag>/// <Tag>protein score</Tag>/// <Tag>definition</Tag>/// </Tags>/// </DiscovererDoc>

        [StringParameter(Category = "Protein Scoring",
            DisplayName = "Default Protein Score",
            Description = "Specifies the guid of the protein score.",
            DefaultValue = "FDF0E2B4-10AA-4205-A928-789B401E2737",
            IntendedPurpose = ParameterPurpose.ProteinValidation,
            PurposeDetails = ProteinValidationPurpose.DefaultProteinScore,
            IsAdvanced = true, IsHidden = true)]
        public StringParameter ProteinScoreGuid;


        /// <DiscovererDoc>/// <Title>definition of psm scores</Title>/// <Description>Definition of psm scores.</Description>/// <Tags>/// <Tag>score</Tag>/// <Tag>psm score</Tag>/// <Tag>definition</Tag>/// </Tags>/// </DiscovererDoc>


        [Score(ProteomicsDataTypes.Psms, isMainScore: true, category: ScoreCategoryType.HigherIsBetter,
            DisplayName = "XCorr",
            Description = "The score of the cross correlation with the theoretical spectrum (Peptide Identifier).",
            FormatString = "F2",
            Guid = "BA959F73-560A-497D-8A5B-50FE6B492CA0")]
        public Score MyScore1;

        [Score(ProteomicsDataTypes.Psms, isMainScore: false, category: ScoreCategoryType.HigherIsBetter,
            DisplayName = "Probability",
            Description = "The probability that this peptide was identified by chance (Peptide Identifier).",
            FormatString = "F2",
            Guid = "4308AFCF-9142-45D2-91B8-A7A39C0BAE31")]
        public Score ProbabilityScore;

        #endregion
        protected override void OnAllSpectraSentForSearch()
        {
            //      throw new NotImplementedException();
        }

        protected override void OnSpectraSentForSearch(IProcessingNode sender, MassSpectrumCollection spectraSent)
        {
            var filter = new SpectrumFilter();
            filter.SetMSOrderFilter(EqualityConditionOperator.IsNot, MSOrderType.MS1);
            filter.SetPolarityFilter(EqualityConditionOperator.Is, PolarityType.Positive);
            var spectraToSearch = filter.FilterSpectra(spectraSent);



            if (spectraSent.Count != spectraToSearch.Count)
            {
                WriteLogMessage(MessageLevel.Debug, "{0}/{1} spectra don't meet the input criteria.", spectraSent.Count - spectraToSearch.Count, spectraSent.Count);
            }

            if (spectraToSearch.Count == 0)
            {
                return;
            }

            // Start the peptide identification
            var psmsCollection = IdentifyPeptides(spectraSent, false);

            PersistTargetPeptideSpectrumMatches(psmsCollection);

            // if a decoy search should be performed, results of decoy search are stored
            if (DecoySearch.Value)
            {
                // perform decoy search
                var decoyPsmsCollection = IdentifyPeptides(spectraSent, true);
                // Store all decoy psms to the result file.
                // Note: In this example node the transfer of the protein entry and the correct reference will be automatically
                // done in the PersistTargetPeptideSpectrumMatches method, which therefore gets the used FastaFile as parameter.

                PersistDecoyPeptideSpectrumMatches(decoyPsmsCollection);

                ProcessingServices.Get<IPeptideSpectrumMatchService>(true).TransferAndPersistConnectedDecoyProteins(
                this,
                decoyPsmsCollection,
                FastaDatabase.Value);
            }
        }

        private PeptideSpectrumMatchesCollection IdentifyPeptides(IEnumerable<MassSpectrum> spectraSent, bool isDecoy)
        {
            string fileName = FastaDatabase.Value.FullPhysicalFileName;
            string decoyFileName = Path.Combine(
            Path.GetDirectoryName(fileName),
            Path.GetFileNameWithoutExtension(fileName) + "_reversed" + Path.GetExtension(fileName));

            // check whether selected FASTA file exists
            if (File.Exists(FastaDatabase.Value.FullPhysicalFileName) == false)
            {
                SendAndLogErrorMessage("Cannot access FASTA file because the file cannot be found!");
                throw new FileNotFoundException(String.Format("The FASTA file {0} cannot be found!", FastaDatabase.Value.VirtualFileName), FastaDatabase.Value.FullPhysicalFileName);
            }

            if (isDecoy && !File.Exists(decoyFileName))
            {
                decoyFileName = FastaFileReverser.CreateReverseDatabase(FastaDatabase.Value.FullPhysicalFileName);
            }
            var psmsCollection = new PeptideSpectrumMatchesCollection();



            // Read the protein from the selected FASTA database
            using (var reader = new FastaFileReader(isDecoy ? decoyFileName : fileName))
            {

                var fastaEntry = reader.First();



                IPeptideSpectrumMatchService peptideSpectrumMatchService = ProcessingServices.Get<IPeptideSpectrumMatchService>(true);



                var random = new Random();



                foreach (var spectrum in spectraSent)
                {
                    // "Digest" the current protein into peptides
                    var peptideSequences = fastaEntry.Sequence.Split(Enzyme.Enzyme.CleavageSites.ToCharArray(), StringSplitOptions.RemoveEmptyEntries);



                    var peptideMatches = new List<PeptideMatch>();



                    // Get discoverer protein IDs. -> these have to be added to the peptideMatch!
                    //
                    // Note: The description of each protein in the FASTA database was replaced during the upload with a unique discoverer database id
                    //
                    // Note: If the unique protein id is missing, because of for e.g. the FASTA File wasn't uploaded and registered before,
                    // then all the new identified proteins must be registered now to obtain the required unique protein id
                    //
                    // In this example node the transfer of the protein entry and the correct reference will be automatically
                    // done in the PersistTargetPeptideSpectrumMatches method, which therefore gets the used FastaFile as parameter.
                    //
                    var proteinID = Int32.Parse(fastaEntry.TitleLine.Substring(1));



                    // Iterate over the identified peptides, but take at maximum MaxPeptidesConsidered peptides
                    foreach (var peptideSequence in peptideSequences.Take(MaxPeptidesConsidered.Value))
                    {
                        // just make up some scores
                        double xscore = random.Next(0, 1000) / 100.0;
                        double probabilityScore = random.Next(0, 50) / 100.0;



                        // Create a new peptide hit using the peptide hit service
                        var peptideMatch = peptideSpectrumMatchService.CreatePeptideMatch(
                        peptideSequence,
                        (short)peptideSequence.Length,
                        (short)random.Next(0, peptideSequence.Length),
                        0);



                        // Add peptide scores
                        peptideMatch.AddScore(Scores["MyScore1"].Name, xscore);
                        peptideMatch.AddScore(Scores["ProbabilityScore"].Name, probabilityScore);



                        // Set the confidence level to low. Validation is performed by a child node
                        peptideMatch.Confidence = MatchConfidence.Low;



                        // Add discoverer protein IDs.
                        // Note: The description of each protein in the FASTA database was replaced during the upload with a unique discoverer database id
                        //
                        // Note: If the unique protein id is missing, because of for e.g. the FASTA File wasn't uploaded and registered before,
                        // then all the new identified proteins must be registered now to obtain the required unique protein id
                        //
                        // In this example node the transfer of the protein entry and the correct reference will be automatically
                        // done in the PersistTargetPeptideSpectrumMatches method, which therefore gets the used FastaFile as parameter.
                        //
                        peptideMatch.AddProteinID(proteinID);

                        // foreach (var index in indices)
                        // {
                        // // Add peptide modifications at the position of the amino acid within the peptide sequence
                        // peptideMatch.AddModification(new PeptideModification(StatMod_1.Modification.ID, index));
                        // }
                        // }
                        //}
                       // peptideMatch.AddUnknownModification(new PeptideUnknownModification(item.Key - 1, item.Value, item.Value.ToString("F5")));
                        // Add the peptide hit to the list of peptide hits identified for the current spectrum
                        peptideMatches.Add(peptideMatch);
                    }



                    var psms = new PeptideSpectrumMatches(
                    spectrum.Header.SpectrumID,
                    peptideMatches.Count,
                    peptideMatches);
                    peptideSpectrumMatchService.CalculateAndAssignRanksAndDeltaScores(MainPsmScore, psms);

                    // Add all PSMs identified for this spectrum
                    psmsCollection.Add(psms);

                    // Read the next protein from FASTA database
                    fastaEntry = reader.Next() ?? reader.First();
                }
            }



            return psmsCollection;
        }
        /// <summary>
        /// This method creates a new <see cref="PeptideMatch"/> for the specified PSM with its modifications and proteins.
        /// </summary>/// <param name="psm">The original PSM.</param>
        /// <param name="scoreProperty">The property to retrieve the psm score from the original psm.</param>
        /// <param name="modifications">The modifications of the original PSM.</param>
        /// <param name="proteins">The proteins of the original PSM.</param>
        /// <returns>The PeptideMatch that is a copy of the original PSM, but with unknow modifications.</returns>
        private PeptideMatch CreatePeptideMatch<TPsm, TProtein>(
        TPsm psm,
        IEnumerable<HierarchicalEntity<Modification>> modifications,
        IEnumerable<TProtein> proteins,
        PropertyAccessor<TPsm> scoreProperty)

        where TPsm : PeptideSpectrumMatch
        where TProtein : ReportProtein
        {
            var newPeptide = ProcessingServices.Get<IPeptideSpectrumMatchService>(true).CreatePeptideMatch(
                psm.Sequence,
                psm.TotalIonsCount,
                psm.MatchedIonsCount,
                psm.MissedCleavages.HasValue ? psm.MissedCleavages.Value : (short)0
                );
            newPeptide.SearchEngineRank = psm.SearchEngineRank;

            if (psm.DeltaCn.HasValue)
            {
                newPeptide.DeltaCn = psm.DeltaCn.Value;
            }

            var targetPsm = psm as TargetPeptideSpectrumMatch;
            if (targetPsm != null && targetPsm.DeltaScore.HasValue)
            {
                newPeptide.DeltaScore = targetPsm.DeltaScore.Value;
            }

            newPeptide.AddScore("Score", (double)scoreProperty.GetValue(psm));

            var newUnknownModsDictionary = new Dictionary<int, double>();

            foreach (var modification in modifications)
            {
                var position = (int)modification.ConnectionProperties.GetValue("Position");

                var modMass = modification.EntityItem.DeltaMonoisotopicMass;

                double existingMass;
                if (newUnknownModsDictionary.TryGetValue(position, out existingMass))
                {
                    newUnknownModsDictionary[position] = existingMass + modMass;
                }
                else
                {
                    newUnknownModsDictionary[position] = modMass;
                }
            }

            if (newUnknownModsDictionary.Count > 0)
            {
                foreach (var item in newUnknownModsDictionary)
                {
                    newPeptide.AddUnknownModification(new PeptideUnknownModification(item.Key - 1, item.Value, item.Value.ToString("F5")));
                }

                newUnknownModsDictionary.Clear();
            }

            foreach (var protein in proteins)
            {
                newPeptide.AddProteinSequenceIds(protein.UniqueSequenceID);
            }

            return newPeptide;
        }
    }

}

