using System.Collections.Generic;
using System.Linq;
using Thermo.Magellan.BL.Processing;
using Thermo.Magellan.BL.Processing.Interfaces;
using Thermo.Magellan.MassSpec;
using Thermo.Magellan.Processing.Parameters;
using Thermo.Magellan.Processing.Parameters.Attributes;
using Thermo.Magellan.Processing.Workflows;
using Thermo.Magellan.Processing.Workflows.Enums;
using Thermo.Magellan.Processing.Workflows.Legal;



namespace PD.OpenMS.AdapterNodes
{
    [ProcessingNode("50AFD3C6-CE57-40AC-ACEF-FB9C5A1DFEFB",

		Category = ProcessingNodeCategories.DataExport,
		DisplayName = "Sample Filter Node",
		Description = "Filters spectra according to charge state",
		MainVersion = 0,
		MinorVersion = 2)]
	[PublisherInformation(Publisher = "Thermo Scientific")]
	[ProcessingNodeConstraints(UsageConstraint = UsageConstraint.OnlyOncePerWorkflow)] //OnlyOncePerWorkflow: the node is allowed once
																					   //in a workflow
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

	[ConnectionPoint(
	"OutgoingSpectra",
	ConnectionDirection = ConnectionDirection.Outgoing,
	ConnectionMultiplicity = ConnectionMultiplicity.Multiple,
	ConnectionMode = ConnectionMode.Manual,
	ConnectionRequirement = ConnectionRequirement.Optional,
	ConnectionDataHandlingType = ConnectionDataHandlingType.InMemory)]
	[ConnectionPointDataContract(
	"OutgoingSpectra",
	MassSpecDataTypes.MSnSpectra,
	DataTypeAttributes = new[] { MassSpecDataTypeAttributes.Filtered })]
	public class SampleFilterNode
    : SpectrumProcessingNode
    {
		[IntegerParameter(
			Category = "1. Most important parameter",
			DisplayName = "Upper Charge Threshold",
			DefaultValue = "2",
			MinimumValue = "1",
			MaximumValue = "5")]
		public IntegerParameter UpperCharge;

		[IntegerParameter(
			Category = "1. Most important parameter",
			DisplayName = "Lower Charge Threshold",
			DefaultValue = "2",
			MinimumValue = "1",
			MaximumValue = "5")]
		public IntegerParameter LowerCharge;
		protected override MassSpectrumCollection ProcessSpectra(MassSpectrumCollection spectra)
        {
			// throw new NotImplementedException();
			return new MassSpectrumCollection();

		}
    }
}
