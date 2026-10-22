"""Static schema definitions for the ADME (OSDU) connector tables.

OSDU records share a common envelope (``id``, ``kind``, ``version``,
``acl``, ``legal``, ``createTime``, ``modifyTime``, ``data``...). The
``data`` payload differs per OSDU kind.

Approach: top-level envelope fields are first-class typed columns;
``data.*`` fields are flattened to a curated subset of typed columns
per kind based on the canonical OSDU master-data shapes documented
in ``adme_api_doc.md``. Complex nested structures (SpatialLocation,
SampleAcquisitionDetail, FacilityNameAliases, VerticalMeasurements,
ExtensionProperties, etc.) are kept as JSON-encoded strings to avoid
fragile StructType layouts that would break on minor schema drift
across OSDU versions and ADME instances.

Reference kinds (per ``adme_api_doc.md``):
  - Wellbore       → osdu:wks:master-data--Wellbore:1.0.0
  - Reservoir      → osdu:wks:master-data--Reservoir:1.2.0
  - Rock_and_Fluid → osdu:wks:master-data--Sample:2.1.0
"""

from pyspark.sql.types import (
    ArrayType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)


# ---------------------------------------------------------------------------
# Connector tables -> OSDU kind strings
# ---------------------------------------------------------------------------
#
# Use the wildcard form for queries so the connector picks up minor
# schema-version updates (e.g. 1.0.0 -> 1.0.1) without code changes;
# the canonical version is recorded for documentation only.
TABLE_TO_KIND_QUERY: dict[str, str] = {
    "Wellbore": "osdu:wks:master-data--Wellbore:*",
    "Reservoir": "osdu:wks:master-data--Reservoir:*",
    "Rock_and_Fluid": "osdu:wks:master-data--Sample:*",
    # Seismic catalog (metadata records; trace data itself stays in ADME / SDMS)
    "SeismicAcquisitionSurvey": "osdu:wks:master-data--SeismicAcquisitionSurvey:*",
    "SeismicProcessingProject": "osdu:wks:master-data--SeismicProcessingProject:*",
    "SeismicTraceData": "osdu:wks:work-product-component--SeismicTraceData:*",
    "SeismicBinGrid": "osdu:wks:work-product-component--SeismicBinGrid:*",
    "SeismicFileDataset": "osdu:wks:dataset--FileCollection.SEGY:*",
}

TABLE_TO_KIND_CANONICAL: dict[str, str] = {
    "Wellbore": "osdu:wks:master-data--Wellbore:1.0.0",
    "Reservoir": "osdu:wks:master-data--Reservoir:1.2.0",
    "Rock_and_Fluid": "osdu:wks:master-data--Sample:2.1.0",
    "SeismicAcquisitionSurvey": "osdu:wks:master-data--SeismicAcquisitionSurvey:1.2.0",
    "SeismicProcessingProject": "osdu:wks:master-data--SeismicProcessingProject:1.2.0",
    "SeismicTraceData": "osdu:wks:work-product-component--SeismicTraceData:1.3.0",
    "SeismicBinGrid": "osdu:wks:work-product-component--SeismicBinGrid:1.1.0",
    "SeismicFileDataset": "osdu:wks:dataset--FileCollection.SEGY:1.1.0",
}

SUPPORTED_TABLES: list[str] = list(TABLE_TO_KIND_QUERY.keys())


# ---------------------------------------------------------------------------
# Common OSDU envelope fields shared by every record kind
# ---------------------------------------------------------------------------
def _envelope_fields() -> list[StructField]:
    return [
        StructField("id", StringType(), False),
        StructField("kind", StringType(), True),
        # ``version`` is microsecond-precision Unix epoch encoded as int64.
        StructField("version", LongType(), True),
        StructField("createTime", StringType(), True),
        StructField("createUser", StringType(), True),
        StructField("modifyTime", StringType(), True),
        StructField("modifyUser", StringType(), True),
        # acl/legal are small structured objects; flatten the most useful
        # arrays so they're queryable without a JSON parse.
        StructField("acl_owners", ArrayType(StringType()), True),
        StructField("acl_viewers", ArrayType(StringType()), True),
        StructField("legal_legaltags", ArrayType(StringType()), True),
        StructField("legal_status", StringType(), True),
        StructField(
            "legal_otherRelevantDataCountries", ArrayType(StringType()), True
        ),
        # Free-form OSDU bits we keep as JSON for fidelity without
        # committing to a specific Spark struct shape.
        StructField("tags_json", StringType(), True),
        StructField("meta_json", StringType(), True),
        # Full original ``data`` envelope as JSON — gives downstream
        # users an escape hatch for fields not lifted to typed columns.
        StructField("data_json", StringType(), True),
    ]


# ---------------------------------------------------------------------------
# Wellbore (osdu:wks:master-data--Wellbore:1.0.0)
# ---------------------------------------------------------------------------
WELLBORE_SCHEMA: StructType = StructType(
    _envelope_fields()
    + [
        StructField("FacilityID", StringType(), True),
        StructField("FacilityName", StringType(), True),
        StructField("FacilityTypeID", StringType(), True),
        StructField("WellID", StringType(), True),
        StructField("KickOffWellboreID", StringType(), True),
        StructField("StatusSummary", StringType(), True),
        StructField("TargetFormation", StringType(), True),
        StructField("FacilityNameAliases_json", StringType(), True),
        StructField("FacilityOperators_json", StringType(), True),
        StructField("VerticalMeasurements_json", StringType(), True),
        StructField("SpatialLocation_json", StringType(), True),
        StructField("GeoContexts_json", StringType(), True),
        StructField("DrillingReasons_json", StringType(), True),
        StructField("InitialCompletion_json", StringType(), True),
        StructField("ExtensionProperties_json", StringType(), True),
    ]
)


# ---------------------------------------------------------------------------
# Reservoir (osdu:wks:master-data--Reservoir:1.2.0)
# ---------------------------------------------------------------------------
RESERVOIR_SCHEMA: StructType = StructType(
    _envelope_fields()
    + [
        StructField("ReservoirID", StringType(), True),
        StructField("ReservoirName", StringType(), True),
        StructField("ReservoirType", StringType(), True),
        StructField("ReservoirDescription", StringType(), True),
        StructField("FieldID", StringType(), True),
        StructField("BasinID", StringType(), True),
        StructField("FormationID", StringType(), True),
        StructField("FluidTypeID", StringType(), True),
        StructField("DepthTopMD", DoubleType(), True),
        StructField("DepthBaseMD", DoubleType(), True),
        StructField("DepthTopTVD", DoubleType(), True),
        StructField("DepthBaseTVD", DoubleType(), True),
        StructField("GrossThickness", DoubleType(), True),
        StructField("NetPayThickness", DoubleType(), True),
        StructField("PorosityAverage", DoubleType(), True),
        StructField("WaterSaturationAverage", DoubleType(), True),
        StructField("PermeabilityHorizontal", DoubleType(), True),
        StructField("PermeabilityVertical", DoubleType(), True),
        StructField("InitialReservoirPressure", DoubleType(), True),
        StructField("ReservoirTemperature", DoubleType(), True),
        StructField("GeoContexts_json", StringType(), True),
        StructField("NameAliases_json", StringType(), True),
        StructField("ExtensionProperties_json", StringType(), True),
    ]
)


# ---------------------------------------------------------------------------
# Rock_and_Fluid (osdu:wks:master-data--Sample:2.1.0)
# ---------------------------------------------------------------------------
# The OSDU Sample kind keeps almost everything inside the
# ``data.SampleAcquisition`` nested struct. Lift the most useful
# scalars to typed columns; preserve the whole nested object as JSON
# for fidelity.
ROCK_AND_FLUID_SCHEMA: StructType = StructType(
    _envelope_fields()
    + [
        StructField("SampleAcquisitionJobID", StringType(), True),
        StructField("SampleAcquisitionTypeID", StringType(), True),
        StructField("SampleAcquisitionContainerID", StringType(), True),
        StructField("AcquisitionStartDate", StringType(), True),
        StructField("AcquisitionEndDate", StringType(), True),
        StructField("CollectionServiceCompanyID", StringType(), True),
        StructField("HandlingServiceCompanyID", StringType(), True),
        StructField("WellboreID", StringType(), True),
        StructField("ToolKind", StringType(), True),
        StructField("RunNumber", StringType(), True),
        StructField("TopDepth", DoubleType(), True),
        StructField("BaseDepth", DoubleType(), True),
        StructField("FormationPressure", DoubleType(), True),
        StructField("FormationTemperature", DoubleType(), True),
        StructField("SampleAcquisition_json", StringType(), True),
        StructField("ExtensionProperties_json", StringType(), True),
    ]
)


# ---------------------------------------------------------------------------
# Seismic catalog kinds
# ---------------------------------------------------------------------------
# Field names follow the OSDU data definitions for the seismic master-data,
# work-product-component and dataset kinds. Confidence is medium (same as
# Reservoir): verify against the Schema Service of the target partition.
# Every record also keeps the full ``data_json``, so nothing is lost if a
# field moves between schema versions; to read OpenVDS / ZGY dataset kinds,
# point SeismicFileDataset at them with ``kind_query_seismicfiledataset``.


def _s(name: str) -> StructField:
    return StructField(name, StringType(), True)


def _d(name: str) -> StructField:
    return StructField(name, DoubleType(), True)


def _l(name: str) -> StructField:
    return StructField(name, LongType(), True)


SEISMIC_ACQUISITION_SURVEY_SCHEMA: StructType = StructType(
    _envelope_fields()
    + [
        _s("ProjectID"), _s("ProjectName"), _s("ProjectBeginDate"), _s("ProjectEndDate"),
        _s("SeismicGeometryTypeID"), _s("OperatingEnvironmentID"), _s("EnergySourceTypeID"),
        _d("ShotpointIncrementDistance"), _l("CableCount"), _d("CableLength"),
        _s("Contractors_json"), _s("Operator_json"), _s("SpatialLocation_json"),
        _s("GeoContexts_json"), _s("ExtensionProperties_json"),
    ]
)

SEISMIC_PROCESSING_PROJECT_SCHEMA: StructType = StructType(
    _envelope_fields()
    + [
        _s("ProjectID"), _s("ProjectName"), _s("ProjectBeginDate"), _s("ProjectEndDate"),
        _s("Contractors_json"), _s("SpatialLocation_json"), _s("GeoContexts_json"),
        _s("ExtensionProperties_json"),
    ]
)

SEISMIC_TRACE_DATA_SCHEMA: StructType = StructType(
    _envelope_fields()
    + [
        _s("Name"), _s("Description"), _s("BinGridID"), _s("ProcessingProjectID"),
        _s("PrincipalAcquisitionProjectID"), _s("SeismicDomainTypeID"), _s("SeismicMigrationTypeID"),
        _s("SeismicStackingTypeID"), _s("SeismicAttributeTypeID"), _s("SeismicTraceDataDimensionalityTypeID"),
        _s("SeismicWaveTypeID"), _s("Phase"), _s("Polarity"),
        _d("SampleInterval"), _l("SampleCount"), _d("StartTime"), _d("EndTime"),
        _l("InlineMin"), _l("InlineMax"), _l("CrosslineMin"), _l("CrosslineMax"),
        _s("Datasets_json"), _s("LiveTraceOutline_json"), _s("SpatialArea_json"),
        _s("ProcessingParameters_json"), _s("TextualFileHeader"), _s("ExtensionProperties_json"),
    ]
)

SEISMIC_BIN_GRID_SCHEMA: StructType = StructType(
    _envelope_fields()
    + [
        _s("Name"), _s("BinGridTypeID"), _s("BinGridDefinitionMethodTypeID"), _d("CoveragePercent"),
        _d("P6BinGridOriginI"), _d("P6BinGridOriginJ"), _d("P6BinGridOriginEasting"),
        _d("P6BinGridOriginNorthing"), _d("P6ScaleFactorOfBinGrid"), _d("P6BinWidthOnIaxis"),
        _d("P6BinWidthOnJaxis"), _d("P6MapGridBearingOfBinGridJaxis"), _d("P6BinNodeIncrementOnIaxis"),
        _d("P6BinNodeIncrementOnJaxis"), _s("ABCDBinGridLocalCoordinates_json"),
        _s("ABCDBinGridSpatialLocation_json"), _s("ExtensionProperties_json"),
    ]
)

SEISMIC_FILE_DATASET_SCHEMA: StructType = StructType(
    _envelope_fields()
    + [
        _s("Name"), _s("Description"), _s("FileCollectionPath"), _s("IndexFilePath"),
        _s("SEGYRevision"), _s("EncodingFormatTypeID"), _l("FileCount"), _l("TotalSize"),
        _s("FileSourceInfos_json"), _s("VectorHeaderMapping_json"), _s("ExtensionProperties_json"),
    ]
)


TABLE_SCHEMAS: dict[str, StructType] = {
    "Wellbore": WELLBORE_SCHEMA,
    "Reservoir": RESERVOIR_SCHEMA,
    "Rock_and_Fluid": ROCK_AND_FLUID_SCHEMA,
    "SeismicAcquisitionSurvey": SEISMIC_ACQUISITION_SURVEY_SCHEMA,
    "SeismicProcessingProject": SEISMIC_PROCESSING_PROJECT_SCHEMA,
    "SeismicTraceData": SEISMIC_TRACE_DATA_SCHEMA,
    "SeismicBinGrid": SEISMIC_BIN_GRID_SCHEMA,
    "SeismicFileDataset": SEISMIC_FILE_DATASET_SCHEMA,
}


# ---------------------------------------------------------------------------
# Per-table metadata
# ---------------------------------------------------------------------------
# All three tables have ``id`` as PK and use ``modifyTime`` for CDC.
# Deletes happen at the OSDU Storage layer and are not reflected in the
# Search index — so ``cdc`` (no deletes) is the correct ingestion type.
TABLE_METADATA: dict[str, dict] = {
    "Wellbore": {
        "primary_keys": ["id"],
        "cursor_field": "modifyTime",
        "ingestion_type": "cdc",
    },
    "Reservoir": {
        "primary_keys": ["id"],
        "cursor_field": "modifyTime",
        "ingestion_type": "cdc",
    },
    "Rock_and_Fluid": {
        "primary_keys": ["id"],
        "cursor_field": "modifyTime",
        "ingestion_type": "cdc",
    },
    **{
        t: {"primary_keys": ["id"], "cursor_field": "modifyTime", "ingestion_type": "cdc"}
        for t in (
            "SeismicAcquisitionSurvey",
            "SeismicProcessingProject",
            "SeismicTraceData",
            "SeismicBinGrid",
            "SeismicFileDataset",
        )
    },
}
