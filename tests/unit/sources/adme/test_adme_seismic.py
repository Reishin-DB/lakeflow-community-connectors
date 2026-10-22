"""Seismic catalog tables: flatteners and schema consistency.

Records use the OSDU seismic kind shapes (master-data--SeismicAcquisitionSurvey,
master-data--SeismicProcessingProject, work-product-component--SeismicTraceData,
work-product-component--SeismicBinGrid, dataset--FileCollection.SEGY). Field
names are medium confidence until checked against a live Schema Service; the
``data_json`` column keeps the full payload either way.
"""

import json

import pytest

from databricks.labs.community_connector.sources.adme.adme import _flatten_record
from databricks.labs.community_connector.sources.adme.adme_schemas import (
    SUPPORTED_TABLES,
    TABLE_METADATA,
    TABLE_SCHEMAS,
    TABLE_TO_KIND_QUERY,
)

P = "opendes"
ENVELOPE = {
    "acl": {"owners": [f"data.default.owners@{P}.dataservices.energy"],
            "viewers": [f"data.default.viewers@{P}.dataservices.energy"]},
    "legal": {"legaltags": [f"{P}-public-usa-dataset"], "otherRelevantDataCountries": ["AU"], "status": "compliant"},
    "version": 1712345678901234,
    "createTime": "2026-01-10T08:00:00.000Z",
    "modifyTime": "2026-02-01T09:30:00.000Z",
}

SURVEY = {
    **ENVELOPE, "id": f"{P}:master-data--SeismicAcquisitionSurvey:ADELE-3D",
    "kind": "osdu:wks:master-data--SeismicAcquisitionSurvey:1.2.0",
    "data": {"ProjectID": "ADELE-3D", "ProjectName": "Adele 3D", "ProjectBeginDate": "2006-03-01",
             "ProjectEndDate": "2006-05-20", "SeismicGeometryTypeID": f"{P}:reference-data--SeismicGeometryType:3D:",
             "OperatingEnvironmentID": f"{P}:reference-data--OperatingEnvironment:Marine:",
             "ShotpointIncrementDistance": 25, "CableCount": "8", "CableLength": 6000.0,
             "Contractors": [{"ContractorOrganisationID": "x"}],
             "SpatialLocation": {"Wgs84Coordinates": {"type": "FeatureCollection", "features": []}}},
}
TRACE = {
    **ENVELOPE, "id": f"{P}:work-product-component--SeismicTraceData:ADELE-PSTM",
    "kind": "osdu:wks:work-product-component--SeismicTraceData:1.3.0",
    "data": {"Name": "adele PSTM_FULL_OFFSET", "BinGridID": f"{P}:work-product-component--SeismicBinGrid:ADELE:",
             "SeismicDomainTypeID": f"{P}:reference-data--SeismicDomainType:Time:",
             "SeismicMigrationTypeID": f"{P}:reference-data--SeismicMigrationType:PSTM:",
             "SampleInterval": 4.0, "SampleCount": 1001, "StartTime": 1500, "EndTime": 5500,
             "FirstInline": 4175, "LastInline": 4775, "CrosslineMin": 5670, "CrosslineMax": 8400,
             "Datasets": [f"{P}:dataset--FileCollection.SEGY:ADELE-PSTM:"], "TextualFileHeader": "C 1 SEGY OUTPUT"},
}
BINGRID = {
    **ENVELOPE, "id": f"{P}:work-product-component--SeismicBinGrid:ADELE",
    "kind": "osdu:wks:work-product-component--SeismicBinGrid:1.1.0",
    "data": {"Name": "Adele grid", "P6BinGridOriginI": 4175, "P6BinGridOriginJ": 5670,
             "P6BinGridOriginEasting": 573238.9, "P6BinGridOriginNorthing": 8477109.08,
             "P6BinWidthOnIaxis": 18.75, "P6BinWidthOnJaxis": 12.5, "P6MapGridBearingOfBinGridJaxis": 270.0,
             "P6BinNodeIncrementOnIaxis": 2, "P6BinNodeIncrementOnJaxis": 1, "CoveragePercent": 100},
}
FILES = {
    **ENVELOPE, "id": f"{P}:dataset--FileCollection.SEGY:ADELE-PSTM",
    "kind": "osdu:wks:dataset--FileCollection.SEGY:1.1.0",
    "data": {"Name": "Adele PSTM SEG-Y", "SEGYRevision": "rev0",
             "DatasetProperties": {"FileCollectionPath": "s3://bucket/adele/",
                                   "FileSourceInfos": [{"FileSource": "adele.segy", "FileSize": "3488703164"}]},
             "VectorHeaderMapping": [{"KeyName": "Inline", "Position": 5, "WordWidth": 4},
                                     {"KeyName": "Crossline", "Position": 21, "WordWidth": 4}]},
}
PROC = {
    **ENVELOPE, "id": f"{P}:master-data--SeismicProcessingProject:ADELE-PSTM-2020",
    "kind": "osdu:wks:master-data--SeismicProcessingProject:1.2.0",
    "data": {"ProjectName": "Adele PSTM 2020", "ProjectBeginDate": "2020-01-15"},
}

TABLE_RECORDS = {
    "SeismicAcquisitionSurvey": SURVEY, "SeismicProcessingProject": PROC, "SeismicTraceData": TRACE,
    "SeismicBinGrid": BINGRID, "SeismicFileDataset": FILES,
}


def test_seismic_tables_registered():
    for t in TABLE_RECORDS:
        assert t in SUPPORTED_TABLES and t in TABLE_SCHEMAS and t in TABLE_METADATA
        assert TABLE_TO_KIND_QUERY[t].endswith(":*")
    assert len(SUPPORTED_TABLES) == 8


@pytest.mark.parametrize("table", list(TABLE_RECORDS))
def test_flattened_keys_match_schema(table):
    out = _flatten_record(TABLE_RECORDS[table])
    schema_cols = {f.name for f in TABLE_SCHEMAS[table].fields}
    assert set(out) <= schema_cols, set(out) - schema_cols
    assert json.loads(out["data_json"]) == TABLE_RECORDS[table]["data"]


def test_trace_data_typed_and_version_drift_names():
    out = _flatten_record(TRACE)
    assert out["SampleInterval"] == 4.0 and out["SampleCount"] == 1001
    assert out["StartTime"] == 1500.0 and out["EndTime"] == 5500.0
    assert (out["InlineMin"], out["InlineMax"]) == (4175, 4775)  # FirstInline / LastInline alias
    assert (out["CrosslineMin"], out["CrosslineMax"]) == (5670, 8400)
    assert json.loads(out["Datasets_json"])[0].startswith(f"{P}:dataset--")


def test_survey_coercions():
    out = _flatten_record(SURVEY)
    assert out["CableCount"] == 8 and out["ShotpointIncrementDistance"] == 25.0
    assert out["ProjectName"] == "Adele 3D"


def test_bin_grid_p6():
    out = _flatten_record(BINGRID)
    assert out["P6BinWidthOnIaxis"] == 18.75 and out["P6MapGridBearingOfBinGridJaxis"] == 270.0


def test_file_dataset_sizes_and_header_mapping():
    out = _flatten_record(FILES)
    assert out["FileCount"] == 1 and out["TotalSize"] == 3488703164
    assert json.loads(out["VectorHeaderMapping_json"])[0]["Position"] == 5


def test_missing_optional_blocks_land_as_nulls():
    rec = {**TRACE, "data": {"Name": "bare"}}
    out = _flatten_record(rec)
    assert out["Name"] == "bare" and out["SampleInterval"] is None and out["Datasets_json"] is None
