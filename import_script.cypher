

// CONSTRAINT creation
// -------------------
//
// Create node uniqueness constraints, ensuring no duplicates for the given node label and ID property exist in the database. This also ensures no duplicates are introduced in future.
//
// NOTE: The following constraint creation syntax is generated based on the current connected database version 5.19-aura.
CREATE CONSTRAINT `ID_Patient_uniq` IF NOT EXISTS
FOR (n: `Patient`)
REQUIRE (n.`ID`) IS UNIQUE;
CREATE CONSTRAINT `Id_Encounter_uniq` IF NOT EXISTS
FOR (n: `Encounter`)
REQUIRE (n.`Id`) IS UNIQUE;
CREATE CONSTRAINT `Id_Careplan__uniq` IF NOT EXISTS
FOR (n: `Careplan `)
REQUIRE (n.`Id`) IS UNIQUE;
CREATE CONSTRAINT `CODE_Has_uniq` IF NOT EXISTS
FOR (n: `Condition`)
REQUIRE (n.`CODE`) IS UNIQUE;
CREATE CONSTRAINT `ENCOUNTER_Procedure_uniq` IF NOT EXISTS
FOR (n: `Procedure`)
REQUIRE (n.`ENCOUNTER`) IS UNIQUE;
CREATE CONSTRAINT `Id_Provider_uniq` IF NOT EXISTS
FOR (n: `Provider`)
REQUIRE (n.`Id`) IS UNIQUE;
CREATE CONSTRAINT `CODE_Observation_uniq` IF NOT EXISTS
FOR (n: `Observation`)
REQUIRE (n.`CODE`) IS UNIQUE;
CREATE CONSTRAINT `CODE_Medication_uniq` IF NOT EXISTS
FOR (n: `Medication`)
REQUIRE (n.`CODE`) IS UNIQUE;
CREATE CONSTRAINT `CODE_Allergies_uniq` IF NOT EXISTS
FOR (n: `Allergies`)
REQUIRE (n.`CODE`) IS UNIQUE;

:param {
  file_path_root: 'file:///',
  file_0: 'patients.csv',
  file_1: 'encounters.csv',
  file_2: 'careplans.csv',
  file_3: 'conditions.csv',
  file_4: 'procedures.csv',
  file_5: 'providers.csv',
  file_6: 'observations.csv',
  file_7: 'allergies.csv',
  file_8: 'medications.csv',
  idsToSkip: []
};
// NODE load
// ---------
//
// Load nodes in batches, one node label at a time. Nodes will be created using a MERGE statement to ensure a node with the same label and ID property remains unique. Pre-existing nodes found by a MERGE statement will have their other properties set to the latest values encountered in a load file.
//
// NOTE: Any nodes with IDs in the 'idsToSkip' list parameter will not be loaded.
:auto LOAD CSV WITH HEADERS FROM ($file_path_root + $file_0) AS row
WITH row
WHERE NOT row.`Id` IN $idsToSkip AND NOT row.`Id` IS NULL
CALL {
  WITH row
  MERGE (n: `Patient` { `ID`: row.`Id` })
  SET n.`ID` = row.`Id`
  SET n.`DOB` = row.`BIRTHDATE`
  SET n.`DOD` = row.`DEATHDATE`
  SET n.`GENDER` = row.`GENDER`
  SET n.`FIRST` = row.`FIRST`
  SET n.`LAST` = row.`LAST`
  SET n.`HEALTHCARE_EXPENSES` = toFloat(trim(row.`HEALTHCARE_EXPENSES`))
} IN TRANSACTIONS OF 10000 ROWS;

:auto LOAD CSV WITH HEADERS FROM ($file_path_root + $file_1) AS row
WITH row
WHERE NOT row.`Id` IN $idsToSkip AND NOT row.`Id` IS NULL
CALL {
  WITH row
  MERGE (n: `Encounter` { `Id`: row.`Id` })
  SET n.`Id` = row.`Id`
  SET n.`PROVIDER` = row.`PROVIDER`
  SET n.`DESCRIPTION` = row.`DESCRIPTION`
  SET n.`BASE_ENCOUNTER_COST` = toFloat(trim(row.`BASE_ENCOUNTER_COST`))
  SET n.`TOTAL_CLAIM_COST` = toFloat(trim(row.`TOTAL_CLAIM_COST`))
  SET n.`PAYER_COVERAGE` = toFloat(trim(row.`PAYER_COVERAGE`))
  SET n.`REASONDESCRIPTION` = row.`REASONDESCRIPTION`
  // Your script contains the datetime datatype. Our app attempts to convert dates to ISO 8601 date format before passing them to the Cypher function.
  // This conversion cannot be done in a Cypher script load. Please ensure that your CSV file columns are in ISO 8601 date format to ensure equivalent loads.
  SET n.`START` = datetime(row.`START`)
} IN TRANSACTIONS OF 10000 ROWS;

:auto LOAD CSV WITH HEADERS FROM ($file_path_root + $file_2) AS row
WITH row
WHERE NOT row.`Id` IN $idsToSkip AND NOT row.`Id` IS NULL
CALL {
  WITH row
  MERGE (n: `Careplan ` { `Id`: row.`Id` })
  SET n.`Id` = row.`Id`
  SET n.`DESCRIPTION` = row.`DESCRIPTION`
  // Your script contains the datetime datatype. Our app attempts to convert dates to ISO 8601 date format before passing them to the Cypher function.
  // This conversion cannot be done in a Cypher script load. Please ensure that your CSV file columns are in ISO 8601 date format to ensure equivalent loads.
  SET n.`START` = datetime(row.`START`)
} IN TRANSACTIONS OF 10000 ROWS;

:auto LOAD CSV WITH HEADERS FROM ($file_path_root + $file_3) AS row
WITH row
WHERE NOT row.`CODE` IN $idsToSkip AND NOT row.`CODE` IS NULL
CALL {
  WITH row
  MERGE (n: `Condition` { `CODE`: row.`CODE` })
  SET n.`CODE` = row.`CODE`
  SET n.`DESCRIPTION` = row.`DESCRIPTION`
  // Your script contains the datetime datatype. Our app attempts to convert dates to ISO 8601 date format before passing them to the Cypher function.
  // This conversion cannot be done in a Cypher script load. Please ensure that your CSV file columns are in ISO 8601 date format to ensure equivalent loads.
  SET n.`START` = datetime(row.`START`)
} IN TRANSACTIONS OF 10000 ROWS;

:auto LOAD CSV WITH HEADERS FROM ($file_path_root + $file_4) AS row
WITH row
WHERE NOT row.`CODE` IN $idsToSkip AND NOT row.`CODE` IS NULL
CALL {
  WITH row
  MERGE (n: `Procedure` { `ENCOUNTER`: row.`CODE` })
  SET n.`ENCOUNTER` = row.`CODE`
  SET n.`DESCRIPTION` = row.`DESCRIPTION`
  SET n.`BASE_COST` = toFloat(trim(row.`BASE_COST`))
  SET n.`REASONDESCRIPTION` = row.`REASONDESCRIPTION`
  // Your script contains the datetime datatype. Our app attempts to convert dates to ISO 8601 date format before passing them to the Cypher function.
  // This conversion cannot be done in a Cypher script load. Please ensure that your CSV file columns are in ISO 8601 date format to ensure equivalent loads.
  SET n.`START` = datetime(row.`START`)
} IN TRANSACTIONS OF 10000 ROWS;

:auto LOAD CSV WITH HEADERS FROM ($file_path_root + $file_5) AS row
WITH row
WHERE NOT row.`Id` IN $idsToSkip AND NOT row.`Id` IS NULL
CALL {
  WITH row
  MERGE (n: `Provider` { `Id`: row.`Id` })
  SET n.`Id` = row.`Id`
  SET n.`NAME` = row.`NAME`
  SET n.`SPECIALITY` = row.`SPECIALITY`
  SET n.`ENCOUNTERS` = toInteger(trim(row.`ENCOUNTERS`))
  SET n.`PROCEDURES` = toLower(trim(row.`PROCEDURES`)) IN ['1','true','yes']
} IN TRANSACTIONS OF 10000 ROWS;

:auto LOAD CSV WITH HEADERS FROM ($file_path_root + $file_6) AS row
WITH row
WHERE NOT row.`CODE` IN $idsToSkip AND NOT row.`CODE` IS NULL
CALL {
  WITH row
  MERGE (n: `Observation` { `CODE`: row.`CODE` })
  SET n.`CODE` = row.`CODE`
  // Your script contains the datetime datatype. Our app attempts to convert dates to ISO 8601 date format before passing them to the Cypher function.
  // This conversion cannot be done in a Cypher script load. Please ensure that your CSV file columns are in ISO 8601 date format to ensure equivalent loads.
  SET n.`DATE` = datetime(row.`DATE`)
  SET n.`ENCOUNTER` = row.`ENCOUNTER`
  SET n.`DESCRIPTION` = row.`DESCRIPTION`
  SET n.`VALUE` = row.`VALUE`
} IN TRANSACTIONS OF 10000 ROWS;

:auto LOAD CSV WITH HEADERS FROM ($file_path_root + $file_8) AS row
WITH row
WHERE NOT row.`CODE` IN $idsToSkip AND NOT toInteger(trim(row.`CODE`)) IS NULL
CALL {
  WITH row
  MERGE (n: `Medication` { `CODE`: toInteger(trim(row.`CODE`)) })
  SET n.`CODE` = toInteger(trim(row.`CODE`))
  SET n.`DESCRIPTION` = row.`DESCRIPTION`
  SET n.`BASE_COST` = toFloat(trim(row.`BASE_COST`))
  SET n.`TOTALCOST` = toFloat(trim(row.`TOTALCOST`))
} IN TRANSACTIONS OF 10000 ROWS;

:auto LOAD CSV WITH HEADERS FROM ($file_path_root + $file_7) AS row
WITH row
WHERE NOT row.`CODE` IN $idsToSkip AND NOT toInteger(trim(row.`CODE`)) IS NULL
CALL {
  WITH row
  MERGE (n: `Allergies` { `CODE`: toInteger(trim(row.`CODE`)) })
  SET n.`CODE` = toInteger(trim(row.`CODE`))
  SET n.`DESCRIPTION` = row.`DESCRIPTION`
  SET n.`TYPE` = row.`TYPE`
  SET n.`CATEGORY` = row.`CATEGORY`
} IN TRANSACTIONS OF 10000 ROWS;


// RELATIONSHIP load
// -----------------
//
// Load relationships in batches, one relationship type at a time. Relationships are created using a MERGE statement, meaning only one relationship of a given type will ever be created between a pair of nodes.
:auto LOAD CSV WITH HEADERS FROM ($file_path_root + $file_1) AS row
WITH row 
CALL {
  WITH row
  MATCH (source: `Patient` { `ID`: row.`PATIENT` })
  MATCH (target: `Encounter` { `Id`: row.`Id` })
  MERGE (source)-[r: `Had`]->(target)
} IN TRANSACTIONS OF 10000 ROWS;

:auto LOAD CSV WITH HEADERS FROM ($file_path_root + $file_2) AS row
WITH row 
CALL {
  WITH row
  MATCH (source: `Encounter` { `Id`: row.`ENCOUNTER` })
  MATCH (target: `Careplan ` { `Id`: row.`Id` })
  MERGE (source)-[r: `Assigned`]->(target)
} IN TRANSACTIONS OF 10000 ROWS;

:auto LOAD CSV WITH HEADERS FROM ($file_path_root + $file_4) AS row
WITH row 
CALL {
  WITH row
  MATCH (source: `Encounter` { `Id`: row.`ENCOUNTER` })
  MATCH (target: `Procedure` { `ENCOUNTER`: row.`CODE` })
  MERGE (source)-[r: `Had`]->(target)
} IN TRANSACTIONS OF 10000 ROWS;

:auto LOAD CSV WITH HEADERS FROM ($file_path_root + $file_1) AS row
WITH row 
CALL {
  WITH row
  MATCH (source: `Provider` { `Id`: row.`PROVIDER` })
  MATCH (target: `Encounter` { `Id`: row.`Id` })
  MERGE (source)-[r: `Treated`]->(target)
} IN TRANSACTIONS OF 10000 ROWS;

:auto LOAD CSV WITH HEADERS FROM ($file_path_root + $file_6) AS row
WITH row 
CALL {
  WITH row
  MATCH (source: `Encounter` { `Id`: row.`ENCOUNTER` })
  MATCH (target: `Observation` { `CODE`: row.`CODE` })
  MERGE (source)-[r: `Had`]->(target)
} IN TRANSACTIONS OF 10000 ROWS;

:auto LOAD CSV WITH HEADERS FROM ($file_path_root + $file_8) AS row
WITH row 
CALL {
  WITH row
  MATCH (source: `Patient` { `ID`: row.`PATIENT` })
  MATCH (target: `Medication` { `CODE`: toInteger(trim(row.`CODE`)) })
  MERGE (source)-[r: `Taking`]->(target)
  SET r.`DESCRIPTION` = row.`DESCRIPTION`
  SET r.`TOTALCOST` = toFloat(trim(row.`TOTALCOST`))
} IN TRANSACTIONS OF 10000 ROWS;

:auto LOAD CSV WITH HEADERS FROM ($file_path_root + $file_7) AS row
WITH row 
CALL {
  WITH row
  MATCH (source: `Patient` { `ID`: row.`PATIENT` })
  MATCH (target: `Allergies` { `CODE`: toInteger(trim(row.`CODE`)) })
  MERGE (source)-[r: `Has`]->(target)
} IN TRANSACTIONS OF 10000 ROWS;

:auto LOAD CSV WITH HEADERS FROM ($file_path_root + $file_3) AS row
WITH row 
CALL {
  WITH row
  MATCH (source: `Patient` { `ID`: row.`PATIENT` })
  MATCH (target: `Condition` { `CODE`: row.`CODE` })
  MERGE (source)-[r: `Has`]->(target)
} IN TRANSACTIONS OF 10000 ROWS;
